#!/usr/bin/env python3
"""Ops-facing Streamlit demo for TCore reduction storytelling."""

from __future__ import annotations

import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

try:
    from analysis_agent import build_agent_context, groq_ready, run_agent
    from demo_queries import (
        DemoConnectionConfig,
        _since_dt,
        build_io_bands,
        get_band_snapshot,
        get_io_outlier_events,
        get_io_series,
        get_latest_data_timestamp,
        get_peak_pressure_series,
        get_raw_dbqlog_samples,
        get_raw_resusage_samples,
        get_raw_workload_join_samples,
        get_recent_query_offenders,
        get_remediation_actions,
        get_tcore_summary,
        get_top_consumers,
        get_top_skew_drivers,
        get_top_tcore_offenders,
        list_entities_for_metric,
    )
except ModuleNotFoundError:
    from scripts.analysis_agent import build_agent_context, groq_ready, run_agent
    from scripts.demo_queries import (
        DemoConnectionConfig,
        _since_dt,
        build_io_bands,
        get_band_snapshot,
        get_io_outlier_events,
        get_io_series,
        get_latest_data_timestamp,
        get_peak_pressure_series,
        get_raw_dbqlog_samples,
        get_raw_resusage_samples,
        get_raw_workload_join_samples,
        get_recent_query_offenders,
        get_remediation_actions,
        get_tcore_summary,
        get_top_consumers,
        get_top_skew_drivers,
        get_top_tcore_offenders,
        list_entities_for_metric,
    )

try:
    from streamlit_plotly_events import plotly_events

    HAS_PLOTLY_EVENTS = True
except ImportError:
    HAS_PLOTLY_EVENTS = False


REMEDIATION_OPTIONS = {
    "none": "No remediation",
    "tune_high_io_etl": "Tune high-IO ETL",
    "reduce_skew": "Reduce skew",
    "shift_batch_off_peak": "Shift batch off peak",
    "combined": "Combined remediation",
}


def _to_naive_dt(raw: str) -> datetime:
    value = str(raw).replace("Z", "+00:00")
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is not None:
        return dt.replace(tzinfo=None)
    return dt


def _build_io_figure(df, outliers, show_trend: bool) -> go.Figure:
    df = df.sort_values("ts").copy()
    fig = go.Figure()
    x = df["ts"]
    y = df["value"]
    p01 = df["p01"]
    p10 = df["p10"]
    p50 = df["p50"]
    p90 = df["p90"]
    p99 = df["p99"]
    if len(df):
        band_cap = float(p99.quantile(0.98))
        value_cap = float(y.quantile(0.99))
        q_cap = max(band_cap, value_cap)
        y_cap = max(q_cap * 1.2, 1.0)
    else:
        y_cap = 1.0

    x_vals = x.tolist()

    def add_band(lower, upper, band_name: str, band_color: str) -> None:
        lower_vals = pd.Series(lower).clip(lower=0.0).tolist()
        upper_vals = pd.Series(upper).clip(lower=0.0).tolist()
        fig.add_trace(
            go.Scatter(
                x=x_vals + x_vals[::-1],
                y=upper_vals + lower_vals[::-1],
                mode="lines",
                line=dict(width=0, color=band_color),
                fill="toself",
                fillcolor=band_color,
                name=band_name,
                hoverinfo="skip",
            )
        )

    add_band([0.0] * len(df), p01, "lower critical", "rgba(239, 68, 68, 0.18)")
    add_band(p01, p10, "lower warning (p01-p10)", "rgba(252, 211, 77, 0.28)")
    add_band(p10, p90, "expected band (p10-p90)", "rgba(64, 190, 116, 0.35)")
    add_band(p90, p99, "upper warning (p90-p99)", "rgba(252, 211, 77, 0.28)")
    add_band(p99, [y_cap] * len(df), "upper critical", "rgba(239, 68, 68, 0.18)")

    for line_name, line_series, color in [
        ("p01", p01, "rgba(185, 28, 28, 0.7)"),
        ("p10", p10, "rgba(202, 138, 4, 0.8)"),
        ("p90", p90, "rgba(21, 128, 61, 0.85)"),
        ("p99", p99, "rgba(185, 28, 28, 0.7)"),
    ]:
        fig.add_trace(
            go.Scatter(
                x=x,
                y=line_series,
                mode="lines",
                line=dict(color=color, width=1),
                name=line_name,
                hovertemplate=f"time=%{{x}}<br>{line_name}=%{{y:.2f}}<extra></extra>",
            )
        )

    fig.add_trace(
        go.Scatter(
            x=x,
            y=y,
            mode="lines+markers",
            line=dict(color="#1f77b4", width=2),
            marker=dict(size=4),
            name="IO actual",
            hovertemplate="time=%{x}<br>io=%{y:.2f}<extra></extra>",
        )
    )
    if show_trend:
        fig.add_trace(
            go.Scatter(
                x=x,
                y=p50,
                mode="lines",
                line=dict(color="#0f766e", width=2, dash="dash"),
                name="trend (p50)",
                hovertemplate="time=%{x}<br>trend=%{y:.2f}<extra></extra>",
            )
        )
    if not outliers.empty:
        oe = outliers[outliers["observed"].notna()].copy()
        if not oe.empty:
            fig.add_trace(
                go.Scatter(
                    x=oe["ts"],
                    y=oe["observed"],
                    mode="markers",
                    marker=dict(color="#dc2626", size=9, symbol="diamond"),
                    name="S2 outliers",
                    hovertemplate="time=%{x}<br>observed=%{y:.2f}<extra></extra>",
                )
            )

    fig.update_layout(
        title="IO Behavior with Learned Dynamic Bands (S2)",
        xaxis_title="Time",
        xaxis=dict(type="date"),
        yaxis_title="IO Count",
        yaxis=dict(rangemode="tozero", range=[0, y_cap]),
        legend_title="Legend",
        template="plotly_white",
        height=460,
        margin=dict(l=12, r=12, t=44, b=12),
        hovermode="x unified",
    )
    return fig


def _build_peak_pressure_figure(peak_pressure: pd.DataFrame, comparison_mode: str) -> go.Figure:
    fig = go.Figure()
    if peak_pressure.empty:
        fig.update_layout(template="plotly_white", height=300, title="Peak Pressure")
        return fig

    if comparison_mode == "Before":
        fig.add_trace(go.Scatter(x=peak_pressure["ts"], y=peak_pressure["excess_tcore"], mode="lines", name="Before excess"))
    elif comparison_mode == "After":
        fig.add_trace(go.Scatter(x=peak_pressure["ts"], y=peak_pressure["adjusted_excess_tcore"], mode="lines", name="After excess"))
    else:
        delta = peak_pressure["adjusted_excess_tcore"] - peak_pressure["excess_tcore"]
        fig.add_trace(go.Bar(x=peak_pressure["ts"], y=delta, name="Delta excess"))

    fig.update_layout(
        title="Peak Window TCore Pressure",
        xaxis_title="Time",
        yaxis_title="Estimated Excess TCore",
        template="plotly_white",
        height=320,
        margin=dict(l=12, r=12, t=44, b=12),
    )
    return fig


def _find_nearest_outlier(outliers, selected_ts: datetime):
    if outliers.empty:
        return None
    d = outliers.copy()
    d["ts_dt"] = d["ts"].apply(_to_naive_dt)
    idx = (d["ts_dt"] - selected_ts).abs().idxmin()
    return d.loc[idx]


def _summary_card(df, outliers, selected_snapshot, nearest_outlier) -> None:
    st.markdown("#### Snapshot")
    if df.empty:
        st.info("No IO data in selected range.")
        return

    latest = df.iloc[-1]
    deviation = float(latest["value"] - latest["p50"])
    st.metric("Scenario", "S2")
    st.metric("Latest Time", str(latest["ts"]))
    st.metric("Latest IO", f"{float(latest['value']):,.2f}")
    st.metric("Expected Baseline (p50)", f"{float(latest['p50']):,.2f}")
    st.metric("Deviation", f"{deviation:,.2f}")
    st.metric("Outliers (window)", int(len(outliers)))

    st.markdown("#### Selected Point")
    if not selected_snapshot:
        st.info("No point selected yet.")
        return

    st.metric("Selected Time", selected_snapshot["ts"])
    st.metric("Selected IO", f"{selected_snapshot['value']:,.2f}")
    st.metric("Selected Zone", selected_snapshot["zone"].upper())
    if nearest_outlier is not None:
        st.write(f"Nearest S2 outlier: `{nearest_outlier['ts']}` | score `{float(nearest_outlier['score']):.3f}`")


def _metric_payload(baseline_summary: dict[str, float], scenario_summary: dict[str, float], comparison_mode: str) -> dict[str, float]:
    before_total = baseline_summary["total_estimated_tcore"]
    after_total = before_total - scenario_summary["projected_savings_score"]
    before_excess = baseline_summary["excess_tcore"]
    after_excess = scenario_summary["adjusted_excess_tcore"]
    if comparison_mode == "Before":
        return {
            "total": before_total,
            "excess": before_excess,
            "reducible_pct": baseline_summary["reducible_tcore_pct"],
            "savings": 0.0,
            "cpu_at_risk": baseline_summary["cpu_at_risk"],
            "io_at_risk": baseline_summary["io_at_risk"],
        }
    if comparison_mode == "After":
        return {
            "total": after_total,
            "excess": after_excess,
            "reducible_pct": scenario_summary["reducible_tcore_pct"],
            "savings": scenario_summary["projected_savings_score"],
            "cpu_at_risk": baseline_summary["cpu_at_risk"],
            "io_at_risk": baseline_summary["io_at_risk"],
        }
    return {
        "total": after_total - before_total,
        "excess": after_excess - before_excess,
        "reducible_pct": scenario_summary["reducible_tcore_pct"],
        "savings": scenario_summary["projected_savings_score"],
        "cpu_at_risk": 0.0,
        "io_at_risk": 0.0,
    }


def _format_chat_prompt(history: list[dict[str, str]], prompt: str) -> str:
    transcript = [f"{msg['role']}: {msg['content']}" for msg in history[-6:]]
    transcript.append(f"user: {prompt}")
    return "\n".join(transcript)


def _render_tcore_dashboard(
    baseline_summary: dict[str, float],
    scenario_summary: dict[str, float],
    comparison_mode: str,
    offenders: pd.DataFrame,
    consumers: pd.DataFrame,
    skew_drivers: pd.DataFrame,
    query_offenders: pd.DataFrame,
    peak_pressure: pd.DataFrame,
    actions: list[dict[str, str]],
) -> None:
    st.markdown("### TCore Reduction Dashboard")
    metrics = _metric_payload(baseline_summary, scenario_summary, comparison_mode)
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Estimated TCore", f"{metrics['total']:.2f}")
    c2.metric("Excess TCore", f"{metrics['excess']:.2f}")
    c3.metric("Reducible %", f"{metrics['reducible_pct']:.1f}%")
    c4.metric("Savings Score", f"{metrics['savings']:.2f}")
    c5.metric("CPU At Risk", f"{metrics['cpu_at_risk']:.2f}")
    c6.metric("IO At Risk", f"{metrics['io_at_risk']:.2f}")

    st.plotly_chart(_build_peak_pressure_figure(peak_pressure, comparison_mode), use_container_width=True)

    t1, t2 = st.columns([3, 2])
    with t1:
        st.markdown("#### Top Workload Offenders")
        offender_view = offenders.rename(columns={"entity_id": "workload"})
        st.dataframe(
            offender_view[["workload", "excess_tcore", "adjusted_excess_tcore", "cpu_excess", "io_excess", "reduction_pct"]],
            use_container_width=True,
            hide_index=True,
        )
        st.markdown("#### High-Risk Queries")
        st.dataframe(query_offenders, use_container_width=True, hide_index=True)
    with t2:
        st.markdown("#### Recommended Actions")
        if not actions:
            st.info("No action recommendations yet.")
        else:
            for action in actions:
                st.write(f"`{action['action']}` on `{action['target']}`: {action['reason']}")
        st.markdown("#### Top Consumers")
        st.dataframe(consumers, use_container_width=True, hide_index=True)
        st.markdown("#### Top Skew Drivers")
        st.dataframe(skew_drivers.rename(columns={"entity_id": "query_step"}), use_container_width=True, hide_index=True)


def _render_agent_panel(
    *,
    workload: str,
    check_mode: str,
    latest_ts: datetime | None,
    selected_snapshot,
    outliers,
    banded,
    tcore_summary: dict[str, float],
    top_offenders: pd.DataFrame,
    top_consumers: pd.DataFrame,
    top_skew_drivers: pd.DataFrame,
    actions: list[dict[str, str]],
    remediation: str,
    comparison_mode: str,
    telemetry_backend: str,
    telemetry_scope: dict[str, str],
) -> None:
    st.markdown("### Analysis Agent")

    if "agent_chat" not in st.session_state:
        st.session_state["agent_chat"] = []
    if "last_auto_analyzed_event" not in st.session_state:
        st.session_state["last_auto_analyzed_event"] = None
    if "latest_agent_analysis" not in st.session_state:
        st.session_state["latest_agent_analysis"] = None

    latest_outlier = None
    if not outliers.empty:
        latest_outlier = outliers.sort_values("ts", ascending=False).iloc[0].to_dict()

    context = build_agent_context(
        workload=workload,
        check_mode=check_mode,
        latest_ts=latest_ts.isoformat(timespec="seconds") if latest_ts else None,
        selected_snapshot=selected_snapshot,
        latest_outlier=latest_outlier,
        band_preview=banded[["ts", "value", "p01", "p10", "p50", "p90", "p99"]].tail(24).to_dict("records"),
        outlier_preview=(
            outliers[["event_id", "ts", "entity_id", "severity", "observed", "expected", "score"]].tail(10).to_dict("records")
            if not outliers.empty
            else []
        ),
        tcore_summary=tcore_summary,
        top_offenders=top_offenders.head(5).to_dict("records"),
        top_consumers=top_consumers.head(5).to_dict("records"),
        top_skew_drivers=top_skew_drivers.head(5).to_dict("records"),
        remediation=remediation,
        comparison_mode=comparison_mode,
        actions=actions,
        telemetry_backend=telemetry_backend,
        telemetry_scope=telemetry_scope,
    )

    if latest_outlier is None:
        st.info("No active S2 outlier in the selected window. The agent can still answer TCore-reduction questions once Groq is configured.")
    elif not groq_ready():
        st.warning("Set `GROQ_API_KEY` in the sidebar or environment to enable live analysis and free-form chat.")
    elif st.session_state["last_auto_analyzed_event"] != latest_outlier["event_id"]:
        prompt = (
            "Explain what is currently driving excess TCore-style consumption, identify the top offenders, and summarize how the selected remediation changes the outcome."
        )
        try:
            st.session_state["latest_agent_analysis"] = run_agent(prompt, context)
            st.session_state["last_auto_analyzed_event"] = latest_outlier["event_id"]
        except Exception as exc:
            st.error(f"Agent analysis failed: {exc}")

    if st.session_state["latest_agent_analysis"]:
        st.info(st.session_state["latest_agent_analysis"])

    for msg in st.session_state["agent_chat"]:
        with st.chat_message(msg["role"]):
            st.write(msg["content"])

    prompt = st.chat_input("Ask the agent which workloads to tune, shift, or investigate next")
    if prompt:
        st.session_state["agent_chat"].append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.write(prompt)
        if not groq_ready():
            reply = "Set `GROQ_API_KEY` in the sidebar or environment to enable agent chat."
        else:
            try:
                reply = run_agent(_format_chat_prompt(st.session_state["agent_chat"], prompt), context)
            except Exception as exc:
                reply = f"Agent request failed: {exc}"
        st.session_state["agent_chat"].append({"role": "assistant", "content": reply})
        with st.chat_message("assistant"):
            st.write(reply)


def _render_band_debug(banded, selected_snapshot) -> None:
    with st.expander("Band + Point Debug (S2)", expanded=False):
        d1, d2 = st.columns([2, 2])
        with d1:
            debug_rows = st.number_input("Band rows to display", min_value=20, max_value=2000, value=200, step=20)
        with d2:
            debug_order = st.selectbox("Band row order", ["Newest first", "Oldest first"], index=0)

        band_debug = banded.copy()

        def zone_for_row(row) -> str:
            v = float(row["value"])
            if v < float(row["p01"]):
                return "red"
            if v < float(row["p10"]):
                return "yellow"
            if v <= float(row["p90"]):
                return "green"
            if v <= float(row["p99"]):
                return "yellow"
            return "red"

        band_debug["zone"] = band_debug.apply(zone_for_row, axis=1)
        zone_counts = band_debug["zone"].value_counts().to_dict()
        st.caption(
            "Zone counts in current chart window: "
            f"green={zone_counts.get('green', 0)}, yellow={zone_counts.get('yellow', 0)}, red={zone_counts.get('red', 0)}"
        )
        band_debug = band_debug.sort_values("ts", ascending=(debug_order == "Oldest first"))
        show_cols = ["ts", "value", "p01", "p10", "p50", "p90", "p99", "zone"]
        st.dataframe(band_debug[show_cols].head(int(debug_rows)), use_container_width=True, hide_index=True)

        if selected_snapshot:
            st.caption("Selected point thresholds")
            st.json(selected_snapshot)


def _resolve_raw_window(since: datetime, mode: str, selected_ts, minutes: int) -> tuple[datetime, datetime]:
    now = datetime.utcnow()
    if mode == "clicked" and selected_ts is not None:
        center = _to_naive_dt(selected_ts)
        return center - timedelta(minutes=minutes), center + timedelta(minutes=minutes)
    return since, now


def main() -> None:
    st.set_page_config(page_title="TCore Reduction Demo", page_icon=":chart_with_upwards_trend:", layout="wide")
    st.title("TCore Reduction Demo")
    st.caption("Reduce avoidable consumption by isolating offenders, simulating remediation, and explaining impact in real time.")

    st.sidebar.header("Data Source")
    default_db = str(Path("data") / "telemetry.db")
    backend = st.sidebar.selectbox("Backend", ["sqlite", "teradata"], index=0)
    db_path = st.sidebar.text_input("SQLite DB path", value=default_db, disabled=(backend != "sqlite"))
    td_demo_schema = st.sidebar.text_input("Demo schema filter", value=os.getenv("TD_DEMO_SCHEMA", ""), disabled=(backend != "teradata"))
    td_demo_user = st.sidebar.text_input("Demo user filter", value=os.getenv("TD_DEMO_USER", ""), disabled=(backend != "teradata"))
    groq_key = st.sidebar.text_input("Groq API key", value=os.getenv("GROQ_API_KEY", ""), type="password")
    if groq_key:
        os.environ["GROQ_API_KEY"] = groq_key
    auto_refresh = st.sidebar.checkbox("Auto refresh every 15s", value=True)
    data_source = DemoConnectionConfig(backend=backend, db_path=db_path, td_demo_schema=td_demo_schema, td_demo_user=td_demo_user)

    st.sidebar.header("Time Range")
    range_key = st.sidebar.selectbox(
        "Range",
        ["last_24h", "last_3d", "custom_hours"],
        format_func=lambda x: {"last_24h": "Last 24 hours", "last_3d": "Last 3 days", "custom_hours": "Custom (hours)"}[x],
        index=0,
    )
    custom_hours = st.sidebar.number_input("Custom hours", min_value=1, max_value=720, value=24, step=1)
    since = _since_dt(range_key, custom_hours=int(custom_hours))

    latest_ts = get_latest_data_timestamp(data_source)
    if latest_ts is None:
        st.warning("No telemetry/scenario data found. Run workload generation and scenario processing first.")
    else:
        st.caption(f"Latest data timestamp: `{latest_ts.isoformat(timespec='seconds')}` UTC")
        age_minutes = (datetime.utcnow() - latest_ts).total_seconds() / 60.0
        if age_minutes > 30:
            st.warning(f"Data appears stale ({age_minutes:.1f} minutes old).")

    ctrl1, ctrl2, ctrl3, ctrl4, ctrl5, ctrl6 = st.columns([1.2, 2.2, 2.2, 2.2, 1.4, 1.2])
    with ctrl1:
        _ = st.selectbox("Dataset", ["Full"], index=0)
    with ctrl2:
        workloads = ["all"] + list_entities_for_metric(data_source, "workload_io_count")
        workload = st.selectbox("S2 Workload", workloads, index=0)
    with ctrl3:
        comparison_mode = st.selectbox("Comparison", ["Before", "After", "Delta"], index=0)
    with ctrl4:
        remediation = st.selectbox("Remediation", list(REMEDIATION_OPTIONS.keys()), format_func=lambda x: REMEDIATION_OPTIONS[x], index=0)
    with ctrl5:
        workload_scope = st.selectbox("Scope", workloads, index=0)
    with ctrl6:
        show_trend = st.checkbox("Trend", value=True)

    io_series = get_io_series(data_source, since=since, workload=workload, check_mode="sum")
    if io_series.empty:
        st.info("No IO data in the selected range/workload.")
        return

    baseline_summary = get_tcore_summary(data_source, since, remediation="none", workload_scope=workload_scope)
    scenario_summary = get_tcore_summary(data_source, since, remediation=remediation, workload_scope=workload_scope)
    offenders = get_top_tcore_offenders(data_source, since, remediation=remediation, workload_scope=workload_scope, limit=8)
    consumers = get_top_consumers(data_source, since, limit=8)
    skew_drivers = get_top_skew_drivers(data_source, since, limit=8)
    peak_pressure = get_peak_pressure_series(data_source, since, remediation=remediation, workload_scope=workload_scope)
    query_offenders = get_recent_query_offenders(data_source, since, workload=workload_scope, limit=12)
    actions = get_remediation_actions(offenders, consumers, skew_drivers)

    _render_tcore_dashboard(
        baseline_summary,
        scenario_summary,
        comparison_mode,
        offenders,
        consumers,
        skew_drivers,
        query_offenders,
        peak_pressure,
        actions,
    )

    banded = build_io_bands(io_series, window=24)
    outliers = get_io_outlier_events(data_source, since=since, workload=workload)
    fig = _build_io_figure(banded, outliers, show_trend=show_trend)

    if "selected_point" not in st.session_state:
        st.session_state["selected_point"] = None

    left, right = st.columns([5, 1.8])
    with left:
        if HAS_PLOTLY_EVENTS:
            clicks = plotly_events(
                fig,
                click_event=True,
                select_event=False,
                hover_event=False,
                override_height=460,
                override_width=1100,
            )
            if clicks:
                click = clicks[-1]
                if "x" in click:
                    st.session_state["selected_point"] = click
        else:
            st.plotly_chart(fig, use_container_width=True)
            st.info("Install `streamlit-plotly-events` to enable point-click drilldown.")

        if st.button("Clear Selected Point"):
            st.session_state["selected_point"] = None
            st.rerun()

    selected_snapshot = None
    nearest_outlier = None
    selected_x = None
    if st.session_state["selected_point"] and "x" in st.session_state["selected_point"]:
        selected_x = str(st.session_state["selected_point"]["x"])
        selected_dt = _to_naive_dt(selected_x)
        selected_snapshot = get_band_snapshot(banded, selected_dt)
        nearest_outlier = _find_nearest_outlier(outliers, selected_dt)

    with right:
        _summary_card(banded, outliers, selected_snapshot, nearest_outlier)

    st.markdown("### Recent S2 Outlier Events")
    if outliers.empty:
        st.success("No S2 outliers in selected window.")
    else:
        st.dataframe(
            outliers.rename(columns={"entity_id": "workload"})[["event_id", "ts", "workload", "severity", "observed", "expected", "score"]],
            use_container_width=True,
            hide_index=True,
        )

    _render_agent_panel(
        workload=workload,
        check_mode="sum",
        latest_ts=latest_ts,
        selected_snapshot=selected_snapshot,
        outliers=outliers,
        banded=banded,
        tcore_summary=scenario_summary,
        top_offenders=offenders,
        top_consumers=consumers,
        top_skew_drivers=skew_drivers,
        actions=actions,
        remediation=remediation,
        comparison_mode=comparison_mode,
        telemetry_backend=backend,
        telemetry_scope={"demo_schema": td_demo_schema, "demo_user": td_demo_user},
    )

    _render_band_debug(banded, selected_snapshot)

    with st.expander("Raw Telemetry", expanded=False):
        r1, r2, r3 = st.columns([2, 2, 1.5])
        with r1:
            raw_mode = st.selectbox("Raw table window", ["selected range", "clicked-point +/- minutes"], index=0)
        with r2:
            plus_minus = st.number_input("+/- minutes around click", min_value=5, max_value=240, value=30, step=5)
        with r3:
            row_limit = st.number_input("Row limit", min_value=10, max_value=1000, value=100, step=10)

        mode = "clicked" if raw_mode.startswith("clicked-point") else "range"
        raw_start, raw_end = _resolve_raw_window(since, mode, selected_x, int(plus_minus))

        if mode == "clicked" and selected_x is None:
            st.info("No point selected yet. Using selected range for raw tables.")
            raw_start, raw_end = since, datetime.utcnow()

        dbq = get_raw_dbqlog_samples(data_source, raw_start, raw_end, workload=workload, row_limit=int(row_limit))
        res = get_raw_resusage_samples(data_source, raw_start, raw_end, row_limit=int(row_limit))
        join = get_raw_workload_join_samples(data_source, raw_start, raw_end, workload=workload, row_limit=int(row_limit))

        st.caption(
            "DBQL drilldown using raw telemetry identifiers. "
            f"Backend={backend}; filters: schema={td_demo_schema or 'n/a'}, user={td_demo_user or 'n/a'}"
        )
        st.dataframe(dbq, use_container_width=True, hide_index=True)

        st.caption("ResUsage drilldown using node/sample telemetry from the selected time window.")
        st.dataframe(res, use_container_width=True, hide_index=True)

        st.caption(
            "Derived workload view from raw DBQL telemetry. "
            "Workload labels are inferred from demo user/schema and query metadata."
        )
        st.dataframe(join, use_container_width=True, hide_index=True)

    if auto_refresh:
        time.sleep(15)
        st.rerun()


if __name__ == "__main__":
    main()
