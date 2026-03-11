"""Query helpers for the Streamlit telemetry demo."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd


def _connect(db_path: str | Path) -> sqlite3.Connection:
    return sqlite3.connect(str(db_path))


def _since_dt(range_key: str, custom_hours: int = 24) -> datetime:
    now = datetime.utcnow()
    if range_key == "last_24h":
        return now - timedelta(hours=24)
    if range_key == "last_3d":
        return now - timedelta(days=3)
    return now - timedelta(hours=max(1, custom_hours))


def get_kpis(db_path: str | Path, since: datetime) -> pd.DataFrame:
    with _connect(db_path) as conn:
        return pd.read_sql_query(
            """
            SELECT ts_window_start, ts_window_end, cpu_at_risk, io_at_risk, incidents_predicted, cost_risk_index
            FROM impact_kpis
            WHERE ts_window_end >= ?
            ORDER BY ts_window_end
            """,
            conn,
            params=[since.isoformat(timespec="seconds")],
        )


def get_metric_series(
    db_path: str | Path,
    metric_name: str,
    since: datetime,
    entity_filter: str | None = None,
) -> pd.DataFrame:
    sql = """
        SELECT ts, entity_id, value
        FROM metric_timeseries
        WHERE metric_name = ? AND ts >= ?
    """
    params: list[Any] = [metric_name, since.isoformat(timespec="seconds")]
    if entity_filter and entity_filter != "all":
        sql += " AND entity_id = ?"
        params.append(entity_filter)
    sql += " ORDER BY ts"
    with _connect(db_path) as conn:
        return pd.read_sql_query(sql, conn, params=params)


def get_model_state(
    db_path: str | Path,
    scenario_id: str | None = None,
    entity_type: str | None = None,
) -> pd.DataFrame:
    sql = """
        SELECT scenario_id, entity_type, entity_id, trained_at, baseline_json
        FROM model_state
        WHERE 1 = 1
    """
    params: list[Any] = []
    if scenario_id and scenario_id != "all":
        sql += " AND scenario_id = ?"
        params.append(scenario_id)
    if entity_type and entity_type != "all":
        sql += " AND entity_type = ?"
        params.append(entity_type)
    sql += " ORDER BY trained_at DESC"

    with _connect(db_path) as conn:
        df = pd.read_sql_query(sql, conn, params=params)

    if not df.empty:
        df["baseline"] = df["baseline_json"].apply(_safe_json)
    return df


def get_anomalies(
    db_path: str | Path,
    since: datetime,
    severity_filter: str | None = None,
    scenario_filter: str | None = None,
) -> pd.DataFrame:
    sql = """
        SELECT event_id, scenario_id, severity, entity_type, entity_id, ts, observed, expected, score, context_json
        FROM anomaly_events
        WHERE ts >= ? AND severity <> 'healthy'
    """
    params: list[Any] = [since.isoformat(timespec="seconds")]
    if severity_filter and severity_filter != "all":
        sql += " AND severity = ?"
        params.append(severity_filter)
    if scenario_filter and scenario_filter != "all":
        sql += " AND scenario_id = ?"
        params.append(scenario_filter)
    sql += " ORDER BY ts DESC"

    with _connect(db_path) as conn:
        return pd.read_sql_query(sql, conn, params=params)


def get_recent_observed_expected(db_path: str | Path, since: datetime) -> pd.DataFrame:
    with _connect(db_path) as conn:
        return pd.read_sql_query(
            """
            SELECT scenario_id, entity_id, ts, observed, expected, score
            FROM anomaly_events
            WHERE ts >= ? AND severity <> 'healthy' AND observed IS NOT NULL AND expected IS NOT NULL
            ORDER BY ts DESC
            """,
            conn,
            params=[since.isoformat(timespec="seconds")],
        )


def get_latest_data_timestamp(db_path: str | Path) -> datetime | None:
    with _connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT MAX(ts) FROM (
                SELECT MAX(ts) AS ts FROM metric_timeseries
                UNION ALL
                SELECT MAX(ts) AS ts FROM anomaly_events
                UNION ALL
                SELECT MAX(ts_window_end) AS ts FROM impact_kpis
            )
            """
        ).fetchone()
    if not row or not row[0]:
        return None
    return datetime.fromisoformat(str(row[0]).replace("Z", "+00:00")).replace(tzinfo=None)


def list_entities_for_metric(db_path: str | Path, metric_name: str) -> list[str]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT DISTINCT entity_id FROM metric_timeseries WHERE metric_name = ? ORDER BY entity_id",
            (metric_name,),
        ).fetchall()
    return [r[0] for r in rows]


def get_io_series(
    db_path: str | Path,
    since: datetime,
    workload: str = "all",
    check_mode: str = "sum",
) -> pd.DataFrame:
    sql = """
        SELECT ts, entity_id, value
        FROM metric_timeseries
        WHERE metric_name = 'workload_io_count' AND ts >= ?
    """
    params: list[Any] = [since.isoformat(timespec="seconds")]
    if workload != "all":
        sql += " AND entity_id = ?"
        params.append(workload)
    sql += " ORDER BY ts"

    with _connect(db_path) as conn:
        df = pd.read_sql_query(sql, conn, params=params)

    if df.empty:
        return df

    agg = "mean" if check_mode == "avg" else "sum"
    out = df.groupby("ts", as_index=False)["value"].agg(agg)
    out = out.sort_values("ts")
    return out


def get_io_outlier_events(db_path: str | Path, since: datetime, workload: str = "all") -> pd.DataFrame:
    sql = """
        SELECT event_id, scenario_id, severity, entity_type, entity_id, ts, observed, expected, score, context_json
        FROM anomaly_events
        WHERE scenario_id = 'S2' AND severity <> 'healthy' AND ts >= ?
    """
    params: list[Any] = [since.isoformat(timespec="seconds")]
    if workload != "all":
        sql += " AND entity_id = ?"
        params.append(workload)
    sql += " ORDER BY ts"

    with _connect(db_path) as conn:
        return pd.read_sql_query(sql, conn, params=params)


def get_tcore_workload_attribution(
    db_path: str | Path,
    since: datetime,
    remediation: str = "none",
    workload_scope: str = "all",
) -> pd.DataFrame:
    cpu = get_metric_series(db_path, "workload_cpu_usage", since)
    io = get_metric_series(db_path, "workload_io_count", since)
    if cpu.empty and io.empty:
        return pd.DataFrame()

    cpu = cpu.rename(columns={"value": "cpu_value"})
    io = io.rename(columns={"value": "io_value"})
    merged = cpu.merge(io, on=["ts", "entity_id"], how="outer").fillna(0.0)
    merged = merged.sort_values(["entity_id", "ts"]).reset_index(drop=True)

    merged["cpu_baseline"] = (
        merged.groupby("entity_id")["cpu_value"].transform(lambda s: s.rolling(12, min_periods=3).median().bfill())
    )
    merged["io_baseline"] = (
        merged.groupby("entity_id")["io_value"].transform(lambda s: s.rolling(12, min_periods=3).median().bfill())
    )
    merged["cpu_excess"] = (merged["cpu_value"] - merged["cpu_baseline"]).clip(lower=0.0)
    merged["io_excess"] = (merged["io_value"] - merged["io_baseline"]).clip(lower=0.0)
    merged["hour"] = pd.to_datetime(merged["ts"], utc=True, errors="coerce").dt.tz_convert(None).dt.hour.fillna(0).astype(int)

    skew = get_metric_series(db_path, "skew_ratio", since)
    if skew.empty:
        skew_pressure = pd.DataFrame(columns=["ts", "skew_pressure"])
    else:
        skew["skew_pressure"] = (skew["value"] - 5.0).clip(lower=0.0)
        skew_pressure = skew.groupby("ts", as_index=False)["skew_pressure"].mean()
    merged = merged.merge(skew_pressure, on="ts", how="left").fillna({"skew_pressure": 0.0})

    merged["baseline_consumption"] = (merged["cpu_baseline"] * 0.01) + (merged["io_baseline"] * 0.001)
    merged["excess_consumption"] = (merged["cpu_excess"] * 0.01) + (merged["io_excess"] * 0.001)
    merged["estimated_tcore_total"] = merged["baseline_consumption"] + merged["excess_consumption"] + (merged["skew_pressure"] * 0.05)
    merged["estimated_tcore_risk"] = merged["excess_consumption"] + (merged["skew_pressure"] * 0.05)

    if remediation != "none":
        merged = _apply_remediation_simulation(merged, remediation, workload_scope)
    else:
        merged["adjusted_cpu_value"] = merged["cpu_value"]
        merged["adjusted_io_value"] = merged["io_value"]
        merged["adjusted_skew_pressure"] = merged["skew_pressure"]
        merged["adjusted_excess_consumption"] = merged["excess_consumption"]
        merged["adjusted_tcore_total"] = merged["estimated_tcore_total"]
        merged["adjusted_tcore_risk"] = merged["estimated_tcore_risk"]
    return merged


def get_tcore_summary(
    db_path: str | Path,
    since: datetime,
    remediation: str = "none",
    workload_scope: str = "all",
) -> dict[str, float]:
    df = get_tcore_workload_attribution(db_path, since, remediation=remediation, workload_scope=workload_scope)
    if df.empty:
        return {
            "total_estimated_tcore": 0.0,
            "excess_tcore": 0.0,
            "adjusted_excess_tcore": 0.0,
            "reducible_tcore_pct": 0.0,
            "projected_savings_score": 0.0,
            "cpu_at_risk": 0.0,
            "io_at_risk": 0.0,
        }

    total_tcore = float(df["estimated_tcore_total"].sum())
    excess_tcore = float(df["estimated_tcore_risk"].sum())
    adjusted_excess = float(df["adjusted_tcore_risk"].sum())
    reducible_pct = float(((excess_tcore - adjusted_excess) / total_tcore) * 100.0) if total_tcore > 0 else 0.0
    reducible_pct = max(0.0, min(100.0, reducible_pct))
    return {
        "total_estimated_tcore": total_tcore,
        "excess_tcore": excess_tcore,
        "reducible_tcore_pct": reducible_pct,
        "projected_savings_score": float(excess_tcore - adjusted_excess),
        "cpu_at_risk": float(df["cpu_excess"].sum()),
        "io_at_risk": float(df["io_excess"].sum()),
        "adjusted_excess_tcore": adjusted_excess,
    }


def get_top_tcore_offenders(
    db_path: str | Path,
    since: datetime,
    remediation: str = "none",
    workload_scope: str = "all",
    limit: int = 10,
) -> pd.DataFrame:
    df = get_tcore_workload_attribution(db_path, since, remediation=remediation, workload_scope=workload_scope)
    if df.empty:
        return df
    out = (
        df.groupby("entity_id", as_index=False)
        .agg(
            total_tcore=("estimated_tcore_total", "sum"),
            excess_tcore=("estimated_tcore_risk", "sum"),
            adjusted_excess_tcore=("adjusted_tcore_risk", "sum"),
            cpu_excess=("cpu_excess", "sum"),
            io_excess=("io_excess", "sum"),
        )
        .sort_values(["excess_tcore", "io_excess"], ascending=False)
    )
    out["reduction_pct"] = (
        ((out["excess_tcore"] - out["adjusted_excess_tcore"]) / out["excess_tcore"].replace(0, pd.NA)) * 100.0
    ).fillna(0.0)
    return out.head(limit)


def get_top_consumers(
    db_path: str | Path,
    since: datetime,
    limit: int = 10,
) -> pd.DataFrame:
    c = get_metric_series(db_path, "consumer_cpu", since)
    if c.empty:
        return c
    c = c.sort_values(["entity_id", "ts"])
    c["baseline_cpu"] = c.groupby("entity_id")["value"].transform(lambda s: s.rolling(12, min_periods=3).median().bfill())
    c["cpu_excess"] = (c["value"] - c["baseline_cpu"]).clip(lower=0.0)
    return (
        c.groupby("entity_id", as_index=False)
        .agg(total_cpu=("value", "sum"), cpu_excess=("cpu_excess", "sum"))
        .sort_values(["cpu_excess", "total_cpu"], ascending=False)
        .head(limit)
    )


def get_top_skew_drivers(
    db_path: str | Path,
    since: datetime,
    limit: int = 10,
) -> pd.DataFrame:
    s = get_metric_series(db_path, "skew_ratio", since)
    if s.empty:
        return s
    s["skew_pressure"] = (s["value"] - 5.0).clip(lower=0.0)
    return (
        s.groupby("entity_id", as_index=False)
        .agg(latest_skew=("value", "last"), skew_pressure=("skew_pressure", "sum"))
        .sort_values(["skew_pressure", "latest_skew"], ascending=False)
        .head(limit)
    )


def get_peak_pressure_series(
    db_path: str | Path,
    since: datetime,
    remediation: str = "none",
    workload_scope: str = "all",
) -> pd.DataFrame:
    df = get_tcore_workload_attribution(db_path, since, remediation=remediation, workload_scope=workload_scope)
    if df.empty:
        return df
    return (
        df.groupby("ts", as_index=False)
        .agg(
            excess_tcore=("estimated_tcore_risk", "sum"),
            adjusted_excess_tcore=("adjusted_tcore_risk", "sum"),
            total_tcore=("estimated_tcore_total", "sum"),
        )
        .sort_values("ts")
    )


def get_recent_query_offenders(
    db_path: str | Path,
    since: datetime,
    workload: str = "all",
    limit: int = 20,
) -> pd.DataFrame:
    start = since
    end = datetime.utcnow()
    queries = get_raw_workload_join_samples(db_path, start, end, workload=workload, row_limit=max(limit * 5, 50))
    if queries.empty:
        return queries
    queries["risk_score"] = (queries["amp_cpu_time"] * 0.01) + (queries["io_count"] * 0.001)
    return queries.sort_values(["risk_score", "amp_cpu_time", "io_count"], ascending=False).head(limit)


def get_remediation_actions(
    offenders: pd.DataFrame,
    consumers: pd.DataFrame,
    skew: pd.DataFrame,
) -> list[dict[str, str]]:
    actions: list[dict[str, str]] = []
    if not offenders.empty:
        top = offenders.iloc[0]
        actions.append(
            {
                "action": "Tune or throttle top workload",
                "target": str(top["entity_id"]),
                "reason": f"Highest excess TCore contributor ({float(top['excess_tcore']):.2f}).",
            }
        )
    if not consumers.empty:
        top = consumers.iloc[0]
        actions.append(
            {
                "action": "Review noisy consumer",
                "target": str(top["entity_id"]),
                "reason": f"Largest CPU excess among consumers ({float(top['cpu_excess']):.2f}).",
            }
        )
    if not skew.empty:
        top = skew.iloc[0]
        actions.append(
            {
                "action": "Investigate skewed step",
                "target": str(top["entity_id"]),
                "reason": f"Highest cumulative skew pressure ({float(top['skew_pressure']):.2f}).",
            }
        )
    return actions


def _apply_remediation_simulation(
    df: pd.DataFrame,
    remediation: str,
    workload_scope: str,
) -> pd.DataFrame:
    out = df.copy()

    def applies(row: pd.Series) -> bool:
        if workload_scope != "all" and str(row["entity_id"]) != workload_scope:
            return False
        if remediation == "tune_high_io_etl":
            return str(row["entity_id"]) == "ETL"
        if remediation == "reduce_skew":
            return True
        if remediation == "shift_batch_off_peak":
            return int(row["hour"]) in range(8, 19)
        if remediation == "combined":
            return True
        return False

    mask = out.apply(applies, axis=1)
    cpu_factor = 1.0
    io_factor = 1.0
    skew_factor = 1.0
    if remediation == "tune_high_io_etl":
        cpu_factor, io_factor, skew_factor = 0.85, 0.45, 1.0
    elif remediation == "reduce_skew":
        cpu_factor, io_factor, skew_factor = 0.95, 0.95, 0.35
    elif remediation == "shift_batch_off_peak":
        cpu_factor, io_factor, skew_factor = 0.7, 0.7, 0.8
    elif remediation == "combined":
        cpu_factor, io_factor, skew_factor = 0.65, 0.4, 0.3

    out["adjusted_cpu_value"] = out["cpu_value"]
    out["adjusted_io_value"] = out["io_value"]
    out["adjusted_skew_pressure"] = out["skew_pressure"]
    out.loc[mask, "adjusted_cpu_value"] = out.loc[mask, "cpu_baseline"] + (out.loc[mask, "cpu_value"] - out.loc[mask, "cpu_baseline"]) * cpu_factor
    out.loc[mask, "adjusted_io_value"] = out.loc[mask, "io_baseline"] + (out.loc[mask, "io_value"] - out.loc[mask, "io_baseline"]) * io_factor
    out.loc[mask, "adjusted_skew_pressure"] = out.loc[mask, "skew_pressure"] * skew_factor

    adjusted_cpu_excess = (out["adjusted_cpu_value"] - out["cpu_baseline"]).clip(lower=0.0)
    adjusted_io_excess = (out["adjusted_io_value"] - out["io_baseline"]).clip(lower=0.0)
    out["adjusted_excess_consumption"] = (adjusted_cpu_excess * 0.01) + (adjusted_io_excess * 0.001)
    out["adjusted_tcore_risk"] = out["adjusted_excess_consumption"] + (out["adjusted_skew_pressure"] * 0.05)
    out["adjusted_tcore_total"] = out["baseline_consumption"] + out["adjusted_excess_consumption"] + (out["adjusted_skew_pressure"] * 0.05)
    return out


def build_io_bands(series: pd.DataFrame, window: int = 24) -> pd.DataFrame:
    if series.empty:
        return series
    df = series.copy().sort_values("ts")
    roll = df["value"].rolling(window=window, min_periods=max(6, window // 4))
    df["p01"] = roll.quantile(0.01)
    df["p10"] = roll.quantile(0.10)
    df["p50"] = roll.quantile(0.50)
    df["p90"] = roll.quantile(0.90)
    df["p99"] = roll.quantile(0.99)

    # Backfill early points to avoid empty bands at the chart head.
    df[["p01", "p10", "p50", "p90", "p99"]] = df[["p01", "p10", "p50", "p90", "p99"]].bfill()

    # Enforce monotonic band ordering for visual consistency.
    df["p10"] = df[["p01", "p10"]].max(axis=1)
    df["p50"] = df[["p10", "p50"]].max(axis=1)
    df["p90"] = df[["p50", "p90"]].max(axis=1)
    df["p99"] = df[["p90", "p99"]].max(axis=1)
    return df


def get_raw_dbqlog_samples(
    db_path: str | Path,
    start: datetime,
    end: datetime,
    workload: str = "all",
    row_limit: int = 100,
) -> pd.DataFrame:
    sql = """
        SELECT
            q.query_id,
            q.user_name,
            q.start_time,
            q.end_time,
            q.elapsed_time,
            q.amp_cpu_time,
            q.io_count,
            q.error_code,
            COALESCE(w.workload_name, 'Unknown') AS workload_name
        FROM dbc_dbqlogtbl q
        LEFT JOIN workload_map w ON w.query_id = q.query_id
        WHERE q.start_time >= ? AND q.start_time <= ?
    """
    params: list[Any] = [start.isoformat(timespec="seconds"), end.isoformat(timespec="seconds")]
    if workload != "all":
        sql += " AND COALESCE(w.workload_name, 'Unknown') = ?"
        params.append(workload)
    sql += " ORDER BY q.start_time DESC LIMIT ?"
    params.append(int(row_limit))
    with _connect(db_path) as conn:
        return pd.read_sql_query(sql, conn, params=params)


def get_raw_resusage_samples(
    db_path: str | Path,
    start: datetime,
    end: datetime,
    row_limit: int = 100,
) -> pd.DataFrame:
    with _connect(db_path) as conn:
        return pd.read_sql_query(
            """
            SELECT sample_time, node_id, cpu_percent, disk_io
            FROM resusage_spma
            WHERE sample_time >= ? AND sample_time <= ?
            ORDER BY sample_time DESC
            LIMIT ?
            """,
            conn,
            params=[start.isoformat(timespec="seconds"), end.isoformat(timespec="seconds"), int(row_limit)],
        )


def get_raw_workload_join_samples(
    db_path: str | Path,
    start: datetime,
    end: datetime,
    workload: str = "all",
    row_limit: int = 100,
) -> pd.DataFrame:
    sql = """
        SELECT
            q.query_id,
            COALESCE(w.workload_name, 'Unknown') AS workload_name,
            q.start_time,
            q.elapsed_time,
            q.amp_cpu_time,
            q.io_count,
            CASE
                WHEN q.amp_cpu_time > 0 THEN ROUND(q.io_count * 1.0 / q.amp_cpu_time, 4)
                ELSE NULL
            END AS io_per_cpu
        FROM dbc_dbqlogtbl q
        LEFT JOIN workload_map w ON w.query_id = q.query_id
        WHERE q.start_time >= ? AND q.start_time <= ?
    """
    params: list[Any] = [start.isoformat(timespec="seconds"), end.isoformat(timespec="seconds")]
    if workload != "all":
        sql += " AND COALESCE(w.workload_name, 'Unknown') = ?"
        params.append(workload)
    sql += " ORDER BY q.start_time DESC LIMIT ?"
    params.append(int(row_limit))
    with _connect(db_path) as conn:
        return pd.read_sql_query(sql, conn, params=params)


def get_band_snapshot(series_with_bands: pd.DataFrame, selected_ts: datetime) -> dict[str, Any] | None:
    if series_with_bands.empty:
        return None
    df = series_with_bands.copy()
    df["ts_dt"] = pd.to_datetime(df["ts"], utc=True, errors="coerce").dt.tz_convert(None)
    if df["ts_dt"].isna().all():
        return None
    idx = (df["ts_dt"] - selected_ts).abs().idxmin()
    row = df.loc[idx]
    value = float(row["value"])
    p01 = float(row["p01"])
    p10 = float(row["p10"])
    p50 = float(row["p50"])
    p90 = float(row["p90"])
    p99 = float(row["p99"])
    if value < p01:
        zone = "red"
    elif value < p10:
        zone = "yellow"
    elif value <= p90:
        zone = "green"
    elif value <= p99:
        zone = "yellow"
    else:
        zone = "red"
    return {
        "ts": str(row["ts"]),
        "value": value,
        "p01": p01,
        "p10": p10,
        "p50": p50,
        "p90": p90,
        "p99": p99,
        "zone": zone,
    }


def _safe_json(raw: str) -> dict[str, Any]:
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass
    return {}
