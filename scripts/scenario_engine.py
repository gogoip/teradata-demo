#!/usr/bin/env python3
"""Digna-style workload scenario engine (SQLite-first)."""

from __future__ import annotations

import argparse
import json
import sqlite3
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import ruptures as rpt
from sklearn.ensemble import IsolationForest
from statsmodels.tsa.seasonal import STL


def parse_iso_datetime(raw: str) -> datetime:
    value = raw.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def to_naive_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(UTC).replace(tzinfo=None)


def floor_window(dt: datetime, window_min: int) -> datetime:
    total_seconds = int(dt.replace(tzinfo=UTC).timestamp())
    bucket = (total_seconds // (window_min * 60)) * (window_min * 60)
    return datetime.fromtimestamp(bucket, tz=UTC).replace(tzinfo=None)


def robust_zscore(value: float, center: float, mad: float) -> float:
    if mad <= 1e-9:
        return 0.0
    return float(0.6745 * (value - center) / mad)


def _json_default(obj: Any) -> Any:
    if isinstance(obj, (np.integer, np.floating)):
        return float(obj)
    return str(obj)


@dataclass
class Event:
    scenario_id: str
    severity: str
    entity_type: str
    entity_id: str
    ts: datetime
    observed: float | None
    expected: float | None
    score: float | None
    context: dict[str, Any]


class TelemetrySource(ABC):
    @abstractmethod
    def query_logs(self, start: datetime, end: datetime) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def resusage(self, start: datetime, end: datetime) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def step_stats(self, start: datetime, end: datetime) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def table_stats(self) -> pd.DataFrame:
        raise NotImplementedError


class SQLiteSource(TelemetrySource):
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def query_logs(self, start: datetime, end: datetime) -> pd.DataFrame:
        sql = """
            SELECT q.query_id,
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
            WHERE q.start_time >= ? AND q.start_time < ?
        """
        return pd.read_sql_query(
            sql,
            self.conn,
            params=[start.isoformat(timespec="seconds"), end.isoformat(timespec="seconds")],
        )

    def resusage(self, start: datetime, end: datetime) -> pd.DataFrame:
        sql = """
            SELECT sample_time, node_id, cpu_percent, disk_io
            FROM resusage_spma
            WHERE sample_time >= ? AND sample_time < ?
        """
        return pd.read_sql_query(
            sql,
            self.conn,
            params=[start.isoformat(timespec="seconds"), end.isoformat(timespec="seconds")],
        )

    def step_stats(self, start: datetime, end: datetime) -> pd.DataFrame:
        sql = """
            SELECT s.query_id, s.step_id, s.amp_id, s.cpu_time, q.start_time
            FROM dbql_step_tbl s
            JOIN dbc_dbqlogtbl q ON q.query_id = s.query_id
            WHERE q.start_time >= ? AND q.start_time < ?
        """
        return pd.read_sql_query(
            sql,
            self.conn,
            params=[start.isoformat(timespec="seconds"), end.isoformat(timespec="seconds")],
        )

    def table_stats(self) -> pd.DataFrame:
        return pd.read_sql_query(
            "SELECT database_name, table_name, row_count, last_collect_stats FROM tables_v",
            self.conn,
        )


class TeradataSource(TelemetrySource):
    """Stub for future Teradata-native extraction."""

    def query_logs(self, start: datetime, end: datetime) -> pd.DataFrame:
        raise NotImplementedError("TeradataSource is not implemented yet.")

    def resusage(self, start: datetime, end: datetime) -> pd.DataFrame:
        raise NotImplementedError("TeradataSource is not implemented yet.")

    def step_stats(self, start: datetime, end: datetime) -> pd.DataFrame:
        raise NotImplementedError("TeradataSource is not implemented yet.")

    def table_stats(self) -> pd.DataFrame:
        raise NotImplementedError("TeradataSource is not implemented yet.")


class ScenarioEngine:
    def __init__(
        self,
        conn: sqlite3.Connection,
        source: TelemetrySource,
        window_min: int,
        lookback_days: int,
    ):
        self.conn = conn
        self.source = source
        self.window_min = window_min
        self.lookback_days = lookback_days
        self.period_day = int((24 * 60) / self.window_min)

    def initialize_schema(self, schema_paths: list[Path]) -> None:
        for path in schema_paths:
            with path.open("r", encoding="utf-8") as f:
                self.conn.executescript(f.read())
        self.conn.commit()

    def run_once(self, run_at: datetime) -> dict[str, Any]:
        run_at = to_naive_utc(run_at)
        window_end = floor_window(run_at, self.window_min)
        window_start = window_end - timedelta(minutes=self.window_min)
        lookback_start = window_end - timedelta(days=self.lookback_days)

        df_q = self.source.query_logs(lookback_start, window_end)
        df_r = self.source.resusage(lookback_start, window_end)
        df_s = self.source.step_stats(lookback_start, window_end)
        df_t = self.source.table_stats()

        metrics = self._build_metric_timeseries(df_q, df_r, df_s, df_t, window_end)
        self._persist_metrics(metrics)

        events: list[Event] = []
        events.extend(self._scenario_s1_cpu_trend(window_end))
        events.extend(self._scenario_s2_io_outliers(window_end))
        events.extend(self._scenario_s3_unstable_consumers(window_end))
        events.extend(self._scenario_s4_change_points(window_end))
        events.extend(self._scenario_s5_seasonality(window_end))
        events.extend(self._scenario_s6_perm_growth(window_end))
        events.extend(self._scenario_s7_skew(window_end))

        self._ensure_healthy(events, window_end)
        self._persist_events(events)
        kpis = self._compute_impact_kpis(window_start, window_end, events)
        self._persist_kpis(kpis)

        self.conn.commit()
        return {
            "window_start": window_start.isoformat(timespec="seconds"),
            "window_end": window_end.isoformat(timespec="seconds"),
            "metric_rows": len(metrics),
            "events": len(events),
            "kpis": kpis,
        }

    def _build_metric_timeseries(
        self,
        df_q: pd.DataFrame,
        df_r: pd.DataFrame,
        df_s: pd.DataFrame,
        df_t: pd.DataFrame,
        snapshot_ts: datetime,
    ) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []

        if not df_q.empty:
            df_q = df_q.copy()
            df_q["start_time"] = pd.to_datetime(df_q["start_time"], utc=True).dt.tz_convert(None)
            df_q["bucket"] = df_q["start_time"].dt.floor(f"{self.window_min}min")
            df_q["consumer_id"] = df_q["user_name"] + "|" + df_q["workload_name"]

            sys_cpu = df_q.groupby("bucket", as_index=False)["amp_cpu_time"].sum()
            for r in sys_cpu.itertuples(index=False):
                rows.append(self._metric_row("system_cpu_usage", "system", "all", r.bucket, float(r.amp_cpu_time), {}))

            w_cpu = df_q.groupby(["bucket", "workload_name"], as_index=False)["amp_cpu_time"].sum()
            for r in w_cpu.itertuples(index=False):
                rows.append(self._metric_row("workload_cpu_usage", "workload", str(r.workload_name), r.bucket, float(r.amp_cpu_time), {}))

            w_io = df_q.groupby(["bucket", "workload_name"], as_index=False)["io_count"].sum()
            for r in w_io.itertuples(index=False):
                rows.append(self._metric_row("workload_io_count", "workload", str(r.workload_name), r.bucket, float(r.io_count), {}))

            w_agg = df_q.groupby(["bucket", "workload_name"], as_index=False).agg(
                io_sum=("io_count", "sum"),
                cpu_sum=("amp_cpu_time", "sum"),
            )
            w_agg["io_per_cpu"] = w_agg.apply(
                lambda x: float(x["io_sum"] / x["cpu_sum"]) if x["cpu_sum"] else 0.0,
                axis=1,
            )
            for r in w_agg.itertuples(index=False):
                rows.append(self._metric_row("workload_io_per_cpu", "workload", str(r.workload_name), r.bucket, float(r.io_per_cpu), {}))

            consumer_cpu = df_q.groupby(["bucket", "consumer_id"], as_index=False)["amp_cpu_time"].sum()
            for r in consumer_cpu.itertuples(index=False):
                rows.append(self._metric_row("consumer_cpu", "consumer", str(r.consumer_id), r.bucket, float(r.amp_cpu_time), {}))

        if not df_r.empty:
            df_r = df_r.copy()
            df_r["sample_time"] = pd.to_datetime(df_r["sample_time"], utc=True).dt.tz_convert(None)
            df_r["bucket"] = df_r["sample_time"].dt.floor(f"{self.window_min}min")
            disk = df_r.groupby("bucket", as_index=False)["disk_io"].sum()
            for r in disk.itertuples(index=False):
                rows.append(self._metric_row("system_disk_io", "system", "all", r.bucket, float(r.disk_io), {}))

        if not df_s.empty:
            df_s = df_s.copy()
            df_s["start_time"] = pd.to_datetime(df_s["start_time"], utc=True).dt.tz_convert(None)
            df_s["bucket"] = df_s["start_time"].dt.floor(f"{self.window_min}min")
            g = df_s.groupby(["bucket", "step_id"], as_index=False).agg(
                max_cpu=("cpu_time", "max"),
                avg_cpu=("cpu_time", "mean"),
            )
            g["skew_ratio"] = g.apply(
                lambda x: float(x["max_cpu"] / x["avg_cpu"]) if x["avg_cpu"] else 0.0,
                axis=1,
            )
            g["entity_id"] = "step:" + g["step_id"].astype(str)
            for r in g.itertuples(index=False):
                rows.append(self._metric_row("skew_ratio", "query_step", str(r.entity_id), r.bucket, float(r.skew_ratio), {}))

        if not df_t.empty:
            for r in df_t.itertuples(index=False):
                entity = f"{r.database_name}.{r.table_name}"
                rows.append(self._metric_row("table_row_count", "table", entity, snapshot_ts, float(r.row_count), {}))

        return pd.DataFrame(rows)

    @staticmethod
    def _metric_row(metric_name: str, entity_type: str, entity_id: str, ts: datetime, value: float, tags: dict[str, Any]) -> dict[str, Any]:
        return {
            "metric_name": metric_name,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "ts": pd.Timestamp(ts).to_pydatetime().isoformat(timespec="seconds"),
            "value": float(value),
            "tags_json": json.dumps(tags, default=_json_default),
        }

    def _persist_metrics(self, metrics: pd.DataFrame) -> None:
        if metrics.empty:
            return
        sql = """
            INSERT INTO metric_timeseries(metric_name, entity_type, entity_id, ts, value, tags_json)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(metric_name, entity_type, entity_id, ts)
            DO UPDATE SET value=excluded.value, tags_json=excluded.tags_json
        """
        self.conn.executemany(
            sql,
            [
                (
                    r.metric_name,
                    r.entity_type,
                    r.entity_id,
                    r.ts,
                    r.value,
                    r.tags_json,
                )
                for r in metrics.itertuples(index=False)
            ],
        )
    def _persist_model_state(
        self,
        scenario_id: str,
        entity_type: str,
        entity_id: str,
        trained_at: datetime,
        baseline: dict[str, Any],
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO model_state(scenario_id, entity_type, entity_id, trained_at, model_blob, baseline_json)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(scenario_id, entity_type, entity_id)
            DO UPDATE SET trained_at=excluded.trained_at, baseline_json=excluded.baseline_json
            """,
            (
                scenario_id,
                entity_type,
                entity_id,
                trained_at.isoformat(timespec="seconds"),
                None,
                json.dumps(baseline, default=_json_default),
            ),
        )

    def _series(self, metric_name: str, entity_type: str | None = None) -> pd.DataFrame:
        params: list[Any] = [metric_name]
        sql = "SELECT entity_type, entity_id, ts, value FROM metric_timeseries WHERE metric_name = ?"
        if entity_type:
            sql += " AND entity_type = ?"
            params.append(entity_type)
        df = pd.read_sql_query(sql, self.conn, params=params)
        if not df.empty:
            df["ts"] = pd.to_datetime(df["ts"], utc=True).dt.tz_convert(None)
            df = df.sort_values(["entity_id", "ts"])
        return df

    def _seasonal_expected(self, values: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        n = len(values)
        if n < max(12, self.period_day):
            roll = pd.Series(values).rolling(6, min_periods=1).median().to_numpy()
            resid = values - roll
            return roll, resid
        period = self.period_day
        if n >= self.period_day * 2:
            stl = STL(values, period=period, robust=True).fit()
            expected = stl.trend + stl.seasonal
            resid = stl.resid
            return expected, resid
        roll = pd.Series(values).rolling(period, min_periods=max(4, period // 3)).median().to_numpy()
        roll = np.nan_to_num(roll, nan=np.nanmedian(values))
        resid = values - roll
        return roll, resid

    def _scenario_s1_cpu_trend(self, window_end: datetime) -> list[Event]:
        out: list[Event] = []
        series = self._series("workload_cpu_usage", "workload")
        series_all = self._series("system_cpu_usage", "system")
        if not series_all.empty:
            series = pd.concat([series, series_all], ignore_index=True)

        for entity_id, group in series.groupby("entity_id"):
            g = group.sort_values("ts")
            vals = g["value"].to_numpy(dtype=float)
            expected, resid = self._seasonal_expected(vals)
            med = float(np.median(resid))
            mad = float(np.median(np.abs(resid - med))) + 1e-6
            scores = np.array([abs(robust_zscore(v, med, mad)) for v in resid])
            self._persist_model_state(
                "S1",
                "workload" if entity_id != "all" else "system",
                entity_id,
                window_end,
                {"median_resid": med, "mad_resid": mad, "period": self.period_day},
            )
            if len(scores) < 2:
                continue
            latest_ts = g["ts"].iloc[-1]
            if latest_ts != window_end - timedelta(minutes=self.window_min):
                continue
            if scores[-1] >= 3.5 and scores[-2] >= 3.5:
                out.append(
                    Event(
                        scenario_id="S1",
                        severity="high" if scores[-1] >= 5 else "medium",
                        entity_type="workload" if entity_id != "all" else "system",
                        entity_id=str(entity_id),
                        ts=window_end,
                        observed=float(vals[-1]),
                        expected=float(expected[-1]),
                        score=float(scores[-1]),
                        context={"consecutive_windows": 2},
                    )
                )
        return out

    def _scenario_s2_io_outliers(self, window_end: datetime) -> list[Event]:
        out: list[Event] = []
        io = self._series("workload_io_count", "workload")
        io_pc = self._series("workload_io_per_cpu", "workload")
        disk = self._series("system_disk_io", "system")
        if io.empty or io_pc.empty:
            return out

        disk_map = {row.ts: row.value for row in disk.itertuples(index=False)}

        for workload, group in io.groupby("entity_id"):
            g1 = group[["ts", "value"]].rename(columns={"value": "io_count"})
            g2 = io_pc[io_pc["entity_id"] == workload][["ts", "value"]].rename(columns={"value": "io_per_cpu"})
            data = g1.merge(g2, on="ts", how="inner")
            if data.empty:
                continue
            data["disk_io"] = data["ts"].map(disk_map).fillna(0.0)
            data = data.sort_values("ts")
            if len(data) < 20:
                continue

            x_values = data[["io_count", "disk_io", "io_per_cpu"]].to_numpy(dtype=float)
            model = IsolationForest(contamination=0.01, random_state=42)
            model.fit(x_values)
            pred = model.predict(x_values)
            decision = model.decision_function(x_values)
            idx = len(data) - 1
            latest_ts = data["ts"].iloc[idx]
            if latest_ts != window_end - timedelta(minutes=self.window_min):
                continue

            self._persist_model_state(
                "S2",
                "workload",
                str(workload),
                window_end,
                {"contamination": 0.01, "samples": len(data)},
            )

            if pred[idx] == -1:
                score = float(-decision[idx])
                out.append(
                    Event(
                        scenario_id="S2",
                        severity="high" if score > 0.2 else "medium",
                        entity_type="workload",
                        entity_id=str(workload),
                        ts=window_end,
                        observed=float(data["io_count"].iloc[idx]),
                        expected=float(np.median(data["io_count"].iloc[:-1])),
                        score=score,
                        context={"io_per_cpu": float(data["io_per_cpu"].iloc[idx])},
                    )
                )
        return out

    def _scenario_s3_unstable_consumers(self, window_end: datetime) -> list[Event]:
        out: list[Event] = []
        c = self._series("consumer_cpu", "consumer")
        if c.empty:
            return out

        for consumer, group in c.groupby("entity_id"):
            g = group.sort_values("ts")
            vals = g["value"].to_numpy(dtype=float)
            if len(vals) < 24:
                continue
            split = max(8, len(vals) // 2)
            prior = vals[:split]
            recent = vals[split:]
            if len(recent) < 8:
                continue
            prior_mean = float(np.mean(prior))
            recent_mean = float(np.mean(recent))
            prior_cv = float(np.std(prior) / prior_mean) if prior_mean > 1e-9 else 0.0
            recent_cv = float(np.std(recent) / recent_mean) if recent_mean > 1e-9 else 0.0
            p75 = float(np.percentile(vals, 75))
            latest = float(vals[-1])

            self._persist_model_state(
                "S3",
                "consumer",
                str(consumer),
                window_end,
                {"prior_cv": prior_cv, "recent_cv": recent_cv, "p75": p75},
            )

            latest_ts = g["ts"].iloc[-1]
            if latest_ts != window_end - timedelta(minutes=self.window_min):
                continue

            if prior_cv > 0 and recent_cv > 2.0 * prior_cv and latest > p75:
                out.append(
                    Event(
                        scenario_id="S3",
                        severity="high" if recent_cv > 3.0 * prior_cv else "medium",
                        entity_type="consumer",
                        entity_id=str(consumer),
                        ts=window_end,
                        observed=latest,
                        expected=recent_mean,
                        score=float(recent_cv / prior_cv),
                        context={"prior_cv": prior_cv, "recent_cv": recent_cv},
                    )
                )
        return out
    def _scenario_s4_change_points(self, window_end: datetime) -> list[Event]:
        out: list[Event] = []
        c = self._series("consumer_cpu", "consumer")
        if c.empty:
            return out

        totals = c.groupby("entity_id", as_index=False)["value"].sum().sort_values("value", ascending=False)
        critical = set(totals["entity_id"].head(20).tolist())

        for consumer, group in c.groupby("entity_id"):
            if consumer not in critical:
                continue
            g = group.sort_values("ts")
            vals = g["value"].to_numpy(dtype=float)
            if len(vals) < 25:
                continue
            _, resid = self._seasonal_expected(vals)
            signal = resid.reshape(-1, 1)
            algo = rpt.Pelt(model="rbf").fit(signal)
            penalty = np.log(len(signal)) * np.var(signal)
            bkps = algo.predict(pen=max(float(penalty), 1.0))
            last_idx = bkps[-2] if len(bkps) >= 2 else None
            latest_ts = g["ts"].iloc[-1]
            self._persist_model_state(
                "S4",
                "consumer",
                str(consumer),
                window_end,
                {"last_break_index": last_idx, "breakpoints": bkps},
            )
            if latest_ts != window_end - timedelta(minutes=self.window_min):
                continue
            if last_idx is None:
                continue
            if len(vals) - last_idx <= 2:
                out.append(
                    Event(
                        scenario_id="S4",
                        severity="critical",
                        entity_type="consumer",
                        entity_id=str(consumer),
                        ts=window_end,
                        observed=float(vals[-1]),
                        expected=float(np.mean(vals[max(0, last_idx - 5):last_idx])) if last_idx > 0 else float(np.mean(vals)),
                        score=float(len(vals) - last_idx),
                        context={"breakpoints": bkps},
                    )
                )
        return out

    def _scenario_s5_seasonality(self, window_end: datetime) -> list[Event]:
        out: list[Event] = []
        s = self._series("system_cpu_usage", "system")
        if s.empty:
            return out

        g = s.sort_values("ts")
        vals = g["value"].to_numpy(dtype=float)
        if len(vals) < 12:
            return out
        expected, resid = self._seasonal_expected(vals)
        resid_std = float(np.std(resid)) + 1e-6
        latest_dev = float(abs(vals[-1] - expected[-1]))
        band = 2.0 * resid_std
        self._persist_model_state(
            "S5",
            "system",
            "all",
            window_end,
            {"residual_std": resid_std, "band": band, "period": self.period_day},
        )

        latest_ts = g["ts"].iloc[-1]
        if latest_ts != window_end - timedelta(minutes=self.window_min):
            return out

        if latest_dev > band:
            out.append(
                Event(
                    scenario_id="S5",
                    severity="medium",
                    entity_type="system",
                    entity_id="all",
                    ts=window_end,
                    observed=float(vals[-1]),
                    expected=float(expected[-1]),
                    score=float(latest_dev / band),
                    context={"seasonal_band": band},
                )
            )
        return out

    def _scenario_s6_perm_growth(self, window_end: datetime) -> list[Event]:
        out: list[Event] = []
        t = self._series("table_row_count", "table")
        if t.empty:
            return out

        for table_name, group in t.groupby("entity_id"):
            g = group.sort_values("ts")
            vals = g["value"].to_numpy(dtype=float)
            if len(vals) < 7:
                continue
            x_hist = np.arange(len(vals) - 1, dtype=float)
            y_hist = vals[:-1]
            m, b = np.polyfit(x_hist, y_hist, 1)
            pred_hist = m * x_hist + b
            resid = y_hist - pred_hist
            std = float(np.std(resid)) + 1e-6
            latest_x = float(len(vals) - 1)
            expected_latest = float((m * latest_x) + b)
            upper = float(expected_latest + 2.58 * std)
            self._persist_model_state(
                "S6",
                "table",
                str(table_name),
                window_end,
                {"slope": float(m), "intercept": float(b), "std": std},
            )
            if vals[-1] > upper:
                out.append(
                    Event(
                        scenario_id="S6",
                        severity="high",
                        entity_type="table",
                        entity_id=str(table_name),
                        ts=window_end,
                        observed=float(vals[-1]),
                        expected=expected_latest,
                        score=float((vals[-1] - expected_latest) / std),
                        context={"upper_99": upper},
                    )
                )
        return out

    def _scenario_s7_skew(self, window_end: datetime) -> list[Event]:
        out: list[Event] = []
        s = self._series("skew_ratio", "query_step")
        if s.empty:
            return out

        for entity, group in s.groupby("entity_id"):
            g = group.sort_values("ts")
            vals = g["value"].to_numpy(dtype=float)
            if len(vals) < 3:
                continue
            self._persist_model_state(
                "S7",
                "query_step",
                str(entity),
                window_end,
                {"latest3": vals[-3:].tolist()},
            )
            latest_ts = g["ts"].iloc[-1]
            if latest_ts != window_end - timedelta(minutes=self.window_min):
                continue
            if vals[-1] > 5.0 and vals[-1] > vals[-2] > vals[-3]:
                out.append(
                    Event(
                        scenario_id="S7",
                        severity="high",
                        entity_type="query_step",
                        entity_id=str(entity),
                        ts=window_end,
                        observed=float(vals[-1]),
                        expected=float(np.mean(vals[-3:-1])),
                        score=float(vals[-1]),
                        context={"trend": vals[-3:].tolist()},
                    )
                )
        return out

    def _ensure_healthy(self, events: list[Event], window_end: datetime) -> None:
        for sid, etype, eid in [
            ("S1", "system", "all"),
            ("S2", "workload", "all"),
            ("S3", "consumer", "all"),
            ("S4", "consumer", "critical"),
            ("S5", "system", "all"),
            ("S6", "table", "all"),
            ("S7", "query_step", "all"),
        ]:
            if not any(e.scenario_id == sid for e in events):
                events.append(
                    Event(
                        scenario_id=sid,
                        severity="healthy",
                        entity_type=etype,
                        entity_id=eid,
                        ts=window_end,
                        observed=None,
                        expected=None,
                        score=0.0,
                        context={"status": "no_anomaly"},
                    )
                )

    def _persist_events(self, events: list[Event]) -> None:
        if not events:
            return
        self.conn.executemany(
            """
            INSERT INTO anomaly_events(
                scenario_id, severity, entity_type, entity_id, ts, observed, expected, score, context_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    e.scenario_id,
                    e.severity,
                    e.entity_type,
                    e.entity_id,
                    e.ts.isoformat(timespec="seconds"),
                    e.observed,
                    e.expected,
                    e.score,
                    json.dumps(e.context, default=_json_default),
                )
                for e in events
            ],
        )

    def _compute_impact_kpis(self, window_start: datetime, window_end: datetime, events: list[Event]) -> dict[str, Any]:
        cpu_risk = 0.0
        io_risk = 0.0
        skew_events = 0
        incidents = 0

        for e in events:
            if e.severity in {"high", "critical"}:
                incidents += 1
            if e.scenario_id in {"S1", "S3", "S4"} and e.observed is not None and e.expected is not None:
                cpu_risk += max(0.0, e.observed - e.expected)
            if e.scenario_id == "S2" and e.observed is not None and e.expected is not None:
                io_risk += max(0.0, e.observed - e.expected)
            if e.scenario_id == "S7" and e.severity != "healthy":
                skew_events += 1

        cost_risk = min(100.0, (cpu_risk / 1000.0) + (io_risk / 5000.0) + (5.0 * skew_events))
        return {
            "ts_window_start": window_start.isoformat(timespec="seconds"),
            "ts_window_end": window_end.isoformat(timespec="seconds"),
            "cpu_at_risk": float(cpu_risk),
            "io_at_risk": float(io_risk),
            "incidents_predicted": int(incidents),
            "cost_risk_index": float(cost_risk),
        }

    def _persist_kpis(self, kpis: dict[str, Any]) -> None:
        self.conn.execute(
            """
            INSERT INTO impact_kpis(ts_window_start, ts_window_end, cpu_at_risk, io_at_risk, incidents_predicted, cost_risk_index)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(ts_window_start, ts_window_end)
            DO UPDATE SET
                cpu_at_risk=excluded.cpu_at_risk,
                io_at_risk=excluded.io_at_risk,
                incidents_predicted=excluded.incidents_predicted,
                cost_risk_index=excluded.cost_risk_index
            """,
            (
                kpis["ts_window_start"],
                kpis["ts_window_end"],
                kpis["cpu_at_risk"],
                kpis["io_at_risk"],
                kpis["incidents_predicted"],
                kpis["cost_risk_index"],
            ),
        )

def cmd_run(args: argparse.Namespace) -> None:
    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        source = SQLiteSource(conn)
        engine = ScenarioEngine(conn, source, args.window_min, args.lookback_days)
        engine.initialize_schema([
            Path(args.schema),
            Path(args.scenario_schema),
        ])
        run_at = parse_iso_datetime(args.at) if args.at else datetime.now(UTC)
        summary = engine.run_once(run_at)
        print(json.dumps(summary, indent=2))


def cmd_backfill(args: argparse.Namespace) -> None:
    start = parse_iso_datetime(args.start)
    end = parse_iso_datetime(args.end)
    if end <= start:
        raise ValueError("--end must be later than --start")

    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        source = SQLiteSource(conn)
        engine = ScenarioEngine(conn, source, args.window_min, args.lookback_days)
        engine.initialize_schema([
            Path(args.schema),
            Path(args.scenario_schema),
        ])

        cur = floor_window(to_naive_utc(start), args.window_min)
        end_floor = floor_window(to_naive_utc(end), args.window_min)

        runs = 0
        while cur <= end_floor:
            engine.run_once(cur + timedelta(minutes=args.window_min))
            cur += timedelta(minutes=args.window_min)
            runs += 1

        print(json.dumps({"backfill_runs": runs}, indent=2))


def cmd_report(args: argparse.Namespace) -> None:
    since = parse_iso_datetime(args.since) if args.since else datetime.now(UTC) - timedelta(hours=24)
    since_s = to_naive_utc(since).isoformat(timespec="seconds")

    with sqlite3.connect(args.db) as conn:
        events = pd.read_sql_query(
            """
            SELECT event_id, scenario_id, severity, entity_type, entity_id, ts, observed, expected, score
            FROM anomaly_events
            WHERE ts >= ?
            ORDER BY ts DESC, severity DESC
            LIMIT ?
            """,
            conn,
            params=[since_s, args.limit],
        )
        kpis = pd.read_sql_query(
            """
            SELECT ts_window_start, ts_window_end, cpu_at_risk, io_at_risk, incidents_predicted, cost_risk_index
            FROM impact_kpis
            WHERE ts_window_end >= ?
            ORDER BY ts_window_end DESC
            LIMIT ?
            """,
            conn,
            params=[since_s, args.limit],
        )

    print("\n=== Recent Anomaly Events ===")
    if events.empty:
        print("No events")
    else:
        print(events.to_string(index=False))

    print("\n=== Recent Impact KPIs ===")
    if kpis.empty:
        print("No KPI rows")
    else:
        print(kpis.to_string(index=False))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="data/telemetry.db", help="SQLite database path")
    parser.add_argument("--schema", default="schema/minimal_telemetry.sql", help="Base telemetry schema path")
    parser.add_argument("--scenario-schema", default="schema/scenario_tables.sql", help="Scenario schema path")
    parser.add_argument("--window-min", type=int, default=15, help="Window size in minutes")
    parser.add_argument("--lookback-days", type=int, default=30, help="Lookback used for model fit")

    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="Run one batch scoring cycle")
    p_run.add_argument("--at", help="Optional run timestamp (ISO-8601 UTC)")
    p_run.set_defaults(func=cmd_run)

    p_back = sub.add_parser("backfill", help="Backfill windows between start and end")
    p_back.add_argument("--start", required=True, help="Start timestamp (ISO-8601 UTC)")
    p_back.add_argument("--end", required=True, help="End timestamp (ISO-8601 UTC)")
    p_back.set_defaults(func=cmd_backfill)

    p_report = sub.add_parser("report", help="Show anomaly + KPI report")
    p_report.add_argument("--since", help="Filter report since timestamp (ISO-8601 UTC)")
    p_report.add_argument("--limit", type=int, default=50, help="Max rows per section")
    p_report.set_defaults(func=cmd_report)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
