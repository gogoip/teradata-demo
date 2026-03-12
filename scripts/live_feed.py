#!/usr/bin/env python3
"""Continuously append synthetic telemetry and refresh scenario outputs."""

from __future__ import annotations

import argparse
import json
import random
import sqlite3
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from generate_telemetry import TABLES, USERS, init_db, weighted_workload
from scenario_engine import SQLiteScenarioStore, SQLiteSource, ScenarioEngine


def utc_minute_now() -> datetime:
    return datetime.now(UTC).replace(second=0, microsecond=0, tzinfo=None)


def parse_iso_naive(raw: str) -> datetime:
    value = str(raw).replace("Z", "+00:00")
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is not None:
        return dt.astimezone(UTC).replace(tzinfo=None)
    return dt


@dataclass
class ProducerState:
    next_query_id: int
    session_ids: list[int]
    next_minute: datetime


class LiveTelemetryProducer:
    def __init__(self, conn: sqlite3.Connection, args: argparse.Namespace):
        self.conn = conn
        self.args = args
        self.rng = random.Random(args.seed)
        self.state = self._initialize_state()

    def _initialize_state(self) -> ProducerState:
        init_db(self.conn, self.args.schema)
        with open(self.args.scenario_schema, "r", encoding="utf-8") as f:
            self.conn.executescript(f.read())
        self.conn.execute("PRAGMA journal_mode = DELETE")

        self._ensure_table_metadata()
        session_ids = self._ensure_sessions()
        next_query_id = int(self.conn.execute("SELECT COALESCE(MAX(query_id), 0) + 1 FROM dbc_dbqlogtbl").fetchone()[0])

        latest_ts = self._latest_telemetry_ts()
        if latest_ts is None:
            next_minute = utc_minute_now() - timedelta(hours=self.args.bootstrap_hours)
        else:
            next_minute = latest_ts + timedelta(minutes=1)
            if self.args.jump_to_now and next_minute < utc_minute_now() - timedelta(minutes=5):
                next_minute = utc_minute_now()

        self.conn.commit()
        return ProducerState(next_query_id=next_query_id, session_ids=session_ids, next_minute=next_minute)

    def _latest_telemetry_ts(self) -> datetime | None:
        rows = [
            self.conn.execute("SELECT MAX(sample_time) FROM resusage_spma").fetchone()[0],
            self.conn.execute("SELECT MAX(start_time) FROM dbc_dbqlogtbl").fetchone()[0],
        ]
        parsed = [parse_iso_naive(value) for value in rows if value]
        return max(parsed) if parsed else None

    def _ensure_table_metadata(self) -> None:
        count = int(self.conn.execute("SELECT COUNT(*) FROM tables_v").fetchone()[0])
        if count:
            return
        now = utc_minute_now()
        for table_name in TABLES:
            self.conn.execute(
                """
                INSERT INTO tables_v(database_name, table_name, row_count, last_collect_stats)
                VALUES (?, ?, ?, ?)
                """,
                (
                    "analytics",
                    table_name,
                    self.rng.randint(100_000, 50_000_000),
                    (now - timedelta(hours=self.rng.randint(1, 72))).isoformat(timespec="seconds"),
                ),
            )

    def _ensure_sessions(self) -> list[int]:
        rows = self.conn.execute("SELECT session_id FROM session_info ORDER BY session_id").fetchall()
        if rows:
            return [int(row[0]) for row in rows]

        now = utc_minute_now()
        session_ids = list(range(1, self.args.session_count + 1))
        for session_id in session_ids:
            logon = now - timedelta(minutes=self.rng.randint(0, 7 * 24 * 60))
            self.conn.execute(
                """
                INSERT INTO session_info(session_id, user_name, logon_time, active_queries)
                VALUES (?, ?, ?, ?)
                """,
                (
                    session_id,
                    self.rng.choice(USERS),
                    logon.isoformat(timespec="seconds"),
                    self.rng.randint(0, 6),
                ),
            )
        return session_ids

    def append_minutes(self, minute_count: int) -> datetime:
        current_minute = self.state.next_minute
        last_minute = current_minute
        for _ in range(minute_count):
            self._append_minute(current_minute)
            last_minute = current_minute
            current_minute += timedelta(minutes=1)
        self.state.next_minute = current_minute
        self.conn.commit()
        return last_minute

    def _append_minute(self, ts: datetime) -> None:
        for node_id in range(1, self.args.nodes + 1):
            base_cpu = self.rng.uniform(20, 60)
            base_io = self.rng.uniform(100, 900)
            if ts.hour in (2, 3, 4):
                base_cpu += self.rng.uniform(10, 25)
                base_io += self.rng.uniform(250, 500)
            if self.rng.random() < 0.02:
                base_cpu += self.rng.uniform(20, 35)
                base_io += self.rng.uniform(500, 1200)
            self.conn.execute(
                """
                INSERT INTO resusage_spma(sample_time, node_id, cpu_percent, disk_io)
                VALUES (?, ?, ?, ?)
                """,
                (ts.isoformat(timespec="seconds"), node_id, min(base_cpu, 100.0), base_io),
            )

        query_count = max(1, int(round(self.rng.gauss(self.args.queries_per_minute, max(1.0, self.args.queries_per_minute * 0.2)))))
        for _ in range(query_count):
            self._append_query(ts)

        if self.rng.random() < 0.1:
            self._append_lock_event(ts)
        if self.rng.random() < 0.08:
            self._grow_table(ts)

    def _append_query(self, minute_ts: datetime) -> None:
        second_offset = self.rng.randint(0, 59)
        q_start = minute_ts + timedelta(seconds=second_offset)
        workload = weighted_workload(q_start.hour)
        elapsed = max(1, int(self.rng.lognormvariate(4.1, 0.7)))
        amp_cpu = int(elapsed * self.rng.uniform(0.7, 3.5))
        io_count = int(elapsed * self.rng.uniform(3, 25))
        if self.rng.random() < 0.03:
            elapsed *= self.rng.randint(6, 18)
            amp_cpu *= self.rng.randint(8, 24)
            io_count *= self.rng.randint(10, 22)
        q_end = q_start + timedelta(seconds=elapsed)
        query_id = self.state.next_query_id
        self.state.next_query_id += 1

        self.conn.execute(
            """
            INSERT INTO dbc_dbqlogtbl(query_id, user_name, start_time, end_time, elapsed_time, amp_cpu_time, io_count, error_code)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                query_id,
                self.rng.choice(USERS),
                q_start.isoformat(timespec="seconds"),
                q_end.isoformat(timespec="seconds"),
                elapsed,
                amp_cpu,
                io_count,
                None if self.rng.random() > 0.03 else self.rng.choice([2631, 2646, 3110, 3807]),
            ),
        )
        self.conn.execute(
            "INSERT INTO workload_map(query_id, workload_name) VALUES (?, ?)",
            (query_id, workload),
        )

        steps = self.rng.randint(2, 8)
        skew_step = self.rng.randint(1, steps) if self.rng.random() < 0.05 else None
        skew_amp = self.rng.randint(1, self.args.amps) if skew_step is not None else None
        for step_id in range(1, steps + 1):
            baseline = max(1, int(amp_cpu / max(1, steps * self.args.amps)))
            for amp_id in range(1, self.args.amps + 1):
                cpu_time = self.rng.randint(max(1, baseline // 2), max(2, baseline * 2))
                if skew_step == step_id and skew_amp == amp_id:
                    cpu_time *= self.rng.randint(10, 30)
                self.conn.execute(
                    """
                    INSERT INTO dbql_step_tbl(query_id, step_id, amp_id, cpu_time)
                    VALUES (?, ?, ?, ?)
                    """,
                    (query_id, step_id, amp_id, cpu_time),
                )

    def _append_lock_event(self, ts: datetime) -> None:
        blocker = self.rng.choice(self.state.session_ids)
        blocked = self.rng.choice([sid for sid in self.state.session_ids if sid != blocker])
        self.conn.execute(
            """
            INSERT INTO lock_info(session_id, blocked_session, locked_table, lock_type, event_time)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                blocker,
                blocked,
                self.rng.choice(TABLES),
                self.rng.choice(["READ", "WRITE"]),
                ts.isoformat(timespec="seconds"),
            ),
        )

    def _grow_table(self, ts: datetime) -> None:
        table_name = self.rng.choice(TABLES)
        growth = self.rng.randint(5_000, 250_000)
        self.conn.execute(
            """
            UPDATE tables_v
            SET row_count = row_count + ?, last_collect_stats = ?
            WHERE database_name = 'analytics' AND table_name = ?
            """,
            (growth, ts.isoformat(timespec="seconds"), table_name),
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="data/telemetry.db", help="SQLite database path")
    parser.add_argument("--schema", default="schema/minimal_telemetry.sql", help="Base telemetry schema path")
    parser.add_argument("--scenario-schema", default="schema/scenario_tables.sql", help="Scenario schema path")
    parser.add_argument("--window-min", type=int, default=15, help="Scenario window size in minutes")
    parser.add_argument("--lookback-days", type=int, default=30, help="Lookback used for model fit")
    parser.add_argument("--interval-sec", type=float, default=60.0, help="Sleep interval between feed cycles")
    parser.add_argument("--minutes-per-tick", type=int, default=1, help="Synthetic minutes to append per cycle")
    parser.add_argument("--queries-per-minute", type=int, default=12, help="Average query rows to append per minute")
    parser.add_argument("--nodes", type=int, default=4, help="Node count for resource telemetry")
    parser.add_argument("--amps", type=int, default=8, help="AMP count for step telemetry")
    parser.add_argument("--session-count", type=int, default=500, help="Seed session rows if table is empty")
    parser.add_argument("--bootstrap-hours", type=int, default=6, help="History to seed when DB is empty")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--iterations", type=int, help="Optional number of feed cycles before exit")
    parser.add_argument("--jump-to-now", action=argparse.BooleanOptionalAction, default=True, help="Skip stale gaps and resume from current UTC minute")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        producer = LiveTelemetryProducer(conn, args)
        engine = ScenarioEngine(conn, SQLiteSource(conn), SQLiteScenarioStore(conn), args.window_min, args.lookback_days)
        engine.initialize_schema([Path(args.schema), Path(args.scenario_schema)])

        iteration = 0
        while True:
            last_minute = producer.append_minutes(args.minutes_per_tick)
            run_at = last_minute + timedelta(minutes=args.window_min)
            summary = engine.run_once(run_at)
            print(
                json.dumps(
                    {
                        "iteration": iteration + 1,
                        "last_minute": last_minute.isoformat(timespec="seconds"),
                        "window_end": summary["window_end"],
                        "metric_rows": summary["metric_rows"],
                        "events": summary["events"],
                    },
                    indent=2,
                ),
                flush=True,
            )
            iteration += 1
            if args.iterations is not None and iteration >= args.iterations:
                break
            time.sleep(max(0.0, args.interval_sec))


if __name__ == "__main__":
    main()
