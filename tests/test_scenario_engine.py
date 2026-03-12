import sqlite3
import sys
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from scenario_engine import SQLiteScenarioStore, SQLiteSource, ScenarioEngine, floor_window  # noqa: E402


class ScenarioEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        data_dir = REPO_ROOT / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = data_dir / f"test_{uuid4().hex}.db"
        self._seed_test_db()

    def tearDown(self) -> None:
        if self.db_path.exists():
            try:
                self.db_path.unlink()
            except PermissionError:
                pass

    def _build_engine(self, conn: sqlite3.Connection) -> ScenarioEngine:
        engine = ScenarioEngine(conn, SQLiteSource(conn), SQLiteScenarioStore(conn), window_min=15, lookback_days=30)
        engine.initialize_schema(
            [
                REPO_ROOT / "schema" / "minimal_telemetry.sql",
                REPO_ROOT / "schema" / "scenario_tables.sql",
            ]
        )
        return engine

    def _seed_test_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            minimal_sql = (REPO_ROOT / "schema" / "minimal_telemetry.sql").read_text(encoding="utf-8")
            conn.executescript(minimal_sql)

            base = datetime(2026, 3, 1, 0, 0, 0)
            qid = 1
            for i in range(10 * 24 * 4):
                ts = base + timedelta(minutes=15 * i)
                end = ts + timedelta(seconds=60)

                amp_cpu_1 = 80 + (i % 10) * 2
                io_1 = 220 + (i % 15) * 3
                conn.execute(
                    """
                    INSERT INTO dbc_dbqlogtbl(query_id, user_name, start_time, end_time, elapsed_time, amp_cpu_time, io_count, error_code)
                    VALUES (?, 'etl_user', ?, ?, 60, ?, ?, NULL)
                    """,
                    (qid, ts.isoformat(timespec="seconds"), end.isoformat(timespec="seconds"), amp_cpu_1, io_1),
                )
                conn.execute("INSERT INTO workload_map(query_id, workload_name) VALUES (?, 'ETL')", (qid,))
                conn.executemany(
                    "INSERT INTO dbql_step_tbl(query_id, step_id, amp_id, cpu_time) VALUES (?, 1, ?, ?)",
                    [(qid, amp, 10 if amp == 1 else 4) for amp in range(1, 9)],
                )
                qid += 1

                amp_cpu_2 = 60 + (i % 7) * 2
                io_2 = 180 + (i % 11) * 4
                conn.execute(
                    """
                    INSERT INTO dbc_dbqlogtbl(query_id, user_name, start_time, end_time, elapsed_time, amp_cpu_time, io_count, error_code)
                    VALUES (?, 'bi_user', ?, ?, 60, ?, ?, NULL)
                    """,
                    (qid, ts.isoformat(timespec="seconds"), end.isoformat(timespec="seconds"), amp_cpu_2, io_2),
                )
                conn.execute("INSERT INTO workload_map(query_id, workload_name) VALUES (?, 'BI')", (qid,))
                conn.executemany(
                    "INSERT INTO dbql_step_tbl(query_id, step_id, amp_id, cpu_time) VALUES (?, 2, ?, ?)",
                    [(qid, amp, 9 if amp == 1 else 4) for amp in range(1, 9)],
                )
                qid += 1

                conn.execute(
                    "INSERT INTO resusage_spma(sample_time, node_id, cpu_percent, disk_io) VALUES (?, 1, ?, ?)",
                    (ts.isoformat(timespec="seconds"), 45 + (i % 20), 1000 + (i % 30) * 30),
                )

            for table_name, rows in [
                ("sales_fact", 1_000_000),
                ("customer_dim", 200_000),
                ("inventory_fact", 300_000),
            ]:
                conn.execute(
                    """
                    INSERT INTO tables_v(database_name, table_name, row_count, last_collect_stats)
                    VALUES ('analytics', ?, ?, ?)
                    """,
                    (table_name, rows, base.isoformat(timespec="seconds")),
                )
            conn.commit()

    def _base_run_at(self, conn: sqlite3.Connection) -> datetime:
        max_start = conn.execute("SELECT MAX(start_time) FROM dbc_dbqlogtbl").fetchone()[0]
        max_dt = datetime.fromisoformat(max_start)
        return floor_window(max_dt.replace(tzinfo=UTC), 15) + timedelta(minutes=15)

    def test_run_populates_outputs(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            engine = self._build_engine(conn)
            run_at = self._base_run_at(conn)
            summary = engine.run_once(run_at)

            self.assertGreater(summary["metric_rows"], 0)
            self.assertGreater(summary["events"], 0)
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM impact_kpis").fetchone()[0],
                1,
            )
            self.assertGreater(
                conn.execute("SELECT COUNT(*) FROM model_state").fetchone()[0],
                0,
            )

    def test_perm_growth_injection_triggers_s6(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            engine = self._build_engine(conn)
            run_at = self._base_run_at(conn)

            for i in range(6):
                engine.run_once(run_at + timedelta(minutes=15 * i))

            conn.execute(
                "UPDATE tables_v SET row_count = row_count * 50 WHERE table_name = 'sales_fact'"
            )
            conn.commit()
            target_run = run_at + timedelta(minutes=15 * 6)
            engine.run_once(target_run)
            target_ts = floor_window(target_run, 15).isoformat(timespec="seconds")

            count = conn.execute(
                """
                SELECT COUNT(*) FROM anomaly_events
                WHERE scenario_id = 'S6' AND ts = ? AND severity != 'healthy'
                """,
                (target_ts,),
            ).fetchone()[0]
            self.assertGreaterEqual(count, 1)

    def test_skew_injection_triggers_s7(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            engine = self._build_engine(conn)
            run_at = self._base_run_at(conn)
            base_bucket = floor_window(run_at, 15) - timedelta(minutes=15)

            start_qid = conn.execute("SELECT COALESCE(MAX(query_id), 0) + 1 FROM dbc_dbqlogtbl").fetchone()[0]

            def add_skew_query(qid: int, ts: datetime, high: int) -> None:
                ts_s = ts.isoformat(timespec="seconds")
                end_s = (ts + timedelta(seconds=60)).isoformat(timespec="seconds")
                conn.execute(
                    """
                    INSERT INTO dbc_dbqlogtbl(query_id, user_name, start_time, end_time, elapsed_time, amp_cpu_time, io_count, error_code)
                    VALUES (?, 'etl_user', ?, ?, 60, 500, 1000, NULL)
                    """,
                    (qid, ts_s, end_s),
                )
                conn.execute(
                    "INSERT INTO workload_map(query_id, workload_name) VALUES (?, 'ETL')",
                    (qid,),
                )
                conn.executemany(
                    "INSERT INTO dbql_step_tbl(query_id, step_id, amp_id, cpu_time) VALUES (?, 99, ?, ?)",
                    [(qid, amp, high if amp == 1 else 3) for amp in range(1, 9)],
                )

            add_skew_query(start_qid, base_bucket - timedelta(minutes=30), 60)
            add_skew_query(start_qid + 1, base_bucket - timedelta(minutes=15), 75)
            add_skew_query(start_qid + 2, base_bucket, 95)
            conn.commit()

            engine.run_once(run_at)
            target_ts = floor_window(run_at, 15).isoformat(timespec="seconds")
            count = conn.execute(
                """
                SELECT COUNT(*) FROM anomaly_events
                WHERE scenario_id = 'S7' AND ts = ? AND severity != 'healthy'
                """,
                (target_ts,),
            ).fetchone()[0]
            self.assertGreaterEqual(count, 1)


if __name__ == "__main__":
    unittest.main()
