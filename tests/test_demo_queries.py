import sqlite3
import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from demo_queries import get_tcore_summary, get_tcore_workload_attribution  # noqa: E402
from scenario_engine import SQLiteSource, ScenarioEngine, floor_window  # noqa: E402


class DemoQueriesTests(unittest.TestCase):
    def setUp(self) -> None:
        data_dir = REPO_ROOT / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = data_dir / f"demo_queries_{uuid4().hex}.db"
        self._seed_db()

    def tearDown(self) -> None:
        if self.db_path.exists():
            try:
                self.db_path.unlink()
            except PermissionError:
                pass

    def _seed_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript((REPO_ROOT / "schema" / "minimal_telemetry.sql").read_text(encoding="utf-8"))
            conn.executescript((REPO_ROOT / "schema" / "scenario_tables.sql").read_text(encoding="utf-8"))

            base = datetime(2026, 3, 1, 0, 0, 0)
            qid = 1
            for i in range(4 * 24 * 4):
                ts = base + timedelta(minutes=15 * i)
                end = ts + timedelta(seconds=60)
                etl_cpu = 120 + (i % 12) * 6
                etl_io = 500 + (i % 9) * 40
                if i > 200:
                    etl_io += 250
                conn.execute(
                    """
                    INSERT INTO dbc_dbqlogtbl(query_id, user_name, start_time, end_time, elapsed_time, amp_cpu_time, io_count, error_code)
                    VALUES (?, 'etl_user', ?, ?, 60, ?, ?, NULL)
                    """,
                    (qid, ts.isoformat(timespec="seconds"), end.isoformat(timespec="seconds"), etl_cpu, etl_io),
                )
                conn.execute("INSERT INTO workload_map(query_id, workload_name) VALUES (?, 'ETL')", (qid,))
                for amp in range(1, 9):
                    cpu_time = 7 if amp > 1 else 40
                    conn.execute(
                        "INSERT INTO dbql_step_tbl(query_id, step_id, amp_id, cpu_time) VALUES (?, 1, ?, ?)",
                        (qid, amp, cpu_time),
                    )
                qid += 1

                bi_cpu = 80 + (i % 10) * 4
                bi_io = 260 + (i % 8) * 20
                conn.execute(
                    """
                    INSERT INTO dbc_dbqlogtbl(query_id, user_name, start_time, end_time, elapsed_time, amp_cpu_time, io_count, error_code)
                    VALUES (?, 'bi_user', ?, ?, 60, ?, ?, NULL)
                    """,
                    (qid, ts.isoformat(timespec="seconds"), end.isoformat(timespec="seconds"), bi_cpu, bi_io),
                )
                conn.execute("INSERT INTO workload_map(query_id, workload_name) VALUES (?, 'BI')", (qid,))
                for amp in range(1, 9):
                    conn.execute(
                        "INSERT INTO dbql_step_tbl(query_id, step_id, amp_id, cpu_time) VALUES (?, 2, ?, ?)",
                        (qid, amp, 5),
                    )
                qid += 1

                conn.execute(
                    "INSERT INTO resusage_spma(sample_time, node_id, cpu_percent, disk_io) VALUES (?, 1, ?, ?)",
                    (ts.isoformat(timespec="seconds"), 45 + (i % 10), 1200 + (i % 20) * 50),
                )

            engine = ScenarioEngine(conn, SQLiteSource(conn), window_min=15, lookback_days=30)
            run_at = floor_window(base + timedelta(minutes=15 * ((4 * 24 * 4) - 1)), 15) + timedelta(minutes=15)
            engine.run_once(run_at)
            conn.commit()

    def test_excess_consumption_is_non_negative(self) -> None:
        df = get_tcore_workload_attribution(self.db_path, datetime(2026, 3, 2))
        self.assertFalse(df.empty)
        self.assertTrue((df["cpu_excess"] >= 0).all())
        self.assertTrue((df["io_excess"] >= 0).all())
        self.assertTrue((df["estimated_tcore_risk"] >= 0).all())

    def test_reducible_percentage_is_bounded(self) -> None:
        summary = get_tcore_summary(self.db_path, datetime(2026, 3, 2), remediation="combined", workload_scope="all")
        self.assertGreaterEqual(summary["reducible_tcore_pct"], 0.0)
        self.assertLessEqual(summary["reducible_tcore_pct"], 100.0)

    def test_remediation_does_not_increase_adjusted_excess(self) -> None:
        summary = get_tcore_summary(self.db_path, datetime(2026, 3, 2), remediation="tune_high_io_etl", workload_scope="ETL")
        self.assertLessEqual(summary["adjusted_excess_tcore"], summary["excess_tcore"])


if __name__ == "__main__":
    unittest.main()
