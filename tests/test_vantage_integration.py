import os
import sqlite3
import sys
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from scenario_engine import SQLiteScenarioStore, SQLiteSource, TeradataScenarioStore, TeradataSource, build_parser, build_source, build_store, connect_teradata  # noqa: E402


class VantageIntegrationTests(unittest.TestCase):
    def test_build_parser_accepts_teradata_source(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["--source", "teradata", "run"])
        self.assertEqual(args.source, "teradata")

    def test_build_source_uses_sqlite_by_default(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["run"])
        with sqlite3.connect(":memory:") as conn:
            source = build_source(args, conn)
            store = build_store(args, conn)
        self.assertIsInstance(source, SQLiteSource)
        self.assertIsInstance(store, SQLiteScenarioStore)

    def test_build_source_uses_teradata_overrides(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "--source",
                "teradata",
                "--sink",
                "teradata",
                "--td-dbql-query-table",
                "DBC.CustomQuery",
                "--td-dbql-step-table",
                "DBC.CustomStep",
                "--td-resusage-table",
                "DBC.CustomRes",
                "--td-tables-table",
                "DBC.CustomTables",
                "--td-metric-table",
                "demo.metric_timeseries",
                "--td-model-state-table",
                "demo.model_state",
                "--td-anomaly-table",
                "demo.anomaly_events",
                "--td-kpi-table",
                "demo.impact_kpis",
                "run",
            ]
        )
        fake_conn = object()
        with sqlite3.connect(":memory:") as sink_conn, mock.patch("scenario_engine.connect_teradata", return_value=fake_conn):
            source = build_source(args, sink_conn)
            store = build_store(args, sink_conn)
        self.assertIsInstance(source, TeradataSource)
        self.assertIsInstance(store, TeradataScenarioStore)
        self.assertEqual(source.dbql_query_table, "DBC.CustomQuery")
        self.assertEqual(source.dbql_step_table, "DBC.CustomStep")
        self.assertEqual(source.resusage_table, "DBC.CustomRes")
        self.assertEqual(source.table_stats_table, "DBC.CustomTables")
        self.assertIs(source.conn, fake_conn)
        self.assertEqual(store.metric_table, "demo.metric_timeseries")

    def test_connect_teradata_requires_env_vars(self) -> None:
        fake_driver = mock.Mock()
        with mock.patch.dict(os.environ, {}, clear=True), mock.patch.dict(sys.modules, {"teradatasql": fake_driver}):
            with self.assertRaisesRegex(RuntimeError, "Missing required Teradata environment variables"):
                connect_teradata()


if __name__ == "__main__":
    unittest.main()
