"""Shared helpers for Teradata-backed demo flows."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import pandas as pd


def connect_teradata(
    *,
    host: str | None = None,
    username: str | None = None,
    password: str | None = None,
) -> Any:
    try:
        import teradatasql  # type: ignore
    except ImportError as exc:  # pragma: no cover - environment-specific
        raise RuntimeError("teradatasql is required for Teradata connectivity.") from exc

    host = host or os.getenv("TD_HOST")
    username = username or os.getenv("TD_USERNAME")
    password = password or os.getenv("TD_PASSWORD")
    missing = [name for name, value in [("TD_HOST", host), ("TD_USERNAME", username), ("TD_PASSWORD", password)] if not value]
    if missing:
        raise RuntimeError(f"Missing required Teradata environment variables: {', '.join(missing)}")
    return teradatasql.connect(host=host, user=username, password=password)


@dataclass(frozen=True)
class DemoConnectionConfig:
    backend: str = "sqlite"
    db_path: str = "data/telemetry.db"
    td_dbql_query_table: str = "DBC.DBQLogTbl"
    td_dbql_step_table: str = "DBC.DBQLStepTbl"
    td_resusage_table: str = "DBC.ResUsageSpma"
    td_tables_table: str = "DBC.TablesV"
    td_metric_table: str = "metric_timeseries"
    td_model_state_table: str = "model_state"
    td_anomaly_table: str = "anomaly_events"
    td_kpi_table: str = "impact_kpis"
    td_demo_schema: str = ""
    td_demo_user: str = ""

    @property
    def demo_schema_upper(self) -> str:
        return self.td_demo_schema.strip().upper()

    @property
    def demo_user_upper(self) -> str:
        return self.td_demo_user.strip().upper()


def workload_label_from_row(row: dict[str, Any] | pd.Series, demo_schema: str = "", demo_user: str = "") -> str:
    query_band = str((row.get("query_band") if isinstance(row, dict) else row.get("query_band", "")) or "").upper()
    user_name = str((row.get("user_name") if isinstance(row, dict) else row.get("user_name", "")) or "").upper()
    sql_text = str((row.get("sql_text") if isinstance(row, dict) else row.get("sql_text", "")) or "").upper()
    default_db = str((row.get("default_database") if isinstance(row, dict) else row.get("default_database", "")) or "").upper()
    statement_type = str((row.get("statement_type") if isinstance(row, dict) else row.get("statement_type", "")) or "").upper()
    schema = demo_schema.strip().upper()
    demo_user = demo_user.strip().upper()

    hints = " ".join(x for x in [query_band, sql_text] if x)
    if "WORKLOAD=ETL" in hints or "MERGE" in hints or "INSERT" in hints or "CREATE TABLE" in hints:
        return "ETL"
    if "WORKLOAD=BI" in hints or statement_type in {"SEL", "SELECT"}:
        return "BI"
    if "WORKLOAD=ADHOC" in hints:
        return "AD_HOC"
    if schema and schema in default_db:
        return "DEMO"
    if demo_user and user_name == demo_user:
        return "DEMO"
    return "UNKNOWN"


def workload_filter_sql(config: DemoConnectionConfig, query_alias: str = "q") -> str:
    clauses: list[str] = []
    if config.demo_user_upper:
        clauses.append(f"UPPER({query_alias}.UserName) = '{config.demo_user_upper}'")
    if config.demo_schema_upper:
        clauses.append(
            "("
            f"UPPER(COALESCE({query_alias}.DefaultDatabase, '')) = '{config.demo_schema_upper}' "
            f"OR UPPER(COALESCE({query_alias}.QueryText, '')) LIKE '%{config.demo_schema_upper}%'"
            ")"
        )
    if not clauses:
        return ""
    return " AND (" + " OR ".join(clauses) + ")"
