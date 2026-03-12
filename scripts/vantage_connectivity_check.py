#!/usr/bin/env python3
"""Read-only connectivity and telemetry discovery for a Teradata Vantage environment."""

from __future__ import annotations

import argparse
import json
import os
from typing import Any

import pandas as pd


DISCOVERY_CANDIDATES = {
    "dbql_query": [
        "DBC.DBQLogTbl",
        "DBC.DBQLogTblV",
        "DBC.QryLogV",
    ],
    "dbql_step": [
        "DBC.DBQLStepTbl",
        "DBC.DBQLStepTblVX",
    ],
    "resusage": [
        "DBC.ResUsageSpma",
        "DBC.ResUsageSPMA",
    ],
}


def connect():
    try:
        import teradatasql  # type: ignore
    except ImportError as exc:
        raise RuntimeError("teradatasql is required. Install dependencies from requirements.txt.") from exc

    host = os.getenv("TD_HOST")
    username = os.getenv("TD_USERNAME")
    password = os.getenv("TD_PASSWORD")
    missing = [name for name, value in [("TD_HOST", host), ("TD_USERNAME", username), ("TD_PASSWORD", password)] if not value]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    return teradatasql.connect(host=host, user=username, password=password)


def query_frame(conn: Any, sql: str) -> pd.DataFrame:
    return pd.read_sql(sql, conn)


def probe_object(conn: Any, object_name: str, limit: int) -> dict[str, Any]:
    try:
        sample = query_frame(conn, f"SELECT * FROM {object_name} SAMPLE {limit}")
        return {
            "object_name": object_name,
            "readable": True,
            "columns": sample.columns.tolist(),
            "sample_rows": sample.head(limit).to_dict("records"),
        }
    except Exception as exc:  # pragma: no cover - depends on live environment
        return {
            "object_name": object_name,
            "readable": False,
            "error": str(exc),
        }


def discover_family(conn: Any, candidates: list[str], limit: int) -> list[dict[str, Any]]:
    return [probe_object(conn, object_name, limit) for object_name in candidates]


def identity_snapshot(conn: Any) -> dict[str, Any]:
    session_df = query_frame(
        conn,
        """
        SELECT
            SESSION AS session_no,
            USER AS user_name,
            CAST(CURRENT_TIMESTAMP(0) AS TIMESTAMP(0)) AS current_ts
        """,
    )
    version = None
    try:
        version_df = query_frame(conn, "SELECT InfoData FROM DBC.DBCInfoV WHERE InfoKey = 'VERSION'")
        if not version_df.empty:
            version = version_df.iloc[0, 0]
    except Exception as exc:  # pragma: no cover - depends on live environment
        version = f"unavailable: {exc}"
    return {
        "session": session_df.iloc[0].to_dict() if not session_df.empty else {},
        "version": version,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample-limit", type=int, default=5, help="Rows to capture from each readable object")
    parser.add_argument("--output", choices=["json", "pretty"], default="pretty", help="Output format")
    args = parser.parse_args()

    conn = connect()
    try:
        payload = {
            "identity": identity_snapshot(conn),
            "discovery": {
                family: discover_family(conn, candidates, args.sample_limit)
                for family, candidates in DISCOVERY_CANDIDATES.items()
            },
        }
    finally:
        conn.close()

    if args.output == "json":
        print(json.dumps(payload, indent=2, default=str))
        return

    print("=== Connection ===")
    print(json.dumps(payload["identity"], indent=2, default=str))
    print("\n=== Telemetry Discovery ===")
    for family, results in payload["discovery"].items():
        print(f"\n[{family}]")
        for result in results:
            print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
