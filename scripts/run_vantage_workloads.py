#!/usr/bin/env python3
"""Create a demo schema and submit repeatable workload patterns to Vantage."""

from __future__ import annotations

import argparse
import time

from teradata_utils import connect_teradata


DDL = [
    "CREATE DATABASE {schema} AS PERMANENT = 100000000",
    """
    CREATE MULTISET TABLE {schema}.orders_demo (
        order_id INTEGER,
        customer_id INTEGER,
        amount DECIMAL(18,2),
        order_ts TIMESTAMP(0)
    )
    PRIMARY INDEX (order_id)
    """,
    """
    CREATE MULTISET TABLE {schema}.customer_demo (
        customer_id INTEGER,
        segment VARCHAR(32),
        region VARCHAR(32)
    )
    PRIMARY INDEX (customer_id)
    """,
]


def _execute(cur, sql: str) -> None:
    try:
        cur.execute(sql)
    except Exception:
        pass


def prepare_schema(conn, schema: str) -> None:
    cur = conn.cursor()
    for stmt in DDL:
        _execute(cur, stmt.format(schema=schema))
    _execute(cur, f"DELETE FROM {schema}.orders_demo ALL")
    _execute(cur, f"DELETE FROM {schema}.customer_demo ALL")
    cur.execute(
        f"""
        INSERT INTO {schema}.customer_demo
        SELECT
            ROW_NUMBER() OVER (ORDER BY calendar_date),
            CASE MOD(ROW_NUMBER() OVER (ORDER BY calendar_date), 3)
                WHEN 0 THEN 'ENT'
                WHEN 1 THEN 'SMB'
                ELSE 'CONSUMER'
            END,
            CASE MOD(ROW_NUMBER() OVER (ORDER BY calendar_date), 4)
                WHEN 0 THEN 'NA'
                WHEN 1 THEN 'EMEA'
                WHEN 2 THEN 'APAC'
                ELSE 'LATAM'
            END
        FROM sys_calendar.calendar
        QUALIFY ROW_NUMBER() OVER (ORDER BY calendar_date) <= 1000
        """
    )
    cur.execute(
        f"""
        INSERT INTO {schema}.orders_demo
        SELECT
            ROW_NUMBER() OVER (ORDER BY calendar_date, c.customer_id),
            c.customer_id,
            CAST((MOD(c.customer_id, 97) * 17 + 100) AS DECIMAL(18,2)),
            CAST(calendar_date AS TIMESTAMP(0))
        FROM sys_calendar.calendar
        CROSS JOIN (
            SELECT customer_id FROM {schema}.customer_demo
            QUALIFY ROW_NUMBER() OVER (ORDER BY customer_id) <= 200
        ) c
        QUALIFY ROW_NUMBER() OVER (ORDER BY calendar_date, c.customer_id) <= 50000
        """
    )
    conn.commit()


def run_workloads(conn, schema: str, loops: int, sleep_seconds: int) -> None:
    cur = conn.cursor()
    statements = [
        (
            "etl",
            f"""
            INSERT INTO {schema}.orders_demo
            SELECT order_id + 1000000, customer_id, amount * 1.02, CURRENT_TIMESTAMP(0)
            FROM {schema}.orders_demo
            QUALIFY ROW_NUMBER() OVER (ORDER BY order_id) <= 5000
            """,
        ),
        (
            "bi",
            f"""
            SELECT c.region, c.segment, COUNT(*), SUM(o.amount), AVG(o.amount)
            FROM {schema}.orders_demo o
            JOIN {schema}.customer_demo c ON c.customer_id = o.customer_id
            GROUP BY 1, 2
            """,
        ),
        (
            "ad_hoc",
            f"""
            SELECT TOP 250 o.order_id, o.customer_id, o.amount, c.region
            FROM {schema}.orders_demo o
            JOIN {schema}.customer_demo c ON c.customer_id = o.customer_id
            WHERE o.amount > 500
            ORDER BY o.amount DESC
            """,
        ),
    ]
    for _ in range(loops):
        for workload_name, sql in statements:
            cur.execute(sql)
            print(f"submitted workload={workload_name}")
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
    conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema", default="td_demo_workload", help="Demo schema/database name")
    parser.add_argument("--loops", type=int, default=3, help="How many passes of the workload set to run")
    parser.add_argument("--sleep-seconds", type=int, default=2, help="Delay between workload submissions")
    parser.add_argument("--prepare-only", action="store_true", help="Only create and seed the demo schema")
    args = parser.parse_args()

    conn = connect_teradata()
    try:
        prepare_schema(conn, args.schema)
        if not args.prepare_only:
            run_workloads(conn, args.schema, args.loops, args.sleep_seconds)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
