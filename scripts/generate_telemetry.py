#!/usr/bin/env python3
"""Generate realistic-ish telemetry data for Teradata workload demos."""

from __future__ import annotations

import argparse
import random
import sqlite3
from datetime import datetime, timedelta

USERS = ["etl_user", "bi_user", "adhoc_user", "admin_user", "finance_user"]
WORKLOADS = ["ETL", "BI", "Adhoc", "Maintenance"]
TABLES = ["sales_fact", "customer_dim", "inventory_fact", "orders_fact", "txn_log"]


def weighted_workload(hour: int) -> str:
    # ETL window at night, BI in business hours, adhoc otherwise.
    if 1 <= hour <= 5:
        return random.choices(WORKLOADS, weights=[70, 10, 15, 5])[0]
    if 8 <= hour <= 18:
        return random.choices(WORKLOADS, weights=[15, 60, 20, 5])[0]
    return random.choices(WORKLOADS, weights=[20, 20, 55, 5])[0]


def init_db(conn: sqlite3.Connection, schema_path: str) -> None:
    with open(schema_path, "r", encoding="utf-8") as f:
        conn.executescript(f.read())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="data/telemetry.db", help="Output sqlite db path")
    parser.add_argument("--schema", default="schema/minimal_telemetry.sql", help="Schema SQL path")
    parser.add_argument("--days", type=int, default=3, help="Days of history")
    parser.add_argument("--queries", type=int, default=50000, help="Rows for dbc_dbqlogtbl")
    parser.add_argument("--nodes", type=int, default=4, help="Node count for resusage_spma")
    parser.add_argument("--amps", type=int, default=8, help="AMP count for dbql_step_tbl")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    args = parser.parse_args()

    random.seed(args.seed)
    end = datetime.utcnow()
    start = end - timedelta(days=args.days)

    conn = sqlite3.connect(args.db)
    # Use a dashboard-friendly journal mode so read-only consumers like Grafana
    # can query the database reliably across host/container boundaries.
    conn.execute("PRAGMA journal_mode = DELETE")
    init_db(conn, args.schema)

    # table metadata
    for t in TABLES:
        conn.execute(
            "INSERT INTO tables_v(database_name, table_name, row_count, last_collect_stats) VALUES (?, ?, ?, ?)",
            (
                "analytics",
                t,
                random.randint(100_000, 50_000_000),
                (end - timedelta(hours=random.randint(1, 72))).isoformat(timespec="seconds"),
            ),
        )

    # session telemetry
    session_ids = list(range(1, 5001))
    for sid in session_ids:
        logon = start + timedelta(minutes=random.randint(0, args.days * 24 * 60))
        conn.execute(
            "INSERT INTO session_info(session_id, user_name, logon_time, active_queries) VALUES (?, ?, ?, ?)",
            (sid, random.choice(USERS), logon.isoformat(timespec="seconds"), random.randint(0, 6)),
        )

    # resource usage every minute with periodic spikes
    minutes = int((end - start).total_seconds() // 60)
    for m in range(minutes):
        ts = start + timedelta(minutes=m)
        for node in range(1, args.nodes + 1):
            base_cpu = random.uniform(20, 60)
            base_io = random.uniform(100, 900)
            if ts.hour in (2, 3, 4):
                base_cpu += random.uniform(10, 25)
                base_io += random.uniform(250, 500)
            if random.random() < 0.01:
                base_cpu += random.uniform(20, 35)
                base_io += random.uniform(500, 1200)
            conn.execute(
                "INSERT INTO resusage_spma(sample_time, node_id, cpu_percent, disk_io) VALUES (?, ?, ?, ?)",
                (ts.isoformat(timespec="seconds"), node, min(base_cpu, 100.0), base_io),
            )

    # query + workload + step telemetry
    for qid in range(1, args.queries + 1):
        q_start = start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))
        workload = weighted_workload(q_start.hour)
        elapsed = max(1, int(random.lognormvariate(4.3, 0.8)))
        amp_cpu = int(elapsed * random.uniform(0.7, 3.5))
        io_count = int(elapsed * random.uniform(3, 25))
        err = None if random.random() > 0.03 else random.choice([2631, 2646, 3110, 3807])

        # occasional expensive queries
        if random.random() < 0.02:
            elapsed *= random.randint(8, 20)
            amp_cpu *= random.randint(10, 30)
            io_count *= random.randint(10, 25)

        q_end = q_start + timedelta(seconds=elapsed)
        user = random.choice(USERS)

        conn.execute(
            "INSERT INTO dbc_dbqlogtbl(query_id, user_name, start_time, end_time, elapsed_time, amp_cpu_time, io_count, error_code) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (qid, user, q_start.isoformat(timespec="seconds"), q_end.isoformat(timespec="seconds"), elapsed, amp_cpu, io_count, err),
        )
        conn.execute(
            "INSERT INTO workload_map(query_id, workload_name) VALUES (?, ?)",
            (qid, workload),
        )

        steps = random.randint(2, 8)
        skew_step = random.randint(1, steps) if random.random() < 0.05 else None
        for step in range(1, steps + 1):
            baseline = max(1, int(amp_cpu / (steps * args.amps)))
            for amp in range(1, args.amps + 1):
                cpu_time = random.randint(max(1, baseline // 2), baseline * 2)
                if skew_step == step and amp == random.randint(1, args.amps):
                    cpu_time *= random.randint(10, 30)
                conn.execute(
                    "INSERT INTO dbql_step_tbl(query_id, step_id, amp_id, cpu_time) VALUES (?, ?, ?, ?)",
                    (qid, step, amp, cpu_time),
                )

    # lock events and lock chains
    for _ in range(500):
        blocker = random.choice(session_ids)
        blocked = random.choice([sid for sid in session_ids if sid != blocker])
        ts = start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))
        conn.execute(
            "INSERT INTO lock_info(session_id, blocked_session, locked_table, lock_type, event_time) VALUES (?, ?, ?, ?, ?)",
            (blocker, blocked, random.choice(TABLES), random.choice(["READ", "WRITE"]), ts.isoformat(timespec="seconds")),
        )

    conn.commit()
    conn.close()
    print(f"Generated telemetry in {args.db}")


if __name__ == "__main__":
    main()
