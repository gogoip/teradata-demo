# Teradata Demo

This repository contains a minimal telemetry schema and synthetic data generator to reproduce Digna-style Teradata workload analysis use cases.

## Files

- `schema/minimal_telemetry.sql`: Minimal table set for query, resource, step, lock, session, metadata, and workload telemetry.
- `scripts/generate_telemetry.py`: Generates realistic telemetry patterns (CPU spikes, ETL windows, expensive queries, AMP skew, lock contention).

## Generate demo data

```bash
python3 scripts/generate_telemetry.py --db data/telemetry.db --queries 50000 --days 3
```

## Example analysis queries

```sql
-- CPU attribution over time
SELECT
  strftime('%Y-%m-%d %H:%M:00', start_time) AS ts,
  SUM(amp_cpu_time) AS cpu_usage
FROM dbc_dbqlogtbl
GROUP BY ts
ORDER BY ts;
```

```sql
-- top expensive queries
SELECT query_id, user_name, elapsed_time, amp_cpu_time, io_count
FROM dbc_dbqlogtbl
ORDER BY amp_cpu_time DESC
LIMIT 20;
```

```sql
-- AMP skew signal by query + step
SELECT query_id, step_id,
       MAX(cpu_time) * 1.0 / NULLIF(AVG(cpu_time), 0) AS skew_ratio
FROM dbql_step_tbl
GROUP BY query_id, step_id
HAVING skew_ratio > 5
ORDER BY skew_ratio DESC;
```
