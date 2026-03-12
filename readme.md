# Teradata Demo

This repository contains a minimal telemetry schema and synthetic data generator to reproduce Digna-style Teradata workload analysis use cases.

## Files

- `schema/minimal_telemetry.sql`: Minimal table set for query, resource, step, lock, session, metadata, and workload telemetry.
- `scripts/generate_telemetry.py`: Generates realistic telemetry patterns (CPU spikes, ETL windows, expensive queries, AMP skew, lock contention).

## Generate demo data

```bash
python3 scripts/generate_telemetry.py --db data/telemetry.db --queries 50000 --days 3
```

## Digna-style scenario engine

Install dependencies:

```bash
python -m pip install -r requirements.txt
```

Run one 15-minute batch:

```bash
python scripts/scenario_engine.py --db data/telemetry.db --window-min 15 --lookback-days 30 run
```

Backfill historical windows:

```bash
python scripts/scenario_engine.py --db data/telemetry.db backfill --start 2026-03-01T00:00:00Z --end 2026-03-02T00:00:00Z
```

View anomaly and KPI report:

```bash
python scripts/scenario_engine.py --db data/telemetry.db report --since 2026-03-01T00:00:00Z
```

Create a Windows scheduled task (every 15 minutes):

```powershell
schtasks /Create /SC MINUTE /MO 15 /TN "TeradataScenarioEngine" /TR "python C:\Users\pran.gogoi\Documents\pythonprj\teradata-demo\scripts\scenario_engine.py --db C:\Users\pran.gogoi\Documents\pythonprj\teradata-demo\data\telemetry.db --window-min 15 --lookback-days 30 run" /F
```

## Live dashboard (Grafana + SQLite)

The repo includes a pre-provisioned Grafana dashboard that reads:
- `impact_kpis`
- `anomaly_events`
- `metric_timeseries`

### Start dashboard

```bash
docker compose up -d
```

Open:
- `http://localhost:3000`
- login: `admin` / `admin`

### Stop dashboard

```bash
docker compose down
```

### Dashboard behavior

- Auto refresh: every `15s`
- Default time range: last `24h`
- Datasource: `Telemetry SQLite` (`frser-sqlite-datasource`)
- Dashboard auto-loaded from `grafana/dashboards/teradata-workload-live.json`

### Verify provisioning

1. In Grafana, go to `Connections -> Data sources`, confirm `Telemetry SQLite` is present.
2. Open `Dashboards` and confirm `Teradata Workload Live Dashboard` exists.
3. Run one scenario batch:

```bash
python scripts/scenario_engine.py --db data/telemetry.db --window-min 15 --lookback-days 30 run
```

4. Refresh dashboard and confirm KPI/anomaly panels update.

### Troubleshooting

- `No data in panels`: ensure `data/telemetry.db` exists and scenario engine has produced rows.
- `Datasource plugin missing`: restart with `docker compose down && docker compose up -d` to reinstall plugin.
- `Unable to open database file`: if the DB was created in WAL mode, convert it before starting Grafana:

```bash
python -c "import sqlite3; conn=sqlite3.connect('data/telemetry.db'); print(conn.execute('PRAGMA wal_checkpoint(FULL)').fetchall()); print(conn.execute('PRAGMA journal_mode=DELETE').fetchone()); conn.close()"
```

- `Database locked/read errors`: stop other processes writing aggressively to DB, rerun scenario batch, then refresh.
- `Stale dashboard`: check `Producer Staleness (minutes)` panel and verify scheduled task is running.

## Streamlit demo (S2-only IO outlier screen)

This demo focuses on a single scenario:
- `S2` Detecting IO Outliers Early

### Install and run

```bash
python -m pip install -r requirements.txt
streamlit run scripts/demo_app.py
```

Open:
- `http://localhost:8501`

### Prerequisites

Generate telemetry and scenario outputs first:

```bash
python scripts/generate_telemetry.py --db data/telemetry.db --queries 50000 --days 3
python scripts/scenario_engine.py --db data/telemetry.db --window-min 15 --lookback-days 30 run
```

### What the demo shows

- Top controls: `Dataset`, `Workload`, `Check (sum/avg)`, `Trend Line`.
- Main chart: IO line + dynamic rolling percentile bands.
- Outlier overlay: `S2` anomaly markers from `anomaly_events`.
- Right summary card: latest timestamp, latest IO, expected baseline (`p50`), deviation, outlier count.
- In-app theory: simple explanation of IO signal, band math, and S2 outlier logic.
- Worked example: one recent outlier with nearby raw telemetry samples.
- Raw telemetry tables:
  - `dbc_dbqlogtbl` query-level samples
  - `resusage_spma` system samples
  - workload join slice (`dbc_dbqlogtbl` + `workload_map`)

### Band semantics

- Green zone: IO up to rolling `p90` (expected range).
- Yellow zone: between rolling `p90` and `p99` (warning range).
- Red zone: above rolling `p99` (critical range).

### How to click and interpret a point

1. Click any point on the IO chart.
2. `Selected Point` panel updates with:
   - selected timestamp/value
   - percentile zone (`green/yellow/red`)
   - nearest S2 outlier (if present)
3. Set `Raw table window` to `clicked-point ± minutes` to drill down into telemetry around that point.

### Troubleshooting click interactions

- If clicking does nothing, install/update dependencies:

```bash
python -m pip install -r requirements.txt
```

- Ensure `streamlit-plotly-events` is installed in the same active `.venv` as Streamlit.
- Use `Clear Selected Point` to reset drilldown state.

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

## Live Vantage connectivity

Use environment variables or notebook secrets. Do not hardcode credentials into repo files.

PowerShell:

```powershell
$env:TD_HOST="demo-env-vvy0mcddq6sdnb38.env.clearscape.teradata.com"
$env:TD_USERNAME="<your-username>"
$env:TD_PASSWORD="<your-password>"
```

Run a read-only smoke test plus telemetry discovery:

```bash
python scripts/vantage_connectivity_check.py --output pretty
```

If the environment exposes different object names, override them when running the scenario engine:

```bash
python scripts/scenario_engine.py --source teradata --db data/telemetry.db --td-dbql-query-table DBC.DBQLogTblV --td-dbql-step-table DBC.DBQLStepTbl --td-resusage-table DBC.ResUsageSpma run
```

Notes:
- The scenario engine can write scenario outputs either to the local SQLite file passed by `--db` or to Vantage via `--sink teradata`.
- The Teradata source reads live telemetry from Vantage using `TD_HOST`, `TD_USERNAME`, and `TD_PASSWORD`.
- Rotate any credentials that were pasted into chat or shared outside your secret store.

## Run against a real Teradata database

Set environment variables first:

```powershell
$env:TD_HOST="demo-env-vvy0mcddq6sdnb38.env.clearscape.teradata.com"
$env:TD_USERNAME="<your-username>"
$env:TD_PASSWORD="<your-password>"
$env:TD_DEMO_SCHEMA="td_demo_workload"
$env:TD_DEMO_USER="<demo-user>"
```

### 1. Create and run demo workloads in Vantage

```bash
python scripts/run_vantage_workloads.py --schema td_demo_workload --loops 3 --sleep-seconds 2
```

This creates a dedicated demo schema and submits repeatable `etl`, `bi`, and `ad_hoc` workload patterns so DBQL captures live telemetry.

### 2. Validate telemetry access

```bash
python scripts/vantage_connectivity_check.py --output pretty
```

### 3. Run scenario processing with Vantage as both source and sink

```bash
python scripts/scenario_engine.py --source teradata --sink teradata --db data/telemetry.db run
```

Optional overrides:

```bash
python scripts/scenario_engine.py --source teradata --sink teradata --td-dbql-query-table DBC.DBQLogTblV --td-dbql-step-table DBC.DBQLStepTbl --td-resusage-table DBC.ResUsageSpma --td-metric-table metric_timeseries --td-model-state-table model_state --td-anomaly-table anomaly_events --td-kpi-table impact_kpis run
```

### 4. Launch the app in Vantage mode

```bash
streamlit run scripts/demo_app.py
```

In the sidebar:
- set `Backend` to `teradata`
- set `Demo schema filter` and `Demo user filter`
- optionally set `GROQ_API_KEY`

The app will read scenario outputs plus raw DBQL/ResUsage drilldowns from Vantage and surface raw telemetry IDs such as `QueryID` and `query:...|step:...`.
