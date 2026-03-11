-- Scenario engine output tables for Digna-style telemetry analysis.

CREATE TABLE IF NOT EXISTS metric_timeseries (
    metric_name TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    ts TIMESTAMP NOT NULL,
    value REAL NOT NULL,
    tags_json TEXT NOT NULL,
    PRIMARY KEY (metric_name, entity_type, entity_id, ts)
);

CREATE TABLE IF NOT EXISTS model_state (
    scenario_id TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    trained_at TIMESTAMP NOT NULL,
    model_blob BLOB,
    baseline_json TEXT NOT NULL,
    PRIMARY KEY (scenario_id, entity_type, entity_id)
);

CREATE TABLE IF NOT EXISTS anomaly_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    scenario_id TEXT NOT NULL,
    severity TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    ts TIMESTAMP NOT NULL,
    observed REAL,
    expected REAL,
    score REAL,
    context_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS impact_kpis (
    ts_window_start TIMESTAMP NOT NULL,
    ts_window_end TIMESTAMP NOT NULL,
    cpu_at_risk REAL NOT NULL,
    io_at_risk REAL NOT NULL,
    incidents_predicted INTEGER NOT NULL,
    cost_risk_index REAL NOT NULL,
    PRIMARY KEY (ts_window_start, ts_window_end)
);
