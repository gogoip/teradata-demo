CREATE MULTISET TABLE metric_timeseries (
    metric_name VARCHAR(128) NOT NULL,
    entity_type VARCHAR(128) NOT NULL,
    entity_id VARCHAR(256) NOT NULL,
    ts TIMESTAMP(0) NOT NULL,
    metric_value FLOAT NOT NULL,
    tags_json CLOB CHARACTER SET UNICODE NOT NULL
) PRIMARY INDEX (metric_name, entity_type, entity_id, ts);

CREATE MULTISET TABLE model_state (
    scenario_id VARCHAR(32) NOT NULL,
    entity_type VARCHAR(128) NOT NULL,
    entity_id VARCHAR(256) NOT NULL,
    trained_at TIMESTAMP(0) NOT NULL,
    model_blob BLOB,
    baseline_json CLOB CHARACTER SET UNICODE NOT NULL
) PRIMARY INDEX (scenario_id, entity_type, entity_id);

CREATE MULTISET TABLE anomaly_events (
    event_id VARCHAR(128) NOT NULL,
    scenario_id VARCHAR(32) NOT NULL,
    severity VARCHAR(32) NOT NULL,
    entity_type VARCHAR(128) NOT NULL,
    entity_id VARCHAR(256) NOT NULL,
    ts TIMESTAMP(0) NOT NULL,
    observed FLOAT,
    expected FLOAT,
    score FLOAT,
    context_json CLOB CHARACTER SET UNICODE NOT NULL
) PRIMARY INDEX (event_id);

CREATE MULTISET TABLE impact_kpis (
    ts_window_start TIMESTAMP(0) NOT NULL,
    ts_window_end TIMESTAMP(0) NOT NULL,
    cpu_at_risk FLOAT NOT NULL,
    io_at_risk FLOAT NOT NULL,
    incidents_predicted INTEGER NOT NULL,
    cost_risk_index FLOAT NOT NULL
) PRIMARY INDEX (ts_window_start, ts_window_end);
