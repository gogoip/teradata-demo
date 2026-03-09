-- Minimal telemetry schema for Teradata-style workload analysis demos.

CREATE TABLE IF NOT EXISTS dbc_dbqlogtbl (
    query_id INTEGER PRIMARY KEY,
    user_name TEXT NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    elapsed_time INTEGER NOT NULL,
    amp_cpu_time INTEGER NOT NULL,
    io_count INTEGER NOT NULL,
    error_code INTEGER
);

CREATE TABLE IF NOT EXISTS resusage_spma (
    sample_time TIMESTAMP NOT NULL,
    node_id INTEGER NOT NULL,
    cpu_percent REAL NOT NULL,
    disk_io REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS dbql_step_tbl (
    query_id INTEGER NOT NULL,
    step_id INTEGER NOT NULL,
    amp_id INTEGER NOT NULL,
    cpu_time INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS lock_info (
    session_id INTEGER NOT NULL,
    blocked_session INTEGER NOT NULL,
    locked_table TEXT NOT NULL,
    lock_type TEXT NOT NULL,
    event_time TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS session_info (
    session_id INTEGER PRIMARY KEY,
    user_name TEXT NOT NULL,
    logon_time TIMESTAMP NOT NULL,
    active_queries INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS tables_v (
    database_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    last_collect_stats TIMESTAMP
);

CREATE TABLE IF NOT EXISTS workload_map (
    query_id INTEGER NOT NULL,
    workload_name TEXT NOT NULL
);
