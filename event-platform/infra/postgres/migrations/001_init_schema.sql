-- Database schema initialization script for PostgreSQL sink

CREATE TABLE IF NOT EXISTS raw_events (
    event_id VARCHAR(255) PRIMARY KEY,
    event_type VARCHAR(255) NOT NULL,
    occurred_at BIGINT NOT NULL,
    payload TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_raw_events_event_type ON raw_events(event_type);
CREATE INDEX IF NOT EXISTS idx_raw_events_occurred_at ON raw_events(occurred_at);

CREATE TABLE IF NOT EXISTS enriched_counts (
    event_type VARCHAR(255) NOT NULL,
    window_start BIGINT NOT NULL,
    event_count BIGINT NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (event_type, window_start)
);

CREATE INDEX IF NOT EXISTS idx_enriched_counts_window ON enriched_counts(window_start);
