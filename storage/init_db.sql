CREATE TABLE IF NOT EXISTS event_aggregates (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    event_type VARCHAR(50),
    country VARCHAR(10),
    event_count INTEGER,
    unique_users INTEGER,
    total_revenue NUMERIC(12,2),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_event_type ON event_aggregates(event_type);
CREATE INDEX idx_window_start ON event_aggregates(window_start);