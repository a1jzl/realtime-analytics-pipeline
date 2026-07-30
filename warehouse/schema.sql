-- Schema PostgreSQL du data warehouse cible

CREATE TABLE IF NOT EXISTS revenue_by_window (
    window_start   TIMESTAMP NOT NULL,
    window_end     TIMESTAMP NOT NULL,
    category       VARCHAR(50) NOT NULL,
    revenue        NUMERIC(12, 2) NOT NULL,
    inserted_at    TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_revenue_window_start ON revenue_by_window (window_start);
CREATE INDEX IF NOT EXISTS idx_revenue_category ON revenue_by_window (category);
