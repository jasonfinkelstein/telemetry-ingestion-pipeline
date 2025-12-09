-- Drop tables if they exist (starting fresh)
DROP TABLE IF EXISTS silver_telemetry CASCADE;
DROP TABLE IF EXISTS silver_rejects CASCADE;

-- Main telemetry data table
CREATE TABLE IF NOT EXISTS silver_telemetry (
    record_id BIGINT PRIMARY KEY, -- Unique identifier for each record
    date TIMESTAMP NOT NULL, -- Timestamps for each recorded data point
    rpm INTEGER NOT NULL CHECK (rpm >= 0), -- Engine revolutions per minute
    speed INTEGER NOT NULL CHECK (speed >= 0), -- Car speed measured in kilometers per hour
    n_gear INTEGER NOT NULL CHECK (n_gear >= -1), -- Gear engaged by the driver at each timestamp
    throttle INTEGER NOT NULL CHECK (throttle >= 0 AND throttle <= 100), -- Percentage of throttle opening
    brake BOOLEAN NOT NULL, -- Data on braking behavior
    drs INTEGER NOT NULL, -- Deployment of the Drag Reduction System
    source TEXT NOT NULL,
    time INTERVAL NOT NULL, -- Timestamp indicating when each data point was recorded during the lap
    session_time INTERVAL NOT NULL, -- Duration of the race session
    _loaded_at TIMESTAMP NOT NULL DEFAULT NOW() -- Record load timestamp
);

-- indexes for faster querying
CREATE INDEX idx_telemetry_date ON silver_telemetry(date);
CREATE INDEX idx_telemetry_speed ON silver_telemetry(speed);
CREATE INDEX idx_telemetry_rpm ON silver_telemetry(rpm);

-- table for rejected records
CREATE TABLE silver_rejects (
    id SERIAL PRIMARY KEY, -- Unique identifier for each rejection
    source_name TEXT NOT NULL, -- Name of the source file
    raw_payload JSONB NOT NULL, -- The actual invalid data
    reason TEXT NOT NULL, -- rejection reason
    rejected_at TIMESTAMP NOT NULL DEFAULT NOW() -- rejection timestamp
);

CREATE INDEX idx_rejects_source ON silver_rejects(source_name);