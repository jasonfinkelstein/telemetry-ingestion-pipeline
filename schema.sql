-- Drop tables if they exist (starting fresh)
DROP TABLE IF EXISTS silver_telemetry CASCADE;
DROP TABLE IF EXISTS silver_rejects CASCADE;

-- Main telemetry data table
CREATE TABLE IF NOT EXISTS silver_telemetry (
    record_id BIGINT PRIMARY KEY, -- Unique identifier for each record
    date TIMESTAMP NOT NULL, -- Timestamps for each recorded data point
    rpm INTEGER NOT NULL CHECK (rpm >= 0), -- Engine revolutions per minute, indicating engine performance and power delivery
    speed INTEGER NOT NULL CHECK (speed >= 0), -- Car speed measured in kilometers per hour, reflecting acceleration and straight-line performance
    n_gear INTEGER NOT NULL CHECK (n_gear >= -1), -- Gear engaged by the driver at each timestamp, influencing speed and acceleration
    throttle INTEGER NOT NULL CHECK (throttle >= 0 AND throttle <= 100), -- Percentage of throttle opening, revealing driver input and acceleration strategy
    brake BOOLEAN NOT NULL, -- Data on braking behavior, crucial for understanding cornering techniques and managing tire wear
    drs INTEGER NOT NULL, -- Deployment of the Drag Reduction System, affecting straight-line speed and overtaking opportunities
    source TEXT NOT NULL,
    time INTERVAL NOT NULL, -- Timestamp indicating when each data point was recorded during the lap
    session_time INTERVAL NOT NULL, -- Duration of the race session, impacting data interpretation due to varying track conditions and fuel loads
    _loaded_at TIMESTAMP NOT NULL DEFAULT NOW() -- Record load timestamp
);

-- Indexes for faster querying
CREATE INDEX idx_silver_telemetry_date ON silver_telemetry(date);
CREATE INDEX idx_silver_telemetry_speed ON silver_telemetry(speed);
CREATE INDEX idx_silver_telemetry_rpm ON silver_telemetry(rpm);

-- Table for rejected records
CREATE TABLE silver_rejects (
    id SERIAL PRIMARY KEY, -- Unique identifier for each rejected record
    source_name TEXT NOT NULL, -- Name of the source file
    raw_payload JSONB NOT NULL, -- The actual invalid data
    reason TEXT NOT NULL, -- Reason for rejection
    rejected_at TIMESTAMP NOT NULL DEFAULT NOW() -- Timestamp of rejection
);

CREATE INDEX idx_rejects_source ON silver_rejects(source_name);

-- End of schema.sql