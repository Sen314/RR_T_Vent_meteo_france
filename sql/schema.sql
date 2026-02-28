CREATE TABLE IF NOT EXISTS dim_station (
    station_id VARCHAR(8) PRIMARY KEY,
    station_name TEXT NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    altitude INTEGER,
    department_code VARCHAR(3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fact_weather (
    station_id VARCHAR(8) REFERENCES dim_station(station_id),
    observation_date TIMESTAMP NOT NULL,

    rainfall_mm DOUBLE PRECISION,
    rain_duration_min INTEGER,

    temp_min_c DOUBLE PRECISION,
    temp_max_c DOUBLE PRECISION,
    temp_mean_c DOUBLE PRECISION,
    temp_amplitude_c DOUBLE PRECISION,

    wind_mean_ms DOUBLE PRECISION,
    wind_max_instant_ms DOUBLE PRECISION,
    wind_max_3s_ms DOUBLE PRECISION,
    wind_direction_deg INTEGER,

    ingestion_timestamp TIMESTAMP,

    PRIMARY KEY (station_id, observation_date)
);

CREATE TABLE IF NOT EXISTS etl_run_log (
    run_id SERIAL PRIMARY KEY,
    pipeline_name TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    rows_processed INTEGER,
    status TEXT
);