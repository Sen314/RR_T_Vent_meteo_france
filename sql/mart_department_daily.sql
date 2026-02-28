CREATE OR REPLACE VIEW mart_department_daily AS
SELECT
    ds.department_code,
    fw.observation_date,
    COUNT(DISTINCT fw.station_id) AS station_count,
    ROUND(AVG(CASE WHEN fw.temp_mean_c IS NULL OR fw.temp_mean_c = 0 
                THEN (fw.temp_max_c + fw.temp_min_c) / 2 
                ELSE fw.temp_mean_c END)::NUMERIC, 2) AS avg_temp_c,
    MIN(fw.temp_min_c) AS min_temp_c,
    MAX(fw.temp_max_c) AS max_temp_c,
    ROUND(SUM(fw.rainfall_mm)::NUMERIC, 2) AS total_rainfall_mm,
    ROUND(AVG(fw.wind_mean_ms)::NUMERIC, 2) AS avg_wind_mean_ms,
    ROUND(MAX(fw.wind_max_instant_ms)::NUMERIC, 2) AS max_wind_instant_ms,
    ROUND(AVG(fw.temp_max_c - fw.temp_min_c)::NUMERIC, 2) AS avg_temp_amplitude,
    ROUND(SUM(fw.rainfall_mm)::NUMERIC / NULLIF(SUM(fw.rain_duration_min)::NUMERIC, 0), 2) AS rainfall_intensity_mm_per_min
FROM fact_weather fw
JOIN dim_station ds
    ON fw.station_id = ds.station_id
GROUP BY
    ds.department_code,
    fw.observation_date;

CREATE INDEX idx_fact_weather_date
ON fact_weather (observation_date);

CREATE INDEX idx_dim_station_department
ON dim_station (department_code);