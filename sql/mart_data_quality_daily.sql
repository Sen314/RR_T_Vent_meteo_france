CREATE OR REPLACE VIEW mart_data_quality_daily AS
SELECT
    fw.observation_date,

    COUNT(*) AS total_rows,
    COUNT(DISTINCT fw.station_id) AS station_count,

    -- Completeness
    SUM(CASE WHEN fw.temp_mean_c IS NULL THEN 1 ELSE 0 END) AS missing_temp_mean,
    SUM(CASE WHEN fw.rainfall_mm IS NULL THEN 1 ELSE 0 END) AS missing_rainfall,

    -- Validity
    SUM(CASE WHEN fw.temp_min_c > fw.temp_max_c THEN 1 ELSE 0 END) AS invalid_temp_range,
    SUM(CASE WHEN fw.temp_min_c < -50 OR fw.temp_max_c > 60 THEN 1 ELSE 0 END) AS extreme_temp_values,
    SUM(CASE WHEN fw.rainfall_mm < 0 THEN 1 ELSE 0 END) AS negative_rainfall,
    SUM(CASE WHEN fw.wind_mean_ms < 0 THEN 1 ELSE 0 END) AS negative_wind,

    -- Logical consistency
    SUM(CASE WHEN fw.rainfall_mm = 0 AND fw.rain_duration_min > 0 THEN 1 ELSE 0 END)
        AS rain_duration_without_rain,

    SUM(CASE WHEN fw.rainfall_mm > 0 AND fw.rain_duration_min = 0 THEN 1 ELSE 0 END)
        AS rain_without_duration
FROM fact_weather fw
GROUP BY fw.observation_date
ORDER BY fw.observation_date;