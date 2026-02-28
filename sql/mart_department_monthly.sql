CREATE OR REPLACE VIEW mart_department_monthly AS
SELECT
    department_code,
    DATE_TRUNC('month', observation_date)::date AS month,
    COUNT(DISTINCT observation_date) AS active_days,
    ROUND(AVG(avg_temp_c), 2) AS avg_temp_c,
    MIN(min_temp_c) AS min_temp_c,
    MAX(max_temp_c) AS max_temp_c,
    ROUND(SUM(total_rainfall_mm), 2) AS total_rainfall_mm,
    ROUND(AVG(avg_wind_mean_ms), 2) AS avg_wind_mean_ms,
    MAX(max_wind_instant_ms) AS max_wind_instant_ms
FROM mart_department_daily
GROUP BY
    department_code,
    DATE_TRUNC('month', observation_date)
ORDER BY
    department_code,
    month;