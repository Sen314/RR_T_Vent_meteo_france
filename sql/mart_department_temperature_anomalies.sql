CREATE OR REPLACE VIEW mart_department_temperature_anomalies AS
SELECT
    d.department_code,
    d.observation_date,
    d.avg_temp_c,

    b.historical_mean,
    b.historical_stddev,

    ROUND(
        (d.avg_temp_c - b.historical_mean) 
        / NULLIF(b.historical_stddev, 0),
        2
    ) AS z_score,

    CASE
        WHEN ABS(
            (d.avg_temp_c - b.historical_mean) 
            / NULLIF(b.historical_stddev, 0)
        ) >= 2 THEN 1
        ELSE 0
    END AS anomaly_flag

FROM mart_department_daily d
JOIN mart_department_temp_baseline b
  ON d.department_code = b.department_code
 AND EXTRACT(DOY FROM d.observation_date) = b.day_of_year;