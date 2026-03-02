{{ config(
    materialized='table',
    unique_key=['department_code', 'observation_date']
) }}

SELECT
    d.department_code,
    d.observation_date,
    d.daily_avg_temp_c,
    b.temp_historical_mean,
    b.temp_historical_stddev,

    ROUND(
        (d.daily_avg_temp_c - b.temp_historical_mean) 
        / NULLIF(b.temp_historical_stddev, 0),
        2
    ) AS z_score,

    CASE
        WHEN ABS(
            (d.daily_avg_temp_c - b.temp_historical_mean) 
            / NULLIF(b.temp_historical_stddev, 0)
        ) >= 2 THEN 1
        ELSE 0
    END AS anomaly_flag

FROM {{ ref('int_weather_db_fact_weather_daily') }} d
JOIN {{ ref('int_weather_db_fact_weather_baseline') }} b
  ON d.department_code = b.department_code
 AND EXTRACT(DOY FROM d.observation_date) = b.day_of_year