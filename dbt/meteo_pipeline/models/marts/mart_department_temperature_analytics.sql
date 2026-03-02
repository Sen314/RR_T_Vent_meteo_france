{{ config(
    materialized='table',
    unique_key=['department_code', 'observation_date']
) }}

WITH base AS (
    SELECT
        department_code,
        observation_date,
        daily_avg_temp_c,
        -- 7-day rolling average
        AVG(daily_avg_temp_c) OVER (
            PARTITION BY department_code
            ORDER BY observation_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_7d_avg_temp,

        -- 30-day rolling average (for trend comparison)
        AVG(daily_avg_temp_c) OVER (
            PARTITION BY department_code
            ORDER BY observation_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS rolling_30d_avg_temp,

        CASE
            WHEN daily_avg_temp_c >= 30 THEN 1
            ELSE 0
        END AS hot_day
        FROM {{ ref('int_weather_db_fact_weather_daily') }}
),

heatwave_flag AS (
    SELECT
        *,
        CASE
            WHEN rolling_7d_avg_temp >= 25 THEN 1
            ELSE 0
        END AS hot_period
    FROM base
),

heatwave_detected AS (
    SELECT
        *,
        SUM(hot_period) OVER (
            PARTITION BY department_code
            ORDER BY observation_date
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS hot_3d_sum
    FROM heatwave_flag
)

SELECT
    department_code,
    observation_date,
    daily_avg_temp_c AS daily_avg_temp_c,
    ROUND(rolling_7d_avg_temp, 2) AS rolling_7d_avg_temp,
    ROUND(rolling_30d_avg_temp, 2) AS rolling_30d_avg_temp,
    CASE
        WHEN hot_3d_sum = 3 THEN 1
        ELSE 0
    END AS heatwave_flag

FROM heatwave_detected
