CREATE OR REPLACE VIEW mart_department_temperature_analytics AS
WITH base AS (
    SELECT
        department_code,
        observation_date,
        avg_temp_c,
        -- 7-day rolling average
        ROUND(
            AVG(avg_temp_c) OVER (
                PARTITION BY department_code
                ORDER BY observation_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ),
            2
        ) AS rolling_7d_avg_temp,

        -- 30-day rolling average (for trend comparison)
        ROUND(
            AVG(avg_temp_c) OVER (
                PARTITION BY department_code
                ORDER BY observation_date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ),
            2
        ) AS rolling_30d_avg_temp,

        CASE
            WHEN avg_temp_c >= 30 THEN 1
            ELSE 0
        END AS hot_day
    FROM mart_department_daily
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
    avg_temp_c AS avg_temp_c,
    ROUND(rolling_7d_avg_temp, 2) AS rolling_7d_avg_temp,
    ROUND(rolling_30d_avg_temp, 2) AS rolling_30d_avg_temp,
    CASE
        WHEN hot_3d_sum = 3 THEN 1
        ELSE 0
    END AS heatwave_flag

FROM heatwave_detected;