{{ config(
    materialized='table',
    unique_key=['department_code', 'observation_date']
) }}

WITH source AS (
    SELECT * FROM {{ ref('int_weather_db_fact_weather_daily') }}
)

SELECT
    department_code,
    observation_date,
    daily_avg_temp_c,
    daily_min_temp_c,
    daily_max_temp_c,
    daily_total_rainfall_mm,
    daily_avg_wind_mean_ms,
    daily_max_wind_instant_ms,
    daily_avg_temp_amplitude_c,
    daily_rainfall_intensity_mm_per_min

FROM source