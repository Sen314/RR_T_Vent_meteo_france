{{ config(
    materialized='incremental',
    unique_key=['department_code', 'observation_date']
) }}

WITH source AS (
    SELECT * FROM {{ ref('int_weather_db_fact_weather') }}
)

SELECT
    department_code,
    observation_date,
    AVG(cleaned_station_avg_temp) AS avg_temp_c,
    MIN(cleaned_station_min_temp) AS min_temp_c,
    MAX(cleaned_station_max_temp) AS max_temp_c
FROM source

{% if is_incremental() %}
WHERE observation_date >= (
    SELECT MAX(observation_date) - INTERVAL '10 day',
    '1900-01-01'
    FROM {{ this }}
)
{% endif %}

GROUP BY department_code, observation_date