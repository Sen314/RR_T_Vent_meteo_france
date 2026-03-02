SELECT
    fw.station_id,
    fw.observation_date,
    fw.rainfall_mm
FROM {{ ref('int_weather_db_fact_weather') }} as fw
WHERE fw.rainfall_mm < 0