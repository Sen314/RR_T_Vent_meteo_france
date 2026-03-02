SELECT
    fw.station_id,
    fw.observation_date,
    fw.wind_mean_ms
FROM {{ ref('int_weather_db_fact_weather') }} as fw
WHERE fw.wind_mean_ms < 0