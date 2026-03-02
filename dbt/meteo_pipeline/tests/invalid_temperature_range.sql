SELECT
    fw.station_id,
    fw.observation_date,
    fw.temp_min_c,
    fw.temp_max_c
FROM {{ ref('int_weather_db_fact_weather') }} as fw
WHERE fw.temp_min_c > fw.temp_max_c