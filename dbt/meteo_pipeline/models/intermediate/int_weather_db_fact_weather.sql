SELECT 
    fw.station_id,
    ds.department_code,
    fw.observation_date,
    fw.rainfall_mm,
    fw.rain_duration_min,
    CASE 
            WHEN fw.temp_min_c = 0
                AND fw.temp_mean_c = 0
                AND fw.temp_max_c = 0
            THEN NULL
            ELSE fw.temp_min_c
        END AS cleaned_station_min_temp,
    CASE 
        WHEN fw.temp_max_c = 0
            AND fw.temp_mean_c = 0
            AND fw.temp_min_c = 0
        THEN NULL
        ELSE fw.temp_max_c
    END AS cleaned_station_max_temp,
    CASE 
        WHEN fw.temp_mean_c = 0
            AND fw.temp_min_c = 0
            AND fw.temp_max_c = 0
        THEN NULL
        WHEN fw.temp_mean_c IS NOT NULL
            AND fw.temp_mean_c <> 0 
        THEN fw.temp_mean_c
        ELSE (fw.temp_max_c + fw.temp_min_c) / 2 
    END AS cleaned_station_avg_temp,
    fw.temp_amplitude_c,
    fw.wind_mean_ms,
    fw.wind_max_instant_ms,
    fw.wind_max_3s_ms,
    fw.wind_direction_deg,
    fw.ingestion_timestamp
FROM 
    {{ ref('stg_weather_db_fact_weather') }} as fw
    INNER JOIN {{ ref('stg_weather_db_dim_station') }} as ds
        ON fw.station_id = ds.station_id