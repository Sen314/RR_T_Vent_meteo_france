SELECT 
    fw.station_id,
    ds.department_code,
    fw.observation_date,
    fw.cleaned_station_min_temp AS temp_min_c,
    fw.cleaned_station_max_temp AS temp_max_c,
    fw.cleaned_station_avg_temp AS temp_mean_c,
    fw.cleaned_station_rainfall_mm AS rainfall_mm,
    fw.cleaned_station_rainfall_duration_min AS rain_duration_min,
    fw.cleaned_station_temp_amplitude_c AS temp_amplitude_c,
    fw.cleaned_station_wind_mean_ms AS wind_mean_ms,
    fw.cleaned_station_wind_max_instant_ms AS wind_max_instant_ms,
    fw.cleaned_station_wind_max_3s_ms AS wind_max_3s_ms,
    fw.cleaned_station_wind_direction_deg AS wind_direction_deg,
    fw.ingestion_timestamp
FROM 
    {{ ref('stg_weather_db_fact_weather') }} as fw
    INNER JOIN {{ ref('stg_weather_db_dim_station') }} as ds
        ON fw.station_id = ds.station_id