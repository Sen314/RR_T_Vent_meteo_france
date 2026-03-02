SELECT 
    ds.department_code,
    fw.observation_date,
    AVG(fw.cleaned_station_avg_temp) AS daily_avg_temp_c,
    MIN(fw.cleaned_station_min_temp) AS daily_min_temp_c,
    MAX(fw.cleaned_station_max_temp) AS daily_max_temp_c,
    AVG(fw.cleaned_station_temp_amplitude_c) AS daily_avg_temp_amplitude_c,
    SUM(fw.cleaned_station_rainfall_mm) AS daily_total_rainfall_mm,
    AVG(fw.cleaned_station_rainfall_duration_min) AS daily_avg_rainfall_duration_min,
    AVG(fw.cleaned_station_wind_mean_ms) AS daily_avg_wind_mean_ms,
    MAX(fw.cleaned_station_wind_max_instant_ms) AS daily_max_wind_instant_ms,
    SUM(fw.cleaned_station_rainfall_mm) / NULLIF(SUM(fw.cleaned_station_rainfall_duration_min), 0) AS daily_rainfall_intensity_mm_per_min
FROM 
    {{ ref('stg_weather_db_fact_weather') }} as fw
    INNER JOIN {{ ref('stg_weather_db_dim_station') }} as ds
        ON fw.station_id = ds.station_id
GROUP BY ds.department_code,
fw.observation_date