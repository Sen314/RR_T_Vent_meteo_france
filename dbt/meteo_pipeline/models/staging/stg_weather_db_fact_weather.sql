SELECT 
    station_id,
    observation_date,

    CASE
        WHEN temp_min_c = 0
            AND temp_mean_c = 0
            AND temp_max_c = 0
        THEN NULL
        ELSE temp_min_c::NUMERIC
    END AS cleaned_station_min_temp,
    CASE 
        WHEN temp_max_c = 0
            AND temp_mean_c = 0
            AND temp_min_c = 0
        THEN NULL
        WHEN temp_max_c = 0 
            AND temp_min_c <> 0 
            AND temp_mean_c <> 0 
            AND temp_min_c > temp_mean_c
            THEN 2 * temp_mean_c - temp_min_c
        ELSE temp_max_c::NUMERIC
    END AS cleaned_station_max_temp,
    CASE 
        WHEN temp_mean_c = 0
            AND temp_min_c = 0
            AND temp_max_c = 0
        THEN NULL
        WHEN temp_mean_c IS NOT NULL
            AND temp_mean_c <> 0 
        THEN temp_mean_c::NUMERIC
        ELSE ((temp_max_c + temp_min_c) / 2)::NUMERIC
    END AS cleaned_station_avg_temp,
    CASE 
        WHEN temp_amplitude_c = 0             
            AND temp_min_c = 0
            AND temp_max_c = 0
            THEN NULL
        WHEN temp_amplitude_c = 0 AND (temp_min_c <> 0 OR temp_max_c <> 0) AND temp_min_c < temp_max_c
            THEN (temp_max_c - temp_min_c)::NUMERIC
        ELSE temp_amplitude_c::NUMERIC
    END AS cleaned_station_temp_amplitude_c,
    CASE
        WHEN temp_min_c = 0
            AND temp_mean_c = 0
            AND temp_max_c = 0
            AND rainfall_mm = 0
        THEN NULL
        ELSE rainfall_mm::NUMERIC
    END AS cleaned_station_rainfall_mm,
    CASE
        WHEN temp_min_c = 0
            AND temp_mean_c = 0
            AND temp_max_c = 0
            AND rainfall_mm = 0
            AND rain_duration_min = 0
        THEN NULL
        WHEN rain_duration_min = 0 AND rainfall_mm > 0
            THEN NULL
        ELSE rain_duration_min::NUMERIC
    END AS cleaned_station_rainfall_duration_min,
    CASE 
        WHEN wind_max_instant_ms = 0
            AND wind_mean_ms = 0
            AND wind_max_3s_ms = 0
            AND wind_direction_deg = 0
            THEN NULL
        ELSE wind_mean_ms::NUMERIC
    END AS cleaned_station_wind_mean_ms,
    CASE
        WHEN wind_mean_ms = 0
            AND wind_max_3s_ms = 0
            AND wind_max_instant_ms = 0
            AND wind_direction_deg = 0
        THEN NULL
        ELSE wind_max_instant_ms::NUMERIC
    END AS cleaned_station_wind_max_instant_ms,
    CASE
        WHEN wind_mean_ms = 0
            AND wind_max_3s_ms = 0
            AND wind_max_instant_ms = 0
            AND wind_direction_deg = 0
        THEN NULL
        ELSE wind_max_3s_ms::NUMERIC
    END AS cleaned_station_wind_max_3s_ms,
    CASE
        WHEN wind_mean_ms = 0
            AND wind_max_3s_ms = 0
            AND wind_max_instant_ms = 0
            AND wind_direction_deg = 0
        THEN NULL
        ELSE wind_direction_deg::NUMERIC
    END AS cleaned_station_wind_direction_deg,
    ingestion_timestamp
FROM 
    {{ source('weather_db', 'fact_weather') }}