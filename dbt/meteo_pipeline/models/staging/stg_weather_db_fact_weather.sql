SELECT 
    station_id,
    observation_date,
    rainfall_mm,
    rain_duration_min,
    temp_min_c,
    temp_max_c,
    temp_mean_c,
    temp_amplitude_c,
    wind_mean_ms,
    wind_max_instant_ms,
    wind_max_3s_ms,
    wind_direction_deg,
    ingestion_timestamp
FROM 
    {{ source('weather_db', 'fact_weather') }}