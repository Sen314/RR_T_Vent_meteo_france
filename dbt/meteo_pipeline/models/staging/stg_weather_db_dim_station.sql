SELECT
    station_id,
    station_name,
    latitude,
    longitude,
    altitude,
    department_code
FROM 
    {{ source('weather_db', 'dim_station') }}
