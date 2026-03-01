SELECT
    ds.station_id,
    ds.station_name,
    ds.latitude,
    ds.longitude,
    ds.altitude,
    ds.department_code
FROM 
    {{ ref('stg_weather_db_dim_station') }} as ds