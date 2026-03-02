
WITH source AS (
    SELECT * FROM {{ ref('stg_weather_db_fact_weather') }} AS fw
    INNER JOIN {{ ref('stg_weather_db_dim_station') }} AS ds
        ON fw.station_id = ds.station_id
)

SELECT
    department_code,
    EXTRACT(DOY FROM observation_date) AS day_of_year,
    AVG(cleaned_station_avg_temp) AS temp_historical_mean,
    AVG(cleaned_station_temp_amplitude_c) AS temp_amplitude_historical_mean,
    AVG(cleaned_station_rainfall_mm) AS rainfall_historical_mean,
    AVG(cleaned_station_wind_mean_ms) AS wind_mean_historical_mean,
    STDDEV(cleaned_station_avg_temp) AS temp_historical_stddev,
    STDDEV(cleaned_station_temp_amplitude_c) AS temp_amplitude_historical_stddev,
    STDDEV(cleaned_station_rainfall_mm) AS rainfall_historical_stddev,
    STDDEV(cleaned_station_wind_mean_ms) AS wind_mean_historical_stddev

FROM source

GROUP BY department_code, day_of_year