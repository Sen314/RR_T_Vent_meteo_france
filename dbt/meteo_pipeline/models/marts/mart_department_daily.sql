WITH cleaned_mean_temp AS (
    SELECT
        ds.department_code,
        fw.observation_date,
        ds.station_id,
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
        CASE 
            WHEN fw.temp_min_c = 0
                AND fw.temp_mean_c = 0
                AND fw.temp_max_c = 0
            THEN NULL
            ELSE fw.temp_min_c
        END AS cleaned_station_min_temp

    FROM fact_weather fw
    INNER JOIN dim_station ds
        ON fw.station_id = ds.station_id
)

SELECT
    ds.department_code,
    fw.observation_date,
    COUNT(DISTINCT fw.station_id) AS station_count,
    ROUND(AVG(cmt.cleaned_station_avg_temp)::NUMERIC, 2) AS avg_temp_c,
    MIN(cmt.cleaned_station_min_temp) AS min_temp_c,
    MAX(fw.temp_max_c) AS max_temp_c,
    ROUND(SUM(fw.rainfall_mm)::NUMERIC, 2) AS total_rainfall_mm,
    ROUND(AVG(fw.wind_mean_ms)::NUMERIC, 2) AS avg_wind_mean_ms,
    ROUND(MAX(fw.wind_max_instant_ms)::NUMERIC, 2) AS max_wind_instant_ms,
    ROUND(AVG(fw.temp_max_c - fw.temp_min_c)::NUMERIC, 2) AS avg_temp_amplitude,
    ROUND(SUM(fw.rainfall_mm)::NUMERIC / NULLIF(SUM(fw.rain_duration_min)::NUMERIC, 0), 2) AS rainfall_intensity_mm_per_min

FROM fact_weather fw
INNER JOIN dim_station ds
    ON fw.station_id = ds.station_id
INNER JOIN cleaned_mean_temp cmt
    ON ds.department_code = cmt.department_code
    AND fw.observation_date = cmt.observation_date
    AND ds.station_id = cmt.station_id

GROUP BY
    ds.department_code,
    fw.observation_date;