CREATE OR REPLACE VIEW mart_department_temp_baseline AS
SELECT
    department_code,
    EXTRACT(DOY FROM observation_date) AS day_of_year,

    AVG(avg_temp_c) AS historical_mean,
    STDDEV(avg_temp_c) AS historical_stddev

FROM mart_department_daily
GROUP BY department_code, EXTRACT(DOY FROM observation_date);