import csv
from pathlib import Path
from datetime import datetime, timedelta
from psycopg2.extras import execute_batch
from src.database.connection import get_connection
from src.utils.logger import get_logger

logger = get_logger(__name__)

CLEAN_FILE = Path("data/processed/weather_observation.csv")

def get_department_watermarks(cur, window_days=3):
    cur.execute("""
        SELECT department_code, MAX(observation_date)
        FROM fact_weather fw
        JOIN dim_station ds ON fw.station_id = ds.station_id
        GROUP BY department_code;
    """)

    result = cur.fetchall()
    watermarks = {}

    for dept, max_date in result:
        if max_date is None:
            continue
        watermarks[dept] = max_date - timedelta(days=window_days)
    return watermarks

def load():
    conn = get_connection()
    cur = conn.cursor()
    start_time = datetime.utcnow()
    station_rows = []
    fact_rows = []
    processed_rows = 0
    watermarks = get_department_watermarks(cur, window_days=3)

    cur.execute("""
        INSERT INTO etl_run_log (pipeline_name, start_time, status)
        VALUES (%s, %s, %s)
        RETURNING run_id;
    """, ("weather_pipeline", start_time, "RUNNING"))

    run_id = cur.fetchone()[0]
    conn.commit()
    
    try:
        with open(CLEAN_FILE, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            #TODO: find another way to to get the total number of rows wihtout reading the file twice
            total_rows = sum(1 for _ in reader)
            f.seek(0)
            next(reader)  

            for row in reader:
                dept_code = row["department_code"]
                row_date = datetime.fromisoformat(row["observation_date"])

                cutoff = watermarks.get(dept_code)
                if cutoff and row_date <= cutoff:
                    #logger.info(f"Skipping row for department {dept_code} with date {row_date}")
                    continue
                
                #pointless in case of an update need to be scaled on the number of rows that will be inserted/updated, not the total number of rows in the file, but it gives a better idea of the progress of the loading step
                processed_rows += 1
                if processed_rows % 1000 == 0:
                    logger.info(f"Processing row {processed_rows}/{total_rows} ({processed_rows/total_rows:.2%})")

                station_rows.append((
                    row["station_id"],
                    row["station_name"],
                    row["latitude"],
                    row["longitude"],
                    row["altitude"],
                    row["department_code"]
                ))

                fact_rows.append((
                    row["station_id"],
                    row["observation_date"],
                    row["rainfall_mm"],
                    row["rain_duration_min"],
                    row["temp_min_c"],
                    row["temp_max_c"],
                    row["temp_mean_c"],
                    row["temp_amplitude_c"],
                    row["wind_mean_ms"],
                    row["wind_max_instant_ms"],
                    row["wind_max_3s_ms"],
                    row["wind_direction_deg"],
                    row["ingestion_timestamp"]
                ))

        # Insert in batches station rows and fact rows every 10000 records to improve performance
        execute_batch(
            cur,
            """
            INSERT INTO dim_station (
                station_id,
                station_name,
                latitude,
                longitude,
                altitude,
                department_code
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (station_id) DO NOTHING;
            """,
            station_rows,
            page_size=10000
        )

        execute_batch(
            cur,
            """
            INSERT INTO fact_weather (
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
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (station_id, observation_date)
            DO UPDATE SET
                rainfall_mm = EXCLUDED.rainfall_mm,
                rain_duration_min = EXCLUDED.rain_duration_min,
                temp_min_c = EXCLUDED.temp_min_c,
                temp_max_c = EXCLUDED.temp_max_c,
                temp_mean_c = EXCLUDED.temp_mean_c,
                temp_amplitude_c = EXCLUDED.temp_amplitude_c,
                wind_mean_ms = EXCLUDED.wind_mean_ms,
                wind_max_instant_ms = EXCLUDED.wind_max_instant_ms,
                wind_max_3s_ms = EXCLUDED.wind_max_3s_ms,
                wind_direction_deg = EXCLUDED.wind_direction_deg,
                ingestion_timestamp = EXCLUDED.ingestion_timestamp;
            """,
            fact_rows,
            page_size=10000
        )
        
        logger.info("Data successfully loaded into Database weather_db - committing transaction")
        conn.commit()

    except Exception as e:
        conn.rollback()
        cur.execute("""
            UPDATE etl_run_log
            SET end_time = %s,
                status = %s
            WHERE run_id = %s;
        """, (datetime.utcnow(), "FAILED", run_id))
        logger.exception("Error during loading, transaction rolled back")
        raise e

    finally:
        end_time = datetime.utcnow()
        cur.execute("""
            UPDATE etl_run_log
            SET end_time = %s,
                rows_processed = %s,
                status = %s
            WHERE run_id = %s;
        """, (end_time, processed_rows, "SUCCESS", run_id))
        conn.commit()
        cur.close()
        conn.close()