import gzip
import csv
from pathlib import Path
from datetime import datetime

from src.utils.logger import get_logger

logger = get_logger(__name__)

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")
OUTPUT_FILE = PROCESSED_DIR / "weather_observation.csv"

def safe_float(value):
    try:
        v = float(value)
        return v
    except ValueError:
        return 0.0

def parse_date(aaaammjj):
    return datetime.strptime(aaaammjj, "%Y%m%d").date().isoformat()

def normalize_row(row: dict, department_code: str, source_file: str) -> dict:
    return {
        "station_id": row.get("NUM_POSTE"),
        "station_name": row.get("NOM_USUEL"),
        "latitude": safe_float(row.get("LAT")),
        "longitude": safe_float(row.get("LON")),
        "altitude": int(row.get("ALTI")) if row.get("ALTI") else 0,

        "department_code": department_code,
        "observation_date": parse_date(row.get("AAAAMMJJ")) if row.get("AAAAMMJJ") else None,

        "rainfall_mm": safe_float(row.get("RR")),
        "rain_duration_min": int(row.get("DRR")) if row.get("DRR") else 0,

        "temp_min_c": safe_float(row.get("TN")),
        "temp_max_c": safe_float(row.get("TX")),
        "temp_mean_c": safe_float(row.get("TM")),
        "temp_amplitude_c": safe_float(row.get("TAMPLI")),

        "wind_mean_ms": safe_float(row.get("FFM")),
        "wind_max_instant_ms": safe_float(row.get("FXI")),
        "wind_max_3s_ms": safe_float(row.get("FXI3S")),
        "wind_direction_deg": int(row.get("DXY")) if row.get("DXY") else 0,

        "source_file": source_file,
        "ingestion_timestamp": datetime.utcnow().isoformat(),
    }

def transform():
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as outfile:
        fieldnames = [
            "station_id",
            "station_name",
            "latitude",
            "longitude",
            "altitude",
            "department_code",
            "observation_date",
            "rainfall_mm",
            "rain_duration_min",
            "temp_min_c",
            "temp_max_c",
            "temp_mean_c",
            "temp_amplitude_c",
            "wind_mean_ms",
            "wind_max_instant_ms",
            "wind_max_3s_ms",
            "wind_direction_deg",
            "source_file",
            "ingestion_timestamp",
        ]

        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for department_dir in RAW_DIR.iterdir():
            if not department_dir.is_dir():
                continue

            department_code = department_dir.name
            
            #focus only on Vent files for now, as they contain the most relevant weather data. We can easily extend to other files later if needed.
            if (department_code > "90"  and department_code < "95") or department_code == "75":
                for gz_file in department_dir.glob("*Vent.csv.gz"):
                    logger.info(f"Processing {gz_file.name}")

                    with gzip.open(gz_file, mode="rt", encoding="utf-8") as f:
                        reader = csv.DictReader(f, delimiter=";")

                        for row in reader:
                            normalized = normalize_row(
                                row,
                                department_code,
                                gz_file.name
                            )

                            writer.writerow(normalized)
            else:
                logger.warning(f"Skipping department {department_code} (not in expected range)")
            logger.info(f"Finished processing department {department_code}")

    logger.info(f"Transformation completed → {OUTPUT_FILE}")