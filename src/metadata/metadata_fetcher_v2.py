import requests
import json
import re
from pathlib import Path
from datetime import datetime
from src.utils.logger import get_logger

DATASET_URL = "https://www.data.gouv.fr/api/1/datasets/6569b51ae64326786e4e8e1a"
#TODO : find a better way to handle paths and dates
OUTPUT_PATH = Path(__file__).resolve().parents[2] / "data" / "metadata" / "metadata.json"
CURRENT_YEAR = datetime.utcnow().year
logger = get_logger(__name__)

def infer_granularity(start_year, end_year):
    if start_year is None or end_year is None:
        return "unknown"

    if end_year <= 1950:
        return "yearly"
    elif end_year <= CURRENT_YEAR - 2:
        return "monthly"
    else:
        return "daily"


def parse_resource(resource: dict) -> dict:
    """
    Extract structured metadata from a dataset resource entry.
    """

    title = resource.get("title", "")

    # Extract department code (Q_06_latest...)
    dept_match = re.search(r"_([0-9]{2})_", title)
    department_code = dept_match.group(1) if dept_match else None

    # Extract period (2025-2026)
    period_match = re.search(r"periode_([0-9]{4})-([0-9]{4})", title)
    period_start_year = int(period_match.group(1)) if period_match else None
    period_end_year = int(period_match.group(2)) if period_match else None

    # Determine granularity
    temporal_granularity = infer_granularity(period_start_year, period_end_year)
    #"daily" if "QUOT" in title else "unknown"

    return {
        "resource_id": resource.get("id"),
        "title": title,
        "description": resource.get("description"),
        "format": resource.get("format"),
        "url": resource.get("url"),
        "filesize": resource.get("filesize"),
        "created_at": resource.get("created_at"),
        "last_modified": resource.get("last_modified"),
        "period_start_year": period_start_year,
        "period_end_year": period_end_year,
        "temporal_granularity": temporal_granularity,
        "department_code": department_code,
    }


def fetch_metadata():
    response = requests.get(DATASET_URL)
    response.raise_for_status()

    dataset = response.json()
    resources = dataset.get("resources", [])

    parsed_resources = [parse_resource(r) for r in resources]

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(parsed_resources, f, indent=2, ensure_ascii=False)

    print(f"Metadata written to {OUTPUT_PATH}")
    logger.info(f"Metadata fetching completed. Total resources: {len(resources)}. Catalog saved to {OUTPUT_PATH}")


if __name__ == "__main__":
    fetch_metadata()