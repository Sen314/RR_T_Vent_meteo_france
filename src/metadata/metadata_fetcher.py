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

def infer_period_from_title(title: str):
    """
    Extract start/end year from patterns like:
    - periode_1875-1949
    - periode_2023
    """
    match = re.search(r"periode_(\d{4})(?:-(\d{4}))?", title)
    if not match:
        return None, None

    start = int(match.group(1))
    end = int(match.group(2)) if match.group(2) else start
    return start, end


def infer_department(title: str):
    match = re.search(r"departement_(\d{2,3})", title)
    return match.group(1) if match else None


def infer_granularity(start_year, end_year):
    if start_year is None or end_year is None:
        return "unknown"
    if end_year <= 1950:
        return "yearly"
    elif end_year <= CURRENT_YEAR - 2:
        return "monthly"
    else:
        return "daily"


def fetch_metadata():
    resp = requests.get(DATASET_URL, timeout=30)
    resp.raise_for_status()
    return resp.json()


def build_resource_catalog(metadata_json):
    rows = []

    for r in metadata_json.get("resources", []):
        title = r.get("title", "")

        start_year, end_year = infer_period_from_title(title)
        granularity = infer_granularity(start_year, end_year)
        department = infer_department(title)

        row = {
            "resource_id": r.get("id"),
            "title": title,
            "description": r.get("description"),
            "format": r.get("format"),
            "url": r.get("url"),
            "filesize": r.get("filesize"),
            "created_at": r.get("created_at"),
            "last_modified": r.get("last_modified"),
            "period_start_year": start_year,
            "period_end_year": end_year,
            "temporal_granularity": granularity,
            "department_code": department,
        }

        rows.append(row)

    return rows


def main():
    metadata = fetch_metadata()
    catalog = build_resource_catalog(metadata)

    # Quick sanity stats
    from collections import Counter
    print("By granularity:", Counter(r["temporal_granularity"] for r in catalog))
    print("By format:", Counter(r["format"] for r in catalog))

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=2, ensure_ascii=False)

    logger.info(f"Metadata fetching completed. Total resources: {len(catalog)}. Catalog saved to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()