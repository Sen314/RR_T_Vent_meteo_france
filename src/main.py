import argparse
import json
import sys
from src.metadata.metadata_fetcher import fetch_metadata
from src.extract.downloader import incremental_download
from src.transform.weather_cleaner import transform
from src.load.weather_loader import load

def run_fetch_metadata():
    fetch_metadata()


def run_download():
    with open("data/metadata/resource_catalog.json", "r", encoding="utf-8") as f:
        metadata_list = json.load(f)

    incremental_download(metadata_list)


def main():
    parser = argparse.ArgumentParser(description="Meteo-France Data Pipeline")

    parser.add_argument(
        "command",
        choices=["fetch-metadata", "download", "transform", "load"],
        help="Pipeline command to execute"
    )

    args = parser.parse_args()

    if args.command == "fetch-metadata":
        run_fetch_metadata()
    elif args.command == "download":
        run_download()
    elif args.command == "transform":
        transform()
    elif args.command == "load":
        load()
    else:
        print("need an argument: fetch-metadata, download, transform, or load")

if __name__ == "__main__":
    print(sys.executable)
    main()