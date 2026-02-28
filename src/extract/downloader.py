import requests
from pathlib import Path
from src.utils.hash_utils import compute_file_hash
from src.utils.state_manager import load_state, save_state
from src.utils.logger import get_logger

RAW_DATA_DIR = Path("data/raw")
logger = get_logger(__name__)

def download_file(url, output_path):
    response = requests.get(url, stream=True)
    response.raise_for_status()

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)


def incremental_download(metadata_list):
    state = load_state()
    updated_state = state.copy()

    for resource in metadata_list:
        resource_id = resource["resource_id"]
        url = resource["url"]

        last_modified = resource.get("last_modified")

        filename = url.split("/")[-1]
        department_code = resource.get("department_code") or "unknown"

        output_path = RAW_DATA_DIR / department_code / filename

        need_download = True

        if resource_id in state:
            if state[resource_id].get("last_modified") == last_modified:
                need_download = False

        if not output_path.exists():
            need_download = True

        if need_download:
            logger.info(f"Downloading {filename} from {url}")
            try:
                download_file(url, output_path)
            except Exception as e:
                logger.error(f"Failed to download {filename}: {e}")
                continue

            file_hash = compute_file_hash(output_path)

            updated_state[resource_id] = {
                "last_modified": last_modified,
                "file_hash": file_hash
            }
        else:
            logger.info(f"Skipping {filename} (up-to-date)")
    save_state(updated_state)
    logger.debug("State loaded successfully")