import json
from pathlib import Path

STATE_PATH = Path("data/state/ingestion_state.json")


def load_state():
    if not STATE_PATH.exists():
        return {}

    with open(STATE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state):
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)

    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)