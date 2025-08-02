# config/config.py

# External imports
from pathlib import Path
import json

PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "output"
SRC_DIR = PROJECT_ROOT / "src"
HEADER_FILE = SRC_DIR / "config/headers.json"

def load_headers() -> dict:
    """Load the headers configuration from a JSON file."""
    if not HEADER_FILE.exists():
        raise FileNotFoundError(
            f"Header file not found: {HEADER_FILE}"
        )

    with open(HEADER_FILE, "r", encoding="utf-8") as file:
        header = json.load(file)

    return header

JIEBA_DICT_PATH = SRC_DIR / "config/jieba_dictionary/dict.txt.big"