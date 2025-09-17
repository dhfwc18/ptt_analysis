# src/ptt_crawler/config/config.py
"""Configuration file for the project."""

import json
import logging
import logging.config
import os
from datetime import datetime
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = Path(__file__).resolve().parents[2]
PACKAGE_ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = Path(__file__).resolve().parent

HEADER_FILEPATH: Path = CONFIG_DIR / "headers.json"
JIEBA_DICT_PATH: Path = CONFIG_DIR / "jieba_dictionary/dict.txt.big"

with open(CONFIG_DIR / "chinese_puncts.json", encoding="utf-8") as file:
    CHINESE_PUNCTS: list[str] = json.load(file)


def load_headers() -> dict:
    """Load the headers configuration from a JSON file."""
    if not HEADER_FILEPATH.exists():
        raise FileNotFoundError(f"Header file not found: {HEADER_FILEPATH}")

    with open(HEADER_FILEPATH, encoding="utf-8") as file:
        header = json.load(file)

    return header


def setup_logging(
    *,
    config_path: Path | None = None,
    project_root: Path | None = None,
    date_fmt: str = "%Y%m%d",
    capture_warnings: bool = True,
) -> None:
    """
    Load logging configuration from YAML, rewrite file paths to use a
    date-based log directory, and apply the configuration.
    """
    config_path = config_path or (CONFIG_DIR / "logging_config.yaml")
    project_root = project_root or PROJECT_ROOT

    with open(config_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Rewrite handler filenames to include date folder
    current_date = datetime.now().strftime(date_fmt)
    for _, handler_cfg in (config.get("handlers") or {}).items():
        filename = handler_cfg.get("filename")
        if not filename:
            continue
        path = Path(filename)
        handler_cfg.setdefault("encoding", "utf-8")
        if path.is_absolute():
            path.parent.mkdir(parents=True, exist_ok=True)
            handler_cfg["filename"] = str(path)
        else:
            dated_dir = project_root / path.parent / current_date
            dated_dir.mkdir(parents=True, exist_ok=True)
            handler_cfg["filename"] = str(dated_dir / path.name)

    # Apply configuration
    try:
        logging.config.dictConfig(config)
        if capture_warnings:
            logging.captureWarnings(True)
    except Exception:
        logging.basicConfig(
            level=os.getenv("LOG_LEVEL", "INFO").upper(),
            format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        logging.getLogger(__name__).exception(
            "Failed to configure logging from YAML; using basicConfig()"
        )
