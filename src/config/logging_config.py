# config/logging_config.py

import logging
import logging.config
import logging.handlers
from pathlib import Path
import sys
import os
import tempfile
from datetime import datetime
import traceback

APP_NAME = "ptt_crawler"
LOG_LEVEL = os.getenv('LOG_LEVEL', 'DEBUG').upper()

LOG_DATE = datetime.now().strftime("%Y%m%d")
LOG_DIR = Path(__file__).parents[2] / "logs" / LOG_DATE

# Filter class for console info output (allows DEBUG and INFO)
class ConsoleInfoFilter(logging.Filter):
    """Filter to allow only DEBUG and INFO level messages for console output"""
    def filter(self, record):
        return record.levelno <= logging.INFO

# Filter class for excluding INFO and DEBUG from error streams
class ErrorOnlyFilter(logging.Filter):
    """Filter to allow only WARNING and above for error streams"""
    def filter(self, record):
        return record.levelno >= logging.WARNING

# Formatter configurations
formatters = {
    "verbose": {
        "format": (
            "%(asctime)s %(levelname)-8s [%(name)s:%(lineno)d]"
            " %(funcName)s() %(message)s"
        ),
        "datefmt": "%Y-%m-%d %H:%M:%S",
    },
    "simple": {
        "format": "%(levelname)-8s %(message)s",
    },
    "console": {
        "format": "%(asctime)s %(levelname)-8s %(message)s",
        "datefmt": "%H:%M:%S",
    },
}

# Filters
filters = {
    "console_info_filter": {
        "()": ConsoleInfoFilter,
    },
    "error_only_filter": {
        "()": ErrorOnlyFilter,
    }
}

def _get_log_handlers():
    """Get handlers configuration with proper file paths"""
    return {
        "console_info": {
            "class": "logging.StreamHandler",
            "formatter": "console",
            "level": "INFO",
            "stream": sys.stdout,
            "filters": ["console_info_filter"]
        },
        "console_error": {
            "class": "logging.StreamHandler",
            "formatter": "simple",
            "level": "WARNING",
            "stream": sys.stderr,
            "filters": ["error_only_filter"]
        },
        "file_debug": {
            "class": "logging.handlers.TimedRotatingFileHandler",
            "formatter": "verbose",
            "level": "DEBUG",
            "filename": str(LOG_DIR / f"{APP_NAME}_debug.log"),
            "when": "midnight",
            "backupCount": 7,
            "encoding": "utf-8",
        },
        "file_error": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "verbose",
            "level": "WARNING",
            "filename": str(LOG_DIR / f"{APP_NAME}_errors.log"),
            "maxBytes": 5 * 1024 * 1024,  # 5 MB
            "backupCount": 7,
            "encoding": "utf-8",
        },
        "file_info": {
            "class": "logging.handlers.TimedRotatingFileHandler",
            "formatter": "verbose",
            "level": "INFO",
            "filename": str(LOG_DIR / f"{APP_NAME}_info.log"),
            "when": "midnight",
            "backupCount": 30,  # Keep info logs longer
            "encoding": "utf-8",
        },
    }

def _ensure_log_directory():
    """Ensure log directory exists and is writable"""
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)

        # Test write permissions
        test_file = LOG_DIR / f".test_permissions_{os.getpid()}.tmp"
        test_file.touch()
        test_file.unlink()

        return LOG_DIR
    except (PermissionError, OSError) as e:
        print(f"WARNING: Cannot write to {LOG_DIR}: {e}")

        fallback_dir = (
            Path(tempfile.gettempdir()) / "ptt_crawler_logs" / LOG_DATE
        )
        try:
            fallback_dir.mkdir(parents=True, exist_ok=True)
            print(f"Using fallback log directory: {fallback_dir}")
            return fallback_dir
        except (PermissionError, OSError):
            fallback_dir = Path.cwd() / "logs" / LOG_DATE
            fallback_dir.mkdir(parents=True, exist_ok=True)
            print(f"Using current directory for logs: {fallback_dir}")
            return fallback_dir

def get_logging_config():
    """Generate logging configuration with proper paths"""
    # Ensure log directory exists
    actual_log_dir = _ensure_log_directory()

    # Update LOG_DIR global for handlers
    global LOG_DIR
    LOG_DIR = actual_log_dir

    # Get handlers with updated paths
    handlers = _get_log_handlers()

    # Logger configurations
    loggers = {
        APP_NAME: {
            "level": LOG_LEVEL,
            "handlers": [
                "console_info", "console_error",
                "file_debug", "file_error", "file_info"
                ],
            "propagate": False,
        },
        f"{APP_NAME}.crawler": {
            "level": LOG_LEVEL,
            "handlers": ["file_debug", "file_error"],
            "propagate": True,
        },
        f"{APP_NAME}.parser": {
            "level": LOG_LEVEL,
            "handlers": ["file_debug", "file_error"],
            "propagate": True,
        },
    }

    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": formatters,
        "filters": filters,
        "handlers": handlers,
        "loggers": loggers,
        "root": {
            "level": "WARNING",
            "handlers": ["console_error"],
        },
    }

def setup_logging():
    """Setup logging configuration with error handling"""
    try:
        config = get_logging_config()
        logging.config.dictConfig(config)
        logging.captureWarnings(True)

        logger = logging.getLogger(APP_NAME)
        logger.info("=" * 80)
        logger.info(f"Initialized logging for {APP_NAME}")
        logger.info(f"Log level: {LOG_LEVEL}")
        logger.info(f"Log directory: {LOG_DIR}")
        logger.info(f"Process ID: {os.getpid()}")
        logger.info("=" * 80)

        return logger

    except Exception as e:
        print(f"CRITICAL LOGGING ERROR: {e}")
        print(traceback.format_exc())

        # Fallback basic logging
        logging.basicConfig(
            level=getattr(logging, LOG_LEVEL, logging.INFO),
            format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            stream=sys.stdout
        )

        fallback_logger = logging.getLogger(APP_NAME)
        fallback_logger.error(
            "Failed to configure advanced logging, using basic config"
        )
        fallback_logger.error(f"Error: {e}")
        return fallback_logger

def get_logger(name=None):
    """Get a logger instance with proper configuration

    Args:
        name (str, optional): Logger name. Defaults to APP_NAME.

    Returns:
        logging.Logger: Configured logger instance
    """
    if name is None:
        name = APP_NAME
    elif not name.startswith(APP_NAME):
        name = f"{APP_NAME}.{name}"

    return logging.getLogger(name)

def log_system_info():
    """Log system information for debugging"""
    logger = get_logger("system")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Platform: {sys.platform}")
    logger.info(f"Working directory: {Path.cwd()}")
    logger.info(f"Log directory: {LOG_DIR}")
    logger.info(f"Environment LOG_LEVEL: {os.getenv('LOG_LEVEL', 'Not set')}")

def set_log_level(level):
    """Dynamically change log level for all loggers

    Args:
        level (str): Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    level = level.upper()
    if level not in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
        raise ValueError(f"Invalid log level: {level}")

    # Update main logger
    main_logger = logging.getLogger(APP_NAME)
    main_logger.setLevel(getattr(logging, level))

    # Update all child loggers
    for name in logging.root.manager.loggerDict:
        if name.startswith(APP_NAME):
            logger = logging.getLogger(name)
            logger.setLevel(getattr(logging, level))

    main_logger.info(f"Log level changed to: {level}")

# Context manager for temporary log level changes
class TemporaryLogLevel:
    """Context manager to temporarily change log level"""

    def __init__(self, level, logger_name=None):
        self.level = level.upper()
        self.logger_name = logger_name or APP_NAME
        self.original_level = None

    def __enter__(self):
        logger = logging.getLogger(self.logger_name)
        self.original_level = logger.level
        logger.setLevel(getattr(logging, self.level))
        return logger

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger = logging.getLogger(self.logger_name)
        logger.setLevel(self.original_level)