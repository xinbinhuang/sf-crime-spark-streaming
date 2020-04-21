import json
import logging
import logging.config
from pathlib import Path

logging.config.fileConfig(Path(__file__).parents[0] / "logging.ini")  # noqa


def get_logger(name: str) -> logging.Logger:
    """Return a logger"""
    return logging.getLogger(name)


def serialize_record(record: dict, encoding="utf-8"):
    """Serialzie a record dict into encoded JSON bytes"""
    return json.dumps(record).encode(encoding)
