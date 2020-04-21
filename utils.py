from configparser import ConfigParser
import json
import logging
import logging.config
from pathlib import Path

logging.config.fileConfig(Path(__file__).parents[0] / "logging.ini")  # noqa

CONF_FILE = "app.cfg"


def get_logger(name: str) -> logging.Logger:
    """Return a logger"""
    return logging.getLogger(name)


def load_config() -> ConfigParser:
    """Load in config"""
    config = ConfigParser()
    config.read(CONF_FILE)
    return config


class JsonSerializer:
    """A helper class to (de)serialize encoded JSON bytes

    Attributes:
        encoding (str): encoding used for serialization, default "utf-8"
    """

    def __init__(self, encoding: str = "utf-8"):
        self.encoding = encoding

    def serialize(self, record: dict):
        """Serialzie a record dict into encoded JSON bytes"""
        return json.dumps(record).encode(self.encoding)

    def deserialize(self, record: bytes):
        """Deserialzie a encoded JSON bytes record into dict"""
        return json.loads(record.decode(self.encoding))
