import configparser
import logging
import os
from pathlib import Path

from rich.logging import RichHandler

logger = logging.getLogger(__name__)

# define default configurations

default_configurations = {
    "database": {
        "host": "localhost",
        "port": "5433",
        "user": "postgres",
        "password": "password",
    },
    "app": {
        "debug": "false",
        "log_level": "INFO",
        "timezone": "Asia/Shanghai",
    },
}

config = configparser.ConfigParser()
config.read_dict(default_configurations)

# load configuration file if exists
config_dir = Path.home() / ".config" / "srt"
if not config_dir.exists():
    os.makedirs(config_dir)
config_file = config_dir / "config.ini"
if config_file.exists():
    config.read(config_file)
    logger.info(f"Loaded configuration from {config_file}")

logger.setLevel(config.get("app", "log_level", fallback="INFO"))
logger.addHandler(RichHandler())
