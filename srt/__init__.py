import configparser
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

# define default configurations

default_configurations = {
    "database": {
        "host": "localhost",
        "port": "5432",
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
