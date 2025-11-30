import configparser
import logging
import os
from functools import cache
from pathlib import Path

# Load configuration from config.ini.
# The config root should be parsed as followed:
# ./.config/srt/
# ~/.config/srt/


logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger(__name__)


USER_CONFIG_PATH = Path(os.path.expanduser("~")) / ".config" / "srt" / "config.ini"


@cache
def get_config() -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    default_config_dict = {
        "database": {
            "url": "",
        },
        "tushare": {
            "api_token": "",
        },
    }

    config.read_dict(default_config_dict, source="default_config_dict")
    config_filenames = config.read(
        (
            Path(os.getcwd()) / ".config" / "srt" / "config.ini",
            Path(os.path.expanduser("~")) / ".config" / "srt" / "config.ini",
        )
    )

    config_root = config_filenames[0] if config_filenames else None

    logger.debug(f"Configuration files loaded: {config_filenames}")

    if config_root:
        logger.info(f"Using configuration file: {config_root}")
    else:
        logger.info("No configuration file found; using default settings.")

    return config
