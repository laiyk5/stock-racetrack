import configparser
import logging
from copy import deepcopy

import srt

logger = logging.getLogger(__name__)

parent_config = srt.config

default_config = {
    "tushare": {
        "token": "",
    },
    "database": {
        "dbname": "srt_ds",
    },
}

config = configparser.ConfigParser()
config.read_dict(parent_config)
config.read_dict(default_config)

config_dir = srt.config_dir / "downloader"
if not config_dir.exists():
    config_dir.mkdir(parents=True, exist_ok=True)
config_file = config_dir / "config.ini"
if config_file.exists():
    config.read(config_file)
