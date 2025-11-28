import configparser
import logging
from datetime import datetime
from typing import Generic, Optional, TypeVar

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


from abc import ABC, abstractmethod

from pydantic import BaseModel


class DataSource(ABC):
    @abstractmethod
    def get_stock_basic_daily(self, symbol: str, start_date: str, end_date: str):
        raise NotImplementedError


class Entity(BaseModel, ABC):
    id: Optional[int] = None


class TimeSeriesData(BaseModel):
    time: datetime


TimeSeriesType = TypeVar("TimeSeriesType", bound=TimeSeriesData)
EntityType = TypeVar("EntityType", bound=Entity)


class EntityStorage(ABC, Generic[EntityType]):
    @abstractmethod
    def save(self, entity: EntityType) -> EntityType:
        raise NotImplementedError

    @abstractmethod
    def load(self, id: str) -> EntityType:
        raise NotImplementedError


class TimeSeriesStorage(ABC, Generic[EntityType, TimeSeriesType]):
    @abstractmethod
    def save(self, data: TimeSeriesType):
        raise NotImplementedError
