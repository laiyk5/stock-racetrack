from abc import ABC, abstractmethod
from datetime import datetime
from typing import Iterable, Optional

from sqlalchemy.orm import Session

from srt.datasource.data import Tradable
from srt.datasource.tables import DataInfoTable


class Updater(ABC):

    DATA_NAME: str | None = None

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls.DATA_NAME is None:
            raise TypeError(f"{cls.__name__} must set DATA_NAME")

    @abstractmethod
    def update(self):
        """Update data from the latest to now."""
        ...

    @classmethod
    def register_data_info(cls, session: Session):
        """Register data info in the database."""

        if cls.DATA_NAME is None:
            raise ValueError("DATA_NAME can not be None.")

        # register data info for stock daily price
        stock_daily_price_data_info = DataInfoTable(
            name=cls.DATA_NAME,
            description=None,
            last_updated=None,
        )
        session.add(stock_daily_price_data_info)

    @classmethod
    def get_data_info_id(cls, session: Session) -> int:
        """Get data info id from the database."""
        if cls.DATA_NAME is None:
            raise ValueError("DATA_NAME can not be None.")

        data_info = (
            session.query(DataInfoTable)
            .filter(DataInfoTable.name == cls.DATA_NAME)
            .one_or_none()
        )
        if data_info is None:
            raise ValueError(f"Data info {cls.DATA_NAME} not found in the database.")
        return data_info.id  # type: ignore # ID would never be None beacause the data_info is retrieved from database.


class StockDownloader(Updater):
    """Downloader for fetching time range tradable data from external sources."""

    DATA_NAME = "stock"


class StockDailyPriceDownloader(Updater):
    """Downloader for fetching time range stock price data from external sources."""

    DATA_NAME = "stock_daily_price"

    @abstractmethod
    def download(
        self,
        start_time: datetime,
        end_time: datetime,
        tradable_set: Optional[Iterable[Tradable]] = None,
    ): ...
