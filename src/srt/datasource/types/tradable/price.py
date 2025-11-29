from abc import ABC, abstractmethod
from datetime import datetime
from typing import Iterable, Optional, overload

from srt.datasource.types.data import TimeRangeData

from . import Tradable


class TradablePrice(TimeRangeData):
    open: float
    high: float
    low: float
    close: float
    volume: int


class TradableDailyData(TradablePrice):
    pass


class TradableWeeklyData(TradablePrice):
    pass


class TradableMonthlyData(TradablePrice):
    pass


class TradableYearlyData(TradablePrice):
    pass


class TradablePriceSource(ABC):
    @overload
    def get_price_data(
        self,
        *,
        tradable: Tradable,
        start_time: datetime,
        end_time: Optional[datetime] = None,
        **kwargs,
    ) -> Iterable[TradablePrice]: ...
    @overload
    def get_price_data(
        self,
        *,
        tradable_set: Tradable,
        start_time: datetime,
        end_time: Optional[datetime] = None,
        **kwargs,
    ) -> Iterable[TradablePrice]: ...
    @overload
    def get_price_data(
        self, *, tradable_set: Tradable, timestamp: datetime, **kwargs
    ) -> Iterable[TradablePrice]: ...
    @abstractmethod
    def get_price_data(self, **kwargs) -> Iterable[TradablePrice]:
        """Fetch price data for tradables based on various criteria.

        1. Omit end_time to get all data from start_time onward to the latest available. This might lead to an infinite stream if new data keeps coming in.
        2. Provide end_time to exclude data beyond and including that timestamp.
        3. Provide a specific timestamp to get data cover that exact time.
        """
        raise NotImplementedError


class TradablePriceDailySource(TradablePriceSource, ABC):
    pass


class TradablePriceWeeklySource(TradablePriceSource, ABC):
    pass


class TradablePriceMonthlySource(TradablePriceSource, ABC):
    pass


class TradablePriceYearlySource(TradablePriceSource, ABC):
    pass
