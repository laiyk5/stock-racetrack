from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Iterable, Optional, overload

from tenacity import sleep

from srt.datasource.types.tradable.price import TradablePrice
from srt.datasource.types.tradable.stock import Stock

from .. import Tradable


class StockPrice(TradablePrice):
    stock: Stock
    pass


class StockDailyPrice(StockPrice):
    pass


class StockWeeklyPrice(StockPrice):
    pass


class StockMonthlyPrice(StockPrice):
    pass


class StockPriceSource(ABC):

    def __init__(self, freq: timedelta, tolerance: timedelta = timedelta(minutes=0)):
        self._freq = freq
        self._tolerance = tolerance

    @overload
    def get_price_data(
        self, *, timestamp: datetime, **kwargs
    ) -> Iterable[StockPrice]: ...
    @overload
    def get_price_data(
        self, *, start_time: datetime, end_time: Optional[datetime] = None, **kwargs
    ) -> Iterable[StockPrice]: ...
    @overload
    def get_price_data(
        self,
        *,
        stock: Stock,
        start_time: datetime,
        end_time: Optional[datetime] = None,
        **kwargs,
    ) -> Iterable[StockPrice]: ...
    @overload
    def get_price_data(
        self,
        *,
        stock_set: Iterable[Stock],
        start_time: datetime,
        end_time: Optional[datetime] = None,
        **kwargs,
    ) -> Iterable[StockPrice]: ...
    @overload
    def get_price_data(
        self, *, stock_set: Iterable[Stock], timestamp: datetime, **kwargs
    ) -> Iterable[StockPrice]: ...
    def get_price_data(self, **kwargs) -> Iterable[StockPrice]:
        """Fetch price data for stocks based on various criteria.

        1. Omit end_time to get all data from start_time onward to the latest available. This might lead to an infinite stream if new data keeps coming in.
        2. Provide end_time to exclude data beyond and including that timestamp.
        3. Provide a specific timestamp to get data cover that exact time.
        """
        start_time, end_time, all_stock, stock_set = (
            self._parse_input_of_get_price_data(kwargs)
        )

        cur_start_time = start_time

        first_time = True
        while first_time or end_time + self._tolerance > datetime.now():
            if not first_time:
                sleep(
                    self._freq.total_seconds() / 2
                )  # sleep for half freq to avoid busy waiting
            first_time = False
            prices = list(self._search(cur_start_time, end_time, all_stock, stock_set))
            # use latset_time in prices to update cur_start_time
            cur_start_time = max(
                [price.end_time for price in prices], default=cur_start_time
            )
            yield from prices

    def _parse_input_of_get_price_data(self, kwargs):
        start_time = kwargs.get("start_time", None)
        end_time = kwargs.get("end_time", None)
        all_stock = False
        stock_set = []

        if start_time is None:
            if "timestamp" in kwargs:
                start_time = kwargs["timestamp"]
                end_time = start_time + self._freq
                all_stock = True
            else:
                raise ValueError("Either start_time or timestamp must be provided")
        else:
            if "timestamp" in kwargs:
                raise ValueError("Cannot provide both start_time and timestamp")

            end_time = datetime.max if end_time is None else end_time

        # set stock_set
        if "stock" in kwargs:
            stock = kwargs["stock"]
            stock_set = [stock]
        elif "stock_set" in kwargs:
            stock_set = kwargs["stock_set"]
            stock_set = list(set(stock_set))
        else:
            all_stock = True
        return (
            start_time,
            end_time,
            all_stock,
            stock_set,
        )  # sleep for 1 hours to wait for new data to come in

    @abstractmethod
    def _search(
        self,
        start_time: datetime,
        end_time: datetime,
        all_stock: bool,
        stock_set: Iterable[Stock],
    ) -> Iterable[StockPrice]:
        raise NotImplementedError


class StockDailyPriceSource(StockPriceSource, ABC):
    pass


class StockWeeklyPriceSource(StockPriceSource, ABC):
    pass


class StockMonthlyPriceSource(StockPriceSource, ABC):
    pass
