import logging
from abc import abstractmethod
from datetime import datetime, timedelta
from time import sleep
from tkinter import W
from typing import Iterable, Literal, Optional, overload

from srt.datasource.storage._sqlalchemy._tables.tradable.stock.price import (
    StockDailyPriceTable,
    StockMonthlyPriceTable,
    StockPriceCoverageTable,
    StockPriceTable,
    StockWeeklyPriceTable,
    StockYearlyPriceTable,
)
from srt.datasource.types.tradable import Tradable
from srt.datasource.types.tradable.stock import Stock
from srt.datasource.types.tradable.stock.price import StockPrice, StockPriceSource


class SQLAlchemyStockPriceSource(StockPriceSource):
    def __init__(self, freq: timedelta, data_table: StockPriceTable, session_factory):
        self._freq = freq
        self._table = data_table
        self._session_factory = session_factory
        self._logger = logging.getLogger(__name__)

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
        4. If no stock_set or stock is provided, fetch data for all possible stocks in the database.
        """

        # finally we want these
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

        last_end_time = start_time
        while end_time is None or end_time > datetime.now() - timedelta(days=1):
            # get datafrom database until there's no more data.
            session = self._session_factory()
            this_end_time = (
                min(end_time, datetime.now())
                if end_time is not None
                else datetime.now()
            )

            if all_stock:
                prices = (
                    session.query(self._table)
                    .filter(
                        self._table.start_time >= last_end_time,
                        self._table.end_time < this_end_time,
                    )
                    .all()
                )
            else:
                # slice the stock_set if its too large
                prices = []
                for i in range(0, len(stock_set), 50):
                    partial_prices = (
                        session.query(self._table)
                        .filter(
                            self._table.stock_id.in_(
                                [stock.id for stock in stock_set[i : i + 50]]
                            ),
                            self._table.start_time >= last_end_time,
                            self._table.end_time < this_end_time,
                        )
                        .all()
                    )
                    prices.extend(partial_prices)

            latest_date_time = None
            for price in prices:
                latest_date_time = (
                    price.end_time
                    if latest_date_time is None
                    else max(latest_date_time, price.end_time)
                )
                yield price.to_stock_price()

            session.close()

            if latest_date_time is not None:
                last_end_time = latest_date_time
            else:
                self._logger.info(
                    f"Data from {last_end_time} to {this_end_time} is missing."
                )
                if last_end_time < datetime.now() - (self._freq + timedelta(hours=1)):
                    self._logger.info(
                        "We have suffered from one hour delay + the frequency interval. We assume no more data would come if no action is taken."
                    )
                    break

            sleep(1 * 60 * 60)  # sleep for 1 hours to wait for new data to come in
