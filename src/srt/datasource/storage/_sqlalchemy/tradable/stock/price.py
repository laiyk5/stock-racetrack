import logging
from datetime import datetime, timedelta
from time import sleep
from typing import Callable, Iterable, Optional, overload

from sqlalchemy.orm import Session

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
    def __init__(
        self,
        freq: timedelta,
        data_table: type[StockPriceTable],
        session_factory: Callable[[], Session],
    ):
        super().__init__(freq)
        self._table = data_table
        self._session_factory = session_factory
        self._logger = logging.getLogger(__name__)

    def _search(
        self,
        start_time: datetime,
        end_time: datetime,
        all_stock: bool,
        stock_set: Iterable[Stock],
    ) -> Iterable[StockPrice]:
        # get datafrom database until there's no more data.
        session = self._session_factory()
        this_end_time = (
            min(end_time, datetime.now()) if end_time is not None else datetime.now()
        )

        left_bracket_filters = (
            self._table.start_time < start_time,
            self._table.end_time >= start_time,
        )
        inner_filters = (
            self._table.start_time >= start_time,
            self._table.end_time < this_end_time,
        )

        if all_stock:
            query = session.query(self._table).filter(
                left_bracket_filters[0]
                | left_bracket_filters[1]
                | inner_filters[0]
                | inner_filters[1]
            )
            records = query.all()
            for record in records:
                yield record.to_stock_price()
        else:
            stock_list = list(stock_set)
            if len(stock_list) == 0:
                session.close()
                return []
            if len(stock_list) > 100:
                self._logger.warning(
                    f"Searching for more than 100 stocks ({len(stock_list)}). This may take a while."
                )
                # slice into chunks of 100
                chunk_size = 100
                for i in range(0, len(stock_list), chunk_size):
                    chunk = stock_list[i : i + chunk_size]
                    stock_filter = self._table.stock_id.in_(
                        [f"{stock.market}.{stock.symbol}" for stock in chunk]
                    )
                    query = session.query(self._table).filter(
                        stock_filter
                        & (
                            left_bracket_filters[0]
                            | left_bracket_filters[1]
                            | inner_filters[0]
                            | inner_filters[1]
                        )
                    )
                    records = query.all()
                    for record in records:
                        yield record.to_stock_price()

        session.close()
