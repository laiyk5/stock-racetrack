import logging
from datetime import datetime, timedelta
from time import sleep
from typing import Iterable, Literal, Optional, overload

from srt.datasource._tushare.tradable.utils import (
    translate_market_and_symbol_to_ts_code,
    translate_ts_code_to_market_and_symbol,
)
from srt.datasource.types.tradable import Tradable
from srt.datasource.types.tradable.stock import Stock, StockSource
from srt.datasource.types.tradable.stock.price import (
    StockDailyPrice,
    StockDailyPriceSource,
    StockMonthlyPrice,
    StockMonthlyPriceSource,
    StockPrice,
    StockPriceSource,
    StockWeeklyPrice,
    StockWeeklyPriceSource,
    StockYearlyPrice,
    StockYearlyPriceSource,
)

logger = logging.getLogger(__name__)


def _translate_ts_price_to_stock_price(row) -> StockPrice:
    market, symbol = translate_ts_code_to_market_and_symbol(row["ts_code"])
    stock = Stock(
        market=market,
        symbol=symbol,
        type="stock",
    )

    trade_date = datetime.strptime(row["trade_date"], "%Y%m%d")

    volume = int(row["vol"])
    bias = volume - row["vol"]
    if bias > 1e-6:
        logger.warning(
            f"Volume has fractional part: {row['vol']} for stock {stock.market}.{stock.symbol} on {trade_date}"
        )
        if bias > 1:
            logger.error(
                f"Significant fractional part in volume: {row['vol']} for stock {stock.market}.{stock.symbol} on {trade_date}"
            )
            raise ValueError("Significant fractional part in volume")

    return StockPrice(
        stock=stock,
        start_time=trade_date,
        end_time=trade_date + timedelta(days=1),
        open=row["open"],
        high=row["high"],
        low=row["low"],
        close=row["close"],
        volume=int(row["vol"]),
    )


class TushareStockPriceSource(StockPriceSource):
    def __init__(self, api_token: str, freq: Literal["weekly", "monthly", "daily"]):
        from tushare import pro_api

        self._api = pro_api(api_token)
        self._logger = logging.getLogger(__name__)
        self._freq = freq

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
        stock_set: list[Stock] = []

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

        tushare_params = {}

        # time range or time stamp setting
        if "timestamp" in kwargs:
            date_str = kwargs["timestamp"].strftime("%Y%m%d")
            tushare_params["trade_date"] = date_str
        else:
            if start_time is not None:
                tushare_params["start_date"] = start_time.strftime("%Y%m%d")
            if end_time is not None:
                tushare_params["end_date"] = end_time.strftime("%Y%m%d")

        tushare_params_a_round = []
        # if too many stocks, we need to split the query
        if not all_stock and len(stock_set) > 100:
            for i in range(0, len(stock_set), 100):
                partial_ts_code = [
                    translate_market_and_symbol_to_ts_code(stock.market, stock.symbol)
                    for stock in stock_set[i : i + 100]
                ]
                partial_params = tushare_params.copy()
                partial_params["ts_code"] = ",".join(partial_ts_code)
                tushare_params_a_round.append(partial_params)

                # query the data
                df = self._api.query(self._freq, **partial_params)

                for _, row in df.iterrows():
                    yield _translate_ts_price_to_stock_price(row)
            return
        else:
            if not all_stock:
                ts_code = [
                    translate_market_and_symbol_to_ts_code(stock.market, stock.symbol)
                    for stock in stock_set
                ]
                tushare_params["ts_code"] = ",".join(ts_code)
                tushare_params_a_round = [tushare_params]
            else:
                tushare_params_a_round = [tushare_params]

        while start_time < end_time:
            max_time = None
            for tushare_params in tushare_params_a_round:
                # query the data
                df = self._api.query(self._freq, **tushare_params)

                for _, row in df.iterrows():
                    max_time = (
                        max(max_time, datetime.strptime(row["trade_date"], "%Y%m%d"))
                        if max_time is not None
                        else datetime.strptime(row["trade_date"], "%Y%m%d")
                    )
                    yield _translate_ts_price_to_stock_price(row)

            if max_time is None:
                self._logger.info(
                    f"No data returned from Tushare for params: {tushare_params}"
                )
            else:
                start_time = max_time + timedelta(days=1)
                tushare_params["start_date"] = max_time.strftime("%Y%m%d")
                tushare_params["end_date"] = min(end_time, datetime.now()).strftime(
                    "%Y%m%d"
                )

            if end_time < datetime.now():
                break

            sleep(timedelta(hours=1).total_seconds())


class TushareStockDailyPriceSource(TushareStockPriceSource, StockDailyPriceSource):
    def __init__(self, api_token: str):
        super().__init__(api_token, freq="daily")


class TushareStockWeeklyPriceSource(TushareStockPriceSource, StockWeeklyPriceSource):
    def __init__(self, api_token: str):
        super().__init__(api_token, freq="weekly")


class TushareStockMonthlyPriceSource(TushareStockPriceSource, StockMonthlyPriceSource):
    def __init__(self, api_token: str):
        super().__init__(api_token, freq="monthly")
