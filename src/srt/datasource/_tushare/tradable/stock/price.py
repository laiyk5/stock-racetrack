import logging
from abc import ABC
from datetime import datetime, time, timedelta
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


class TushareStockPriceSource(StockPriceSource, ABC):
    def __init__(self, api_token: str, api_name: Literal["daily", "weekly", "monthly"]):
        _freq_df = {
            "daily": timedelta(days=1),
            "weekly": timedelta(weeks=1),
            "monthly": timedelta(days=30),
        }

        self._api_name = api_name

        super().__init__(_freq_df[api_name])

        from tushare import pro_api

        self._api = pro_api(api_token)
        self._logger = logging.getLogger(__name__)

    def _search(
        self,
        start_time: datetime,
        end_time: datetime,
        all_stock: bool,
        stock_set: Iterable[Stock],
    ) -> Iterable[StockPrice]:

        if all_stock:
            start_date = start_time.date()
            end_date = end_time.date()
            # loop over each day to avoid API limit
            current_date = start_date
            while current_date < end_date:
                params = {
                    "trade_date": current_date.strftime("%Y%m%d"),
                }
                df = self._api.query(api_name=self._api_name, **params)
                for _, row in df.iterrows():
                    yield _translate_ts_price_to_stock_price(row)
                sleep(0.5)  # to avoid rate limit
                current_date += self._freq
            return
        else:
            params = {
                "start_date": start_time.strftime("%Y%m%d"),
                "end_date": end_time.strftime("%Y%m%d"),
            }
            ts_code_list = [
                translate_market_and_symbol_to_ts_code(stock.market, stock.symbol)
                for stock in stock_set
            ]

            ts_code_chunks = [
                ts_code_list[i : i + 100] for i in range(0, len(ts_code_list), 100)
            ]

            for ts_code_chunk in ts_code_chunks:
                if len(ts_code_chunk) == 0:
                    break
                params["ts_code"] = ",".join(ts_code_chunk)
                df = self._api.query(api_name=self._api_name, **params)
                for _, row in df.iterrows():
                    yield _translate_ts_price_to_stock_price(row)
                sleep(0.5)  # to avoid rate limit


class TushareStockDailyPriceSource(TushareStockPriceSource):
    def __init__(self, api_token: str):
        super().__init__(api_token, api_name="daily")


class TushareStockWeeklyPriceSource(TushareStockPriceSource):
    def __init__(self, api_token: str):
        super().__init__(api_token, api_name="weekly")


class TushareStockMonthlyPriceSource(TushareStockPriceSource):
    def __init__(self, api_token: str):
        super().__init__(api_token, api_name="monthly")


if __name__ == "__main__":
    import os

    from dotenv import load_dotenv

    load_dotenv()
    api_token = os.getenv("TUSHARE_API_TOKEN")
    if api_token is None:
        raise KeyError("TUSHARE_API_TOKEN not set")

    source = TushareStockDailyPriceSource(api_token=api_token)
    stock = Stock(market="CN.SZSE", symbol="000001", type="stock")
    prices = list(
        source.get_price_data(
            start_time=datetime(2023, 6, 1),
            end_time=datetime(2023, 6, 5),
        )
    )
    for price in prices:
        print(price)
