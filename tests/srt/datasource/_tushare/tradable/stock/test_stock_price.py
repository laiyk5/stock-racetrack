import os
from datetime import datetime
from json import load
from typing import TypeVar

import pytest
from dotenv import load_dotenv

from srt.datasource.types.tradable.stock import Stock

load_dotenv()

from srt.datasource._tushare.tradable.stock.price import (
    TushareStockDailyPriceSource,
    TushareStockMonthlyPriceSource,
    TushareStockWeeklyPriceSource,
)


class TestTushareStockPriceSource:
    @pytest.fixture(
        params=[
            TushareStockDailyPriceSource,
            TushareStockWeeklyPriceSource,
            TushareStockMonthlyPriceSource,
        ]
    )
    def tushare_stock_price_source_cls(self, request):
        return request.param

    TushareStockPriceSourceType = TypeVar(
        "TushareStockPriceSourceType",
        TushareStockDailyPriceSource,
        TushareStockWeeklyPriceSource,
        TushareStockMonthlyPriceSource,
    )

    def test_get_price_data(
        self, tushare_stock_price_source_cls: type[TushareStockPriceSourceType]
    ):
        api_token = os.getenv("TUSHARE_API_TOKEN")
        if api_token is None:
            raise KeyError("TUSHARE_API_TOKEN not set")

        price_source = tushare_stock_price_source_cls(api_token=api_token)

        stock = Stock(market="CN.SZSE", symbol="000001")

        start_time = datetime(2025, 11, 27)
        end_time = datetime(2025, 11, 28)

        prices = list(
            price_source.get_price_data(
                stock=stock,
                start_time=start_time,
                end_time=end_time,
            )
        )

        assert len(prices) > 0
        for price in prices:
            assert price.stock == stock
            assert start_time <= price.start_time <= end_time
