import logging
import os

import pytest
from dotenv import load_dotenv
from matplotlib.pylab import f

from srt.datasource._tushare.tradable.stock.stock import (
    TushareStockSource,
)

load_dotenv()
logger = logging.getLogger(__name__)


@pytest.fixture
def tushare_stock_source():
    api_token = os.getenv("TUSHARE_API_TOKEN")
    if api_token is None:
        raise KeyError("TUSHARE_API_TOKEN not set")
    yield TushareStockSource(api_token=api_token)


class TestTushareStockSource:
    def test_search_stocks(self, tushare_stock_source):
        stocks = list(
            tushare_stock_source.search_stocks(market="CN.SZSE", symbol="000001")
        )
        logger.info(f"Found stocks: {stocks[:5]}... and total {len(stocks)}")
        assert len(stocks) > 0
        for stock in stocks:
            assert stock.market == "CN.SZSE"
            assert stock.symbol == "000001"

    def test_get_all_stocks(self, tushare_stock_source):
        stocks = list(tushare_stock_source.get_all_stocks())
        assert len(stocks) > 1000  # There should be more than 1000 stocks
