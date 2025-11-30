import sys
import types
from datetime import datetime, timedelta

import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from srt.datasource.data import Coverage
from srt.datasource.downloader.downloader import (
    StockDailyPriceDownloader,
    StockDownloader,
)
from srt.datasource.downloader.tushare import (
    TushareStockDailykPriceDownloader,
    TushareStockDownloader,
)
from srt.datasource.tables import Base, TradablePriceTable, TradableTable


@pytest.fixture
def session_factory():
    """Spin up an isolated in-memory database for each test run."""

    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine)

    session = factory()
    StockDownloader.register_data_info(session)
    StockDailyPriceDownloader.register_data_info(session)
    session.commit()
    session.close()

    return factory


@pytest.fixture
def tradable_id(session_factory):
    session = session_factory()
    tradable = TradableTable(
        exchange="CN.SZSE",
        symbol="000001",
        alias="Ping An Bank",
        type="stock",
        data_info_id=StockDownloader.get_data_info_id(session),
    )
    session.add(tradable)
    session.commit()
    session.refresh(tradable)
    tid = tradable.id
    session.close()
    return tid


def test_update_inserts_tradables(monkeypatch, session_factory):
    records = [
        {
            "ts_code": "000001.SZ",
            "symbol": "000001",
            "name": "Ping An Bank",
            "area": "Shenzhen",
            "industry": "Banking",
            "list_date": "19910403",
        },
        {
            "ts_code": "600000.SH",
            "symbol": "600000",
            "name": "Shanghai Bank",
            "area": "Shanghai",
            "industry": "Banking",
            "list_date": "19920101",
        },
    ]
    fake_df = pd.DataFrame.from_records(records)

    class FakeProAPI:
        def __init__(self, token: str):
            self.token = token
            self.calls = 0

        def stock_basic(self, **kwargs):
            self.calls += 1
            return fake_df

    fake_module = types.SimpleNamespace(pro_api=lambda token: FakeProAPI(token))
    monkeypatch.setitem(sys.modules, "tushare", fake_module)

    downloader = TushareStockDownloader("fake-token", session_factory)
    downloader.update()

    session = session_factory()
    try:
        stored = session.query(TradableTable).order_by(TradableTable.symbol).all()

        assert len(stored) == 2
        assert stored[0].exchange.endswith("SZSE")
        assert stored[0].alias == "Ping An Bank"
        assert stored[1].exchange.endswith("SSE")
        assert stored[1].alias == "Shanghai Bank"
        data_info_id = StockDownloader.get_data_info_id(session)
        assert {row.data_info_id for row in stored} == {data_info_id}
    finally:
        session.close()


def test_download_inserts_price_data(monkeypatch, session_factory, tradable_id):
    fake_price_df = pd.DataFrame.from_records(
        [
            {
                "ts_code": "000001.SZ",
                "trade_date": "20251127",
                "open": 10.0,
                "high": 11.0,
                "low": 9.5,
                "close": 10.5,
                "vol": 1000,
            }
        ]
    )

    class FakeProAPI:
        def __init__(self, token: str):
            self.token = token
            self.calls = []

        def daily(self, **kwargs):
            self.calls.append(kwargs)
            return fake_price_df

    fake_module = types.SimpleNamespace(pro_api=lambda token: FakeProAPI(token))
    monkeypatch.setitem(sys.modules, "tushare", fake_module)

    start_time = datetime(2025, 11, 27)
    end_time = datetime(2025, 11, 27, 23, 59, 59)

    def fake_get_missing_coverages(session, tradables, *_):
        return {tradable_id: [Coverage(start_time=start_time, end_time=end_time)]}

    monkeypatch.setattr(
        "srt.datasource.downloader.tushare.get_missing_coverages",
        fake_get_missing_coverages,
    )

    downloader = TushareStockDailykPriceDownloader(
        session_factory=session_factory,
        api_token="fake-token",
    )

    downloader.download(
        start_time=start_time,
        end_time=end_time,
        tradable_set=None,
    )

    session = session_factory()
    try:
        prices = session.query(TradablePriceTable).all()
        assert len(prices) == 1
        record = prices[0]
        assert record.tradable_id == tradable_id
        assert record.open == 10.0
        assert record.high == 11.0
        assert record.low == 9.5
        assert record.close == 10.5
        assert record.volume == 1000
        assert record.end_time - record.start_time == timedelta(days=1)
        assert record.data_info_id == StockDailyPriceDownloader.get_data_info_id(
            session
        )
    finally:
        session.close()
