"""Tests after tushare stock source tests are stable."""

import os

import pytest
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from srt.datasource.storage._sqlalchemy._tables import Base
from srt.datasource.storage._sqlalchemy.tradable.stock.stock import (
    SQLAlchemyStockSource,
)


class TestSQLAlchemyStockSource:
    def test_get_all_stocks(self):
        from srt.datasource.storage._sqlalchemy._tables.tradable.stock.stock import (
            StockTable,
        )

        engine = create_engine("sqlite:///:memory:", echo=False)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)

        sqlalchemy_stock_source = SQLAlchemyStockSource(
            session_factory=Session, external_source=None
        )
        stocks = list(sqlalchemy_stock_source.get_all_stocks())
        assert len(stocks) == 0  # There should be no stocks in an empty database

    def test_get_all_stocks_with_external_source(self):
        from srt.datasource.storage._sqlalchemy._tables.tradable.stock.stock import (
            StockTable,
        )

        # engine = create_engine("sqlite:///:memory:", echo=False)
        engine = create_engine("sqlite:///:memory:", echo=False)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)

        from dotenv import load_dotenv

        load_dotenv()

        from srt.datasource._tushare.tradable.stock.stock import TushareStockSource

        api_key = os.getenv("TUSHARE_API_TOKEN")
        if api_key is None:
            raise KeyError("TUSHARE_API_TOKEN not set")
        tushare_stock_source = TushareStockSource(api_token=api_key)
        sqlalchemy_stock_source = SQLAlchemyStockSource(
            session_factory=Session, external_source=tushare_stock_source
        )
        stocks = list(sqlalchemy_stock_source.get_all_stocks())
        assert len(stocks) > 1000  # There should be more than 1000 stocks
