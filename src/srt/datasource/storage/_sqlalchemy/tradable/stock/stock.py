from abc import abstractmethod
from typing import Iterable, Optional, overload

from srt.datasource.storage._sqlalchemy._tables.tradable.stock.stock import StockTable
from srt.datasource.types.tradable.stock import Stock, StockSource


class SQLAlchemyStockSource(StockSource):
    def __init__(self, session_factory, external_source: Optional[StockSource] = None):
        self._session_factory = session_factory
        self._external_source = external_source

    def get_all_stocks(self) -> Iterable[Stock]:
        if self._external_source is not None:
            self._update_database_if_needed()
        # Fetch all stocks from the database
        session = self._session_factory()
        from srt.datasource.storage._sqlalchemy._tables.tradable.stock.stock import (
            StockTable,
        )

        stock_records = session.query(StockTable).all()
        for record in stock_records:
            yield Stock(
                market=record.market,
                symbol=record.symbol,
                alias=record.alias,
            )
        session.close()

    @overload
    def search_stocks(self, *, market: str, **kwargs) -> Iterable[Stock]: ...
    @overload
    def search_stocks(self, *, symbol: str, **kwargs) -> Iterable[Stock]: ...
    @overload
    def search_stocks(self, *, alias: str, **kwargs) -> Iterable[Stock]: ...
    @overload
    def search_stocks(self, *, type: str, **kwargs) -> Iterable[Stock]: ...
    @overload
    def search_stocks(
        self, *, market: str, symbol: str, **kwargs
    ) -> Iterable[Stock]: ...
    @overload
    def search_stocks(self, *, market: str, type: str, **kwargs) -> Iterable[Stock]: ...
    @overload
    def search_stocks(
        self, *, market: str, alias: str, **kwargs
    ) -> Iterable[Stock]: ...
    @overload
    def search_stocks(
        self, *, market: str, symbol: str, type: str, **kwargs
    ) -> Stock | None: ...
    def search_stocks(self, **kwargs) -> Iterable[Stock] | Stock | None:
        """
        Search stocks based on various criteria. If no stocks are presented in the database,
        or the database has incomplete or outdated data, fetch from the external source and store them in the database.
        """

        if self._external_source is not None:
            self._update_database_if_needed()

        # Fetch stocks from the database based on the provided criteria
        session = self._session_factory()

        query = session.query(StockTable)

        # Apply filters based on kwargs
        if "market" in kwargs:
            query = query.filter(StockTable.market == kwargs["market"])
        if "symbol" in kwargs:
            query = query.filter(StockTable.symbol == kwargs["symbol"])
        if "alias" in kwargs:
            query = query.filter(StockTable.alias == kwargs["alias"])
        if "type" in kwargs:
            query = query.filter(StockTable.type == kwargs["type"])

        stock_records = query.all()

        # If market, symbol, and type are all provided, return a single Stock or None
        if "market" in kwargs and "symbol" in kwargs and "type" in kwargs:
            if stock_records:
                record = stock_records[0]
                result = Stock(
                    market=record.market,
                    symbol=record.symbol,
                    alias=record.alias,
                )
                session.close()
                return result
            else:
                session.close()
                return None

        # Otherwise, return an iterable of stocks
        def generate_stocks():
            for record in stock_records:
                yield Stock(
                    market=record.market,
                    symbol=record.symbol,
                    alias=record.alias,
                )
            session.close()

        return generate_stocks()

    def _update_database_if_needed(self):
        # if the database is updated one day ago, we consider it as outdated, and we would fully refresh the data
        from datetime import datetime, timedelta

        session = self._session_factory()
        from srt.datasource.storage._sqlalchemy._tables import Base
        from srt.datasource.storage._sqlalchemy._tables.tradable.stock.stock import (
            StockTable,
        )

        last_updated_record: StockTable | None = (
            session.query(StockTable).order_by(StockTable._last_updated).first()
        )

        if (
            last_updated_record is None
            or last_updated_record._last_updated is None
            or last_updated_record._last_updated < datetime.now() - timedelta(days=1)
        ):
            # Clear the table
            session.query(StockTable).delete()

            # Fetch all stocks from external source
            all_stocks = (
                self._external_source.get_all_stocks() if self._external_source else []
            )
            for stock in all_stocks:
                stock_record = StockTable(
                    market=stock.market,
                    symbol=stock.symbol,
                    alias=stock.alias,
                    _last_updated=datetime.now(),
                )
                session.add(stock_record)
            session.commit()
