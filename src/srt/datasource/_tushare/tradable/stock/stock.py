import logging
from abc import ABC, abstractmethod
from typing import Iterable, overload

from tushare import pro_api
from tushare.pro.client import DataApi

from srt.datasource._tushare.tradable.utils import (
    translate_market_and_symbol_to_ts_code,
    translate_ts_code_to_market_and_symbol,
)
from srt.datasource.types.tradable import Tradable, parse_market
from srt.datasource.types.tradable.stock import Stock, StockSource


class TushareStockSource(StockSource):
    def __init__(self, api_token: str):
        self._api: DataApi = pro_api(api_token)
        self._logger = logging.getLogger(__name__)

    def _translate_ts_stock_to_stock(self, ts_code: str, name: str) -> Stock:
        market, symbol = translate_ts_code_to_market_and_symbol(ts_code)
        return Stock(
            market=market,
            symbol=symbol,
            alias=name,
            type="stock",
        )

    def get_all_stocks(self) -> Iterable[Tradable]:
        df = self._api.stock_basic(
            exchange="",
            list_status="L",
            fields="ts_code,symbol,name,area,industry,list_date",
        )

        for _, row in df.iterrows():
            try:
                yield self._translate_ts_stock_to_stock(row["ts_code"], row["name"])
            except ValueError as e:
                self._logger.warning(
                    f"Skipping stock with ts_code {row['ts_code']} due to error: {e}"
                )
                continue

    @overload
    def search_stocks(self, *, market: str, **kwargs) -> Iterable[Tradable]: ...
    @overload
    def search_stocks(self, *, symbol: str, **kwargs) -> Iterable[Tradable]: ...
    @overload
    def search_stocks(self, *, alias: str, **kwargs) -> Iterable[Tradable]: ...
    @overload
    def search_stocks(self, *, type: str, **kwargs) -> Iterable[Tradable]: ...
    @overload
    def search_stocks(
        self, *, market: str, symbol: str, **kwargs
    ) -> Iterable[Tradable]: ...
    @overload
    def search_stocks(
        self, *, market: str, type: str, **kwargs
    ) -> Iterable[Tradable]: ...
    @overload
    def search_stocks(
        self, *, market: str, alias: str, **kwargs
    ) -> Iterable[Tradable]: ...
    @overload
    def search_stocks(
        self, *, market: str, symbol: str, type: str, **kwargs
    ) -> Tradable | None: ...
    def search_stocks(self, **kwargs) -> Iterable[Tradable] | Tradable | None:
        # Implement search logic based on kwargs if needed
        LEGAL_KEYS = {"market", "symbol", "alias", "type"}
        # drop illegal keys
        filter_kwargs = {k: v for k, v in kwargs.items() if k in LEGAL_KEYS}
        if len(filter_kwargs) == 0:
            return self.get_all_stocks()

        illegal_keys = set(kwargs.keys()) - LEGAL_KEYS
        if len(illegal_keys) > 0:
            self._logger.warning(
                f"Dropping illegal keys in search_stocks: {illegal_keys}"
            )

        params = {}

        _, exchange = (
            parse_market(filter_kwargs["market"])
            if "market" in filter_kwargs
            else (None, None)
        )
        if exchange is not None or exchange in {"SSE", "SZSE", "BSE"}:
            params["exchange"] = exchange if exchange is not None else ""

        if "symbol" in filter_kwargs:
            ts_code = translate_market_and_symbol_to_ts_code(
                filter_kwargs.get("market", ""), filter_kwargs["symbol"]
            )
            params["ts_code"] = ts_code

        if "alias" in filter_kwargs:
            params["name"] = filter_kwargs["alias"]

        if "type" in filter_kwargs:
            if filter_kwargs["type"] != "stock":
                return []  # No match since this is stock source

        df = self._api.stock_basic(
            **params,
            list_status="L",
            fields="ts_code,symbol,name,area,industry,list_date",
        )

        if (
            "market" in filter_kwargs
            and "symbol" in filter_kwargs
            and "type" in filter_kwargs
        ):
            # If we are searching for a specific stock, return None if not found
            if df.empty:
                return None
            if len(df) > 1:
                self._logger.warning(
                    f"Multiple stocks found for market {filter_kwargs['market']} and symbol {filter_kwargs['symbol']}. Returning the first one."
                )
            row = df.iloc[0]
            return self._translate_ts_stock_to_stock(row["ts_code"], row["name"])

        for _, row in df.iterrows():
            try:
                stock = self._translate_ts_stock_to_stock(row["ts_code"], row["name"])
                yield stock
            except ValueError as e:
                self._logger.warning(
                    f"Skipping stock with ts_code {row['ts_code']} due to error: {e}"
                )
                continue
