from abc import ABC, abstractmethod
from typing import Annotated, Generic, Iterable, Optional, TypeVar, overload

from attr import has
from pydantic import BeforeValidator, Field

from srt.datasource.types.data import Entity, TimeRangeData

# A market validator to validate the market input. The market should be in the form: <Country>.<Exchange> or <Country>
# And <Country> should be ISO 3166-1 alpha-2 code, e.g., US, CN, HK, etc.
# The <Exchange> should be the exchange code, e.g., NYSE, NASDAQ, SSE, SZSE, HKEX, etc.
# For example: US.NYSE, CN.SSE, HK.HKEX, etc


def parse_market(market: str) -> tuple[str, Optional[str]]:
    """Parse the market string into country and exchange parts."""
    parts = market.split(".")
    if len(parts) == 1:
        return parts[0], None
    elif len(parts) == 2:
        return parts[0], parts[1]
    else:
        raise ValueError(f"Invalid market format: {market}")


def validate_market(market: str) -> str:
    country_code, exchange_code = parse_market(market)

    def is_valid_country_code(code: str) -> bool:
        return len(code) == 2 and code.isalpha() and code.isupper()  # Simplified check

    parts = market.split(".")
    if len(parts) == 1:
        if not is_valid_country_code(parts[0]):
            raise ValueError(f"Invalid market format: {market}")
    elif len(parts) == 2:
        if not is_valid_country_code(parts[0]):
            raise ValueError(f"Invalid country code in market: {market}")
        if not parts[1].isalnum():
            raise ValueError(f"Invalid exchange code in market: {market}")
    else:
        raise ValueError(f"Invalid market format: {market}")

    return market


class Tradable(Entity):
    market: Annotated[
        str,
        BeforeValidator(validate_market),
        Field(..., description="Market identifier, e.g., 'US.NYSE'"),
    ]
    symbol: str
    type: str
    alias: Optional[str] = None

    def __hash__(self) -> int:
        return hash((self.market, self.symbol, self.type))


TradableType = TypeVar("TradableType", bound=Tradable)


class TradableSource(ABC, Generic[TradableType]):
    @abstractmethod
    def get_all_stocks(self) -> Iterable[TradableType]:
        raise NotImplementedError

    @overload
    def search_stocks(self, *, market: str, **kwargs) -> Iterable[TradableType]: ...
    @overload
    def search_stocks(self, *, symbol: str, **kwargs) -> Iterable[TradableType]: ...
    @overload
    def search_stocks(self, *, alias: str, **kwargs) -> Iterable[TradableType]: ...
    @overload
    def search_stocks(self, *, type: str, **kwargs) -> Iterable[TradableType]: ...
    @overload
    def search_stocks(
        self, *, market: str, symbol: str, **kwargs
    ) -> Iterable[TradableType]: ...
    @overload
    def search_stocks(
        self, *, market: str, type: str, **kwargs
    ) -> Iterable[TradableType]: ...
    @overload
    def search_stocks(
        self, *, market: str, alias: str, **kwargs
    ) -> Iterable[TradableType]: ...
    @overload
    def search_stocks(
        self, *, market: str, symbol: str, type: str, **kwargs
    ) -> TradableType | None: ...
    @abstractmethod
    def search_stocks(self, **kwargs) -> Iterable[TradableType] | TradableType | None:
        raise NotImplementedError
