from pydantic import Field

from srt.datasource.types.tradable import Tradable, TradableSource


class Stock(Tradable):
    type: str = Field(default="stock", frozen=True)


class StockSource(TradableSource[Stock]):
    pass
