from datetime import datetime
from typing import Iterable, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator


class TimePointData(BaseModel):
    time: datetime


class Entity(BaseModel):
    pass


class TimeRangeData(BaseModel):
    start_time: datetime
    end_time: datetime


class Coverage(TimeRangeData):
    pass


class Exchange(Entity):
    country: str
    name: str

    @field_validator("country")
    def validate_country(cls, v: str) -> str:
        """Check if its a valid ISO 3166-1 alpha-2 country code."""
        if len(v) != 2 or not v.isalpha() or not v.isupper():
            raise ValueError(f"Invalid country code: {v}")
        return v

    @field_validator("name")
    def validate_name(cls, v: str) -> str:
        """Check if exchange name is alphanumeric and all uppercase."""
        if not v.isalnum() or not v.isupper():
            raise ValueError(f"Invalid exchange name: {v}")
        return v

    def to_string(self) -> str:
        return f"{self.country}.{self.name}"

    @classmethod
    def parse_string(cls, exchange_str: str) -> "Exchange":
        parts = exchange_str.split(".")
        if len(parts) != 2:
            raise ValueError(f"Invalid market format: {exchange_str}")

        return Exchange(country=parts[0], name=parts[1])


class Tradable(Entity):
    exchange: Exchange
    symbol: str
    type: Optional[str] = None  # e.g., "stock", "bond", etc.

    def __hash__(self) -> int:
        return hash((self.exchange.to_string(), self.symbol))


class TradablePrice(TimeRangeData):
    open: float
    high: float
    low: float
    close: float
    volume: int
