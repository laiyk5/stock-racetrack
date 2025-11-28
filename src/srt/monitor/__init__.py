from abc import ABC, abstractmethod
from datetime import datetime
from typing import Generic, Iterable, Literal, Optional, TypeVar
from zoneinfo import ZoneInfo

from pydantic import BaseModel


class Event(BaseModel, ABC):
    event_timestamp: datetime


E = TypeVar("E", bound=Event)


class EventSource(ABC, Generic[E]):
    @abstractmethod
    def fetch(self, start_time: datetime, end_time: datetime) -> Iterable[E]:
        """Fetch events in the given time range."""
        raise NotImplementedError


class Currency(BaseModel):
    code: str  # e.g., "USD", "CNY", "EUR"
    alias: Optional[str] = None


class Security(BaseModel):
    market: str
    symbol: str
    type: Literal["share", "etf", "bond", "fund"] = "share"
    alias: Optional[str] = None


class Asset(BaseModel):
    target: Security | Currency
    quantity: float
    average_cost: float  # per unit


class Portfolio(BaseModel):
    assets: list[Asset]


class PortfolioProvider(ABC):
    @abstractmethod
    def get_portfolio(self) -> Portfolio:
        raise NotImplementedError


class DeltaAsset(BaseModel):
    target: Security | Currency
    delta_quantity: float
    target_price: float  # per unit


def apply_delta_asset(asset: Asset, delta: DeltaAsset) -> Asset:
    if asset.target != delta.target:
        raise ValueError("Asset target and DeltaAsset target do not match.")
    new_quantity = asset.quantity + delta.delta_quantity
    if new_quantity == 0:
        new_average_cost = 0.0
    else:
        total_cost = (
            asset.average_cost * asset.quantity
            + delta.target_price * delta.delta_quantity
        )
        new_average_cost = total_cost / new_quantity
    return Asset(
        target=asset.target,
        quantity=new_quantity,
        average_cost=new_average_cost,
    )


class Suggestion(BaseModel):
    suggest_at: datetime
    delta_asset: DeltaAsset
    reason: Optional[str] = None
    relative_events: Optional[list[Event]] = None


class SuggestionPublisher(ABC):
    @abstractmethod
    def publish(self, suggestion: Suggestion) -> None:
        raise NotImplementedError


class Clock(ABC):
    def __init__(self, tzinfo: ZoneInfo = ZoneInfo("UTC")):
        self._tzinfo = tzinfo

    @property
    def tzinfo(self) -> ZoneInfo:
        return self._tzinfo

    @abstractmethod
    def now(self) -> datetime:
        raise NotImplementedError


class RealTimeClock(Clock):
    def __init__(self, tzinfo: ZoneInfo = ZoneInfo("UTC")):
        super().__init__(tzinfo)

    def now(self) -> datetime:
        return datetime.now(tz=self._tzinfo)


class Consultant(ABC):
    def __init__(self, clock: Clock = RealTimeClock()):
        self._clock = clock

    @abstractmethod
    def consult(
        self, portfolio: Portfolio, start_time: datetime, end_time: datetime
    ) -> Iterable[Suggestion]:
        """Provide suggestions based on the portfolio and time range.

        Args:
            portfolio (Portfolio): The current portfolio.
            start_time (datetime): The start time for consideration.
            end_time (datetime): The end time for consideration.
        """
        raise NotImplementedError


class Monitor(ABC):
    @abstractmethod
    def loop(
        self, since: datetime, interval: float = 300, until: Optional[datetime] = None
    ) -> None:
        """Boost the monitoring process and let it load and process messages since the given time."""
        raise NotImplementedError
