import logging
from abc import ABC, abstractmethod
from datetime import datetime, time
from typing import Generic, Iterable, Literal, Optional, TypeVar

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


class Monitor(ABC):
    def __init__(
        self,
        portfolio_provider: PortfolioProvider,
        suggestion_publishers: Iterable[SuggestionPublisher],
    ):
        self._portfolio_provider = portfolio_provider
        self._suggestion_publishers = suggestion_publishers
        self._logger = logging.getLogger(__name__ + "." + self.__class__.__name__)

    def loop(
        self, since: datetime, interval: float = 300, until: Optional[datetime] = None
    ) -> None:
        """Boost the monitoring process and let it load and process messages since the given time."""
        import time

        if interval < 30:
            self._logger.warning(
                "Interval is set to less than 30 seconds, which may lead to rate limiting."
            )

        repeat_condition = lambda _: until is None or since < until

        loop_count = 0
        while repeat_condition(since):
            self._logger.info(f"This is the {loop_count}-th monitoring loop.")
            now = datetime.now(tz=since.tzinfo)
            interval_timer = time.time()

            suggestions = self.gen_suggestions(since, now)
            since = now

            suggestions_counter = 0
            long_sug_counter = 0
            short_sug_counter = 0

            for suggestion in suggestions:
                suggestions_counter += 1
                long_sug_counter += (
                    1 if suggestion.delta_asset.delta_quantity > 0 else 0
                )
                short_sug_counter += (
                    1 if suggestion.delta_asset.delta_quantity < 0 else 0
                )
                self._logger.debug(
                    f"Publishing the {suggestions_counter}-th suggestion: {suggestion}"
                )
                for publisher in self._suggestion_publishers:
                    publisher.publish(suggestion)

            self._logger.info(
                f"From {since} to {now}, generated {suggestions_counter} suggestions (total), {long_sug_counter} BUY, {short_sug_counter} SELL"
            )

            elapsed = time.time() - interval_timer

            if elapsed < interval:
                self._logger.info(
                    "Sleeping for %s seconds to respect the interval.",
                    interval - elapsed,
                )
                # check the repeat condition before sleeping
                if not repeat_condition(since):
                    break
                time.sleep(interval - elapsed)
            else:
                self._logger.warning(
                    "Processing took longer (%s seconds) than the interval (%s seconds).",
                    elapsed,
                    interval,
                )

            loop_count += 1

        self._logger.info("Monitoring loop has ended.")

    @abstractmethod
    def gen_suggestions(
        self, start_time: datetime, end_time: datetime
    ) -> Iterable[Suggestion]:
        """Process messages in the given time range."""
        raise NotImplementedError
