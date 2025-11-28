import logging
from abc import ABC, abstractmethod
from datetime import datetime, time
from typing import Iterable, Literal, Optional

from pydantic import BaseModel


class Event(BaseModel):
    event_timestamp: datetime
    content: str


class EventSource(ABC):
    @abstractmethod
    def fetch(self, start_time: datetime, end_time: datetime) -> Iterable[Event]:
        """Fetch events in the given time range."""
        raise NotImplementedError


class Stock(BaseModel):
    market: str
    symbol: str
    type: Literal["share", "etf", "bond", "fund"] = "share"
    alias: Optional[str] = None


class StockListProvider(ABC):
    @abstractmethod
    def get_stocks(self) -> Iterable[Stock]:
        raise NotImplementedError


class Suggestion(BaseModel):
    suggest_at: datetime
    stock: Stock
    direction: Literal["BUY", "SELL"]
    reason: Optional[str] = None
    relative_events: Optional[Iterable[Event]] = None


class SuggestionPublisher(ABC):
    @abstractmethod
    def publish(self, suggestion: Suggestion) -> None:
        raise NotImplementedError


class Monitor(ABC):
    def __init__(
        self,
        stock_list_provider: StockListProvider,
        message_sources: Iterable[EventSource],
        suggestion_publishers: Iterable[SuggestionPublisher],
    ):
        self._stock_list_provider = stock_list_provider
        self._message_sources = message_sources
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

        repeat_condition = lambda x: until is None or since < until

        loop_count = 0
        while repeat_condition(since):
            self._logger.info(f"This is the {loop_count}-th monitoring loop.")
            now = datetime.now(tz=since.tzinfo)
            interval_timer = time.time()

            suggestions = self.gen_suggestions(since, now)
            since = now

            suggestions_counter = 0
            buy_sug_counter = 0
            sell_sug_counter = 0

            for suggestion in suggestions:
                suggestions_counter += 1
                buy_sug_counter += 1 if suggestion.direction == "BUY" else 0
                sell_sug_counter += 1 if suggestion.direction == "SELL" else 0
                self._logger.debug(
                    f"Publishing the {suggestions_counter}-th suggestion: {suggestion}"
                )
                for publisher in self._suggestion_publishers:
                    publisher.publish(suggestion)

            self._logger.info(
                f"From {since} to {now}, generated {suggestions_counter} suggestions (total), {buy_sug_counter} BUY, {sell_sug_counter} SELL"
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
