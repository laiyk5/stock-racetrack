import logging
from datetime import datetime
from typing import Iterable, Optional

from . import Consultant, Monitor, PortfolioProvider, SuggestionPublisher

logger = logging.getLogger(__name__)


class SimpleMonitor(Monitor):
    def __init__(
        self,
        consultant: Consultant,
        portfolio_provider: PortfolioProvider,
        suggestion_publishers: Iterable[SuggestionPublisher],
    ):
        self._consultant = consultant
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

            portfolio = self._portfolio_provider.get_portfolio()
            suggestions = self._consultant.consult(
                portfolio=portfolio, start_time=since, end_time=now
            )
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


__all__ = ["SimpleMonitor"]
