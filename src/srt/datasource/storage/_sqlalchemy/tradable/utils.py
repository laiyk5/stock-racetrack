from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Iterable, Optional

from srt.datasource.types.data import Entity
from srt.datasource.types.tradable import Tradable


class Request:
    entity: Entity
    start_time: datetime
    end_time: datetime


class RequestAnalyzer(ABC):
    def analyze(self, requests: Iterable[Request]) -> Iterable[Request]:
        """Analyze and possibly merge requests to optimize data fetching."""
        raise NotImplementedError


class TimeLocalRequestAnalyzer(RequestAnalyzer):
    def __init__(self, minimum_gap: timedelta):
        self._minimum_gap = minimum_gap

    def analyze(self, requests: Iterable[Request]) -> Iterable[Request]:
        request_by_entity: dict[Entity, list[Request]] = defaultdict(list)
        for request in requests:
            request_by_entity[request.entity].append(request)

        # sort and merge adjacent requests
        for entity, entity_requests in request_by_entity.items():
            entity_requests.sort(key=lambda r: r.start_time)
            current_request = None
            for request in entity_requests:
                if current_request is None:
                    current_request = request
                else:
                    gap = request.start_time - (
                        current_request.end_time or datetime.max
                    )
                    if gap <= self._minimum_gap:
                        # merge
                        current_request.end_time = max(
                            current_request.end_time or datetime.min,
                            request.end_time or datetime.min,
                        )
                    else:
                        yield current_request
                        current_request = request
            if current_request is not None:
                yield current_request
