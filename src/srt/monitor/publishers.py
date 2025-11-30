import sys

from . import Suggestion, SuggestionPublisher


class FileSuggestionPublisher(SuggestionPublisher):
    """This publisher writes suggestions to a file-like object.

    Note: it flushes the file after publishing each suggestion.
    """

    def __init__(self, fp=sys.stdout):
        self._fp = fp

    def publish(self, suggestion: Suggestion) -> None:
        print(str(suggestion), file=self._fp, flush=True)


class LoggerSuggestionPublisher(SuggestionPublisher):
    """This publisher logs suggestions using the logging module.

    Note: it flushes the logger handlers after publishing each suggestion.
    """

    def __init__(self, logger=None):
        if logger is None:
            import logging

            logger = logging.getLogger(__name__)
        self._logger = logger

    def publish(self, suggestion: Suggestion) -> None:
        self._logger.info(f"Published suggestion: {str(suggestion)}")
        # flush the logger handlers if possible
        for handler in self._logger.handlers:
            handler.flush()


__all__ = ["FileSuggestionPublisher", "LoggerSuggestionPublisher"]
