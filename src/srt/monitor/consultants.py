import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Iterable, Optional

import tenacity
from openai.types.chat import ChatCompletionMessageParam
from pydantic import BaseModel
from tenacity import retry

from . import (
    Clock,
    Consultant,
    Event,
    EventSource,
    Portfolio,
    RealTimeClock,
    Suggestion,
)

logger = logging.getLogger(__name__)


def pydantic_dumps(obj: BaseModel):
    logger.debug("Serializing object of type %s", type(obj).__name__)
    return json.dumps(obj.model_dump(), ensure_ascii=False, indent=2)


def pydantic_schema_dumps(model: type[BaseModel]):
    return json.dumps(model.model_json_schema(), ensure_ascii=False, indent=2)


class PureTextEvent(Event):
    content: str


class UserInput(BaseModel):
    now: str  # ISO 8601 format
    start_time: str  # ISO 8601 format
    end_time: str  # ISO 8601 format
    portfolio: Portfolio  # Comma-separated stock symbols
    additional_events: list[PureTextEvent]  # Additional context or events


class Document(BaseModel):
    title: str
    source: str
    url: Optional[str] = None
    summary: Optional[str] = None


class LLMOutput(BaseModel):
    current_time: str  # ISO 8601 format
    suggestions: list[Suggestion]
    document_read: list[Document]
    note: Optional[str] = None


class ChatBot(ABC):
    @abstractmethod
    def chat(self, messages: Iterable[ChatCompletionMessageParam]) -> str:
        pass  # Placeholder for chat method


class LLMMConsultant(Consultant):
    def __init__(
        self,
        chatbot: ChatBot,
        pure_text_event_sources: Iterable[EventSource[PureTextEvent]],
        parsed_err_tolerance: float = 0.8,
        clock: Clock = RealTimeClock(),
    ):
        super().__init__(clock=clock)
        self._logger = logging.getLogger(__name__ + "." + self.__class__.__name__)
        self._chatbot = chatbot
        self._pure_text_event_sources = pure_text_event_sources

        if parsed_err_tolerance < 0.0 or parsed_err_tolerance > 1.0:
            raise ValueError("parsed_err_tolerance must be between 0.0 and 1.0")

        self._parsed_err_tolerance = parsed_err_tolerance

    def consult(self, portfolio, start_time, end_time):
        # make sure the tzinfo is consistent
        if (
            start_time.tzinfo != end_time.tzinfo
            or end_time.tzinfo != self._clock.tzinfo
        ):
            # adjust to the clock's tzinfo
            start_time = start_time.astimezone(self._clock.tzinfo)
            end_time = end_time.astimezone(self._clock.tzinfo)
            self._logger.warning(
                "start_time, end_time and clock must have the same tzinfo"
            )

        if end_time > self._clock.now():
            self._logger.warning(
                "End time %s is in the future. Adjusting to current time.", end_time
            )
            end_time = self._clock.now()

        # Gather all events.
        all_events: list[PureTextEvent] = []
        for source in self._pure_text_event_sources:
            events = source.fetch(start_time, end_time)
            all_events.extend(events)

        user_input = UserInput(
            now=self._clock.now().isoformat(),
            start_time=start_time.isoformat(),
            end_time=end_time.isoformat(),
            portfolio=portfolio,
            additional_events=all_events,
        )

        suggestions = self._call_llm(user_input)
        yield from suggestions  # Assuming suggestions is an iterable of Suggestion objects

    PROMPT_SYS = (
        "You are a stock monitoring assistant. The user would provide a JSON input, its schema is as below:\n"
        + pydantic_schema_dumps(UserInput)
        + "\nWhat you need to do are:"
        + "1. Search the web for any important news or events related to the provided asset that happened between 'start_time' and 'end_time'.\n"
        + "2. Analyze the events from the input and the additional events you found from the web, and identify any significant events that could impact the stock prices of the provided portfolio.\n"
        + "3. Respond with a JSON object LLMOutput, It contains a list of Suggestions you made, along with the all document you read. If you have any other things want to convey, put them into the 'note' field. The schema of LLMOutput is as below:\n"
        + pydantic_schema_dumps(LLMOutput)
        + "4. If there is no suggestion, explain why in 'note' and always response a valid JSON object with an empty array in 'suggestions' field.\n"
        + "\nNotice:\n"
        + "1. The 'now' field in User input is correct. Use it as the current time reference.\n"
        + "2. User might provide no events. You should try to gather events by yourself. If you can't, return an empty JSON array: []\n"
        + "3. Make sure the response is a valid JSON array, and can be parsed correctly."
        + "4. You can search any additional information from the web if needed. But the suggestions should only based on the events happend between start_time and end_time."
        + "5. If you make a suggestion, always provide the 'reason' field and 'relative_events' field with at least one event. Don't suggest if you can't provide reason or relative events."
    )

    @retry(
        wait=tenacity.wait_exponential(min=1, max=10),
        stop=tenacity.stop_after_attempt(5),
        reraise=True,
        before_sleep=tenacity.before_sleep_log(logger, logging.WARNING),
    )
    def _call_llm(self, user_input: UserInput) -> Iterable[Suggestion]:
        try:
            logger.debug("Calling LLM with user input: %s", pydantic_dumps(user_input))
        except Exception as e:
            logger.error("Failed to serialize user input for logging: %s", e)

        try:
            response_str = self._chatbot.chat(
                messages=[
                    {"role": "system", "content": self.PROMPT_SYS},
                    {"role": "user", "content": pydantic_dumps(user_input)},
                ]
            )
        except Exception as e:
            logger.error("Error calling chatbot: %s", e)
            raise e

        # try parse the response content as JSON
        try:
            if response_str is None:
                raise ValueError("Response content is None")

            logger.debug("LLM response content: %s", response_str)

            # try locade the first and last square brackets
            first_bracket = response_str.index("{")
            last_bracket = response_str.rindex("}") + 1
            response_str = response_str[first_bracket:last_bracket].strip()
            if len(response_str) == 0:
                raise ValueError("No JSON object found in response")
            llm_output_data = json.loads(response_str)

            try:
                llm_output = LLMOutput.model_validate(llm_output_data)
                logger.debug("Documents read: %d", len(llm_output.document_read))
                logger.info("Note: %s", llm_output.note)
                return llm_output.suggestions
            except Exception as e:
                logger.error("Failed to validate LLM output data: %s", e)
                try:
                    logger.debug("Trying to parse suggestions individually.")
                    suggestions_data = llm_output_data["suggestions"]
                    parsed_suggestions = []

                    for item in suggestions_data:
                        suggestion = Suggestion.model_validate(item)
                        parsed_suggestions.append(suggestion)

                    n_expected = len(suggestions_data)
                    n_parsed = len(parsed_suggestions)

                    if n_expected != 0 and n_parsed == 0:
                        raise ValueError(
                            "Failed to parse any suggestions from LLM response"
                        )

                    # allow partial parsing with warning
                    if n_expected != 0 and n_parsed < n_expected:
                        logger.warning(
                            "Some suggestions failed to parse from LLM response: expected %d, got %d",
                            n_expected,
                            n_parsed,
                        )
                        if n_parsed / n_expected < self._parsed_err_tolerance:
                            raise ValueError(
                                "Parsed suggestions ratio below tolerance: expected %d, got %d"
                                % (n_expected, n_parsed)
                            )
                    return parsed_suggestions
                except Exception as e2:
                    logger.error(
                        "Failed to parse suggestions from LLM output data: %s", e2
                    )
                    raise e2 from e

        except Exception as e:
            logger.error("Failed to parse LLM response: %s", e)
            raise e


__all__ = ["LLMMConsultant", "PureTextEvent", "ChatBot"]
