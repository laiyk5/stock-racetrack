import logging
from datetime import timedelta
from typing import Iterable

from openai import OpenAI

from srt import check
from srt.monitor import (
    Asset,
    Currency,
    Portfolio,
    PortfolioProvider,
    RealTimeClock,
    Security,
)
from srt.monitor.consultants import ChatBot, LLMMConsultant, PureTextEvent
from srt.monitor.monitors import SimpleMonitor
from srt.monitor.publishers import FileSuggestionPublisher, LoggerSuggestionPublisher


class DemoPortfolioProvider(PortfolioProvider):
    def get_portfolio(self) -> Portfolio:
        return Portfolio(
            assets=[
                Asset(
                    target=Currency(code="CNY"), quantity=18833.32, average_cost=1.0
                ),  # !NOTE: Should currency have average_cost?
                Asset(
                    target=Security(
                        market="Shanghai",
                        symbol="600132",
                        type="share",
                        alias="重庆啤酒",
                    ),
                    quantity=100,
                    average_cost=54.11,
                ),  # !NOTE: Should average_cost have unit?
                Asset(
                    target=Security(
                        market="China", symbol="159842", type="etf", alias="券商ETF"
                    ),
                    quantity=4500,
                    average_cost=1.172,
                ),
                Asset(
                    target=Security(
                        market="China",
                        symbol="515100",
                        type="etf",
                        alias="红利低波100ETF",
                    ),
                    quantity=3200,
                    average_cost=1.557,
                ),
                Asset(
                    target=Security(
                        market="China", symbol="600938", type="share", alias="中国海油"
                    ),
                    quantity=100,
                    average_cost=29.289,
                ),
            ]
        )


class DashscopeChatBot(ChatBot):
    _logger = logging.getLogger(__name__)

    def __init__(self, client: OpenAI, model: str):
        self._client = client
        self._model = model

    def chat(self, messages) -> str:
        try:
            response = self._client.chat.completions.create(
                model=self._model,
                messages=messages,
                stream=False,
                extra_body={"enable_search": True, "enable_thinking": True},
            )
            content = response.choices[0].message.content
            if content is None:
                self._logger.warning(
                    "DashscopeChatBot returned no content; returning empty string"
                )
                return ""
            return content
        except Exception as e:
            self._logger.error(f"Error calling DashscopeChatBot: {e}")
            raise e


if __name__ == "__main__":
    import os
    from datetime import datetime
    from zoneinfo import ZoneInfo

    import dotenv
    from openai import OpenAI

    logger = logging.getLogger("srt.demo")
    logger.info(f"This is a run at {datetime.now().isoformat()}")

    dotenv.load_dotenv()

    API_KEY = os.getenv("DASHSCOPE_API_KEY")
    BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
    MODEL = "deepseek-r1"

    assert API_KEY is not None, "Please set DASHSCOPE_API_KEY in your environment."

    # hide api key except last 4 chars
    hidden_key = "*" * (len(API_KEY) - 4) + API_KEY[-4:]

    logger.info("Using API key: %s", hidden_key)

    ai_client = OpenAI(api_key=API_KEY, base_url=BASE_URL)

    assert check.check_deepseek(
        ai_client, model=MODEL
    ), "DeepSeek model is not available. Please check your API key and DeepSeek account."

    logger.info("DeepSeek model is available. Starting LLM Monitor...")

    tzinfo = ZoneInfo("Asia/Shanghai")

    clock = RealTimeClock(tzinfo=tzinfo)

    consultant = LLMMConsultant(
        clock=clock,
        pure_text_event_sources=[],
        chatbot=DashscopeChatBot(ai_client, model=MODEL),
        parsed_err_tolerance=0.8,
    )

    with open("srt_demo_suggestions.log", "a") as f:
        f.write(f"\n\n--- New Run at {datetime.now().isoformat()} ---\n")
        monitor = SimpleMonitor(
            consultant=consultant,
            portfolio_provider=DemoPortfolioProvider(),
            suggestion_publishers=[
                LoggerSuggestionPublisher(),
                FileSuggestionPublisher(f),
            ],  # Replace with an actual SuggestionPublisher
        )
        now = clock.now()
        # only run once.
        monitor.loop(since=now - timedelta(days=1), until=None, interval=60 * 5)
