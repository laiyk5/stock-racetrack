import logging
from datetime import timedelta
from typing import Iterable

from openai import OpenAI

from srt import check
from srt.monitor import Stock, StockListProvider
from srt.monitor.monitors import ChatBot, LLMMonitor
from srt.monitor.publishers import FileSuggestionPublisher, LoggerSuggestionPublisher


class DemoStockListProvider(StockListProvider):
    def get_stocks(self):
        return [
            # Stock(market="NASDAQ", symbol="AAPL", type="share", alias="Apple Inc."),
            # Stock(market="NASDAQ", symbol="GOOGL", type="share", alias="Alphabet Inc."),
            # Stock(market="NYSE", symbol="TSLA", type="share", alias="Tesla Inc."),
            # Stock(market="NYSE", symbol="AMZN", type="share", alias="Amazon.com Inc."),
            # Stock(market="Shanghai", symbol="600938", type="share", alias="中国海油"),
            # Stock(market="Shanghai", symbol="600132", type="share", alias="重庆啤酒"),
            # Stock(market="China", symbol="159842", type="etf", alias="券商ETF"),
            # Stock(market="China", symbol="515100", type="etf", alias="红利低波100ETF"),
            # Stock(market="China", symbol="512800", type="etf", alias="银行ETF"),
            Stock(market="ShenZhen", alias="三花智控", symbol="002050", type="share"),
            Stock(market="ShenZhen", symbol="002510", alias="天汽模", type="share"),
            Stock(market="ShenZhen", symbol="003008", alias="开普检测", type="share"),
            Stock(market="ShenZhen", symbol="002970", alias="锐明技术", type="share"),
            Stock(market="ShenZhen", symbol="002589", alias="瑞康医药", type="share"),
            Stock(market="ShenZhen", symbol="300632", alias="光莆股份", type="share"),
            Stock(market="ShenZhen", symbol="002108", alias="沧州明珠", type="share"),
            Stock(market="ShenZhen", symbol="300558", alias="贝达药业", type="share"),
            Stock(market="ShenZhen", symbol="300260", alias="新莱应材", type="share"),
            Stock(market="ShenZhen", alias="比亚迪", symbol="002594", type="share"),
        ]


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

    logger.info("Using API key: %s", API_KEY)

    ai_client = OpenAI(api_key=API_KEY, base_url=BASE_URL)

    assert check.check_deepseek(
        ai_client, model=MODEL
    ), "DeepSeek model is not available. Please check your API key and DeepSeek account."

    logger.info("DeepSeek model is available. Starting LLM Monitor...")

    with open("srt_demo_suggestions.log", "a") as f:
        f.write(f"\n\n--- New Run at {datetime.now().isoformat()} ---\n")
        monitor = LLMMonitor(
            stock_list_provider=DemoStockListProvider(),
            message_sources=[],
            suggestion_publishers=[
                LoggerSuggestionPublisher(),
                FileSuggestionPublisher(f),
            ],  # Replace with an actual SuggestionPublisher
            chatbot=DashscopeChatBot(ai_client, model=MODEL),
        )
        now = datetime.now(tz=ZoneInfo("Asia/Shanghai"))

        # only run once.
        monitor.loop(since=now - timedelta(days=1), until=None, interval=60 * 5)
