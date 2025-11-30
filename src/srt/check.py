import logging

from openai import OpenAI

logger = logging.getLogger(__name__)


def check_deepseek(client: OpenAI, model="deepseek-chat") -> bool:
    try:
        # try call the model to see if it's available
        response = client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": "You are a helpful assistant that helps people find information.",
                },
                {"role": "user", "content": "Hello!"},
            ],
            max_tokens=5,
        )
        logger.debug("DeepSeek model response: %s", response)
        return True
    except Exception as e:
        logger.error(f"Error checking DeepSeek availability: {e}")
        return False
