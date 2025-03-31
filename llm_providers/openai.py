import openai
import logging
from .base import LLMProvider
from monitor.metrics import metrics_collector
import time

logger = logging.getLogger(__name__)

class OpenAILLM(LLMProvider):
    def __init__(self, api_key: str, model: str):
        self.api_key = api_key
        self.model = model
        openai.api_key = self.api_key
        logger.info(f"Initialized OpenAILLM with model={self.model}")

    async def generate(self, prompt: str, system_prompt: str = None) -> str:
        start_time = time.monotonic()
        is_error = True
        response_text = ""

        try:
            response = openai.Completion.create(
                model=self.model,
                prompt=prompt,
                max_tokens=1024,
                temperature=0.7
            )
            response_text = response.choices[0].text.strip()
            is_error = False
        except Exception as e:
            logger.error(f"Error calling OpenAI API: {e}", exc_info=True)
        finally:
            latency = time.monotonic() - start_time
            metrics_collector.record_llm_call(
                provider="openai",
                model=self.model,
                latency=latency,
                is_error=is_error
            )
        return response_text
