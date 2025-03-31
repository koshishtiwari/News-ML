import logging
from .base import LLMProvider
from monitor.metrics import metrics_collector
import time
import anthropic

logger = logging.getLogger(__name__)

class AnthropicLLM(LLMProvider):
    def __init__(self, api_key: str, model: str):
        self.api_key = api_key
        self.model = model
        self.client = anthropic.Client(api_key=self.api_key)
        logger.info(f"Initialized AnthropicLLM with model={self.model}")

    async def generate(self, prompt: str, system_prompt: str = None) -> str:
        start_time = time.monotonic()
        is_error = True
        response_text = ""

        try:
            response = self.client.completion(
                prompt=prompt,
                model=self.model,
                max_tokens_to_sample=1024,
                temperature=0.7
            )
            response_text = response.get("completion", "").strip()
            is_error = False
        except Exception as e:
            logger.error(f"Error calling Anthropic API: {e}", exc_info=True)
        finally:
            latency = time.monotonic() - start_time
            metrics_collector.record_llm_call(
                provider="anthropic",
                model=self.model,
                latency=latency,
                is_error=is_error
            )
        return response_text
