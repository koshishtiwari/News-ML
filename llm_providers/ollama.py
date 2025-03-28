import logging
import json
import asyncio
import aiohttp
import time # For latency calculation

# Import base and global collector
from .base import LLMProvider
from monitor.metrics import metrics_collector

logger = logging.getLogger(__name__)

class OllamaLLM(LLMProvider):
    """Integration with a local Ollama API server."""

    def __init__(self, model: str, base_url: str, request_timeout: int):
        self.model = model
        self.base_url = base_url.rstrip('/')
        self.api_endpoint = f"{self.base_url}/api/generate"
        self.request_timeout = request_timeout
        logger.info(f"Initialized OllamaLLM: endpoint={self.api_endpoint}, model={self.model}, timeout={self.request_timeout}s")

    async def generate(self, prompt: str, system_prompt: str = None) -> str:
        headers = {"Content-Type": "application/json"}
        data = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0.2}
        }
        if system_prompt:
            data["system"] = system_prompt

        logger.debug(f"Ollama Request: model={self.model}, system={bool(system_prompt)}, prompt_len={len(prompt)}")

        response_text = ""
        is_error = True # Assume error initially
        start_time = time.monotonic()
        latency = 0.0

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.api_endpoint, headers=headers, json=data, timeout=self.request_timeout) as response:
                    if response.status == 200:
                        try:
                            result = await response.json()
                            response_text = result.get("response", "").strip()
                            is_error = False # Success!
                            logger.debug(f"Ollama Response received (len={len(response_text)})")
                        except (json.JSONDecodeError, KeyError) as e:
                            logger.error(f"Failed parsing Ollama success response: {e}", exc_info=True)
                            # is_error remains True
                    else:
                        error_body = await response.text()
                        logger.error(f"Ollama API Error ({response.status}): {error_body[:500]}...")
                        # is_error remains True

        except aiohttp.ClientConnectorError as e:
             logger.error(f"Connection failed to Ollama at {self.api_endpoint}: {e}")
             # is_error remains True
        except asyncio.TimeoutError:
            logger.error(f"Ollama request timed out after {self.request_timeout}s (model: {self.model}).")
            # is_error remains True
        except Exception as e:
            logger.error(f"Unexpected error calling Ollama API: {e}", exc_info=True)
            # is_error remains True
        finally:
            latency = time.monotonic() - start_time
            # ---> CALL METRICS COLLECTOR <---
            # Use the correct method and arguments
            metrics_collector.record_llm_call(
                 provider="ollama", # Hardcode provider name
                 model=self.model,
                 latency=latency,
                 is_error=is_error
            )
            logger.debug(f"Ollama call complete: latency={latency:.3f}s, error={is_error}")

        return response_text