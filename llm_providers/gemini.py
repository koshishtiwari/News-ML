import logging
import json
import time
import asyncio
from typing import Optional

import google.generativeai as genai
from google.generativeai.types import GenerateContentResponse

from .base import LLMProvider
from monitor.metrics import metrics_collector

logger = logging.getLogger(__name__)

class GeminiLLM(LLMProvider):
    """Integration with Google's Gemini API."""

    def __init__(self, api_key: str, model: str = "gemini-1.5-flash"):
        self.model = model
        genai.configure(api_key=api_key)
        self.generation_config = {
            "temperature": 0.2,
            "top_p": 0.95,
            "top_k": 0
        }
        logger.info(f"Initialized GeminiLLM with model={self.model}")

    async def generate(self, prompt: str, system_prompt: str = None) -> str:
        """Generate text from Gemini API using asyncio to prevent blocking."""
        response_text = ""
        is_error = True  # Assume error initially
        start_time = time.monotonic()
        latency = 0.0
        
        try:
            # Configure the model with system prompt if provided
            model = genai.GenerativeModel(
                model_name=self.model,
                generation_config=self.generation_config,
                system_instruction=system_prompt if system_prompt else None
            )
            
            # Use asyncio to run the blocking call in a thread pool
            loop = asyncio.get_event_loop()
            response: Optional[GenerateContentResponse] = await loop.run_in_executor(
                None,  # Use default executor
                lambda: model.generate_content(prompt)
            )
            
            # Extract text from response
            if response and hasattr(response, "text"):
                response_text = response.text.strip()
                is_error = False  # Success!
                logger.debug(f"Gemini Response received (len={len(response_text)})")
            else:
                logger.error("Gemini returned empty or invalid response")
                
        except Exception as e:
            logger.error(f"Error calling Gemini API: {e}", exc_info=True)
            # is_error remains True
            
        finally:
            latency = time.monotonic() - start_time
            metrics_collector.record_llm_call(
                provider="gemini",
                model=self.model,
                latency=latency,
                is_error=is_error
            )
            logger.debug(f"Gemini call complete: latency={latency:.3f}s, error={is_error}")
            
        return response_text
