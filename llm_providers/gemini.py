import logging
import asyncio
import time
import traceback
from typing import Optional
from google import genai
from .base import LLMProvider
from monitor.metrics import metrics_collector

logger = logging.getLogger(__name__)

class GeminiLLM(LLMProvider):
    """Integration with Google's Gemini API using the new client library."""
    
    def __init__(self, api_key: str, model: str = "gemini-1.5-flash", request_timeout: int = 30):
        self.model_name = model
        self.request_timeout = request_timeout
        
        logger.info(f"Configuring Gemini with API key: {api_key[:4]}...{api_key[-4:] if len(api_key) > 8 else ''}")
        try:
            # Initialize the client with the API key - updated API pattern
            self.client = genai.Client(api_key=api_key)
            logger.info("Gemini API client configuration successful")
        except Exception as e:
            logger.error(f"Failed to configure Gemini API client: {e}", exc_info=True)
            raise
            
        # # Set parameters to be used when generating
        # self.temperature = 0.2
        # self.top_p = 0.95
        # self.top_k = 0
        
        logger.info(f"Initialized GeminiLLM with model={self.model_name}, timeout={self.request_timeout}s")

    async def generate(self, prompt: str, system_prompt: str = None) -> str:
        """Generate text from Gemini API using asyncio to prevent blocking."""
        response_text = ""
        is_error = True  # Assume error initially
        start_time = time.monotonic()
        latency = 0.0
        
        try:
            logger.info(f"Sending request to Gemini API with model={self.model_name}, prompt_length={len(prompt)}")
            
            # Create the content structure based on updated API
            contents = []
            if system_prompt:
                contents.append({
                    "role": "user",
                    "parts": [{"text": system_prompt}]
                })
            
            contents.append({
                "role": "user",
                "parts": [{"text": prompt}]
            })
            
            # Use asyncio with timeout to run the blocking call in a thread pool
            loop = asyncio.get_event_loop()
            try:
                logger.debug("Starting Gemini API request with timeout")
                
                # Run the API call in a thread pool to avoid blocking
                response = await asyncio.wait_for(
                    loop.run_in_executor(
                        None,  # Use default executor
                        lambda: self.client.models.generate_content(
                            model=self.model_name,
                            contents=contents
                            # temperature=self.temperature,
                            # top_p=self.top_p,
                            # top_k=self.top_k
                        )
                    ),
                    timeout=self.request_timeout
                )
                
                # Extract text from response
                if response and hasattr(response, "text"):
                    response_text = response.text.strip()
                    is_error = False  # Success!
                    logger.info(f"Gemini response received successfully (length={len(response_text)})")
                else:
                    logger.error("Gemini returned empty or invalid response")
                    response_text = "Error: Empty or invalid response from Gemini API"
                    
            except asyncio.TimeoutError:
                logger.error(f"Gemini API request timed out after {self.request_timeout}s")
                response_text = f"Error: Gemini API request timed out after {self.request_timeout} seconds"
                
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error calling Gemini API: {error_msg}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            response_text = f"Error calling Gemini API: {error_msg}"
            # is_error remains True
            
        finally:
            latency = time.monotonic() - start_time
            metrics_collector.record_llm_call(
                provider="gemini",
                model=self.model_name,
                latency=latency,
                is_error=is_error
            )
            logger.info(f"Gemini call complete: latency={latency:.3f}s, error={is_error}")
            
        return response_text