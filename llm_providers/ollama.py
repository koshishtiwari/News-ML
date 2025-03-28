import logging
import json
import asyncio
import aiohttp
import time  # For latency calculation
from typing import Optional, Dict, Any

# Import base and global collector
from .base import LLMProvider
from monitor.metrics import metrics_collector

logger = logging.getLogger(__name__)

class OllamaLLM(LLMProvider):
    """Integration with a local Ollama API server with improved error handling and retry logic."""

    def __init__(self, model: str, base_url: str, request_timeout: int, max_retries: int = 3, retry_delay: float = 2.0):
        self.model = model
        self.base_url = base_url.rstrip('/')
        self.api_endpoint = f"{self.base_url}/api/generate"
        self.request_timeout = request_timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._session: Optional[aiohttp.ClientSession] = None
        logger.info(f"Initialized OllamaLLM: endpoint={self.api_endpoint}, model={self.model}, "
                   f"timeout={self.request_timeout}s, retries={self.max_retries}")

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create an aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.request_timeout),
                connector=aiohttp.TCPConnector(limit=5)  # Limit connections to avoid overloading
            )
        return self._session
        
    async def close(self):
        """Close the session when done using this provider."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
            logger.debug("Closed OllamaLLM session")

    def _prepare_request_data(self, prompt: str, system_prompt: Optional[str] = None) -> Dict[str, Any]:
        """Prepare the request data for Ollama API."""
        data = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": 0.2,
                "num_predict": 1024  # Reasonable token limit
            }
        }
        if system_prompt:
            data["system"] = system_prompt
        return data

    async def generate(self, prompt: str, system_prompt: str = None) -> str:
        """
        Generates text based on the provided prompt with retry logic.
        
        Args:
            prompt: The main user prompt
            system_prompt: Optional system-level instruction
            
        Returns:
            Generated text or error message
        """
        headers = {"Content-Type": "application/json"}
        data = self._prepare_request_data(prompt, system_prompt)
        
        logger.debug(f"Ollama Request: model={self.model}, system={bool(system_prompt)}, prompt_len={len(prompt)}")

        response_text = ""
        is_error = True  # Assume error initially
        start_time = time.monotonic()
        latency = 0.0
        
        # Implement retry logic for transient errors
        retries = 0
        last_error = None

        while retries <= self.max_retries:
            if retries > 0:
                wait_time = self.retry_delay * retries  # Increasing backoff
                logger.info(f"Retrying Ollama request (attempt {retries}/{self.max_retries}) after {wait_time:.1f}s")
                await asyncio.sleep(wait_time)
            
            try:
                session = await self._get_session()
                async with session.post(
                    self.api_endpoint, 
                    headers=headers, 
                    json=data,
                    raise_for_status=False  # Handle HTTP errors ourselves
                ) as response:
                    if response.status == 200:
                        try:
                            result = await response.json()
                            response_text = result.get("response", "").strip()
                            if not response_text:
                                logger.warning("Ollama returned empty response with status 200")
                                last_error = ValueError("Empty response from Ollama")
                                retries += 1
                                continue
                                
                            is_error = False  # Success!
                            logger.debug(f"Ollama Response received (len={len(response_text)})")
                            break  # Exit retry loop on success
                        except (json.JSONDecodeError, KeyError) as e:
                            last_error = e
                            logger.error(f"Failed parsing Ollama success response: {e}", exc_info=True)
                            # Try again if retries remain
                    elif response.status == 408 or response.status >= 500:
                        # Server errors or timeouts are candidates for retry
                        error_body = await response.text()
                        last_error = Exception(f"Ollama API Error ({response.status}): {error_body[:100]}...")
                        logger.warning(f"Retryable Ollama error: {response.status}")
                        retries += 1
                    else:
                        # Client errors (4xx except 408) are not retried
                        error_body = await response.text()
                        last_error = Exception(f"Ollama API Error ({response.status}): {error_body[:100]}...")
                        logger.error(f"Non-retryable Ollama API Error ({response.status}): {error_body[:500]}...")
                        break  # Exit retry loop, won't retry client errors

            except aiohttp.ClientConnectorError as e:
                last_error = e
                logger.error(f"Connection failed to Ollama at {self.api_endpoint}: {e}")
                retries += 1  # Connection errors are retriable
            except asyncio.TimeoutError:
                last_error = asyncio.TimeoutError(f"Request timed out after {self.request_timeout}s")
                logger.error(f"Ollama request timed out after {self.request_timeout}s (model: {self.model}).")
                retries += 1  # Timeouts are retriable
            except Exception as e:
                last_error = e
                logger.error(f"Unexpected error calling Ollama API: {e}", exc_info=True)
                retries += 1  # Other errors are retriable, but might be futile
        
            finally:
                latency = time.monotonic() - start_time
                metrics_collector.record_llm_call(
                    provider="ollama",
                    model=self.model,
                    latency=latency,
                    is_error=is_error
                )
                logger.debug(f"Ollama call complete: latency={latency:.3f}s, error={is_error}, retries={retries}")

        # If we've exhausted retries and still have an error
        if is_error and last_error:
            logger.error(f"All {retries} retries failed for Ollama request. Last error: {last_error}")
            # Return a friendly error message that could be shown to users
            return f"Error: Unable to generate a response from the language model. Please try again later."

        return response_text