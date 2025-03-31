import logging

from .base import LLMProvider
from .ollama import OllamaLLM
from .gemini import GeminiLLM  # Import the implemented GeminiLLM
from .openai import OpenAILLM
from .anthropic import AnthropicLLM

# Import config (make sure this doesn't create circular dependencies)
from config import (
    OLLAMA_BASE_URL, OLLAMA_REQUEST_TIMEOUT,
    GEMINI_API_KEY, OPENAI_API_KEY, ANTHROPIC_API_KEY
)

logger = logging.getLogger(__name__)

def create_llm_provider(provider_type: str, model: str = None, api_key: str = None) -> LLMProvider:
    """
    Factory function to create an instance of an LLM provider.

    Args:
        provider_type: The type of provider (e.g., "ollama", "gemini").
        model: The specific model name to use.
        api_key: The API key for the provider (if required).

    Returns:
        An instance of the requested LLMProvider.

    Raises:
        ValueError: If the provider type is unsupported or configuration is missing.
    """
    provider_type = provider_type.lower()
    logger.info(f"Attempting to create LLM provider: type='{provider_type}', model='{model}'")

    if provider_type == "ollama":
        if not model:
            raise ValueError("Model name is required for Ollama provider.")
        return OllamaLLM(
            model=model,
            base_url=OLLAMA_BASE_URL,
            request_timeout=OLLAMA_REQUEST_TIMEOUT
        )
    elif provider_type == "gemini":
        if not api_key:
            raise ValueError("API key is required for Gemini provider.")
        model = model or "gemini-1.5-flash"
        return GeminiLLM(api_key=api_key, model=model)
    elif provider_type == "openai":
        if not api_key:
            raise ValueError("API key is required for OpenAI provider.")
        return OpenAILLM(api_key=api_key, model=model)
    elif provider_type == "anthropic":
        if not api_key:
            raise ValueError("API key is required for Anthropic provider.")
        return AnthropicLLM(api_key=api_key, model=model)
    else:
        raise ValueError(f"Unsupported LLM provider type: '{provider_type}'.")