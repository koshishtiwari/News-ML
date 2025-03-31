import logging
from typing import Dict, Any, Optional

# Import all providers
from .base import LLMProvider
from .ollama import OllamaLLM
from .openai import OpenAILLM
from .claude import ClaudeLLM
# Add imports for other providers as they're created

logger = logging.getLogger(__name__)

class LLMFactory:
    """Factory for creating LLM provider instances based on configuration."""
    
    @staticmethod
    def create_provider(config: Dict[str, Any]) -> Optional[LLMProvider]:
        """
        Create an LLM provider instance based on configuration.
        
        Args:
            config: Dictionary containing provider configuration
                - provider: Type of provider (ollama, openai, claude, etc.)
                - model: Model name to use
                - Other provider-specific config (api_key, base_url, etc.)
                
        Returns:
            An instance of LLMProvider or None if configuration is invalid
        """
        provider_type = config.get("provider", "").lower()
        model = config.get("model")
        
        if not model:
            logger.error("No model specified in LLM configuration")
            return None
            
        try:
            if provider_type == "ollama":
                base_url = config.get("base_url", "http://localhost:11434")
                request_timeout = int(config.get("request_timeout", 60))
                max_retries = int(config.get("max_retries", 3))
                retry_delay = float(config.get("retry_delay", 2.0))
                
                return OllamaLLM(
                    model=model,
                    base_url=base_url,
                    request_timeout=request_timeout,
                    max_retries=max_retries,
                    retry_delay=retry_delay
                )
                
            elif provider_type == "openai":
                api_key = config.get("api_key")
                if not api_key:
                    logger.error("No API key provided for OpenAI")
                    return None
                    
                request_timeout = int(config.get("request_timeout", 30))
                max_retries = int(config.get("max_retries", 3))
                retry_delay = float(config.get("retry_delay", 2.0))
                
                return OpenAILLM(
                    model=model,
                    api_key=api_key,
                    request_timeout=request_timeout, 
                    max_retries=max_retries,
                    retry_delay=retry_delay
                )
                
            elif provider_type == "claude":
                api_key = config.get("api_key")
                if not api_key:
                    logger.error("No API key provided for Claude")
                    return None
                    
                request_timeout = int(config.get("request_timeout", 60))
                max_retries = int(config.get("max_retries", 3))
                retry_delay = float(config.get("retry_delay", 2.0))
                
                return ClaudeLLM(
                    model=model,
                    api_key=api_key,
                    request_timeout=request_timeout,
                    max_retries=max_retries,
                    retry_delay=retry_delay
                )
                
            # Add more provider types as needed
                
            else:
                logger.error(f"Unknown LLM provider type: {provider_type}")
                return None
                
        except Exception as e:
            logger.error(f"Error creating LLM provider '{provider_type}': {e}", exc_info=True)
            return None
