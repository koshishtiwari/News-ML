import asyncio
import logging
import argparse
import sys
from typing import Optional

# Import needed components
from monitor.metrics import metrics_collector
from monitor.backend.server import app
from core.system import NewsAggregationSystem
from llm_providers.factory import create_llm_provider

logger = logging.getLogger(__name__)

async def initialize_system(provider_type: str = "ollama", model_name: Optional[str] = None):
    """Initialize the news system and connect it to the metrics collector"""
    try:
        # Set the event loop for the metrics collector
        metrics_collector.set_loop(asyncio.get_running_loop())
        
        # Create the LLM provider
        llm_provider = create_llm_provider(provider_type, model=model_name)
        logger.info(f"Using LLM Provider: {type(llm_provider).__name__} with model '{model_name or 'default'}'")
        
        # Initialize the news system
        system = NewsAggregationSystem(llm_provider)
        
        # Set the system reference in metrics collector
        metrics_collector.set_news_system(system)
        logger.info("News system reference set in metrics collector")
        
        return system
    except Exception as e:
        logger.error(f"Failed to initialize news system: {e}", exc_info=True)
        return None

def setup_and_run_server(host: str = "0.0.0.0", port: int = 8000, 
                        provider: str = "ollama", model: Optional[str] = None):
    """Set up the system and run the server with proper integrations"""
    import uvicorn
    
    if sys.platform == "win32":
        # This policy is generally needed for libraries like aiohttp on Windows
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    # Create a new event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # Run the initialization
    system = loop.run_until_complete(initialize_system(provider, model))
    
    if not system:
        logger.error("Failed to initialize news system. Server will run but processing will fail.")
    
    # Run the server
    logger.info(f"Starting monitor server on {host}:{port}")
    uvicorn.run(app, host=host, port=port)

if __name__ == "__main__":
    # Simple argument parsing
    parser = argparse.ArgumentParser(description="Run the monitor server with news system integration")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Host to bind the server to")
    parser.add_argument("--port", type=int, default=8000, help="Port to run the server on")
    parser.add_argument("--provider", type=str, default="ollama", help="LLM provider to use")
    parser.add_argument("--model", type=str, help="LLM model name (required for some providers like ollama)")
    
    args = parser.parse_args()
    
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the server
    setup_and_run_server(args.host, args.port, args.provider, args.model)