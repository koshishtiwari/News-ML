import asyncio
import logging
import sys
import argparse
import threading
import uvicorn

# Setup utils and config first
from utils.logging_config import setup_logging
setup_logging(log_level=logging.INFO) # Setup logging first
logger = logging.getLogger(__name__) # Then get logger

import config

# Import core system and LLM factory
from core.system import NewsAggregationSystem
from llm_providers.factory import create_llm_provider

# Import monitor components
from monitor.metrics import metrics_collector, MetricsLogHandler
from monitor.backend.server import app as monitor_app 

# Import the FastAPI app

# --- Add Custom Log Handler ---
# Check if handler already exists to avoid duplicates during potential reloads
root_logger = logging.getLogger()
if not any(isinstance(h, MetricsLogHandler) for h in root_logger.handlers):
    metrics_log_handler = MetricsLogHandler(metrics_collector)
    root_logger.addHandler(metrics_log_handler)
    logger.info("Added MetricsLogHandler to root logger.")
else:
    logger.debug("MetricsLogHandler already present.")


def start_monitor_server(host="0.0.0.0", port=8000):
    """Runs the FastAPI monitor server using Uvicorn in a separate thread."""
    logger.info(f"Starting monitor server on {host}:{port} in thread: {threading.current_thread().name}")
    try:
        # Configure uvicorn to run the FastAPI app
        # Use loop='asyncio' to ensure it uses the correct event loop policy if needed
        uvicorn_config = uvicorn.Config(monitor_app, host=host, port=port, log_level="warning", loop="asyncio")
        server = uvicorn.Server(uvicorn_config)
        # This runs the server synchronously within this thread
        server.run()
        logger.info("Monitor server thread finished.") # Should ideally not be reached unless stopped
    except Exception as e:
        logger.critical(f"Failed to run monitor server: {e}", exc_info=True)

async def run_news_system(run_monitor: bool, monitor_host: str, monitor_port: int):
    """Handles user interaction and runs the news aggregation loop."""
    logger.info("==========================================")
    logger.info(" Location-Based Agentic News System (V3)")
    logger.info("==========================================")

    # --- Get Current Event Loop & Pass to Collector ---
    try:
        current_loop = asyncio.get_running_loop()
        metrics_collector.set_loop(current_loop)
    except RuntimeError as e:
        logger.critical(f"Could not get running event loop: {e}. Ensure running via asyncio.run().")
        sys.exit(1)

    # --- Start Monitor Server (if requested) ---
    monitor_thread = None
    if run_monitor:
        logger.info("Starting monitor server thread...")
        monitor_thread = threading.Thread(
            target=start_monitor_server,
            args=(monitor_host, monitor_port),
            daemon=True
        )
        monitor_thread.start()
        # Brief pause to allow server thread to initialize
        await asyncio.sleep(3)
        logger.info(f"Monitor server should be running at http://{monitor_host}:{monitor_port}")


    # --- Argument Parsing & LLM Setup (moved inside async func) ---
    parser = argparse.ArgumentParser(description="Run the Location-Based Agentic News System.", add_help=False) # add_help=False if redefined below
    # ... Add arguments as before ...
    # Parse args *within* the async function if needed, or pass from main scope
    # Re-parsing here for clarity if needed, but could be passed as args to run_news_system
    parser = argparse.ArgumentParser(description="Run the Location-Based Agentic News System.")
    parser.add_argument("--monitor", action="store_true", help="Run the real-time web monitor.") # Already used to start thread
    parser.add_argument("--monitor-host", type=str, default="0.0.0.0", help="Host for the monitor server.")
    parser.add_argument("--monitor-port", type=int, default=8000, help="Port for the monitor server.")
    parser.add_argument("--provider", type=str, default="ollama", choices=["ollama", "gemini", "openai", "anthropic"], help="LLM provider.")
    parser.add_argument("--model", type=str, help="LLM model name.")
    parser.add_argument("--location", type=str, help="Run for a specific location and exit.")
    args = parser.parse_args() # Parse again if needed inside


    llm_provider = None
    while llm_provider is None:
        # Determine provider/model based on args or interactive input
        provider_type = args.provider
        model_name = args.model
        is_interactive = not args.location and sys.stdin.isatty()

        if is_interactive and not args.provider: # Allow override if interactive
             provider_type = input(f"Enter LLM provider (ollama, ...) [default: {args.provider}]: ").strip().lower() or args.provider

        try:
            if provider_type == "ollama" and not model_name:
                 if args.location or not is_interactive: # Non-interactive requires model
                     logger.error("Ollama model name required (--model) for non-interactive mode.")
                     # Cannot proceed without model, exit or raise
                     raise ValueError("Ollama model required.")
                 model_name = input("Enter the EXACT Ollama model name (e.g., gemma:2b): ").strip()
                 if not model_name: raise ValueError("Ollama model name cannot be empty.") # Validate input

            llm_provider = create_llm_provider(provider_type, model=model_name)
            logger.info(f"Using LLM Provider: {type(llm_provider).__name__} with model '{model_name or 'default'}'")

        except (ValueError, NotImplementedError) as e:
            logger.error(f"LLM Provider Error: {e}")
            if args.location: sys.exit(1) # Exit non-interactive on error
            # Loop again for interactive mode
        except Exception as e:
             logger.critical(f"Failed during LLM setup: {e}", exc_info=True)
             sys.exit(1)


    # --- System Initialization ---
    try:
        system = NewsAggregationSystem(llm_provider)
    except Exception as e:
        logger.critical(f"Failed to initialize NewsAggregationSystem: {e}", exc_info=True)
        sys.exit(1)


    # --- Main Loop / Single Run ---
    if args.location:
        logger.info(f"Running single query for location: {args.location}")
        try:
            result = await system.process_location(args.location)
            print("\n--- News Report ---")
            print(result)
            print("--- End Report ---")
        except Exception as e:
            logger.critical(f"Error during single run for '{args.location}': {e}", exc_info=True)
            # Don't sys.exit here, let finally block run
    else:
        # Interactive loop
        while True:
            try:
                location_input = input("\nEnter location (or 'exit' to quit): ").strip()
                if not location_input: continue
                if location_input.lower() == 'exit': break

                result = await system.process_location(location_input)
                print("\n--- News Report ---")
                print(result)
                print("--- End Report ---")

            except KeyboardInterrupt:
                logger.info("User interrupted loop.")
                break
            except Exception as e:
                logger.error(f"Error during interactive processing loop: {e}", exc_info=True)
                print(f"\nAn error occurred. Please check logs and try again.")


    logger.info("News Agent System shutting down.")
    # --- Stop Monitor Background Tasks ---
    if run_monitor:
         logger.info("Stopping monitor background tasks...")
         await metrics_collector.stop_background_tasks()

    # Monitor thread is daemon, will exit when main process ends.

if __name__ == "__main__":
    if sys.platform == "win32":
        # This policy is generally needed for libraries like aiohttp on Windows
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Parse minimal args needed *before* starting async loop
    temp_parser = argparse.ArgumentParser(add_help=False)
    temp_parser.add_argument("--monitor", action="store_true")
    temp_parser.add_argument("--monitor-host", type=str, default="0.0.0.0")
    temp_parser.add_argument("--monitor-port", type=int, default=8000)
    known_args, _ = temp_parser.parse_known_args()

    try:
        # Pass monitor args to the main async function
        asyncio.run(run_news_system(
            run_monitor=known_args.monitor,
            monitor_host=known_args.monitor_host,
            monitor_port=known_args.monitor_port
        ))
    except KeyboardInterrupt:
        logger.info("Process terminated by user.")
    except Exception as e:
         # Catch any other exception escaping asyncio.run
         logger.critical(f"Unhandled exception in main execution: {e}", exc_info=True)
         sys.exit(1)