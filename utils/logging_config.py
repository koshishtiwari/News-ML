# This is a placeholder file
import logging
import sys

def setup_logging(log_level=logging.INFO):
    """Configures logging for the application."""
    # Define format
    log_format = '%(asctime)s - %(levelname)s - [%(name)s] - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'

    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Remove existing handlers if any
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)

    # Create formatter and add it to the handler
    formatter = logging.Formatter(log_format, datefmt=date_format)
    console_handler.setFormatter(formatter)

    # Add the handler to the root logger
    root_logger.addHandler(console_handler)

    # Optional: Set levels for noisy libraries
    logging.getLogger('newspaper').setLevel(logging.WARNING) # Reduce newspaper noise
    logging.getLogger('urllib3').setLevel(logging.WARNING)   # Reduce network noise
    logging.getLogger('asyncio').setLevel(logging.WARNING)

    # Example usage within modules: logger = logging.getLogger(__name__)
    logging.getLogger(__name__).info("Logging configured.")

# Call setup_logging() early in your main.py or application entry point
# setup_logging()