"""
Enhanced logging configuration with structured logging, colorized output, and comprehensive context.
"""
import logging
import logging.config
import sys
import os
import json
import traceback
import threading
from typing import Dict, Any, Optional
from datetime import datetime
import time

# Try to import optional dependencies for enhanced logging
try:
    import colorama
    from colorama import Fore, Style
    colorama.init(autoreset=True)
    COLORS_AVAILABLE = True
except ImportError:
    COLORS_AVAILABLE = False
    # Create dummy color objects
    class DummyColors:
        def __getattr__(self, name):
            return ""
    Fore = Style = DummyColors()


class StructuredFormatter(logging.Formatter):
    """
    Formatter that outputs JSON or colorized, structured logs depending on the target.
    Also adds extra contextual information to logs.
    """
    def __init__(self, fmt=None, datefmt=None, style='%', json_output=False):
        super().__init__(fmt, datefmt, style)
        self.json_output = json_output
        
    def formatException(self, exc_info):
        """Format an exception with traceback and context"""
        if self.json_output:
            return traceback.format_exception(*exc_info)
        else:
            formatted = super().formatException(exc_info)
            if COLORS_AVAILABLE:
                # Add red color to the traceback
                return f"{Fore.RED}{formatted}{Style.RESET_ALL}"
            return formatted
    
    def format(self, record):
        """
        Format the log record with JSON or colored structured format.
        """
        # Extract standard record attributes
        log_data = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'level': record.levelname,
            'name': record.name,
            'message': record.getMessage(),
            'thread': threading.current_thread().name,
            'thread_id': threading.get_ident(),
            'process_id': os.getpid(),
        }
        
        # Add location info
        if hasattr(record, 'pathname') and record.pathname:
            log_data['file'] = record.pathname
            log_data['line'] = record.lineno
            # Extract just the function name without full path
            log_data['function'] = record.funcName
            
        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)
            
        # Add any custom fields from the record
        for key, value in getattr(record, 'custom_fields', {}).items():
            log_data[key] = value
            
        # Include any extra attributes added by the LoggerAdapter
        if hasattr(record, 'component'):
            log_data['component'] = record.component
        if hasattr(record, 'task'):
            log_data['task'] = record.task
        
        # JSON output for file handlers and structured colorized for console
        if self.json_output:
            return json.dumps(log_data)
        else:
            # Create a colorized structured output for console
            colored_level = self._colorize_level(record.levelno, log_data['level'])
            timestamp = log_data['timestamp'].split('T')[1]  # Just the time portion
            name = f"{Fore.CYAN}{log_data['name']}{Style.RESET_ALL}" if COLORS_AVAILABLE else log_data['name']
            
            # Basic log line with timestamp, level, logger name, and message
            log_line = f"{timestamp} {colored_level} {name}: {log_data['message']}"
            
            # Add location info if debug level
            if record.levelno <= logging.DEBUG and 'file' in log_data:
                location = f"({os.path.basename(log_data['file'])}:{log_data['line']})"
                log_line += f" {Fore.BLUE}{location}{Style.RESET_ALL}" if COLORS_AVAILABLE else f" {location}"
                
            # Add thread info for concurrent operations
            if log_data['thread'] != 'MainThread':
                thread_info = f"[{log_data['thread']}]"
                log_line += f" {Fore.MAGENTA}{thread_info}{Style.RESET_ALL}" if COLORS_AVAILABLE else f" {thread_info}"
                
            # Add exception info if present
            if 'exception' in log_data:
                log_line += f"\n{''.join(log_data['exception'])}" if isinstance(log_data['exception'], list) else f"\n{log_data['exception']}"
                
            # Add extra context for specific log types
            extra_context = []
            for key in ['component', 'task']:
                if key in log_data:
                    extra_context.append(f"{key}={log_data[key]}")
                    
            if extra_context:
                context_str = ", ".join(extra_context)
                log_line += f" ({context_str})"
                
            return log_line
    
    def _colorize_level(self, levelno: int, levelname: str) -> str:
        """Add color to the log level name based on severity."""
        if not COLORS_AVAILABLE:
            return f"[{levelname}]"
            
        if levelno >= logging.CRITICAL:
            return f"{Fore.RED}{Style.BRIGHT}[{levelname}]{Style.RESET_ALL}"
        elif levelno >= logging.ERROR:
            return f"{Fore.RED}[{levelname}]{Style.RESET_ALL}"
        elif levelno >= logging.WARNING:
            return f"{Fore.YELLOW}[{levelname}]{Style.RESET_ALL}"
        elif levelno >= logging.INFO:
            return f"{Fore.GREEN}[{levelname}]{Style.RESET_ALL}"
        else:  # DEBUG and lower
            return f"{Fore.BLUE}[{levelname}]{Style.RESET_ALL}"


class ContextLogger(logging.LoggerAdapter):
    """
    Logger adapter that allows adding context to all log messages.
    """
    def __init__(self, logger, extra=None):
        super().__init__(logger, extra or {})
        
    def process(self, msg, kwargs):
        # Ensure we have an 'extra' dict
        kwargs.setdefault('extra', {})
        
        # Add our stored extra data to the extra
        for key, value in self.extra.items():
            if key not in kwargs['extra']:
                kwargs['extra'][key] = value
                
        return msg, kwargs
        
    def with_context(self, **kwargs) -> 'ContextLogger':
        """Create a new logger with additional context."""
        # Create a new dict combining existing and new context
        new_context = {**self.extra, **kwargs}
        return ContextLogger(self.logger, new_context)

    def get_context(self) -> Dict[str, Any]:
        """Get the current context as a dictionary."""
        return self.extra.copy()


def get_logger(name: str, context: Optional[Dict[str, Any]] = None) -> ContextLogger:
    """
    Get a logger with the specified name and optional context.
    
    Args:
        name: Logger name (typically __name__ from the calling module)
        context: Optional context to add to all log messages
        
    Returns:
        A ContextLogger adapter
    """
    logger = logging.getLogger(name)
    return ContextLogger(logger, context or {})


def setup_logging(log_level: int = logging.INFO, log_file: Optional[str] = None):
    """
    Configure the logging system for the news application.
    
    Args:
        log_level: The minimum log level to capture
        log_file: Optional path to a log file. If None, logs go to console only.
    """
    handlers = {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'colored',
            'stream': sys.stdout,
            'level': log_level,
        },
    }
    
    # Add file handler if log file is specified
    if log_file:
        os.makedirs(os.path.dirname(os.path.abspath(log_file)), exist_ok=True)
        handlers['file'] = {
            'class': 'logging.FileHandler',
            'formatter': 'json',
            'filename': log_file,
            'level': min(log_level, logging.INFO),  # Capture at least INFO for file logs
            'mode': 'a',
        }
    
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'colored': {
                '()': StructuredFormatter,
                'format': '%(levelname)s - %(name)s - %(message)s',
                'json_output': False,
            },
            'json': {
                '()': StructuredFormatter,
                'json_output': True,
            },
        },
        'handlers': handlers,
        'loggers': {
            '': {  # Root logger
                'level': 'DEBUG',
                'handlers': list(handlers.keys()),
                'propagate': True,
            },
            # Configure specific loggers with different levels
            'asyncio': {'level': 'WARNING'},
            'urllib3': {'level': 'WARNING'},
            'newspaper': {'level': 'WARNING'},
            'aiohttp': {'level': 'WARNING'},
            'uvicorn': {'level': 'WARNING'},
            'websockets': {'level': 'WARNING'},
        },
    })

    # Log the startup banner with version info
    root_logger = get_logger('news')
    root_logger.info("=" * 60)
    root_logger.info("NEWS AGGREGATION SYSTEM")
    root_logger.info("=" * 60)
    root_logger.info(f"Python version: {sys.version}")
    root_logger.info(f"Logging level: {logging.getLevelName(log_level)}")
    if log_file:
        root_logger.info(f"Log file: {log_file}")
    root_logger.info("=" * 60)