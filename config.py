# This is a placeholder file
import os
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from .env file
dotenv_path = Path('.') / '.env'
load_dotenv(dotenv_path=dotenv_path)

# LLM Keys
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

# Ollama Settings
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_REQUEST_TIMEOUT = int(os.getenv("OLLAMA_REQUEST_TIMEOUT", 300))

# Database
DATABASE_PATH = os.getenv("DATABASE_PATH", "./data/news_archive.db")

# App Settings
DEFAULT_TIMEZONE = os.getenv("DEFAULT_TIMEZONE", "UTC") # Default to UTC if not set

# Crawling settings
MAX_CONTENT_SIZE_MB = 10  # Maximum content size in MB
CRAWL_TIMEOUT_SECONDS = 30  # Timeout for requests
MAX_RETRY_ATTEMPTS = 2  # Number of retry attempts for failed fetches
RETRY_DELAY_SECONDS = 1  # Delay between retries