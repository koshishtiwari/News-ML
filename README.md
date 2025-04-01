# News Agent System

# Location-Based Agentic News System (V4)

This system discovers local news sources for a given location, crawls articles, analyzes them using an LLM, stores them in a database, and generates a formatted presentation.

## Features

* **Modular agent-based architecture** with clean separation of concerns
* Uses `newspaper3k` for article fetching and initial parsing
* Validates extracted content to avoid boilerplate/errors
* **Multiple LLM Provider Support**:
  * Ollama (local models)
  * Gemini (Google's API)
  * Support structure for OpenAI/Anthropic (implementation ready)
* LLM-powered features:
  * Source discovery for any location
  * Article analysis (summary, importance, category)
  * Presentation generation
* **Enhanced Data Storage**:
  * Fully async SQLite support via `aiosqlite`
  * Article metrics tracking
  * Database maintenance features
* **Multiple Interface Options**:
  * Command-line interactive mode
  * Web monitoring dashboard
  * **REST API** for programmatic access
* **Robust Error Handling**:
  * Automatic retry logic for LLM requests
  * Comprehensive logging
  * Real-time monitoring
* Configurable via environment variables (`.env` file)
* Production-ready Docker support with health checks

## Setup

1. **Clone the repository:**
   ```bash
   git clone <repository_url>
   cd news_agent_system
   ```

2. **Create a virtual environment:**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows use `.venv\Scripts\activate`
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure Environment:**
   * Copy `.env.example` to `.env` (if you create an example file) or create `.env` manually.
   * Fill in your LLM API keys (if using cloud providers) or verify Ollama settings.
   * Adjust `DATABASE_PATH` or `DEFAULT_TIMEZONE` if needed.

5. **Ensure LLM Backend is Ready:**
   * If using Ollama, make sure the Ollama service is running and you have pulled the desired model (e.g., `ollama run gemma:2b`)
   * If using Gemini, ensure your API key is set in the `.env` file

## Running the System

### Interactive Mode

```bash
python main.py
```
Follow the prompts to select the LLM provider, model, and enter locations.

### Single Location Run (via arguments)

```bash
python main.py --provider ollama --model gemma:2b --location "San Francisco"
```

### With Web Monitor Dashboard

```bash
python main.py --monitor
```

### With API Server

```bash
python main.py --api
```

### Run Both Services (API + Monitor)

```bash
python main.py --monitor --api --server-only
```

The monitor interface will be available at http://localhost:8000  
The API documentation will be available at http://localhost:8001/docs

### Using Docker

1. Build the image:
   ```bash
   docker build -t news-agent-system .
   ```

2. Run the container (with volume for data persistence):
   ```bash
   # Example for Ollama running on host machine
   docker run --rm -it \
     -p 8000:8000 -p 8001:8001 \
     -v ./data:/app/data \
     -e OLLAMA_BASE_URL="http://host.docker.internal:11434" \
     news-agent-system
   ```
   *(Note: `host.docker.internal` works on Docker Desktop for Mac/Windows to connect to the host. On Linux, you might use `--network host` or the host's IP address)*

## API Usage

The system includes a RESTful API for accessing news data:

* `GET /api/articles/location/{location}` - Get articles for a specific location
* `GET /api/articles/search?keyword=term` - Search articles by keyword
* `GET /api/stats` - Get database statistics
* `DELETE /api/maintenance/cleanup?days_to_keep=30` - Cleanup old articles

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `OLLAMA_BASE_URL` | URL for Ollama API | http://localhost:11434 |
| `OLLAMA_REQUEST_TIMEOUT` | Timeout for Ollama requests (seconds) | 300 |
| `GEMINI_API_KEY` | API key for Google's Gemini | None |
| `OPENAI_API_KEY` | API key for OpenAI | None |
| `ANTHROPIC_API_KEY` | API key for Anthropic | None |
| `DATABASE_PATH` | Path to SQLite database | ./data/news_archive.db |
| `DEFAULT_TIMEZONE` | Default timezone for timestamps | UTC |
## Architecture

This project uses a micro-agent architecture where specialized agents handle specific tasks:

- **Source Discovery Agent**: Identifies news sources for a location
- **RSS Discovery Agent**: Finds and processes RSS feeds
- **Content Extraction Agent**: Extracts and cleans article content
- **Content Analysis Agent**: Analyzes article content using LLM

The system is coordinated by the **NewsOrchestrator** which manages the workflow between agents through a task manager.
