# News Agent System

# Location-Based Agentic News System (V3)

This system discovers local news sources for a given location, crawls articles, analyzes them using an LLM, stores them in a database, and generates a formatted presentation.

## Features

*   Modular agent-based architecture.
*   Uses `newspaper3k` for article fetching and initial parsing.
*   Validates extracted content to avoid boilerplate/errors.
*   Utilizes LLMs (Ollama configured by default) for source discovery and article analysis (summary, importance, category).
*   Stores processed articles in an SQLite database.
*   Configurable via environment variables (`.env` file).
*   Includes Docker support.


## Setup

1.  **Clone the repository:**
    ```bash
    git clone <repository_url>
    cd news_agent_system
    ```
2.  **Create a virtual environment:**
    ```bash
    python -m venv .venv
    source .venv/bin/activate  # On Windows use `.venv\Scripts\activate`
    ```
3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
4.  **Configure Environment:**
    *   Copy `.env.example` to `.env` (if you create an example file) or create `.env` manually.
    *   Fill in your LLM API keys (if using cloud providers) or verify Ollama settings.
    *   Adjust `DATABASE_PATH` or `DEFAULT_TIMEZONE` if needed.
5.  **Ensure LLM Backend is Ready:**
    *   If using Ollama, make sure the Ollama service is running and you have pulled the desired model (e.g., `ollama run gemma3:4b`).

## Running the System

*   **Interactive Mode:**
    ```bash
    python main.py
    ```
    Follow the prompts to select the LLM provider, model, and enter locations.

*   **Single Location Run (via arguments):**
    ```bash
    python main.py --provider ollama --model gemma:2b --location "San Francisco"
    ```

*   **Using Docker:**
    1.  Build the image:
        ```bash
        docker build -t news-agent-system .
        ```
    2.  Run the container (passing environment variables if needed, mounting data volume):
        ```bash
        # Example for Ollama running on host machine (adjust URL if needed)
        docker run --rm -it \
          -v ./data:/app/data \
          -e OLLAMA_BASE_URL="http://host.docker.internal:11434" \
          news-agent-system \
          --provider ollama --model gemma:2b --location "New York City"

        # Or run interactively
        docker run --rm -it \
          -v ./data:/app/data \
          -e OLLAMA_BASE_URL="http://host.docker.internal:11434" \
          news-agent-system
        ```
        *(Note: `host.docker.internal` works on Docker Desktop for Mac/Windows to connect to the host. On Linux, you might use `--network host` or find the host IP).*