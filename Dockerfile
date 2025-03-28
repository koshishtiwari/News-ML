# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV MONITOR_HOST=0.0.0.0
ENV MONITOR_PORT=8000
ENV API_HOST=0.0.0.0
ENV API_PORT=8001
ENV OLLAMA_BASE_URL="http://host.docker.internal:11434"

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libxml2-dev \
    libxslt1-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user to run the application
RUN groupadd -r newsapp && useradd -r -g newsapp newsapp

# Create directory for data persistence
RUN mkdir -p /app/data && chown -R newsapp:newsapp /app/data

# Copy requirements file first for better cache usage
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY --chown=newsapp:newsapp . .

# Create health check script
RUN echo '#!/bin/bash\n\
curl -f http://localhost:${API_PORT}/api/health || exit 1\n\
curl -f http://localhost:${MONITOR_PORT} || exit 1' > /app/healthcheck.sh && \
    chmod +x /app/healthcheck.sh

# Expose ports for monitor and API
EXPOSE 8000 8001

# Set user to run the application
USER newsapp

# Command to run the application with both monitor and API
CMD ["python", "main.py", "--monitor", "--monitor-host", "0.0.0.0", "--monitor-port", "8000", "--api", "--api-host", "0.0.0.0", "--api-port", "8001", "--server-only"]

# Health check to verify the application is running
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 CMD ["/app/healthcheck.sh"]