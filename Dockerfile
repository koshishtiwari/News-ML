# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV MONITOR_HOST=0.0.0.0
ENV MONITOR_PORT=8000

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libxml2-dev \
    libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose the monitor port
EXPOSE 8000

# Command to run the application (runs main.py which starts monitor thread)
# Pass --monitor flag to activate it
CMD ["python", "main.py", "--monitor", "--monitor-host", "0.0.0.0", "--monitor-port", "8000"]