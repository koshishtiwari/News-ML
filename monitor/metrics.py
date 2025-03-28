import logging
import time
import asyncio
import psutil
from collections import defaultdict, deque
from typing import Dict, List, Any, Optional, Deque, Set
from dataclasses import dataclass, field
import json
import threading
from weakref import WeakSet # To hold WebSocket connections without preventing cleanup

logger = logging.getLogger(__name__)

# Configure history limits
MAX_METRIC_POINTS = 360 # e.g., 60 minutes at 1 point per 10 seconds
MAX_LOG_ENTRIES = 250 # Increased slightly
MAX_ERROR_ENTRIES = 100
# MAX_AGENT_STATUS_UPDATES = 50 # Not strictly limited this way anymore

@dataclass
class TimeSeriesData:
    timestamps: Deque[float] = field(default_factory=lambda: deque(maxlen=MAX_METRIC_POINTS))
    values: Deque[Any] = field(default_factory=lambda: deque(maxlen=MAX_METRIC_POINTS))

@dataclass
class LLMMetrics:
    provider: str
    model: str
    call_count: int = 0
    error_count: int = 0
    latencies: Deque[float] = field(default_factory=lambda: deque(maxlen=100)) # Store recent latencies

    def avg_latency(self) -> Optional[float]:
        return sum(self.latencies) / len(self.latencies) if self.latencies else None

    def error_rate(self) -> float:
        return (self.error_count / self.call_count * 100) if self.call_count > 0 else 0.0


class MetricsCollector:
    """
    Collects, stores (in-memory), and distributes monitoring data via WebSockets.
    Designed to be thread-safe for data collection methods.
    """
    def __init__(self):
        # --- System State ---
        self.system_status = "Initializing" # OK, Warning, Error
        self.active_location: Optional[str] = None
        self.last_error_ts: Optional[float] = None

        # --- In-Memory Data Store ---
        # Time Series Metrics
        self.resource_cpu: TimeSeriesData = TimeSeriesData()
        self.resource_memory: TimeSeriesData = TimeSeriesData()
        self.processing_time_hist: Deque[float] = deque(maxlen=100) # Store recent durations
        self.articles_processed_rate: TimeSeriesData = TimeSeriesData() # Articles per interval
        self.errors_per_interval: TimeSeriesData = TimeSeriesData() # Errors per interval

        # Stage Timings (Aggregated) - Store recent timings per stage
        self.stage_timings_agg: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=50))

        # Counts & Funnel
        self.funnel_counts: Dict[str, int] = defaultdict(int) # Accumulating counts

        # Logs & Errors
        self.log_history: Deque[Dict[str, Any]] = deque(maxlen=MAX_LOG_ENTRIES)
        self.error_history: Deque[Dict[str, Any]] = deque(maxlen=MAX_ERROR_ENTRIES)

        # Agent Status - {agent_id: status_dict}
        self.agent_status: Dict[str, Dict[str, Any]] = {}

        # LLM Metrics - {"provider/model": LLMMetrics}
        self.llm_metrics: Dict[str, LLMMetrics] = {}

        # --- WebSocket Management ---
        self.websocket_connections: WeakSet = WeakSet() # Use WeakSet

        # --- Background Tasks ---
        self.resource_monitor_task: Optional[asyncio.Task] = None
        self.rate_aggregator_task: Optional[asyncio.Task] = None
        self._rate_interval_seconds = 10 # Aggregation interval
        self._articles_in_interval = 0 # Counter for articles rate calculation

        # --- Threading & Async ---
        self._lock = threading.Lock() # Lock for thread-safe access to shared data
        self._loop: Optional[asyncio.AbstractEventLoop] = None # Main event loop

        logger.info("MetricsCollector initialized.")

    def set_loop(self, loop: asyncio.AbstractEventLoop):
        """Stores the main event loop for threadsafe scheduling."""
        self._loop = loop
        logger.info("Main event loop set for MetricsCollector.")

    async def register_websocket(self, websocket: Any):
        """Adds a WebSocket connection to the set."""
        self.websocket_connections.add(websocket)
        logger.info(f"WebSocket registered. Total connections: {len(self.websocket_connections)}")

    def unregister_websocket(self, websocket: Any):
        """Removes a WebSocket connection."""
        # This method might be called from an exception handler in the websocket endpoint,
        # so it should be relatively safe (no heavy locking needed just for discard).
        self.websocket_connections.discard(websocket)
        logger.info(f"WebSocket unregistered. Total connections: {len(self.websocket_connections)}")

    async def broadcast(self, message: Dict[str, Any]):
        """Sends a JSON message to all connected WebSocket clients."""
        if not self.websocket_connections:
            return

        try:
            message_json = json.dumps(message) # Prepare JSON once
        except Exception as e:
            logger.error(f"Failed to serialize broadcast message: {e} - Message: {message}", exc_info=True)
            return

        # Use gather to send concurrently, collecting potential exceptions
        tasks = [conn.send_text(message_json) for conn in self.websocket_connections]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Log errors without trying complex removal logic here
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.warning(f"Error broadcasting to a WebSocket client: {result}")
                # Potential improvement: If specific connection info is available, log it.

    # --- Data Collection Methods (Thread-Safe where needed) ---

    def log_event_sync(self, level: int, name: str, message: str, context: Optional[Dict] = None):
        """
        Synchronous part of logging: Appends to history safely from any thread.
        Schedules asynchronous broadcast and status update on the main event loop.
        """
        log_entry = {
            "timestamp": time.time(),
            "level": logging.getLevelName(level),
            "name": name,
            "message": message,
            "context": context or {}
        }

        with self._lock:
            self.log_history.append(log_entry)
            is_error = level >= logging.ERROR
            if is_error:
                self.error_history.append(log_entry)
                self.last_error_ts = log_entry["timestamp"]

        # Schedule async operations on the main loop if available
        if self._loop and self._loop.is_running():
            # Schedule broadcast
            self._loop.call_soon_threadsafe(
                asyncio.create_task,
                self._broadcast_log_entry(log_entry, is_error) # Pass is_error flag
            )
            # Schedule status update if it's an error
            if is_error:
                 status = "Error" if level >= logging.CRITICAL else "Warning"
                 self._loop.call_soon_threadsafe(
                     asyncio.create_task,
                     self.update_system_status(status) # Update status async
                 )
        elif not self._loop:
            logger.warning("Event loop not set for broadcasting log entry.")
        # else: loop exists but isn't running (e.g., during shutdown)

    async def _broadcast_log_entry(self, log_entry: Dict, is_error: bool):
        """Async helper to broadcast log/error entries."""
        await self.broadcast({"type": "log", "payload": log_entry})
        if is_error:
            await self.broadcast({"type": "error", "payload": log_entry})

    async def update_system_status(self, status: str, location: Optional[str] = None):
         """Async method to update system status and broadcast."""
         # Location update might need locking if accessed/modified by multiple tasks concurrently
         # But typically only the main processing task updates it.
         # Let's assume status/location updates are primarily from the main async flow for now.
         should_broadcast = False
         with self._lock:
             if status != self.system_status:
                 self.system_status = status
                 should_broadcast = True
             if location is not None and location != self.active_location:
                 self.active_location = location
                 should_broadcast = True

         if should_broadcast:
             payload = {"status": self.system_status, "active_location": self.active_location}
             await self.broadcast({"type": "system_status", "payload": payload})

    # --- Methods primarily called from the main async loop ---
    # --- These don't strictly need locking if only called by main loop/tasks ---
    # --- but adding locks makes them safer if that assumption changes ---

    def update_system_status_threadsafe(self, status: str):
         """
         Threadsafe wrapper to schedule status update from non-async contexts.
         NOTE: Does not support updating location thread-safely easily,
               primarily used for setting Warning/Error status from log handler.
         """
         if self._loop and self._loop.is_running():
              self._loop.call_soon_threadsafe(
                   asyncio.create_task,
                   # Call the main async update method
                   self.update_system_status(status) # Location won't be updated here
              )


    def record_processing_time(self, duration: float):
        """Records a completed location processing duration."""
        with self._lock:
            self.processing_time_hist.append(duration)
            payload = {"duration": duration}
        # Schedule broadcast from main loop is simpler
        asyncio.create_task(self.broadcast({"type": "processing_time", "payload": payload}))

    def record_stage_timing(self, stage_name: str, duration: float):
        """Records timing for a specific stage."""
        avg_duration = 0
        with self._lock:
            stage_deque = self.stage_timings_agg[stage_name]
            stage_deque.append(duration)
            # Calculate average inside lock for consistency
            avg_duration = sum(stage_deque) / len(stage_deque) if stage_deque else 0
            payload = {"stage": stage_name, "duration": duration, "avg_last_50": avg_duration}
        asyncio.create_task(self.broadcast({"type": "stage_timing", "payload": payload}))

    def increment_funnel_count(self, stage_key: str, value: int = 1):
        """Increments counts for the data processing funnel."""
        with self._lock:
            self.funnel_counts[stage_key] += value
            current_counts = self.funnel_counts.copy() # Copy inside lock
            if stage_key == "articles_validated":
                self._articles_in_interval += value
        asyncio.create_task(self.broadcast({"type": "funnel_update", "payload": current_counts}))

    def update_agent_status(self, agent_id: str, name: str, task: str, context: Optional[str] = None):
        """Updates the status of a specific agent instance."""
        now = time.time()
        status_copy = {}
        with self._lock:
            status = self.agent_status.get(agent_id)
            if status is None:
                status = {"start_time": now}
                self.agent_status[agent_id] = status

            last_task = status.get("_last_task")
            # Reset start time when task changes
            if last_task != task:
                status["start_time"] = now
                status["duration"] = 0
            else: # Calculate duration since task started
                 status["duration"] = now - status["start_time"]

            status.update({
                "name": name,
                "task": task,
                "context": context,
                "timestamp": now,
                "_last_task": task # Store last task for comparison
            })
            status_copy = status.copy() # Copy inside lock

        # Schedule broadcast
        asyncio.create_task(self.broadcast({
            "type": "agent_status",
            "payload": {"agent_id": agent_id, "status": status_copy}
        }))

    def record_llm_call(self, provider: str, model: str, latency: float, is_error: bool):
        """Records details about an LLM API call."""
        key = f"{provider}/{model}"
        summary = {}
        with self._lock:
            if key not in self.llm_metrics:
                self.llm_metrics[key] = LLMMetrics(provider=provider, model=model)

            metric = self.llm_metrics[key]
            metric.call_count += 1
            if is_error:
                metric.error_count += 1
            else:
                # Only store latency for successful calls
                metric.latencies.append(latency)

            # Calculate summary inside lock for consistency
            summary = {
                "provider": provider, "model": model,
                "calls": metric.call_count, "errors": metric.error_count,
                "avg_latency_ms": round(metric.avg_latency() * 1000) if metric.avg_latency() is not None else None,
                "error_rate_pct": round(metric.error_rate(), 1)
            }
        # Schedule broadcast
        asyncio.create_task(self.broadcast({"type": "llm_metric", "payload": summary}))

    def snapshot_resources(self):
        """Takes a snapshot of current CPU and Memory usage (called from main loop)."""
        # No broadcast needed here; _monitor_resources handles periodic broadcast
        try:
             # Update deques within lock
             with self._lock:
                  cpu = psutil.cpu_percent(interval=None)
                  mem = psutil.virtual_memory().percent
                  ts = time.time()
                  self.resource_cpu.timestamps.append(ts)
                  self.resource_cpu.values.append(cpu)
                  self.resource_memory.timestamps.append(ts)
                  self.resource_memory.values.append(mem)
        except Exception as e:
            logger.warning(f"Failed to capture resource usage snapshot: {e}")


    # --- Background Tasks ---
    async def _monitor_resources(self):
        """Periodically captures CPU/Memory and broadcasts."""
        logger.info("Resource monitor background task started.")
        while True:
            payload = None
            try:
                with self._lock:
                    cpu = psutil.cpu_percent(interval=None)
                    mem = psutil.virtual_memory().percent
                    ts = time.time()
                    # Append to deques AND prepare payload for broadcast
                    self.resource_cpu.timestamps.append(ts)
                    self.resource_cpu.values.append(cpu)
                    self.resource_memory.timestamps.append(ts)
                    self.resource_memory.values.append(mem)
                    payload = {"timestamp": ts, "cpu_percent": cpu, "memory_percent": mem}

                # Broadcast outside lock
                if payload:
                    await self.broadcast({"type": "resource_update", "payload": payload})
            except Exception as e:
                logger.warning(f"Resource monitoring error: {e}")

            await asyncio.sleep(5) # Interval

    async def _aggregate_rates(self):
         """Periodically calculates and broadcasts event rates."""
         logger.info("Rate aggregation background task started.")
         while True:
             await asyncio.sleep(self._rate_interval_seconds)
             ts = time.time()
             payload = {}
             try:
                 with self._lock:
                     # Articles processed rate
                     current_articles = self._articles_in_interval
                     articles_rate = current_articles / self._rate_interval_seconds
                     self.articles_processed_rate.timestamps.append(ts)
                     self.articles_processed_rate.values.append(articles_rate)
                     self._articles_in_interval = 0 # Reset counter

                     # Error rate (approximate based on recent history)
                     error_threshold_ts = ts - self._rate_interval_seconds * 1.5 # Look slightly further back
                     current_errors = sum(1 for e in self.error_history if e['timestamp'] > error_threshold_ts)
                     # Calculate rate based on actual window duration used for count
                     error_rate = current_errors / self._rate_interval_seconds

                     self.errors_per_interval.timestamps.append(ts)
                     self.errors_per_interval.values.append(error_rate)

                     payload = {
                         "timestamp": ts,
                         "articles_per_sec": articles_rate,
                         "errors_per_sec": error_rate
                     }
                 # Broadcast outside lock
                 await self.broadcast({"type": "rate_update", "payload": payload})
             except Exception as e:
                  logger.warning(f"Rate aggregation error: {e}")


    async def start_background_tasks(self):
        """Starts the periodic monitoring tasks."""
        if self._loop is None:
            logger.error("Cannot start background tasks: Event loop not set.")
            return
        if self.resource_monitor_task is None or self.resource_monitor_task.done():
            self.resource_monitor_task = self._loop.create_task(self._monitor_resources())
            logger.info("Started resource monitoring task.")
        if self.rate_aggregator_task is None or self.rate_aggregator_task.done():
            self.rate_aggregator_task = self._loop.create_task(self._aggregate_rates())
            logger.info("Started rate aggregation task.")

    async def stop_background_tasks(self):
        """Stops the periodic monitoring tasks."""
        tasks_to_stop = []
        if self.resource_monitor_task and not self.resource_monitor_task.done():
            self.resource_monitor_task.cancel()
            tasks_to_stop.append(self.resource_monitor_task)
            logger.info("Stopping resource monitoring task...")
        if self.rate_aggregator_task and not self.rate_aggregator_task.done():
             self.rate_aggregator_task.cancel()
             tasks_to_stop.append(self.rate_aggregator_task)
             logger.info("Stopping rate aggregation task...")

        if tasks_to_stop:
             await asyncio.gather(*tasks_to_stop, return_exceptions=True) # Wait for cancellation
             logger.info("Background tasks stopped.")
        self.resource_monitor_task = None
        self.rate_aggregator_task = None


    # --- Data Access for Initial Load ---
    def get_initial_data(self) -> Dict[str, Any]:
        """Returns a snapshot of current data for new client connections (thread-safe)."""
        # Ensure lock is acquired for consistency
        with self._lock:
            try:
                # Create copies of mutable data structures *inside* lock
                llm_metrics_summary = [
                     {
                         "provider": m.provider, "model": m.model,
                         "calls": m.call_count, "errors": m.error_count,
                         "avg_latency_ms": round(m.avg_latency() * 1000) if m.avg_latency() is not None else None,
                         "error_rate_pct": round(m.error_rate(), 1)
                     } for m in self.llm_metrics.values()
                ]
                # Ensure all data is serializable *before* releasing lock
                initial_data = {
                    "system_status": {"status": self.system_status, "active_location": self.active_location},
                    "resources": {
                        "cpu": {"timestamps": list(self.resource_cpu.timestamps), "values": list(self.resource_cpu.values)},
                        "memory": {"timestamps": list(self.resource_memory.timestamps), "values": list(self.resource_memory.values)},
                    },
                    "rates": {
                         "articles": {"timestamps": list(self.articles_processed_rate.timestamps), "values": list(self.articles_processed_rate.values)},
                         "errors": {"timestamps": list(self.errors_per_interval.timestamps), "values": list(self.errors_per_interval.values)},
                    },
                    "processing_times": list(self.processing_time_hist),
                    "funnel_counts": self.funnel_counts.copy(),
                    "agent_status": self.agent_status.copy(),
                    "logs": list(self.log_history),
                    "errors": list(self.error_history),
                    "llm_metrics": llm_metrics_summary
                }
                # Perform a quick self-check serialization here if needed
                _ = json.dumps(initial_data, default=str) # Test serialization with fallback
                logger.debug("Successfully prepared initial data snapshot.")
                return initial_data
            except Exception as e:
                 logger.error(f"!!! Failed to prepare or serialize initial_data within get_initial_data: {e}", exc_info=True)
                 # Log the problematic structure as best as possible
                 logger.error(f"Problematic data state (partial): {str(self.__dict__)[:2000]}") # Log internal state
                 # Return a safe default indicating error
                 return {"system_status": {"status": "Error - Data Serialization Failed"}}


# --- Global Instance ---
metrics_collector = MetricsCollector()

# --- Custom Log Handler ---
class MetricsLogHandler(logging.Handler):
    """Custom handler to forward logs to the MetricsCollector (using sync method)."""
    def __init__(self, collector: MetricsCollector, level=logging.NOTSET):
        super().__init__(level=level)
        self.collector = collector

    def emit(self, record: logging.LogRecord):
        # Avoid self-logging loops from monitor/server components
        if record.name.startswith('monitor.') or record.name.startswith('uvicorn') or record.name.startswith('websockets'):
             return
        try:
            context = {} # Placeholder for potential future context extraction
            message = self.format(record)
            # Call the synchronous part of the collector safely
            self.collector.log_event_sync(record.levelno, record.name, message, context)
        except Exception:
            # Use standard logging error handling if emit fails
            self.handleError(record)