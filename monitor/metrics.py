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
from fastapi import WebSocket

logger = logging.getLogger(__name__)

# Configure history limits
MAX_METRIC_POINTS = 360 # e.g., 60 minutes at 1 point per 10 seconds
MAX_LOG_ENTRIES = 250 # Increased slightly
MAX_ERROR_ENTRIES = 100

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

        # Add agent status tracking
        self.agent_status = {
            "discovery": {"status": "Idle", "last_update": time.time()},
            "crawling": {"status": "Idle", "last_update": time.time()},
            "analysis": {"status": "Idle", "last_update": time.time()},
            "organization": {"status": "Idle", "last_update": time.time()},
            "storage": {"status": "Idle", "last_update": time.time()}
        }

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

    async def register_websocket(self, websocket: WebSocket):
        """Adds a WebSocket connection to the set."""
        self.websocket_connections.add(websocket)
        logger.info(f"WebSocket registered. Total connections: {len(self.websocket_connections)}")

    def unregister_websocket(self, websocket: WebSocket):
        """Removes a WebSocket connection."""
        self.websocket_connections.discard(websocket)
        logger.info(f"WebSocket unregistered. Total connections: {len(self.websocket_connections)}")

    async def broadcast(self, message_type: str, data: Dict[str, Any]):
        """Sends a JSON message to all connected WebSocket clients."""
        if not self.websocket_connections:
            return

        try:
            message_json = json.dumps({"type": message_type, "data": data}) # Prepare JSON once
        except Exception as e:
            logger.error(f"Failed to serialize broadcast message: {e} - Data: {data}", exc_info=True)
            return

        dead_sockets = set()
        for websocket in self.websocket_connections:
            try:
                await websocket.send_text(message_json)
            except Exception as e:
                logger.error(f"Failed to send to websocket: {e}")
                dead_sockets.add(websocket)

        # Clean up any dead connections
        for dead_socket in dead_sockets:
            self.websocket_connections.discard(dead_socket)

    async def update_agent_status(self, agent_id: str, status: str, details: Optional[Dict[str, Any]] = None):
        """Update the status of an agent and broadcast the change."""
        now = time.time()

        # Create a status object
        status_obj = {
            "name": agent_id,
            "task": status,
            "context": str(details) if details else "",
            "timestamp": now,
            "duration": 0  # Will be calculated on the client side
        }

        # Save to internal state
        self.agent_status[agent_id] = status_obj

        # Broadcast the update
        await self.broadcast("agent_status", {agent_id: status_obj})

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

    async def log_event(self, level: int, name: str, message: str, context: Optional[Dict] = None):
        """Async wrapper for log_event_sync that also broadcasts the log."""
        # First call the sync version to record the log
        self.log_event_sync(level, name, message, context)
        
        # If this is an error level or higher, try to broadcast it immediately
        if level >= logging.ERROR:
            log_entry = self.error_history[-1] if self.error_history else None
            if log_entry:
                await self._broadcast_log_entry(log_entry, is_error=True)

    async def _broadcast_log_entry(self, log_entry: Dict, is_error: bool):
        """Async helper to broadcast log/error entries."""
        await self.broadcast("log", log_entry)
        if is_error:
            await self.broadcast("error", log_entry)

    async def update_system_status(self, status: str, location: Optional[str] = None):
         """Async method to update system status and broadcast."""
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
             await self.broadcast("system_status", payload)

    def update_system_status_threadsafe(self, status: str):
         """
         Threadsafe wrapper to schedule status update from non-async contexts.
         NOTE: Does not support updating location thread-safely easily,
               primarily used for setting Warning/Error status from log handler.
         """
         if self._loop and self._loop.is_running():
              self._loop.call_soon_threadsafe(
                   asyncio.create_task,
                   self.update_system_status(status) # Location won't be updated here
              )

    def record_processing_time(self, duration: float):
        """Records a completed location processing duration."""
        with self._lock:
            self.processing_time_hist.append(duration)
            payload = {"duration": duration}
        asyncio.create_task(self.broadcast("processing_time", payload))

    def record_stage_timing(self, stage_name: str, duration: float):
        """Records timing for a specific stage."""
        avg_duration = 0
        with self._lock:
            stage_deque = self.stage_timings_agg[stage_name]
            stage_deque.append(duration)
            avg_duration = sum(stage_deque) / len(stage_deque) if stage_deque else 0
            payload = {"stage": stage_name, "duration": duration, "avg_last_50": avg_duration}
        asyncio.create_task(self.broadcast("stage_timing", payload))

    def increment_funnel_count(self, stage_key: str, value: int = 1):
        """Increments counts for the data processing funnel."""
        with self._lock:
            self.funnel_counts[stage_key] += value
            current_counts = self.funnel_counts.copy()
            if stage_key == "articles_validated":
                self._articles_in_interval += value
        asyncio.create_task(self.broadcast("funnel_update", current_counts))

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
                metric.latencies.append(latency)

            summary = {
                "provider": provider, "model": model,
                "calls": metric.call_count, "errors": metric.error_count,
                "avg_latency_ms": round(metric.avg_latency() * 1000) if metric.avg_latency() is not None else None,
                "error_rate_pct": round(metric.error_rate(), 1)
            }
        asyncio.create_task(self.broadcast("llm_metric", summary))

    def snapshot_resources(self):
        """Takes a snapshot of current CPU and Memory usage (called from main loop)."""
        try:
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
                    self.resource_cpu.timestamps.append(ts)
                    self.resource_cpu.values.append(cpu)
                    self.resource_memory.timestamps.append(ts)
                    self.resource_memory.values.append(mem)
                    payload = {"timestamp": ts, "cpu_percent": cpu, "memory_percent": mem}

                if payload:
                    await self.broadcast("resource_update", payload)
            except Exception as e:
                logger.warning(f"Resource monitoring error: {e}")

            await asyncio.sleep(5)

    async def _aggregate_rates(self):
         """Periodically calculates and broadcasts event rates."""
         logger.info("Rate aggregation background task started.")
         while True:
             await asyncio.sleep(self._rate_interval_seconds)
             ts = time.time()
             payload = {}
             try:
                 with self._lock:
                     current_articles = self._articles_in_interval
                     articles_rate = current_articles / self._rate_interval_seconds
                     self.articles_processed_rate.timestamps.append(ts)
                     self.articles_processed_rate.values.append(articles_rate)
                     self._articles_in_interval = 0

                     error_threshold_ts = ts - self._rate_interval_seconds * 1.5
                     current_errors = sum(1 for e in self.error_history if e['timestamp'] > error_threshold_ts)
                     error_rate = current_errors / self._rate_interval_seconds

                     self.errors_per_interval.timestamps.append(ts)
                     self.errors_per_interval.values.append(error_rate)

                     payload = {
                         "timestamp": ts,
                         "articles_per_sec": articles_rate,
                         "errors_per_sec": error_rate
                     }
                 await self.broadcast("rate_update", payload)
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
             await asyncio.gather(*tasks_to_stop, return_exceptions=True)
             logger.info("Background tasks stopped.")
        self.resource_monitor_task = None
        self.rate_aggregator_task = None

    def get_initial_data(self) -> Dict[str, Any]:
        """Returns a snapshot of current data for new client connections (thread-safe)."""
        with self._lock:
            try:
                llm_metrics_summary = [
                     {
                         "provider": m.provider, "model": m.model,
                         "calls": m.call_count, "errors": m.error_count,
                         "avg_latency_ms": round(m.avg_latency() * 1000) if m.avg_latency() is not None else None,
                         "error_rate_pct": round(m.error_rate(), 1)
                     } for m in self.llm_metrics.values()
                ]
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
                _ = json.dumps(initial_data, default=str)
                logger.debug("Successfully prepared initial data snapshot.")
                return initial_data
            except Exception as e:
                 logger.error(f"Failed to prepare or serialize initial_data within get_initial_data: {e}", exc_info=True)
                 logger.error(f"Problematic data state (partial): {str(self.__dict__)[:2000]}")
                 return {"system_status": {"status": "Error - Data Serialization Failed"}}


metrics_collector = MetricsCollector()

class MetricsLogHandler(logging.Handler):
    """Custom handler to forward logs to the MetricsCollector (using sync method)."""
    def __init__(self, collector: MetricsCollector, level=logging.NOTSET):
        super().__init__(level=level)
        self.collector = collector

    def emit(self, record: logging.LogRecord):
        if record.name.startswith('monitor.') or record.name.startswith('uvicorn') or record.name.startswith('websockets'):
             return
        try:
            context = {}
            message = self.format(record)
            self.collector.log_event_sync(record.levelno, record.name, message, context)
        except Exception:
            self.handleError(record)