from monitor.metrics import metrics_collector # Import global instance
import logging
import time
import asyncio
import psutil # Import psutil
from collections import defaultdict
import statistics
from typing import List, Dict, Optional

from agents.discovery_agent import NewsSourceDiscoveryAgent
from agents.crawling_agent import NewsCrawlingAgent
from agents.analysis_agent import NewsAnalysisAgent
from agents.organization_agent import NewsOrganizationAgent
from agents.storage_agent import NewsStorageAgent
from llm_providers.base import LLMProvider
import config

logger = logging.getLogger(__name__)

class ProcessingMetrics:
    """Simple class to hold metrics for a single process_location run."""
    def __init__(self, location: str):
        self.location = location
        self.start_time = time.monotonic()
        self.end_time: Optional[float] = None
        self.stage_timings: Dict[str, float] = {}
        self.counts = defaultdict(int) # sources_discovered, sources_verified, links_found, articles_fetched, articles_analyzed, articles_stored, errors
        self.resource_usage = {"cpu_percent": [], "memory_percent": []}
        self.errors: List[str] = []

    def record_stage(self, stage_name: str, start_time: float):
        """Records the duration of a stage."""
        duration = time.monotonic() - start_time
        self.stage_timings[stage_name] = duration
        logger.info(f"[Metrics] Stage '{stage_name}' completed in {duration:.2f}s")

    def increment_count(self, key: str, value: int = 1):
        """Increments a count metric."""
        self.counts[key] += value

    def add_error(self, message: str):
        """Records an error message."""
        self.errors.append(message)
        self.increment_count("errors")

    def snapshot_resources(self):
        """Takes a snapshot of current CPU and Memory usage."""
        try:
            self.resource_usage["cpu_percent"].append(psutil.cpu_percent(interval=None))
            self.resource_usage["memory_percent"].append(psutil.virtual_memory().percent)
        except Exception as e:
            logger.warning(f"Failed to capture resource usage: {e}")

    def finalize(self):
        """Calculates total time and finalizes metrics."""
        self.end_time = time.monotonic()
        self.stage_timings["total_duration"] = self.end_time - self.start_time

    def get_summary(self) -> str:
        """Generates a summary string of the collected metrics."""
        if self.end_time is None: self.finalize()

        summary = [f"--- Processing Summary for '{self.location}' ---"]
        summary.append(f"Total Duration: {self.stage_timings.get('total_duration', 0):.2f}s")

        summary.append("\nStage Timings:")
        for stage, duration in self.stage_timings.items():
            if stage != "total_duration":
                summary.append(f"  - {stage}: {duration:.2f}s")

        summary.append("\nCounts:")
        for key, value in self.counts.items():
            summary.append(f"  - {key}: {value}")

        summary.append("\nResource Usage (Approx Peak):")
        try:
            max_cpu = max(self.resource_usage['cpu_percent']) if self.resource_usage['cpu_percent'] else 'N/A'
            max_mem = max(self.resource_usage['memory_percent']) if self.resource_usage['memory_percent'] else 'N/A'
            summary.append(f"  - Max CPU Usage: {max_cpu}%")
            summary.append(f"  - Max Memory Usage: {max_mem}%")
        except Exception:
             summary.append("  - Failed to calculate resource peaks.")


        if self.errors:
            summary.append(f"\nErrors ({len(self.errors)}):")
            # Optionally list first few errors
            # for i, err in enumerate(self.errors[:3]):
            #     summary.append(f"  {i+1}. {err[:100]}...") # Truncate long errors
        summary.append("---------------------------------------")
        return "\n".join(summary)


class NewsAggregationSystem:
    """Orchestrates the news aggregation process with monitoring."""

    def __init__(self, llm_provider: LLMProvider):
        self.llm_provider = llm_provider
        # Initialize agents (pass shared session if optimizing later)
        self.discovery_agent = NewsSourceDiscoveryAgent(llm_provider)
        self.crawling_agent = NewsCrawlingAgent()
        self.analysis_agent = NewsAnalysisAgent(llm_provider)
        self.organization_agent = NewsOrganizationAgent(llm_provider)
        self.storage_agent = NewsStorageAgent(db_path=config.DATABASE_PATH)
        logger.info("NewsAggregationSystem initialized with all agents.")
        # Simple health check on init
        asyncio.create_task(self._initial_health_check())

    async def _initial_health_check(self):
        """Performs a basic check of DB and LLM connectivity."""
        logger.info("Performing initial health checks...")
        # DB Check
        try:
            # Perform a simple query
            await self.storage_agent.get_articles_by_location("__health_check__", limit=1)
            logger.info("DB connection check: OK")
        except Exception as e:
            logger.error(f"DB connection check failed: {e}", exc_info=True)

        # LLM Check (Simple Ping/Short Generate)
        try:
            # Use a very short, harmless prompt
            response = await self.llm_provider.generate("Respond with 'OK'")
            if "ok" in response.lower():
                logger.info("LLM connectivity check: OK")
            else:
                logger.warning(f"LLM connectivity check: Unexpected response - '{response[:50]}...'")
        except Exception as e:
            logger.error(f"LLM connectivity check failed: {e}", exc_info=True)

    async def process_location(self, location: str) -> str:
        """Processes news aggregation for a location with metrics."""
        # Use the global metrics_collector
        global metrics_collector
        logger.info(f"===== Processing Location: {location} =====")
        await metrics_collector.update_system_status("Processing", location=location)

        metrics_collector.snapshot_resources() # Initial resource usage
        process_start_time = time.monotonic()

        final_presentation = f"Processing failed for {location}."
        articles_to_present = []
        note = ""
        stage_start = time.monotonic() # Start timer for discovery

        try:
            # 1. Discover Sources
            sources = await self.discovery_agent.discover_sources(location)
            metrics_collector.record_stage_timing("discover_sources", time.monotonic() - stage_start)
            metrics_collector.increment_funnel_count("sources_discovered", len(sources)) # Verified count now
            metrics_collector.snapshot_resources()

            if not sources:
                msg = f"No verifiable sources discovered for '{location}'."
                logger.warning(msg)
                logger.warning(f"No verifiable sources discovered for '{location}'")
                final_presentation = f"No verifiable news sources found for {location}."
                await metrics_collector.update_system_status("Idle")
                # Finalize and log summary on early exit
                total_duration = time.monotonic() - process_start_time
                metrics_collector.record_processing_time(total_duration)
                logger.warning(f"Summary for '{location}' (failed early):\n{metrics_collector.get_initial_data()}") # Log snapshot
                return final_presentation + f"\n\n(Processing time: {total_duration:.2f} seconds)"


            # 2. Crawl Sources
            stage_start = time.monotonic()
            new_articles = await self.crawling_agent.crawl_sources(sources, location_query=location)
            metrics_collector.record_stage_timing("crawl_sources", time.monotonic() - stage_start)
            # Assuming crawl_sources returns validated articles
            metrics_collector.increment_funnel_count("articles_validated", len(new_articles))
            metrics_collector.snapshot_resources()

            if not new_articles:
                 logger.warning(f"No new articles fetched for '{location}'. Checking database...")
                 logger.warning(f"No new articles fetched for '{location}'")
                 # ... (DB check logic - record timing for it) ...
                 stage_start_db = time.monotonic()
                 existing_articles = await self.storage_agent.get_articles_by_location(location, limit=25)
                 metrics_collector.record_stage_timing("db_check_existing", time.monotonic() - stage_start_db)
                 if existing_articles:
                      metrics_collector.increment_funnel_count("articles_from_db", len(existing_articles))
                      articles_to_present = existing_articles
                      note = "\n\n(Note: Displaying stored articles; no new ones fetched.)"
                 else:
                      logger.warning(f"No stored articles found for '{location}'")
                      final_presentation = f"No articles found for {location}."
                      await metrics_collector.update_system_status("Idle")
                      total_duration = time.monotonic() - process_start_time
                      metrics_collector.record_processing_time(total_duration)
                      logger.warning(f"Summary for '{location}' (no articles):\n{metrics_collector.get_initial_data()}") # Log snapshot
                      return final_presentation + f"\n\n(Processing time: {total_duration:.2f} seconds)"
            else:
                 articles_to_present = new_articles


            # 3. Analyze Articles (only if new)
            analyzed_articles = []
            if new_articles:
                 stage_start = time.monotonic()
                 # Instrument analysis agent if possible, or just time the whole batch
                 analyzed_articles = await self.analysis_agent.analyze_articles(articles_to_present)
                 metrics_collector.record_stage_timing("analyze_articles", time.monotonic() - stage_start)
                 metrics_collector.increment_funnel_count("articles_analyzed", len(analyzed_articles))
                 metrics_collector.snapshot_resources()
                 articles_to_present = analyzed_articles

                 # 4. Store Analyzed Articles (async task)
                 stage_start = time.monotonic()
                 storage_task = asyncio.create_task(self.storage_agent.save_or_update_articles(analyzed_articles))
                 metrics_collector.record_stage_timing("storage_initiated", time.monotonic() - stage_start)
                 metrics_collector.increment_funnel_count("articles_stored", len(analyzed_articles))
                 # Let storage_task run, no direct timing here unless awaited
            else:
                 logger.info("Skipping analysis/storage for stored articles.")


            # 5. Generate Presentation
            stage_start = time.monotonic()
            presentation = await self.organization_agent.generate_presentation(location, articles_to_present)
            metrics_collector.record_stage_timing("generate_presentation", time.monotonic() - stage_start)
            final_presentation = presentation + note
            metrics_collector.snapshot_resources() # Final check

        except Exception as e:
            logger.critical(f"!!! Critical error processing '{location}': {e}", exc_info=True)
            logger.critical(f"Critical system error processing '{location}': {e}")
            final_presentation = f"An unexpected error occurred while processing {location}. Check logs."

        finally:
            total_duration = time.monotonic() - process_start_time
            metrics_collector.record_processing_time(total_duration)
            await metrics_collector.update_system_status("Idle") # Reset status
            logger.info(f"===== Finished Processing {location} in {total_duration:.2f} seconds =====")
            logger.info(f"Final metrics summary for '{location}':\n{self._format_metrics_summary(location)}")
            final_presentation += f"\n\n(Processing time: {total_duration:.2f} seconds)"
            pass

        return final_presentation
    
    def _format_metrics_summary(self, location: str) -> str:
        """Creates a concise summary of the metrics for logging."""
        funnel = metrics_collector.funnel_counts
        timings = {}
        
        # Get stage timings if available
        with metrics_collector._lock:
            if hasattr(metrics_collector, 'stage_timings'):
                timings = metrics_collector.stage_timings
        
        summary = [
            f"===== Metrics Summary for {location} =====",
            f"Processing Pipeline:",
            f"  Sources: discovered={funnel.get('sources_discovered', 0)}, verified={funnel.get('sources_verified', 0)}",
            f"  Articles: fetched={funnel.get('articles_fetched', 0)}, validated={funnel.get('articles_validated', 0)}, analyzed={funnel.get('articles_analyzed', 0)}, stored={funnel.get('articles_stored', 0)}",
            f"  From DB: {funnel.get('articles_from_db', 0)}",
            f"  Errors: {funnel.get('errors', 0)}"
        ]
        
        # Add processing time summary
        if timings:
            summary.append("\nStage Timings (seconds):")
            for stage, duration in timings.items():
                summary.append(f"  - {stage}: {duration:.2f}s")
        
        # Add latest LLM metrics if available
        llm_metrics = {}
        with metrics_collector._lock:
            if hasattr(metrics_collector, 'llm_metrics'):
                for key, metric in metrics_collector.llm_metrics.items():
                    llm_metrics[key] = f"calls={metric.call_count}, errors={metric.error_count}, avg_latency={round(metric.avg_latency() * 1000) if metric.avg_latency() is not None else 'N/A'}ms"
        
        if llm_metrics:
            summary.append("\nLLM Usage:")
            for model, stats in llm_metrics.items():
                summary.append(f"  - {model}: {stats}")
        
        # Add resource usage if available
        with metrics_collector._lock:
            if hasattr(metrics_collector, 'resource_snapshots') and metrics_collector.resource_snapshots:
                latest = metrics_collector.resource_snapshots[-1]
                cpu = latest.get('cpu', 'N/A')
                memory = latest.get('memory_percent', 'N/A')
                summary.append(f"\nResource Usage (Latest): CPU {cpu}%, Memory {memory}%")
        
        summary.append("=" * 35)
        return "\n".join(summary)