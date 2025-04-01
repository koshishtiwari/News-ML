from monitor.metrics import metrics_collector
import logging
import time
import asyncio
import psutil
from collections import defaultdict
import statistics
from typing import List, Dict, Optional, Any
import uuid

from agents.task_manager import TaskManager, TaskPriority
from agents.base_agent import BaseAgent
from agents.micro_agents.source_discovery_agent import SourceDiscoveryAgent
from agents.micro_agents.rss_discovery_agent import RSSDiscoveryAgent
from agents.micro_agents.content_extraction_agent import ContentExtractionAgent
from agents.micro_agents.content_analysis_agent import ContentAnalysisAgent
from models.data_models import NewsSource, NewsArticle
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


class NewsOrchestrator:
    """
    Main orchestrator for the news aggregation system using micro-agents.
    
    This is the primary entry point for processing location-based news queries.
    It coordinates the workflow between specialized micro-agents through the task manager.
    """

    def __init__(self, llm_provider: LLMProvider):
        self.llm_provider = llm_provider
        
        # Create task manager
        self.task_manager = TaskManager(max_concurrent_tasks=15)
        
        # Initialize agent instances
        self.source_discovery_agent = SourceDiscoveryAgent(llm_provider)
        self.rss_discovery_agent = RSSDiscoveryAgent()
        self.content_extraction_agent = ContentExtractionAgent()
        self.content_analysis_agent = ContentAnalysisAgent(llm_provider)
        
        # Additional agent instances can be created for scalability
        # For example, multiple content extraction agents for parallelization:
        self.extraction_agents = [self.content_extraction_agent]
        for i in range(2):  # Create 2 more extraction agents
            self.extraction_agents.append(ContentExtractionAgent())
            
        # Register all agents with the task manager
        self._register_agents()
        
        # Database agent will be handled separately for now
        # self.db_agent = NewsStorageAgent(db_path=config.DATABASE_PATH)
        
        logger.info("NewsOrchestrator initialized with all micro-agents")
        
    def _register_agents(self):
        """Register all agents with the task manager"""
        # Register the source discovery agent with the correct task type mapping
        self.task_manager.register_agent_for_task_types(
            self.source_discovery_agent,
            ["discover_sources", "verify_source"]
        )
        
        # Register the RSS discovery agent
        self.task_manager.register_agent_for_task_types(
            self.rss_discovery_agent,
            ["discover_feeds", "fetch_feed"]
        )
        
        # Register the content analysis agent
        self.task_manager.register_agent_for_task_types(
            self.content_analysis_agent,
            ["analyze_article", "analyze_batch", "summarize_location"]
        )
        
        # Register all extraction agents
        for agent in self.extraction_agents:
            self.task_manager.register_agent_for_task_types(
                agent,
                ["extract_article", "extract_article_batch"]
            )
            
    async def initialize(self):
        """Initialize the orchestrator and all agents"""
        await metrics_collector.update_system_status("Initializing")
        
        # Initialize the task manager (which initializes all agents)
        logger.info("Initializing task manager")
        await self.task_manager.initialize()
        
        # Start the task manager
        logger.info("Starting task manager")
        await self.task_manager.start()
        
        # Verify task manager is ready
        logger.info("Verifying task manager is properly running")
        self.task_manager._log_worker_status()
        
        # Perform initial system health check
        logger.info("Performing initial health check")
        await self._initial_health_check()
        
        # Wait a moment to ensure everything is fully initialized
        await asyncio.sleep(1)
        
        await metrics_collector.update_system_status("Ready")
        logger.info("NewsOrchestrator initialization complete and ready for processing")
        
    async def shutdown(self):
        """Shutdown the orchestrator and all agents"""
        await metrics_collector.update_system_status("Shutting down")
        
        # Shutdown the task manager (which shuts down all agents)
        await self.task_manager.shutdown()
        
        await metrics_collector.update_system_status("Offline")
        logger.info("NewsOrchestrator shutdown complete")

    async def _initial_health_check(self):
        """Performs a basic check of DB and LLM connectivity."""
        logger.info("Performing initial health checks...")
        
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
        """Process news for a location using the micro-agent workflow"""
        metrics = ProcessingMetrics(location)
        logger.info(f"===== Processing Location: {location} =====")
        logger.info(f"Task Manager status before processing - Workers: {len(self.task_manager._workers)}, Running: {self.task_manager.running}")
        await metrics_collector.update_system_status("Processing", location=location)
        
        metrics.snapshot_resources()  # Initial resource usage
        
        try:
            # Step 1: Discover sources for the location
            source_discovery_start = time.monotonic()
            await metrics_collector.update_system_status("Discovering sources", location=location)
            
            logger.info(f"Creating source discovery task for location: {location}")
            # Create source discovery task
            source_discovery_task_id = await self.task_manager.create_task(
                "discover_sources",
                {"location": location, "limit": 10},
                TaskPriority.HIGH
            )
            
            logger.info(f"Waiting for source discovery task {source_discovery_task_id} to complete")
            # Wait for source discovery to complete (with shorter timeout)
            try:
                # Add a timeout of 2 minutes to prevent indefinite hanging
                sources = await asyncio.wait_for(
                    self.task_manager.get_task_result(source_discovery_task_id),
                    timeout=120  # 2 minutes timeout
                )
                logger.info(f"Source discovery task completed with {len(sources) if sources else 0} sources")
            except (asyncio.TimeoutError, Exception) as e:
                error_msg = "timeout" if isinstance(e, asyncio.TimeoutError) else str(e)
                logger.error(f"Source discovery task failed with error: {error_msg}")
                metrics.record_stage("discover_sources", source_discovery_start)
                return f"Error: Failed to discover news sources for {location}. Please try again later. Details: {error_msg}"
                
            metrics.record_stage("discover_sources", source_discovery_start)
            metrics.increment_count("sources_discovered", len(sources) if sources else 0)
            metrics.snapshot_resources()
            
            if not sources:
                logger.warning(f"No sources discovered for '{location}'")
                return f"No verifiable news sources found for {location}."
            
            # Step 2: Process sources to find RSS feeds and extract articles
            articles = []
            rss_discovery_start = time.monotonic()
            
            # We'll use a simpler approach with error handling throughout
            all_feed_urls = []
            processed_sources = 0
            
            logger.info(f"Processing {len(sources)} sources for RSS feeds")
            for source in sources:
                try:
                    # Create tasks for RSS discovery with timeout protection
                    source_task_id = await self.task_manager.create_task(
                        "discover_feeds",
                        {"source": source.model_dump()},
                        TaskPriority.MEDIUM
                    )
                    
                    # Wait for feed discovery with shorter timeout
                    try:
                        feed_urls = await asyncio.wait_for(
                            self.task_manager.get_task_result(source_task_id),
                            timeout=45  # 45 second timeout per source
                        )
                        
                        if feed_urls:
                            all_feed_urls.extend(feed_urls)
                            logger.info(f"Found {len(feed_urls)} feeds for source: {source.name}")
                        
                        processed_sources += 1
                        
                    except asyncio.TimeoutError:
                        logger.warning(f"Timed out processing source: {source.name}")
                        continue
                        
                except Exception as src_error:
                    logger.error(f"Error processing source {source.name}: {src_error}")
                    continue
            
            logger.info(f"Discovered {len(all_feed_urls)} total RSS feeds from {processed_sources} sources")
            metrics.increment_count("feeds_discovered", len(all_feed_urls))
            
            # Now process the feeds to get articles
            logger.info(f"Processing {len(all_feed_urls)} RSS feeds for articles")
            for feed_url in all_feed_urls[:20]:  # Limit to 20 feeds for performance
                try:
                    # Create a task to fetch this feed
                    feed_task_id = await self.task_manager.create_task(
                        "fetch_feed",
                        {"feed_url": feed_url},
                        TaskPriority.MEDIUM
                    )
                    
                    # Wait for feed processing with timeout
                    try:
                        feed_result = await asyncio.wait_for(
                            self.task_manager.get_task_result(feed_task_id),
                            timeout=30  # 30 second timeout per feed
                        )
                        
                        if feed_result.get("success") and feed_result.get("articles"):
                            # Add location information to each article
                            for article in feed_result["articles"]:
                                article["location_query"] = location
                                
                            # Add to the list of articles to process
                            articles.extend(feed_result["articles"])
                            logger.info(f"Added {len(feed_result['articles'])} articles from feed: {feed_url}")
                            
                    except asyncio.TimeoutError:
                        logger.warning(f"Timed out processing feed: {feed_url}")
                        continue
                        
                except Exception as feed_error:
                    logger.error(f"Error processing feed {feed_url}: {feed_error}")
                    continue
            
            metrics.record_stage("discover_and_fetch_feeds", rss_discovery_start)
            metrics.increment_count("feeds_processed", processed_sources)
            metrics.increment_count("articles_from_feeds", len(articles))
            
            if not articles:
                logger.warning(f"No articles found from RSS feeds for '{location}'")
                return f"No news articles found for {location}. Try another location or try again later."
        
        except Exception as e:
            logger.critical(f"Error processing '{location}': {e}", exc_info=True)
            metrics.add_error(str(e))
            return f"An error occurred while processing news for {location}: {str(e)}"
        
        # Finalize metrics
        metrics.finalize()
        logger.info(f"Processing for '{location}' completed in {metrics.stage_timings.get('total_duration', 0):.2f}s")
        logger.info(metrics.get_summary())
        
        # Reset system status
        await metrics_collector.update_system_status("Ready")
        
        return "Processing complete."

# Legacy class name alias for backward compatibility
NewsAggregationSystem = NewsOrchestrator