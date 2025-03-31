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
    """Orchestrates the news aggregation process with micro-agents and monitoring."""

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
        # Register the main agents
        self.task_manager.register_agent(self.source_discovery_agent)
        self.task_manager.register_agent(self.rss_discovery_agent)
        self.task_manager.register_agent(self.content_analysis_agent)
        
        # Register all extraction agents
        for agent in self.extraction_agents:
            self.task_manager.register_agent(agent)
            
    async def initialize(self):
        """Initialize the orchestrator and all agents"""
        await metrics_collector.update_system_status("Initializing")
        
        # Initialize the task manager (which initializes all agents)
        await self.task_manager.initialize()
        
        # Start the task manager
        await self.task_manager.start()
        
        # Perform initial system health check
        await self._initial_health_check()
        
        await metrics_collector.update_system_status("Ready")
        logger.info("NewsOrchestrator initialization complete")
        
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
        await metrics_collector.update_system_status("Processing", location=location)
        
        metrics.snapshot_resources()  # Initial resource usage
        
        try:
            # Step 1: Discover sources for the location
            source_discovery_start = time.monotonic()
            await metrics_collector.update_system_status("Discovering sources", location=location)
            
            # Create source discovery task
            source_discovery_task_id = await self.task_manager.create_task(
                "discover_sources",
                {"location": location, "limit": 10},
                TaskPriority.HIGH
            )
            
            # Wait for source discovery to complete
            sources = await self.task_manager.get_task_result(source_discovery_task_id)
            metrics.record_stage("discover_sources", source_discovery_start)
            metrics.increment_count("sources_discovered", len(sources))
            metrics.snapshot_resources()
            
            if not sources:
                logger.warning(f"No sources discovered for '{location}'")
                return f"No verifiable news sources found for {location}."
            
            # Step 2: Process sources to find RSS feeds and extract articles
            articles = []
            rss_discovery_start = time.monotonic()
            
            # Process each source concurrently
            source_tasks = []
            source_task_ids = []
            
            for source in sources:
                # Create tasks for each source
                source_task_id = await self.task_manager.create_task(
                    "discover_feeds",
                    {"source": source.model_dump()},
                    TaskPriority.MEDIUM
                )
                source_task_ids.append(source_task_id)
                
            # Wait for all source processing to complete
            for task_id in source_task_ids:
                feed_urls = await self.task_manager.get_task_result(task_id)
                
                if not feed_urls:
                    continue
                
                # For each feed URL, create a task to fetch and parse
                feed_task_ids = []
                for feed_url in feed_urls:
                    feed_task_id = await self.task_manager.create_task(
                        "fetch_feed",
                        {"feed_url": feed_url},
                        TaskPriority.MEDIUM
                    )
                    feed_task_ids.append(feed_task_id)
                
                # Process the feeds
                for feed_task_id in feed_task_ids:
                    feed_result = await self.task_manager.get_task_result(feed_task_id)
                    
                    if feed_result.get("success") and feed_result.get("articles"):
                        # Add location information to each article
                        for article in feed_result["articles"]:
                            article["location_query"] = location
                            
                        # Add to the list of articles to process
                        articles.extend(feed_result["articles"])
            
            metrics.record_stage("discover_and_fetch_feeds", rss_discovery_start)
            metrics.increment_count("feeds_processed", len(source_task_ids))
            metrics.increment_count("articles_from_feeds", len(articles))
            
            if not articles:
                logger.warning(f"No articles found from RSS feeds for '{location}'")
                return f"No articles found for {location}."
            
            # Step 3: Extract full content for each article
            extraction_start = time.monotonic()
            await metrics_collector.update_system_status("Extracting content", location=location, articles=len(articles))
            
            # Process articles in batches for efficiency
            batch_size = 20
            extracted_articles = []
            
            for i in range(0, len(articles), batch_size):
                batch = articles[i:i+batch_size]
                
                # Create a batch extraction task
                extraction_task_id = await self.task_manager.create_task(
                    "extract_batch",
                    {"articles": batch},
                    TaskPriority.MEDIUM
                )
                
                # Wait for the batch to complete
                batch_results = await self.task_manager.get_task_result(extraction_task_id)
                
                # Filter out failed extractions
                successful_extractions = [a for a in batch_results if a.get("success", False) and a.get("content")]
                extracted_articles.extend(successful_extractions)
            
            metrics.record_stage("extract_content", extraction_start)
            metrics.increment_count("articles_extracted", len(extracted_articles))
            metrics.snapshot_resources()
            
            if not extracted_articles:
                logger.warning(f"No article content could be extracted for '{location}'")
                return f"No article content could be extracted for {location}."
            
            # Step 4: Analyze articles for summaries, importance, and categories
            analysis_start = time.monotonic()
            await metrics_collector.update_system_status("Analyzing content", location=location, articles=len(extracted_articles))
            
            # Create analysis task
            analysis_task_id = await self.task_manager.create_task(
                "analyze_batch",
                {"articles": extracted_articles},
                TaskPriority.HIGH
            )
            
            # Wait for analysis to complete
            analyzed_articles = await self.task_manager.get_task_result(analysis_task_id)
            
            metrics.record_stage("analyze_content", analysis_start)
            metrics.increment_count("articles_analyzed", len(analyzed_articles))
            metrics.snapshot_resources()
            
            if not analyzed_articles:
                logger.warning(f"No articles were successfully analyzed for '{location}'")
                return f"No articles could be analyzed for {location}."
            
            # Step 5: Generate a summary for the location
            summary_start = time.monotonic()
            await metrics_collector.update_system_status("Generating summary", location=location)
            
            # Create summary task
            summary_task_id = await self.task_manager.create_task(
                "summarize_location",
                {"location": location, "articles": analyzed_articles},
                TaskPriority.CRITICAL
            )
            
            # Wait for summary to complete
            location_summary = await self.task_manager.get_task_result(summary_task_id)
            
            metrics.record_stage("generate_summary", summary_start)
            metrics.snapshot_resources()
            
            # Step 6: Format the final presentation
            presentation_start = time.monotonic()
            
            # Sort articles by importance and recency
            importance_order = {"Critical": 0, "High": 1, "Medium": 2, "Low": 3}
            sorted_articles = sorted(
                analyzed_articles,
                key=lambda a: (
                    importance_order.get(a.get("importance", "Medium"), 4),
                    # Sort by published_at if available, otherwise by order in list
                    -1 * (a.get("published_at_timestamp", 0) or -1 * analyzed_articles.index(a))
                )
            )
            
            # Format the presentation
            presentation = [f"# News from {location}"]
            presentation.append("")
            presentation.append(f"## Summary")
            presentation.append(location_summary)
            presentation.append("")
            presentation.append(f"## Top Articles")
            presentation.append("")
            
            # Add top articles by importance
            for i, article in enumerate(sorted_articles[:15], 1):
                title = article.get("title", "Untitled")
                url = article.get("url", "")
                importance = article.get("importance", "Medium")
                category = article.get("category", "General")
                summary = article.get("summary", "No summary available")
                
                presentation.append(f"### {i}. {title}")
                presentation.append(f"**Importance**: {importance} | **Category**: {category}")
                presentation.append(f"**Link**: {url}")
                presentation.append(f"{summary}")
                presentation.append("")
            
            metrics.record_stage("format_presentation", presentation_start)
            
            # Combine into final presentation
            final_presentation = "\n".join(presentation)
            
        except Exception as e:
            logger.critical(f"Error processing '{location}': {e}", exc_info=True)
            metrics.add_error(str(e))
            final_presentation = f"An error occurred while processing news for {location}: {str(e)}"
        
        # Finalize metrics
        metrics.finalize()
        logger.info(f"Processing for '{location}' completed in {metrics.stage_timings.get('total_duration', 0):.2f}s")
        logger.info(metrics.get_summary())
        
        # Reset system status
        await metrics_collector.update_system_status("Ready")
        
        return final_presentation

# Legacy class name alias for backward compatibility
NewsAggregationSystem = NewsOrchestrator