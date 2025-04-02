import logging
import aiohttp
import asyncio
import json
import re
import time
import hashlib
from typing import List, Dict, Any, Optional, Set, Tuple, cast
from urllib.parse import urlparse
from pathlib import Path
import aiofiles
from dataclasses import asdict, is_dataclass
from collections import defaultdict
import random
from datetime import datetime, timedelta
import os

from models.data_models import NewsSource, LocationType
from llm_providers.base import LLMProvider
from ..base_agent import BaseAgent, Task, TaskPriority
from monitor.metrics import metrics_collector

# Constants
SOURCE_CACHE_DIR = Path("./data/cache/news_sources")
SOURCE_CACHE_TTL = 7 * 24 * 60 * 60  # 7 days in seconds
MAX_CONCURRENT_VERIFICATIONS = 15
VERIFY_TIMEOUT = 12  # seconds
DEFAULT_CONNECT_TIMEOUT = 10  # seconds
DEFAULT_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

logger = logging.getLogger(__name__)

class SourceDiscoveryAgent(BaseAgent):
    """
    Enhanced micro-agent responsible for discovering and verifying news sources for locations
    with improved caching, parallel processing, and intelligent source ranking.
    """
    
    def __init__(self, llm_provider: LLMProvider):
        super().__init__("source_discovery")
        self.llm_provider = llm_provider
        self._session: Optional[aiohttp.ClientSession] = None
        self._source_quality_cache: Dict[str, Dict[str, Any]] = {}  # url hash -> verification result
        self._source_database: Dict[str, NewsSource] = {}  # source_id -> NewsSource
        self._source_database_location_index: Dict[str, Set[str]] = defaultdict(set)  # location -> source_ids
        self._source_rank_scores: Dict[str, float] = {}  # source_id -> rank score (higher is better)
        self._source_verification_semaphore = asyncio.Semaphore(MAX_CONCURRENT_VERIFICATIONS)
        self._source_location_map: Dict[str, List[str]] = defaultdict(list)  # source_id -> locations
        
        # Domain verification cache
        self._domain_verification_cache: Dict[str, Dict[str, Any]] = {}
        self._domain_cache_timestamps: Dict[str, float] = {}
        
        # Verification metadata store (instead of setting attributes directly on NewsSource objects)
        self._source_verification_metadata: Dict[str, Dict[str, Any]] = {}  # source_id -> verification metadata
        
        # Validation patterns
        self._news_content_patterns = [
            r'<a[^>]*?class=["\']?(?:[^"\'>]*?\s)?(?:headline|article|story|news)[^"\']*?["\'][^>]*?>',
            r'class=["\'](?:[^"\']*?\s)?(?:article|story|news-item|post|news-article)[^"\']*?["\']',
            r'<div[^>]*?id=["\'](?:main-content|content|news|articles)["\'][^>]*?>',
            r'<section[^>]*?class=["\'](?:[^"\']*?\s)?(?:news|articles|stories)[^"\']*?["\'][^>]*?>',
            r'<meta[^>]*?property=["\']og:type["\'][^>]*?content=["\']article["\'][^>]*?>'
        ]
        
        # Ensure cache directory exists
        SOURCE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        
    async def _initialize_resources(self):
        """Initialize the HTTP session and load cached source database with connection pooling"""
        # Create a shared connection pool with generous limits for parallel connections
        conn = aiohttp.TCPConnector(
            limit=100,  # Allow up to 100 connections
            limit_per_host=8,  # Maximum of 8 concurrent connections per host
            ttl_dns_cache=300,  # Cache DNS results for 5 minutes
            enable_cleanup_closed=True,
            force_close=False  # Allow connection reuse
        )
        
        self._session = aiohttp.ClientSession(
            connector=conn,
            timeout=aiohttp.ClientTimeout(
                total=30,
                connect=DEFAULT_CONNECT_TIMEOUT,
                sock_connect=DEFAULT_CONNECT_TIMEOUT,
                sock_read=DEFAULT_CONNECT_TIMEOUT
            ),
            headers={
                'User-Agent': DEFAULT_UA,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Cache-Control': 'max-age=0'
            }
        )
        
        # Load cached source database and clean up expired entries
        await self._load_source_database()
        await self._cleanup_expired_cache()
        
        # Warm up the LLM with a simple prompt
        try:
            await self.llm_provider.generate("List 3 well-known news websites.", max_tokens=30)
        except Exception as e:
            logger.warning(f"LLM warm-up failed, but continuing: {e}")
        
    async def _cleanup_resources(self):
        """Close the HTTP session and save source database"""
        # Save any updated source data
        await self._save_source_database()
        
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
            
    async def _process_task_internal(self, task: Task) -> Any:
        """Process source discovery tasks"""
        task_type = task.task_type
        
        if task_type == "discover_sources":
            return await self._discover_sources(task)
        elif task_type == "verify_source":
            return await self._verify_source(task)
        elif task_type == "bulk_verify_sources":
            return await self._bulk_verify_sources(task)
        elif task_type == "store_fallback_source":
            return await self._store_fallback_source(task)
        else:
            raise ValueError(f"Unknown task type: {task_type}")
            
    async def _store_fallback_source(self, task: Task) -> bool:
        """Store a verified source in the fallback database for future reuse"""
        source_data = task.data.get("source")
        if not source_data:
            raise ValueError("Source data is required")
            
        location = task.data.get("location", "")
        if not location:
            raise ValueError("Location is required for fallback source storage")
            
        try:
            # Create a task for the database storage agent
            db_task = Task(
                task_id=f"store_fallback_{time.time()}",
                task_type="store_fallback_source",
                data={
                    "source": source_data,
                    "location_pattern": location.lower(),
                    "priority": task.data.get("priority", 10),
                    "verified_count": task.data.get("verified_count", 1)
                }
            )
            
            # Get the task manager from parent system
            task_manager = task.data.get("task_manager")
            if task_manager:
                # Submit task to the task manager
                return await task_manager.create_task("store_fallback_source", db_task.data, TaskPriority.LOW)
            else:
                logger.warning("No task manager available for storing fallback source")
                return False
                
        except Exception as e:
            logger.error(f"Error storing fallback source: {e}")
            return False
            
    async def _discover_sources(self, task: Task) -> List[NewsSource]:
        """
        Discover and verify news sources for a location with enhanced caching,
        parallel verification, and intelligent ranking
        """
        location = task.data.get("location")
        if not location:
            raise ValueError("Location is required for source discovery")
            
        limit = task.data.get("limit", 10)
        verify = task.data.get("verify", True)
        use_llm = task.data.get("use_llm", True)
        
        await self.update_status("Discovering sources", 
                               {"location": location, "limit": limit, "verify": verify})
        
        # Check if we have cached sources for this location
        cached_sources = await self._get_cached_sources(location)
        
        # Determine whether we need to refresh the cache or can use existing sources
        need_refresh = self._should_refresh_cache(location, cached_sources, limit)
        
        if not need_refresh and len(cached_sources) >= limit:
            logger.info(f"Using {len(cached_sources)} cached sources for {location}")
            # Sort by rank score (if available) before returning the top sources
            return self._rank_and_sort_sources(cached_sources, location)[:limit]
        
        # Track metrics for source discovery
        discovery_start = time.monotonic()
        sources_from_llm = []
        sources_from_fallback = []
        
        # Try to use LLM to generate sources if needed
        if use_llm:
            try:
                sources_from_llm = await self._generate_sources_via_llm(location, limit)
                
                if sources_from_llm:
                    logger.info(f"Generated {len(sources_from_llm)} sources via LLM for {location}")
                    await metrics_collector.update_agent_status(
                        self.agent_type, 
                        f"Found {len(sources_from_llm)} sources via LLM",
                        {"location": location}
                    )
            except Exception as e:
                logger.warning(f"Failed to generate sources via LLM for {location}: {str(e)}")
        
        # Always get fallback sources to ensure we have a minimum set
        fallback_sources = await self._create_fallback_sources(location)
        sources_from_fallback = fallback_sources
        
        # Combine sources ensuring no duplicates
        all_sources = self._merge_sources(cached_sources, sources_from_llm, sources_from_fallback)
        
        # Verify sources if required
        verified_sources = all_sources
        if verify:
            await self.update_status("Verifying sources", 
                                   {"location": location, "count": len(all_sources)})
            
            # Verify sources in parallel with a concurrency limit
            verification_start = time.monotonic()
            verified_sources = await self._verify_sources_parallel(all_sources)
            
            verification_time = time.monotonic() - verification_start
            logger.info(f"Verified {len(verified_sources)}/{len(all_sources)} sources for {location} "
                       f"in {verification_time:.2f}s")
        
        # Update cache with the validated sources
        if verified_sources:
            await self._update_source_cache(location, verified_sources)
            
            # Save verified sources to fallback database for future use
            # This makes our system learn and improve over time
            task_manager = getattr(self, "task_manager", None)
            if task_manager:
                try:
                    # Store only high-quality sources in fallback database
                    for source in verified_sources:
                        if source.reliability_score >= 0.7:  # Only store reliable sources
                            source_id = self._get_source_id(source)
                            verification_metadata = self._source_verification_metadata.get(source_id, {})
                            
                            # Add source to fallback database with proper metadata
                            await self._store_fallback_source(Task(
                                task_id=f"fallback_store_{source_id}",
                                task_type="store_fallback_source",
                                data={
                                    "source": source,
                                    "location": location,
                                    "task_manager": task_manager,
                                    "priority": 5 if source.location_type == "Local" else 8,
                                    "verified_count": 1
                                }
                            ))
                except Exception as e:
                    logger.warning(f"Error storing sources in fallback database: {e}")
        
        # Log metrics
        discovery_time = time.monotonic() - discovery_start
        logger.info(f"Discovered and processed {len(verified_sources)} sources for {location} "
                   f"in {discovery_time:.2f}s")
        
        # Rank and sort sources by relevance and reliability
        ranked_sources = self._rank_and_sort_sources(verified_sources, location)
        
        # Return the top sources up to the requested limit
        return ranked_sources[:limit]
        
    async def _verify_source(self, task: Task) -> Dict[str, Any]:
        """
        Verify a news source URL works and extract relevant metadata
        with improved error handling and validation
        """
        source_data = task.data.get("source")
        if not source_data:
            raise ValueError("Source data is required for verification")
            
        url = source_data.get("url") if isinstance(source_data, dict) else source_data.url if hasattr(source_data, "url") else None
        name = source_data.get("name") if isinstance(source_data, dict) else source_data.name if hasattr(source_data, "name") else "Unknown"
        
        if not url:
            raise ValueError("URL is required for source verification")
            
        await self.update_status("Verifying source", {"url": url, "name": name})
        
        # Generate cache key from URL
        domain = urlparse(url).netloc
        cache_key = self._get_url_hash(url)
        
        # Check if we have this in our source quality cache
        if cache_key in self._source_quality_cache:
            logger.debug(f"Using cached verification for {url}")
            return self._source_quality_cache[cache_key]
            
        # Check if we've verified this domain already
        domain_key = self._get_url_hash(domain)
        if domain_key in self._domain_verification_cache:
            # Use domain verification cache with the specific URL
            domain_result = self._domain_verification_cache[domain_key].copy()
            domain_result["url"] = url  # Update URL to the specific one
            self._source_quality_cache[cache_key] = domain_result
            logger.debug(f"Using domain-level cached verification for {url}")
            return domain_result
        
        # Use a semaphore to limit concurrent connections
        async with self._source_verification_semaphore:
            try:
                # Attempt to connect with a shorter timeout for efficiency
                start_time = time.monotonic()
                
                # First try a HEAD request to quickly check if the site is responsive
                try:
                    async with self._session.head(
                        url, 
                        allow_redirects=True, 
                        timeout=VERIFY_TIMEOUT,
                        raise_for_status=False
                    ) as head_response:
                        if head_response.status >= 400:
                            raise aiohttp.ClientResponseError(
                                head_response.request_info,
                                head_response.history,
                                status=head_response.status,
                                message=f"HTTP error: {head_response.status}",
                                headers=head_response.headers
                            )
                        
                        # Get content type from headers
                        content_type = head_response.headers.get("Content-Type", "")
                        final_url = str(head_response.url)
                except (asyncio.TimeoutError, aiohttp.ClientError):
                    # HEAD request failed, we'll try GET instead
                    content_type = ""
                    final_url = url
                
                # Now do a GET request to fetch the actual content
                async with self._session.get(
                    url, 
                    allow_redirects=True, 
                    timeout=VERIFY_TIMEOUT,
                    raise_for_status=False,
                    ssl=False  # Ignore SSL errors
                ) as response:
                    latency = time.monotonic() - start_time
                    
                    if response.status >= 400:
                        result = {
                            "url": url,
                            "verified": False,
                            "status_code": response.status,
                            "reason": f"HTTP error: {response.status}",
                            "latency": latency
                        }
                        self._source_quality_cache[cache_key] = result
                        return result
                    
                    # Update content type if not previously set
                    content_type = content_type or response.headers.get("Content-Type", "")
                    final_url = str(response.url)
                    
                    # Check if it's HTML content
                    is_html = "text/html" in content_type.lower() or "application/xhtml+xml" in content_type.lower()
                    if not is_html:
                        result = {
                            "url": url,
                            "verified": False,
                            "reason": f"Not HTML content: {content_type}",
                            "latency": latency
                        }
                        self._source_quality_cache[cache_key] = result
                        return result
                    
                    # Get a sample of the page content (first 40KB is usually enough for verification)
                    # This improves performance when dealing with large pages
                    html_sample = await response.content.read(40960)
                    try:
                        html_content = html_sample.decode('utf-8', errors='ignore')
                    except Exception:
                        html_content = str(html_sample)
                    
                    # Extract basic metadata
                    metadata = self._extract_page_metadata(html_content, url)
                    
                    # Perform additional news source validation
                    is_news_source = self._validate_as_news_source(html_content, url, metadata)
                    
                    if not is_news_source:
                        result = {
                            "url": url,
                            "verified": False,
                            "reason": "Not validated as a news source",
                            "latency": latency,
                            "metadata": metadata
                        }
                        self._source_quality_cache[cache_key] = result
                        return result
                    
                    # Return verification result with metadata
                    result = {
                        "url": url,
                        "verified": True,
                        "title": metadata.get("title", name or urlparse(url).netloc),
                        "description": metadata.get("description", ""),
                        "content_type": content_type,
                        "final_url": final_url,
                        "site_name": metadata.get("site_name", metadata.get("title", name)),
                        "latency": latency,
                        "has_rss": metadata.get("has_rss", False),
                        "has_news_content": True,
                        "verify_time": datetime.now().timestamp()
                    }
                    
                    # Cache at both URL and domain level
                    self._source_quality_cache[cache_key] = result
                    self._domain_verification_cache[domain_key] = result.copy()
                    self._domain_cache_timestamps[domain_key] = time.time()
                    
                    return result
                    
            except asyncio.TimeoutError:
                result = {
                    "url": url,
                    "verified": False,
                    "reason": "Connection timeout",
                    "latency": VERIFY_TIMEOUT
                }
                self._source_quality_cache[cache_key] = result
                return result
            except Exception as e:
                # Detailed error info for debugging
                error_type = type(e).__name__
                result = {
                    "url": url,
                    "verified": False,
                    "reason": f"Error ({error_type}): {str(e)}",
                    "latency": time.monotonic() - start_time
                }
                self._source_quality_cache[cache_key] = result
                return result
    
    async def _bulk_verify_sources(self, task: Task) -> Dict[str, Any]:
        """Verify multiple sources in parallel and return stats"""
        sources = task.data.get("sources", [])
        if not sources:
            return {"verified_count": 0, "total": 0, "sources": []}
            
        results = await self._verify_sources_parallel(sources)
        
        return {
            "verified_count": len(results),
            "total": len(sources),
            "sources": results
        }
    
    async def _verify_sources_parallel(self, sources: List[NewsSource]) -> List[NewsSource]:
        """Verify multiple sources in parallel with improved error handling"""
        if not sources:
            return []
            
        logger.info(f"Starting parallel verification of {len(sources)} sources")
            
        async def verify_source_wrapper(source: NewsSource) -> Tuple[bool, NewsSource]:
            try:
                # Properly handle dataclass conversion
                source_data = asdict(source) if is_dataclass(source) else source
                result = await self._verify_source(Task(
                    task_id=f"verify_{self._get_source_id(source)}",
                    task_type="verify_source",
                    data={"source": source_data}
                ))
                
                if result and result.get("verified", False):
                    # Update source name if site_name is available
                    if "site_name" in result and result["site_name"]:
                        source.name = result["site_name"]
                        
                    # Store verification metadata in our dictionary instead of on the object
                    source_id = self._get_source_id(source)
                    self._source_verification_metadata[source_id] = {
                        "verified": True,
                        "last_verified": datetime.now().timestamp()
                    }
                    
                    # Ensure reliability score is reasonable if we have latency data
                    if source.reliability_score < 0.4 and result.get("latency", 10) < 2:
                        source.reliability_score = max(source.reliability_score, 0.7)
                    
                    return True, source
                return False, source
            except Exception as e:
                logger.warning(f"Error verifying source {source.url}: {str(e)}")
                return False, source
        
        # Break into smaller batches for better progress tracking
        batch_size = 10
        verified_sources = []
        
        # Process in batches to avoid overwhelming the network and provide progress updates
        for i in range(0, len(sources), batch_size):
            batch = sources[i:i+batch_size]
            batch_tasks = [verify_source_wrapper(source) for source in batch]
            
            # Wait for all verifications to complete
            batch_results = await asyncio.gather(*batch_tasks)
            
            # Filter verified sources
            for verified, source in batch_results:
                if verified:
                    verified_sources.append(source)
                    
            # Update progress
            await self.update_status(
                f"Verified {i + len(batch)}/{len(sources)} sources", 
                {"verified": len(verified_sources)}
            )
            
        return verified_sources
                                
    async def _generate_sources_via_llm(self, location: str, limit: int = 10) -> List[NewsSource]:
        """
        Use LLM to generate potential news sources for a location with improved
        prompt engineering and error handling
        """
        try:
            # Track LLM usage for this operation
            await metrics_collector.update_system_status("Generating sources via LLM")
            
            # Enhanced prompt with more specific instructions
            prompt = f"""
            I need to find reputable news sources for {location}. 
            
            Please provide a list of {limit} news sources that:
            1. Have reliable coverage of {location}
            2. Include a mix of local, national and international sources
            3. Have working websites that are still publishing news
            4. Are known for accurate reporting
            
            For each source, provide:
            - name: The publication name (e.g. "The Guardian")
            - url: The direct website URL (e.g. "https://www.theguardian.com")
            - location_type: EXACTLY one of these values: "Local", "National/Local", "National", or "International"
            - reliability_score: A score from 0.0 to 1.0 (where 1.0 is most reliable)
            
            Return your response as a valid JSON array with no additional text.
            Example format:
            [
                {{"name": "Source Name", "url": "https://example.com", "location_type": "Local", "reliability_score": 0.9}}
            ]
            """
            
            # Log the LLM request for analysis
            logger.info(f"Generating sources via LLM for {location}")
            llm_response = await self.llm_provider.generate(prompt)
            
            # Enhanced JSON extraction with multiple patterns
            json_matches = [
                # Look for JSON array
                re.search(r'\[\s*\{.+\}\s*\]', llm_response, re.DOTALL),
                # Look for JSON array with triple backticks
                re.search(r'```json\s*(\[\s*\{.+\}\s*\])\s*```', llm_response, re.DOTALL),
                # Look for JSON array with single backticks
                re.search(r'`(\[\s*\{.+\}\s*\])`', llm_response, re.DOTALL)
            ]
            
            json_text = None
            for match in json_matches:
                if match:
                    # Extract the first group if it exists, otherwise use the whole match
                    json_text = match.group(1) if match.groups() else match.group(0)
                    break
                    
            if not json_text:
                logger.warning(f"Could not extract JSON from LLM response for {location}")
                return []
                
            try:
                sources_data = json.loads(json_text)
                news_sources = []
                
                for source in sources_data:
                    # Validate required fields
                    if not source.get("url", "").startswith(("http://", "https://")):
                        continue  # Skip invalid URLs
                    
                    # Normalize location_type to match our enum
                    location_type = source.get("location_type", "Unknown")
                    normalized_type = self._normalize_location_type(location_type)
                    
                    # Create the NewsSource object with validated data
                    try:
                        news_sources.append(
                            NewsSource(
                                name=source.get("name", "Unknown Source"),
                                url=source.get("url"),
                                location_type=normalized_type,
                                reliability_score=float(source.get("reliability_score", 0.7))
                            )
                        )
                    except (ValueError, TypeError) as e:
                        logger.debug(f"Skipping invalid source {source.get('name')}: {e}")
                        continue
                
                logger.info(f"Generated {len(news_sources)} sources via LLM for {location}")
                return news_sources
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse JSON from LLM response for {location}: {e}")
                
                # Attempt more aggressive pattern matching as a fallback
                try:
                    # Try to extract individual sources with regex and build manually
                    name_matches = re.findall(r'"name"\s*:\s*"([^"]+)"', llm_response)
                    url_matches = re.findall(r'"url"\s*:\s*"([^"]+)"', llm_response)
                    type_matches = re.findall(r'"location_type"\s*:\s*"([^"]+)"', llm_response)
                    score_matches = re.findall(r'"reliability_score"\s*:\s*(\d+\.\d+|\d+)', llm_response)
                    
                    # Zip together as many complete sources as we have matches for
                    min_length = min(len(name_matches), len(url_matches), len(type_matches), len(score_matches))
                    if min_length > 0:
                        sources = []
                        for i in range(min_length):
                            url = url_matches[i]
                            if url.startswith(("http://", "https://")):
                                sources.append(NewsSource(
                                    name=name_matches[i],
                                    url=url,
                                    location_type=self._normalize_location_type(type_matches[i]),
                                    reliability_score=float(score_matches[i])
                                ))
                        
                        if sources:
                            logger.info(f"Extracted {len(sources)} sources through fallback regex for {location}")
                            return sources
                except Exception as regex_error:
                    logger.warning(f"Fallback regex extraction also failed: {regex_error}")
                
                return []
                
        except Exception as e:
            logger.error(f"Error generating sources via LLM for {location}: {str(e)}", exc_info=True)
            return []
    
    async def _create_fallback_sources(self, location: str) -> List[NewsSource]:
        """
        Get fallback sources from the database instead of hardcoded values,
        falling back to a few global sources only if necessary
        """
        try:
            # Get the task manager from our task context or use a direct connection
            task_manager = getattr(self, "task_manager", None)
            db_task_id = None
            
            if task_manager:
                # Create a task to get fallback sources from database
                db_task_id = await task_manager.create_task(
                    "get_fallback_sources_by_location", 
                    {
                        "location": location.lower(),
                        "limit": 10
                    },
                    TaskPriority.HIGH  # High priority since this is needed right away
                )
                
                # Wait for results with a timeout to prevent blocking
                try:
                    fallback_sources = await asyncio.wait_for(
                        task_manager.get_task_result(db_task_id),
                        timeout=10  # 10 second timeout
                    )
                    
                    if fallback_sources and len(fallback_sources) > 0:
                        logger.info(f"Found {len(fallback_sources)} fallback sources from database for {location}")
                        return fallback_sources
                
                except (asyncio.TimeoutError, Exception) as e:
                    error_type = "timeout" if isinstance(e, asyncio.TimeoutError) else str(e)
                    logger.warning(f"Error getting fallback sources from database: {error_type}")
            
            # If we get here, we need to use a minimal set of global sources as a last resort
            # These should rarely be used as the database will populate over time
            global_sources = [
                NewsSource(name="Reuters", url="https://www.reuters.com/", location_type="International", reliability_score=0.95),
                NewsSource(name="Associated Press", url="https://apnews.com/", location_type="International", reliability_score=0.95),
                NewsSource(name="BBC News", url="https://www.bbc.com/news", location_type="International", reliability_score=0.9),
                NewsSource(name="Al Jazeera", url="https://www.aljazeera.com/", location_type="International", reliability_score=0.85),
                NewsSource(name="The Guardian", url="https://www.theguardian.com/", location_type="International", reliability_score=0.9)
            ]
            
            logger.info(f"Using {len(global_sources)} global fallback sources for {location} (database unavailable)")
            return global_sources
            
        except Exception as e:
            logger.error(f"Error creating fallback sources: {e}")
            # Absolute minimum fallback in case of total failure
            return [
                NewsSource(name="Reuters", url="https://www.reuters.com/", location_type="International", reliability_score=0.95),
                NewsSource(name="BBC News", url="https://www.bbc.com/news", location_type="International", reliability_score=0.9)
            ]
    
    def _extract_page_metadata(self, html_content: str, url: str) -> Dict[str, Any]:
        """
        Extract comprehensive metadata from HTML content with improved
        pattern matching and validation
        """
        metadata = {}
        
        # Extract title with fallbacks
        title_patterns = [
            r'<title[^>]*>(.*?)</title>',  # Standard title tag
            r'<meta\s+property=["\'](og:title)["\'][^>]*content=["\'](.*?)["\']',  # OpenGraph title
            r'<meta\s+name=["\'](twitter:title)["\'][^>]*content=["\'](.*?)["\']',  # Twitter title
            r'<h1[^>]*>(.*?)</h1>'  # Main heading as fallback
        ]
        
        for pattern in title_patterns:
            match = re.search(pattern, html_content, re.IGNORECASE | re.DOTALL)
            if match:
                # For meta tags, use group 2, otherwise use group 1
                title = match.group(2) if 'meta' in pattern and len(match.groups()) > 1 else match.group(1)
                metadata["title"] = title.strip()
                break
        
        # Extract meta description with fallbacks
        desc_patterns = [
            r'<meta\s+name=["\'](description)["\'][^>]*content=["\'](.*?)["\']',  # Standard description
            r'<meta\s+property=["\'](og:description)["\'][^>]*content=["\'](.*?)["\']',  # OpenGraph
            r'<meta\s+name=["\'](twitter:description)["\'][^>]*content=["\'](.*?)["\']',  # Twitter
        ]
        
        for pattern in desc_patterns:
            match = re.search(pattern, html_content, re.IGNORECASE)
            if match and len(match.groups()) > 1:
                metadata["description"] = match.group(2).strip()
                break
            
        # Extract site name with fallbacks
        site_patterns = [
            r'<meta\s+property=["\'](og:site_name)["\'][^>]*content=["\'](.*?)["\']',  # OpenGraph
            r'<meta\s+name=["\'](application-name)["\'][^>]*content=["\'](.*?)["\']',  # Application name
            r'<meta\s+name=["\'](twitter:site)["\'][^>]*content=["\'](.*?)["\']',  # Twitter site
        ]
        
        for pattern in site_patterns:
            match = re.search(pattern, html_content, re.IGNORECASE)
            if match and len(match.groups()) > 1:
                metadata["site_name"] = match.group(2).strip()
                break
        
        # Check for RSS feed links
        rss_patterns = [
            r'<link[^>]*type=["\'](application/rss\+xml|application/atom\+xml)["\'][^>]*href=["\'](.*?)["\']',
            r'<link[^>]*href=["\'](.*?)["\'][^>]*type=["\'](application/rss\+xml|application/atom\+xml)["\']',
            r'<a[^>]*href=["\'](.*?(?:feed|rss|atom).*?)["\'][^>]*>.*?(?:RSS|Feed|Atom|Subscribe).*?</a>',
        ]
        
        for pattern in rss_patterns:
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            if matches:
                metadata["has_rss"] = True
                feed_urls = []
                
                # Extract feed URLs based on the pattern matched
                if "type=" in pattern:
                    for match in matches:
                        if isinstance(match, tuple):
                            feed_url = match[1] if len(match) > 1 else match[0]
                            feed_urls.append(feed_url)
                else:
                    feed_urls = [match[0] if isinstance(match, tuple) else match for match in matches]
                    
                metadata["feed_urls"] = feed_urls
                break
        
        # If we couldn't extract a title, use the domain
        if "title" not in metadata:
            metadata["title"] = urlparse(url).netloc
            
        # Determine if site has news content indicators
        metadata["news_indicators"] = self._count_news_indicators(html_content)
            
        return metadata
    
    def _validate_as_news_source(self, html_content: str, url: str, metadata: Dict[str, Any]) -> bool:
        """Validate if a website is likely a news source using multiple heuristics"""
        # Check domain for common news keywords
        domain = urlparse(url).netloc.lower()
        news_domain_indicators = ["news", "times", "herald", "daily", "post", "tribune", "journal", "gazette"]
        
        # Initialize with low score, then add points for news indicators
        news_score = 0
        
        # 1. Check domain name for news keywords
        if any(indicator in domain for indicator in news_domain_indicators):
            news_score += 3
            
        # 2. Check for RSS feeds (strong indicator of news site)
        if metadata.get("has_rss", False):
            news_score += 5
            
        # 3. Check title for news keywords
        title = metadata.get("title", "").lower()
        if any(kw in title for kw in ["news", "latest", "headlines", "breaking"]):
            news_score += 2
            
        # 4. Check description for news indicators
        description = metadata.get("description", "").lower()
        if any(kw in description for kw in ["news", "latest", "coverage", "reporting", "journalism"]):
            news_score += 2
            
        # 5. Check for news content patterns in the HTML
        news_indicators = metadata.get("news_indicators", 0)
        news_score += min(news_indicators * 2, 6)  # Cap at 6 points
        
        # Consider it a news source if score is high enough (threshold determined empirically)
        return news_score >= 6
    
    def _count_news_indicators(self, html_content: str) -> int:
        """Count the number of news content indicators in HTML"""
        count = 0
        
        for pattern in self._news_content_patterns:
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            count += len(matches)
            
            # Early exit if we have enough indicators
            if count >= 5:
                return count
                
        # Check for additional news-specific elements
        if re.search(r'<time[^>]*>', html_content, re.IGNORECASE):
            count += 1
            
        if re.search(r'<div[^>]*byline[^>]*>', html_content, re.IGNORECASE):
            count += 1
            
        if re.search(r'author|published|posted', html_content, re.IGNORECASE):
            count += 1
            
        return count
    
    def _merge_sources(self, *source_lists: List[NewsSource]) -> List[NewsSource]:
        """Merge multiple source lists, avoiding duplicates by URL"""
        seen_urls = set()
        merged_sources = []
        
        for source_list in source_lists:
            for source in source_list:
                if source.url not in seen_urls:
                    seen_urls.add(source.url)
                    merged_sources.append(source)
                    
        return merged_sources
    
    def _rank_and_sort_sources(self, sources: List[NewsSource], location: str) -> List[NewsSource]:
        """
        Rank and sort sources based on relevance to location, reliability,
        and other quality factors
        """
        if not sources:
            return []
            
        location_lower = location.lower()
        
        # Score sources based on multiple factors
        scored_sources = []
        for source in sources:
            source_id = self._get_source_id(source)
            
            # Start with the base reliability score (0-1)
            score = source.reliability_score
            
            # Adjust score based on location type relevance
            location_type_bonus = {
                "Local": 0.4,
                "National/Local": 0.3,
                "National": 0.2,
                "International": 0.1
            }.get(source.location_type, 0)
            
            # Higher bonus for local sources when the location is specific
            # (assuming that more specific locations have longer names)
            if len(location) > 10 and source.location_type == "Local":
                location_type_bonus += 0.2
                
            score += location_type_bonus
            
            # Check if the source name or domain contains the location name
            domain = urlparse(source.url).netloc.lower()
            name_lower = source.name.lower()
            
            if location_lower in name_lower or location_lower in domain:
                score += 0.3  # Big boost for location-specific sources
            
            # Check for previous verification success using our metadata store
            verification_metadata = self._source_verification_metadata.get(source_id, {})
            if verification_metadata.get("verified", False):
                verification_bonus = 0.2
                
                # Extra bonus for recently verified sources
                last_verified = verification_metadata.get("last_verified", 0)
                if last_verified and time.time() - last_verified < 86400:  # Within 24 hours
                    verification_bonus += 0.1
                    
                score += verification_bonus
            
            # Apply any cached rank score if available
            if source_id in self._source_rank_scores:
                cached_score = self._source_rank_scores[source_id]
                # Blend new score with cached score (with more weight to new)
                score = (score * 0.7) + (cached_score * 0.3)
            
            # Store the rank score for future use
            self._source_rank_scores[source_id] = score
            
            scored_sources.append((source, score))
        
        # Sort by score (higher is better) and return the sources
        scored_sources.sort(key=lambda x: x[1], reverse=True)
        
        return [source for source, _ in scored_sources]
    
    async def _get_cached_sources(self, location: str) -> List[NewsSource]:
        """Get cached sources for a location with TTL checking"""
        location_key = location.lower()
        
        # Check memory cache first
        if location_key in self._source_database_location_index:
            source_ids = self._source_database_location_index[location_key]
            cached_sources = [self._source_database[sid] for sid in source_ids if sid in self._source_database]
            
            if cached_sources:
                logger.info(f"Found {len(cached_sources)} sources for {location} in memory cache")
                return cached_sources
            
        # Try to load from disk cache
        cache_path = SOURCE_CACHE_DIR / f"{self._get_location_hash(location)}.json"
        if cache_path.exists():
            try:
                # Check if cache file is recent enough
                cache_age = time.time() - os.path.getmtime(cache_path)
                if cache_age > SOURCE_CACHE_TTL:
                    logger.info(f"Cache for {location} is {cache_age/86400:.1f} days old, will refresh")
                    return []  # Cache is too old, will refresh
                
                # Load and parse the cache file
                async with aiofiles.open(cache_path, 'r') as f:
                    data = json.loads(await f.read())
                    
                sources = []
                for source_data in data.get("sources", []):
                    try:
                        source = NewsSource(
                            name=source_data.get("name", "Unknown"),
                            url=source_data.get("url", ""),
                            location_type=source_data.get("location_type", "Unknown"),
                            reliability_score=float(source_data.get("reliability_score", 0.5))
                        )
                        
                        # Add to in-memory database
                        source_id = self._get_source_id(source)
                        self._source_database[source_id] = source
                        
                        # Add to location index
                        if location_key not in self._source_database_location_index:
                            self._source_database_location_index[location_key] = set()
                        self._source_database_location_index[location_key].add(source_id)
                        
                        # Add location to source location map
                        self._source_location_map[source_id].append(location_key)
                        
                        sources.append(source)
                    except Exception as e:
                        logger.debug(f"Error parsing cached source: {e}")
                        continue
                    
                if sources:
                    logger.info(f"Loaded {len(sources)} cached sources for {location} from disk")
                    return sources
            except Exception as e:
                logger.warning(f"Error loading cached sources for {location}: {str(e)}")
                
        return []
    
    async def _update_source_cache(self, location: str, sources: List[NewsSource]):
        """Update the source cache for a location with improved metadata"""
        if not sources:
            return
            
        location_key = location.lower()
        
        # Update in-memory database
        source_ids = set()
        for source in sources:
            source_id = self._get_source_id(source)
            self._source_database[source_id] = source
            source_ids.add(source_id)
            
            # Update source location map
            if location_key not in self._source_location_map[source_id]:
                self._source_location_map[source_id].append(location_key)
            
        # Update location index
        self._source_database_location_index[location_key] = source_ids
        
        # Save to disk with cache timestamp and metadata
        cache_path = SOURCE_CACHE_DIR / f"{self._get_location_hash(location)}.json"
        try:
            # Convert to serializable format
            serialized_sources = []
            for source in sources:
                source_id = self._get_source_id(source)
                # Get verification metadata from our store
                verification_metadata = self._source_verification_metadata.get(source_id, {})
                
                serialized_sources.append({
                    "name": source.name,
                    "url": source.url,
                    "location_type": source.location_type,
                    "reliability_score": source.reliability_score,
                    "verified": verification_metadata.get("verified", False),
                    "last_verified": verification_metadata.get("last_verified", None)
                })
                
            cache_data = {
                "location": location,
                "created_at": datetime.now().isoformat(),
                "expires_at": (datetime.now() + timedelta(seconds=SOURCE_CACHE_TTL)).isoformat(),
                "source_count": len(sources),
                "sources": serialized_sources
            }
                
            async with aiofiles.open(cache_path, 'w') as f:
                await f.write(json.dumps(cache_data, indent=2))
                
            logger.info(f"Saved {len(sources)} sources for {location} to disk cache")
        except Exception as e:
            logger.warning(f"Error saving sources for {location} to disk: {str(e)}")
    
    async def _load_source_database(self):
        """Load the entire source database from disk with improved error handling"""
        try:
            source_count = 0
            file_count = 0
            
            for cache_file in SOURCE_CACHE_DIR.glob("*.json"):
                file_count += 1
                try:
                    async with aiofiles.open(cache_file, 'r') as f:
                        data = json.loads(await f.read())
                    
                    # Handle both old and new format cache files
                    sources_data = data.get("sources", data if isinstance(data, list) else [])
                    location = data.get("location", cache_file.stem)
                    location_key = str(location).lower()
                    
                    for source_data in sources_data:
                        try:
                            source = NewsSource(
                                name=source_data.get("name", "Unknown"),
                                url=source_data.get("url", ""),
                                location_type=source_data.get("location_type", "Unknown"),
                                reliability_score=float(source_data.get("reliability_score", 0.5))
                            )
                            
                            # Add to in-memory database
                            source_id = self._get_source_id(source)
                            self._source_database[source_id] = source
                            
                            # Add to location index
                            if location_key not in self._source_database_location_index:
                                self._source_database_location_index[location_key] = set()
                            self._source_database_location_index[location_key].add(source_id)
                            
                            # Add to source location map
                            self._source_location_map[source_id].append(location_key)
                            
                            source_count += 1
                        except Exception as e:
                            logger.debug(f"Error parsing source in {cache_file}: {e}")
                            continue
                    
                except Exception as e:
                    logger.warning(f"Error loading source cache file {cache_file}: {str(e)}")
                    
            logger.info(f"Loaded {source_count} sources from {file_count} cache files")
        except Exception as e:
            logger.warning(f"Error loading source database: {str(e)}")
    
    async def _save_source_database(self):
        """Save the entire source database to disk efficiently"""
        # Each location's sources are saved separately in _update_source_cache
        # Here we save an index file to speedup future loads
        
        try:
            # Save a database index file with metadata
            index_path = SOURCE_CACHE_DIR / "database_index.json"
            
            index_data = {
                "updated_at": datetime.now().isoformat(),
                "locations": list(self._source_database_location_index.keys()),
                "source_count": len(self._source_database),
                "location_count": len(self._source_database_location_index)
            }
            
            async with aiofiles.open(index_path, 'w') as f:
                await f.write(json.dumps(index_data, indent=2))
                
            logger.info(f"Saved database index with {len(self._source_database)} sources")
        except Exception as e:
            logger.warning(f"Error saving database index: {str(e)}")
    
    async def _cleanup_expired_cache(self):
        """Clean up expired cache files to free disk space"""
        try:
            now = time.time()
            cleanup_count = 0
            
            for cache_file in SOURCE_CACHE_DIR.glob("*.json"):
                if cache_file.name == "database_index.json":
                    continue
                    
                # Check if file is older than cache TTL
                file_mtime = os.path.getmtime(cache_file)
                if now - file_mtime > SOURCE_CACHE_TTL:
                    # Check if it's empty or corrupted
                    try:
                        file_size = os.path.getsize(cache_file)
                        if file_size < 10:  # Empty or nearly empty file
                            os.unlink(cache_file)
                            cleanup_count += 1
                            continue
                            
                        # For larger files, check if they're very old (3x TTL)
                        if now - file_mtime > SOURCE_CACHE_TTL * 3:
                            os.unlink(cache_file)
                            cleanup_count += 1
                    except Exception:
                        # On any error, we try to clean up the file
                        try:
                            os.unlink(cache_file)
                            cleanup_count += 1
                        except Exception:
                            pass
            
            if cleanup_count:
                logger.info(f"Cleaned up {cleanup_count} expired cache files")
        except Exception as e:
            logger.warning(f"Error during cache cleanup: {str(e)}")
    
    def _should_refresh_cache(self, location: str, cached_sources: List[NewsSource], min_sources: int) -> bool:
        """Determine if we should refresh the cache for a location"""
        # Always refresh if we don't have enough sources
        if len(cached_sources) < min_sources:
            return True
            
        # Check if cache file exists and is recent enough
        cache_path = SOURCE_CACHE_DIR / f"{self._get_location_hash(location)}.json"
        if not cache_path.exists():
            return True
            
        try:
            # Check if cache file is recent
            cache_age = time.time() - os.path.getmtime(cache_path)
            
            # Use a probabilistic refresh strategy:
            # - Always refresh if older than TTL
            # - Increasingly likely to refresh as file gets older
            # This spreads out cache refreshes to avoid cache stampedes
            if cache_age > SOURCE_CACHE_TTL:
                return True
                
            # After half the TTL, start increasing probability of refresh
            if cache_age > (SOURCE_CACHE_TTL / 2):
                # Scale from 0% at half TTL to 100% at full TTL
                refresh_probability = (cache_age - (SOURCE_CACHE_TTL / 2)) / (SOURCE_CACHE_TTL / 2)
                return random.random() < refresh_probability
                
            # Recent cache, no need to refresh
            return False
        except Exception:
            # On any error reading the cache file, refresh
            return True
    
    @staticmethod
    def _get_source_id(source: NewsSource) -> str:
        """Generate a unique ID for a source"""
        return hashlib.md5(source.url.encode()).hexdigest()
    
    @staticmethod
    def _get_url_hash(url: str) -> str:
        """Generate a hash for a URL"""
        return hashlib.md5(url.encode()).hexdigest()
    
    @staticmethod
    def _get_location_hash(location: str) -> str:
        """Generate a hash for a location"""
        return hashlib.md5(location.lower().encode()).hexdigest()
        
    @staticmethod
    def _normalize_location_type(location_type: str) -> str:
        """Normalize location type string to match our enum"""
        location_type = location_type.strip()
        
        # Direct mapping of common values
        mapping = {
            "local": "Local",
            "national/local": "National/Local",
            "national": "National", 
            "international": "International",
            "global": "International",
            "worldwide": "International",
            "regional": "National/Local",
            "national & local": "National/Local",
            "local & national": "National/Local",
        }
        
        # Try direct lookup first
        if location_type.lower() in mapping:
            return mapping[location_type.lower()]
            
        # Check for partial matches
        lower_type = location_type.lower()
        if "local" in lower_type and "national" in lower_type:
            return "National/Local"
        elif "local" in lower_type:
            return "Local"
        elif "national" in lower_type:
            return "National"
        elif "international" in lower_type or "global" in lower_type:
            return "International"
            
        # Check if it's already a valid value
        valid_types = [t.value for t in LocationType.__members__.values()]
        if location_type in valid_types:
            return location_type
            
        # Default to National if we can't determine
        return "National"