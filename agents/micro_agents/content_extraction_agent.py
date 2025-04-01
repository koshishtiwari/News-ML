import logging
import asyncio
import aiohttp
import time
import re
import hashlib
import aiofiles
from pathlib import Path
from typing import Dict, Any, List, Optional, Set, Tuple, Union
from urllib.parse import urlparse, urljoin
from datetime import datetime, timezone
import traceback
from concurrent.futures import ThreadPoolExecutor

from newspaper import Article as NewspaperArticle
from newspaper import Config as NewspaperConfig
from bs4 import BeautifulSoup
import trafilatura
from readability import Document as ReadabilityDocument

from models.data_models import NewsArticle
from agents.base_agent import BaseAgent, Task, TaskPriority
from monitor.metrics import metrics_collector

# Constants
DEFAULT_TIMEOUT = 30  # seconds
PAGE_CACHE_DIR = "data/cache/pages"
DEFAULT_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
MAX_CONTENT_LENGTH = 60000  # Characters
MAX_CONCURRENT_EXTRACTIONS = 10  # Maximum concurrent extractions
CACHE_EXPIRY_HOURS = 24  # Cache expiry in hours

# Additional headers for improved site compatibility
DEFAULT_HEADERS = {
    'User-Agent': DEFAULT_USER_AGENT,
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}

logger = logging.getLogger(__name__)

class ContentExtractionAgent(BaseAgent):
    """Micro-agent responsible for fetching and extracting content from web pages"""
    
    def __init__(self):
        super().__init__("content_extraction")
        self._session: Optional[aiohttp.ClientSession] = None
        self._extraction_semaphore = asyncio.Semaphore(MAX_CONCURRENT_EXTRACTIONS)
        self._thread_pool = ThreadPoolExecutor(max_workers=4)  # For CPU-intensive parsing tasks
        
        # Setup cache directory with sharding
        self.cache_dir = Path(PAGE_CACHE_DIR)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        for i in range(256):  # Create shard directories (00-ff)
            shard = f"{i:02x}"
            (self.cache_dir / shard).mkdir(exist_ok=True)
        
        # Configure newspaper
        self.newspaper_config = NewspaperConfig()
        self.newspaper_config.browser_user_agent = DEFAULT_USER_AGENT
        self.newspaper_config.request_timeout = DEFAULT_TIMEOUT
        self.newspaper_config.fetch_images = False
        self.newspaper_config.memoize_articles = False
        
        # Metrics tracking
        self.extraction_stats = {
            "cache_hits": 0,
            "extraction_failures": 0,
            "extraction_success": 0,
            "total_latency": 0,
            "trafilatura_used": 0,
            "newspaper_used": 0,
            "readability_used": 0,
        }
        
    async def _initialize_resources(self):
        """Initialize HTTP session"""
        conn = aiohttp.TCPConnector(
            limit=MAX_CONCURRENT_EXTRACTIONS,
            ttl_dns_cache=300,  # Cache DNS results for 5 minutes
            ssl=False,          # Don't verify SSL to avoid some connection issues
        )
        
        self._session = aiohttp.ClientSession(
            connector=conn,
            timeout=aiohttp.ClientTimeout(total=DEFAULT_TIMEOUT),
            headers=DEFAULT_HEADERS
        )
        
        # Clean old cache entries
        asyncio.create_task(self._cleanup_old_cache())
        
    async def _cleanup_resources(self):
        """Close HTTP session"""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
            
        self._thread_pool.shutdown(wait=False)
        
        # Log extraction statistics
        if sum(self.extraction_stats.values()) > 0:
            logger.info(f"Content extraction statistics: {self.extraction_stats}")
            
    async def _process_task_internal(self, task: Task) -> Any:
        """Process content extraction tasks"""
        task_type = task.task_type
        
        if task_type == "extract_article":
            return await self._extract_article(task)
        elif task_type == "extract_batch":
            return await self._extract_article_batch(task)
        else:
            raise ValueError(f"Unknown task type: {task_type}")
            
    def _get_cache_path(self, url: str) -> Path:
        """Generate a cache file path for a URL using sharding"""
        url_hash = hashlib.md5(url.encode()).hexdigest()
        shard = url_hash[:2]  # Use first 2 characters for sharding (00-ff)
        return self.cache_dir / shard / f"{url_hash}.html"
            
    async def _extract_article(self, task: Task) -> Dict[str, Any]:
        """Extract article content from a URL with multi-engine approach and better error handling"""
        article_data = task.data.get("article")
        if not article_data:
            raise ValueError("Article data is required for extraction")
            
        # Get the URL
        url = article_data.get("url")
        title = article_data.get("title")
        
        if not url:
            raise ValueError("URL is required for article extraction")
            
        await self.update_status("Extracting content", {"url": url})
        
        # Track processing time
        start_time = time.time()
        
        try:
            # Process with semaphore to limit concurrent operations
            async with self._extraction_semaphore:
                # Check the cache first
                html_content, cache_hit = await self._get_cached_content(url)
                
                if cache_hit:
                    self.extraction_stats["cache_hits"] += 1
                    logger.debug(f"Cache hit for {url}")
                
                # Fetch the page if not in cache
                if not html_content:
                    html_content = await self._fetch_page_content(url)
                    if not html_content:
                        return {
                            "url": url,
                            "success": False,
                            "error": "Failed to retrieve content"
                        }
                
                # Multi-engine extraction: Trafilatura, Newspaper, Readability
                extracted_content = await self._multi_engine_extraction(url, html_content, title)
                
                # Calculate and track latency
                latency = time.time() - start_time
                self.extraction_stats["total_latency"] += latency
                
                if extracted_content["success"]:
                    self.extraction_stats["extraction_success"] += 1
                else:
                    self.extraction_stats["extraction_failures"] += 1
                
                # Add latency information
                extracted_content["extraction_latency"] = latency
                
                return extracted_content
                
        except Exception as e:
            self.extraction_stats["extraction_failures"] += 1
            logger.error(f"Error extracting article from {url}: {e}", exc_info=True)
            return {
                "url": url,
                "success": False,
                "error": str(e),
                "error_trace": traceback.format_exc()
            }
            
    async def _get_cached_content(self, url: str) -> Tuple[Optional[str], bool]:
        """Get cached content with expiry handling"""
        cache_path = self._get_cache_path(url)
        
        if not cache_path.exists():
            return None, False
            
        # Check cache expiry
        cache_age_hours = (time.time() - cache_path.stat().st_mtime) / 3600
        if cache_age_hours > CACHE_EXPIRY_HOURS:
            logger.debug(f"Cache expired for {url}")
            return None, False
            
        try:
            async with aiofiles.open(cache_path, 'r', encoding='utf-8', errors='replace') as f:
                content = await f.read()
                return content, True
                
        except Exception as e:
            logger.warning(f"Failed to read cache for {url}: {e}")
            return None, False
            
    async def _fetch_page_content(self, url: str) -> Optional[str]:
        """Fetch page content with retry logic and enhanced error handling"""
        retries = 2
        backoff_factor = 1.5
        
        for attempt in range(retries + 1):
            try:
                async with self._session.get(url, allow_redirects=True, timeout=DEFAULT_TIMEOUT) as response:
                    if response.status != 200:
                        logger.warning(f"HTTP error {response.status} for {url}")
                        return None
                    
                    # Check content type to make sure it's HTML
                    content_type = response.headers.get('Content-Type', '').lower()
                    if not ('html' in content_type or 'text' in content_type):
                        logger.warning(f"Not HTML content: {content_type} for {url}")
                        return None
                    
                    # Get the content
                    html_content = await response.text(errors='replace')
                    
                    # Save to cache
                    cache_path = self._get_cache_path(url)
                    try:
                        async with aiofiles.open(cache_path, 'w', encoding='utf-8') as f:
                            await f.write(html_content)
                    except Exception as e:
                        logger.warning(f"Failed to cache content for {url}: {e}")
                        
                    return html_content
                    
            except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                if attempt < retries:
                    logger.warning(f"Retry {attempt+1}/{retries} for {url}: {str(e)}")
                    # Exponential backoff
                    await asyncio.sleep(backoff_factor ** attempt)
                else:
                    logger.error(f"Failed to fetch {url} after {retries} retries: {str(e)}")
                    return None
        
        return None
            
    async def _multi_engine_extraction(self, url: str, html_content: str, provided_title: Optional[str] = None) -> Dict[str, Any]:
        """Extract content using multiple extraction engines for better results"""
        # Create base results structure
        result = {
            "url": url,
            "title": provided_title or "",
            "content": None,
            "published_at": None,
            "source_name": urlparse(url).netloc,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "success": False,
            "extraction_method": None,
        }
        
        # Try trafilatura first (usually best quality)
        try:
            loop = asyncio.get_event_loop()
            trafilatura_result = await loop.run_in_executor(
                self._thread_pool, 
                lambda: trafilatura.extract(
                    html_content, 
                    output_format='json',
                    include_comments=False,
                    include_links=False,
                    include_images=False,
                    include_tables=False,
                    favor_precision=True
                )
            )
            
            if trafilatura_result:
                self.extraction_stats["trafilatura_used"] += 1
                trafilatura_data = json.loads(trafilatura_result)
                
                if trafilatura_data.get('text') and len(trafilatura_data['text']) > 100:
                    result["title"] = trafilatura_data.get('title') or provided_title or ""
                    result["content"] = trafilatura_data.get('text')[:MAX_CONTENT_LENGTH]
                    result["published_at"] = trafilatura_data.get('date')
                    result["success"] = True
                    result["extraction_method"] = "trafilatura"
                    
                    # Return early if we got good results
                    return result
        except Exception as e:
            logger.debug(f"Trafilatura extraction failed for {url}: {e}")
        
        # Try newspaper3k
        try:
            def parse_with_newspaper():
                article = NewspaperArticle(url, config=self.newspaper_config)
                article.download(input_html=html_content)
                article.parse()
                return article
                
            article = await asyncio.to_thread(parse_with_newspaper)
            
            if article.text and len(article.text) > 100:
                self.extraction_stats["newspaper_used"] += 1
                
                # Get published date with timezone awareness
                publish_date = None
                if article.publish_date:
                    publish_date = article.publish_date
                    if publish_date.tzinfo is None:
                        publish_date = publish_date.replace(tzinfo=timezone.utc)
                
                result["title"] = article.title or provided_title or ""
                result["content"] = article.text[:MAX_CONTENT_LENGTH]
                result["published_at"] = publish_date.isoformat() if publish_date else None
                result["source_name"] = article.meta_data.get('og:site_name', urlparse(url).netloc)
                result["success"] = True
                result["extraction_method"] = "newspaper3k"
                
                # Return if we have good content
                if len(article.text) > 500:
                    return result
        except Exception as e:
            logger.debug(f"Newspaper extraction failed for {url}: {e}")
        
        # Last resort: try readability
        if not result["success"] or len(result.get("content", "")) < 300:
            try:
                def parse_with_readability():
                    doc = ReadabilityDocument(html_content)
                    return {
                        "title": doc.title(),
                        "content": doc.summary()
                    }
                    
                readability_result = await asyncio.to_thread(parse_with_readability)
                
                # Clean HTML from readability output
                soup = BeautifulSoup(readability_result["content"], 'html.parser')
                text_content = soup.get_text(' ', strip=True)
                
                if text_content and len(text_content) > 100:
                    self.extraction_stats["readability_used"] += 1
                    
                    result["title"] = readability_result["title"] or result["title"] or provided_title or ""
                    result["content"] = text_content[:MAX_CONTENT_LENGTH]
                    result["success"] = True
                    result["extraction_method"] = "readability"
            except Exception as e:
                logger.debug(f"Readability extraction failed for {url}: {e}")
        
        # Extract source name from meta tags if still just using domain
        if result["source_name"] == urlparse(url).netloc:
            try:
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # Try common meta tags for site name
                for meta_selector in [
                    ('meta', {'property': 'og:site_name'}),
                    ('meta', {'name': 'application-name'}),
                    ('meta', {'name': 'publisher'}),
                    ('meta', {'name': 'twitter:site'})
                ]:
                    site_meta = soup.find(*meta_selector)
                    if site_meta and site_meta.get('content'):
                        result["source_name"] = site_meta.get('content')
                        break
            except Exception:
                pass
            
        # If we still have no success, but we do have a title, consider it partial success
        if not result["success"] and result["title"]:
            result["success"] = True
            result["content"] = "Content extraction failed, but title was retrieved."
            result["extraction_method"] = "title_only"
            
        return result
            
    async def _extract_article_batch(self, task: Task) -> List[Dict[str, Any]]:
        """Extract content from a batch of articles with parallel processing"""
        articles_data = task.data.get("articles", [])
        if not articles_data:
            return []
            
        batch_size = len(articles_data)
        await self.update_status("Extracting batch", {"count": batch_size})
        
        # Use asyncio.gather for parallel processing with semaphore control
        tasks = []
        for i, article_data in enumerate(articles_data):
            # Create a subtask for each article
            subtask = Task(
                task_id=f"extract_{i}",
                task_type="extract_article",
                data={"article": article_data}
            )
            tasks.append(self._extract_article(subtask))
        
        # Track progress
        start_time = time.time()
        
        # Process all tasks concurrently (semaphore in _extract_article will control concurrency)
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results to filter out exceptions
        valid_results = []
        error_count = 0
        
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Error in batch extraction: {result}")
                error_count += 1
            else:
                valid_results.append(result)
                
        # Report batch statistics
        duration = time.time() - start_time
        success_rate = len(valid_results) / batch_size if batch_size > 0 else 0
        
        logger.info(
            f"Batch extraction completed: {len(valid_results)}/{batch_size} successful "
            f"({success_rate:.1%}) in {duration:.2f}s ({duration/batch_size:.2f}s per article)"
        )
        
        await self.update_status(
            "Batch extraction completed", 
            {
                "success_count": len(valid_results),
                "error_count": error_count,
                "duration_seconds": round(duration, 2)
            }
        )
        
        # Update metrics
        await metrics_collector.update_system_status(
            "Content extraction complete", 
            {
                "articles_processed": batch_size,
                "success_rate": round(success_rate * 100, 1)
            }
        )
                
        return valid_results
        
    async def _cleanup_old_cache(self):
        """Clean up old cache entries to prevent unlimited growth"""
        try:
            cutoff_time = time.time() - (CACHE_EXPIRY_HOURS * 3600)
            deleted_count = 0
            
            for shard_dir in self.cache_dir.glob('*'):
                if not shard_dir.is_dir():
                    continue
                    
                for cache_file in shard_dir.glob('*.html'):
                    try:
                        if cache_file.stat().st_mtime < cutoff_time:
                            cache_file.unlink()
                            deleted_count += 1
                    except (OSError, PermissionError):
                        continue
            
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} expired cache entries")
        except Exception as e:
            logger.warning(f"Error during cache cleanup: {e}")
        
    @staticmethod
    def article_dict_to_model(article_dict: Dict[str, Any], location_query: str = None) -> NewsArticle:
        """Convert article dictionary to NewsArticle model"""
        # Parse date strings to datetime objects
        published_at = None
        if article_dict.get('published_at'):
            try:
                published_at = datetime.fromisoformat(article_dict['published_at'].replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                pass
                
        fetched_at = None
        if article_dict.get('fetched_at'):
            try:
                fetched_at = datetime.fromisoformat(article_dict['fetched_at'].replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                fetched_at = datetime.now(timezone.utc)
        else:
            fetched_at = datetime.now(timezone.utc)
            
        # Create the article object
        return NewsArticle(
            url=article_dict['url'],
            title=article_dict.get('title'),
            source_name=article_dict.get('source_name'),
            content=article_dict.get('content'),
            summary=article_dict.get('summary'),
            published_at=published_at,
            fetched_at=fetched_at,
            location_query=location_query or article_dict.get('location_query'),
            category=article_dict.get('category'),
            importance=article_dict.get('importance', 'Medium')
        )