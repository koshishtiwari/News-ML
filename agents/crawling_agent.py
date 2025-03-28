# This is an enhanced crawling agent with parallel processing
import logging
import asyncio
from typing import List, Optional, Dict, Any, Set, Tuple
from urllib.parse import urlparse, urljoin
import time
from datetime import datetime, timezone
import hashlib
import os
import re

# For HTTP requests and session management
import aiohttp
from aiohttp import ClientTimeout, TCPConnector
from aiohttp.client_exceptions import ClientError

# For HTML parsing and content extraction
import newspaper
from newspaper import Article as NewspaperArticle
from newspaper import Config as NewspaperConfig
from bs4 import BeautifulSoup

# For progress display
from tqdm.asyncio import tqdm as async_tqdm

# Import models and utilities
from models.data_models import NewsSource, NewsArticle
from utils.logging_config import get_logger

# Import metrics collector for real-time updates
from monitor.metrics import metrics_collector

# Use our enhanced logger
logger = get_logger(__name__, {"component": "crawler"})

# Constants for crawling
MAX_CONNECTIONS_PER_HOST = 8
MAX_CONNECTIONS_TOTAL = 100
DEFAULT_TIMEOUT = 20  # seconds
MAX_CONTENT_SIZE = 10 * 1024 * 1024  # 10MB
CRAWL_DELAY = 0.5  # seconds between requests to the same domain
PAGE_CACHE_DIR = "data/cache/pages"

# Define boilerplate/error patterns to check against extracted text
BOILERPLATE_STRINGS = [
    "javascript is required",
    "enable javascript",
    "browser doesn't support",
    "please enable cookies",
    "verify you are human",
    "site doesn't work properly without javascript",
    "access to this page has been denied",
    "please complete the security check",
    "are you a robot",
    "captcha",
]

# Regex patterns for improved content quality detection
CONTENT_QUALITY_PATTERNS = {
    "subscription_wall": re.compile(r"subscribe|subscription|pay.?wall|premium.?(content|article)", re.I),
    "cookie_notice": re.compile(r"cookie.?(policy|notice|consent|settings)", re.I),
    "social_share": re.compile(r"share.?(on|with|via).?(facebook|twitter|linkedin)", re.I),
}


class NewsCrawlingAgent:
    """Agent responsible for crawling news sources and extracting article content."""

    def __init__(self, cache_dir: str = PAGE_CACHE_DIR):
        """
        Initialize the crawling agent with shared resources.
        
        Args:
            cache_dir: Directory to store cached pages
        """
        # HTTP session will be created when needed
        self._session: Optional[aiohttp.ClientSession] = None
        
        # Configure newspaper
        self.newspaper_config = NewspaperConfig()
        self.newspaper_config.browser_user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        self.newspaper_config.request_timeout = DEFAULT_TIMEOUT
        self.newspaper_config.fetch_images = False
        self.newspaper_config.memoize_articles = False
        self.newspaper_config.verbose = False
        
        # Throttling for politeness - track last request time per domain
        self.domain_last_request: Dict[str, float] = {}
        
        # Initialize the cache directory
        self.cache_dir = cache_dir
        os.makedirs(self.cache_dir, exist_ok=True)
        
        logger.info("NewsCrawlingAgent initialized")

    async def _get_session(self) -> aiohttp.ClientSession:
        """Gets or creates a shared aiohttp session for all requests."""
        if self._session is None or self._session.closed:
            # Configure connection limits and timeouts
            connector = TCPConnector(
                limit_per_host=MAX_CONNECTIONS_PER_HOST,
                limit=MAX_CONNECTIONS_TOTAL,
                ssl=False  # Disable SSL verification for simplicity
            )
            timeout = ClientTimeout(total=DEFAULT_TIMEOUT)
            
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers={
                    'User-Agent': self.newspaper_config.browser_user_agent,
                    'Accept': 'text/html,application/xhtml+xml,application/xml',
                    'Accept-Language': 'en-US,en;q=0.9',
                }
            )
        return self._session

    async def close(self):
        """Close the HTTP session when done."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
            logger.debug("Closed aiohttp session")

    def _get_cache_path(self, url: str) -> str:
        """Generate a cache file path for a URL."""
        # Create a unique filename based on the URL
        url_hash = hashlib.md5(url.encode()).hexdigest()
        return os.path.join(self.cache_dir, f"{url_hash}.html")

    async def _fetch_with_throttling(self, url: str) -> Optional[str]:
        """
        Fetch a URL with polite throttling per domain.
        
        Args:
            url: The URL to fetch
            
        Returns:
            The HTML content as string, or None if fetch fails
        """
        # Extract domain for per-domain throttling
        domain = urlparse(url).netloc
        
        # Get the session
        session = await self._get_session()
        
        # Check if we need to throttle based on previous requests
        now = time.monotonic()
        last_request_time = self.domain_last_request.get(domain, 0)
        if now - last_request_time < CRAWL_DELAY:
            delay = CRAWL_DELAY - (now - last_request_time)
            logger.debug(f"Throttling request to {domain} for {delay:.2f}s")
            await asyncio.sleep(delay)
        
        # Update the last request time for this domain
        self.domain_last_request[domain] = time.monotonic()
        
        try:
            # First check cache
            cache_path = self._get_cache_path(url)
            if os.path.exists(cache_path):
                try:
                    with open(cache_path, 'r', encoding='utf-8') as f:
                        logger.debug(f"Loading from cache: {url}")
                        return f.read()
                except Exception as e:
                    logger.warning(f"Failed to read cache for {url}: {e}")
            
            # Fetch from network
            logger.debug(f"Fetching: {url}")
            async with session.get(url, allow_redirects=True, raise_for_status=False) as response:
                if response.status != 200:
                    logger.warning(f"HTTP error {response.status} fetching {url}")
                    return None
                
                # Check content type
                content_type = response.headers.get('Content-Type', '')
                if 'text/html' not in content_type and 'application/xhtml+xml' not in content_type:
                    logger.warning(f"Non-HTML content type for {url}: {content_type}")
                    return None
                
                # Check content size
                content_length = int(response.headers.get('Content-Length', '0')) or float('inf')
                if content_length > MAX_CONTENT_SIZE:
                    logger.warning(f"Content too large for {url}: {content_length} bytes")
                    return None
                
                # Get the content with a timeout
                try:
                    html = await response.text()
                    
                    # Cache the content
                    try:
                        with open(cache_path, 'w', encoding='utf-8') as f:
                            f.write(html)
                    except Exception as e:
                        logger.warning(f"Failed to cache content for {url}: {e}")
                    
                    return html
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout reading content for {url}")
                    return None
                
        except ClientError as e:
            logger.warning(f"HTTP client error for {url}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching {url}: {e}", exc_info=True)
            return None

    def _is_valid_content(self, html: Optional[str], url: str) -> bool:
        """
        Check if the HTML content is valid and not just boilerplate/error page.
        
        Args:
            html: The HTML content to check
            url: The source URL for logging
            
        Returns:
            True if the content seems valid, False otherwise
        """
        if not html or len(html.strip()) < 500:  # Minimum length to be considered content
            logger.debug(f"Content too short for {url}")
            return False
        
        # Convert to lowercase for case-insensitive matching
        html_lower = html.lower()
        
        # Check for common boilerplate strings
        for pattern in BOILERPLATE_STRINGS:
            if pattern in html_lower:
                logger.debug(f"Boilerplate pattern '{pattern}' found in {url}")
                return False
        
        # Look for basic article indicators (approximate heuristic)
        has_article_structure = '<article' in html_lower or '<div class="article' in html_lower
        has_paragraph_content = html_lower.count('<p>') >= 3  # At least 3 paragraphs
        
        if not (has_article_structure or has_paragraph_content):
            logger.debug(f"No article structure detected in {url}")
            return False
        
        # Check for paywalls and other obstructions
        for pattern_name, pattern in CONTENT_QUALITY_PATTERNS.items():
            if pattern.search(html_lower):
                logger.debug(f"{pattern_name.replace('_', ' ').title()} detected in {url}")
                # We don't reject based on these patterns, just log them

        return True

    async def _extract_article_with_newspaper(self, html: str, url: str) -> Optional[NewspaperArticle]:
        """
        Extract an article using newspaper3k.
        
        Args:
            html: The HTML content
            url: The source URL
            
        Returns:
            A newspaper Article object if successful, None otherwise
        """
        loop = asyncio.get_running_loop()
        try:
            # Use a thread pool to run the synchronous newspaper3k code
            article = await loop.run_in_executor(
                None,
                lambda: self._sync_parse_article(url, html)
            )
            
            if not article or not article.is_parsed:
                logger.warning(f"Failed to parse article: {url}")
                return None
            
            return article
        except Exception as e:
            logger.error(f"Error extracting article from {url}: {e}", exc_info=True)
            return None

    def _sync_parse_article(self, url: str, html: Optional[str] = None) -> Optional[NewspaperArticle]:
        """
        Synchronous helper for newspaper3k parsing.
        
        Args:
            url: The article URL
            html: Optional pre-fetched HTML content
            
        Returns:
            A newspaper Article object if successful, None otherwise
        """
        try:
            article = NewspaperArticle(url, config=self.newspaper_config)
            if html:
                article.set_html(html)
                article.parse()
            else:
                article.download()
                article.parse()
            return article
        except Exception as e:
            logger.debug(f"Newspaper parse error for {url}: {e}")
            return None

    async def _extract_links_from_html(self, html: str, source_url: str) -> List[str]:
        """
        Extract article links from HTML content.
        
        Args:
            html: The HTML content
            source_url: The base URL for resolving relative links
            
        Returns:
            List of absolute URLs found
        """
        if not html:
            return []
        
        links = set()
        base_url = source_url
        domain = urlparse(source_url).netloc.lower()
        
        try:
            # Parse with BeautifulSoup for better link extraction
            soup = BeautifulSoup(html, 'html.parser')
            
            # Find the base tag if it exists
            base_tag = soup.find('base', href=True)
            if base_tag:
                base_url = base_tag['href']
            
            # Extract links
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                if not href or href.startswith(('#', 'javascript:', 'mailto:')):
                    continue
                
                # Convert to absolute URL
                absolute_url = urljoin(base_url, href)
                parsed_url = urlparse(absolute_url)
                
                # Skip non-HTTP(S) URLs
                if parsed_url.scheme not in ('http', 'https'):
                    continue
                
                # Only keep URLs from the same domain
                link_domain = parsed_url.netloc.lower()
                if link_domain != domain and not link_domain.endswith('.' + domain):
                    continue
                
                # Skip common non-article paths
                skip_paths = ['/tag/', '/category/', '/author/', '/about/', '/contact/', 
                              '/advertise/', '/terms/', '/privacy/', '/search']
                path = parsed_url.path.lower()
                if any(path.startswith(skip) for skip in skip_paths):
                    continue
                
                # Articles often have date components in the URL
                has_date_component = re.search(r'/(19|20)\d\d/\d{1,2}/', path) is not None
                
                # Articles often have multiple path segments
                path_depth = len([p for p in path.split('/') if p])
                
                # Heuristic: Articles tend to have longer paths with dates or at least 2 segments
                if has_date_component or path_depth >= 2:
                    links.add(absolute_url)
            
            logger.debug(f"Found {len(links)} candidate article links on {source_url}")
            return list(links)
        
        except Exception as e:
            logger.error(f"Error extracting links from {source_url}: {e}", exc_info=True)
            return []

    async def _process_article(self, url: str, source_name: str, location_query: str) -> Optional[NewsArticle]:
        """
        Process a single article - fetch, validate, extract content.
        
        Args:
            url: The article URL
            source_name: The name of the news source
            location_query: The location this article is related to
            
        Returns:
            A NewsArticle object if successful, None otherwise
        """
        task_logger = logger.with_context(task=f"process_article:{url[-20:]}")
        task_logger.debug(f"Processing article: {url}")
        
        # Update agent status to show currently processing article
        await metrics_collector.update_agent_status(
            "crawling",
            "Crawling",
            {"url": url, "source": source_name}
        )
        
        # Fetch the HTML content
        html = await self._fetch_with_throttling(url)
        
        # Validate the content
        if not html or not self._is_valid_content(html, url):
            task_logger.debug(f"Invalid or empty content for {url}")
            return None
        
        # Extract the article content using newspaper3k
        article_obj = await self._extract_article_with_newspaper(html, url)
        
        if not article_obj:
            task_logger.debug(f"Failed to extract content from {url}")
            return None
        
        # Validate the extracted content
        if not article_obj.title or not article_obj.text:
            task_logger.debug(f"Missing title or body for {url}")
            return None
        
        # Convert article date to timezone-aware
        publish_dt = None
        if article_obj.publish_date:
            pub_date = article_obj.publish_date
            # Ensure timezone-aware UTC
            if pub_date.tzinfo is None:
                publish_dt = pub_date.replace(tzinfo=timezone.utc)
            else:
                publish_dt = pub_date.astimezone(timezone.utc)
        
        # Create and return a NewsArticle object
        article = NewsArticle(
            url=url,
            title=article_obj.title,
            source_name=source_name,
            content=article_obj.text,
            published_at=publish_dt,
            location_query=location_query,
            fetched_at=datetime.now(timezone.utc)
        )
        
        # Send real-time update to the frontend
        # Note: The article will appear with "Processing..." status since it hasn't been analyzed yet
        await metrics_collector.update_article(article)
        
        return article

    async def discover_links_from_source(self, source: NewsSource, limit: int = 25) -> List[str]:
        """
        Discover article links from a news source's homepage and section pages.
        
        Args:
            source: The news source to crawl
            limit: Maximum number of links to return
            
        Returns:
            List of article URLs
        """
        task_logger = logger.with_context(task=f"discover_links:{source.name}")
        task_logger.info(f"Discovering links for {source.name} ({source.url})")
        
        all_links = set()
        visited_urls = set()
        
        # First, extract links from the homepage
        homepage_html = await self._fetch_with_throttling(source.url)
        if not homepage_html:
            task_logger.warning(f"Failed to fetch homepage for {source.name}")
            return []
        
        homepage_links = await self._extract_links_from_html(homepage_html, source.url)
        all_links.update(homepage_links)
        visited_urls.add(source.url)
        
        # Extract additional section links from homepage
        section_links = []
        domain = urlparse(source.url).netloc.lower()
        
        for link in homepage_links:
            # Identify potential section pages
            parsed = urlparse(link)
            if (parsed.netloc.lower() == domain and 
                    parsed.path.count('/') <= 2 and  # Not too deep
                    not parsed.path.endswith(('.html', '.php')) and  # Not article files
                    link != source.url and  # Not the homepage
                    link not in visited_urls):
                section_links.append(link)
        
        # Limit to a reasonable number of section pages to check
        section_links = section_links[:5]
        task_logger.debug(f"Found {len(section_links)} section pages to crawl")
        
        # Fetch articles from section pages in parallel
        section_crawl_tasks = []
        for link in section_links:
            task = asyncio.create_task(self._fetch_section_links(link, visited_urls))
            section_crawl_tasks.append(task)
        
        section_results = await asyncio.gather(*section_crawl_tasks, return_exceptions=True)
        
        # Process results, handling any exceptions
        for result in section_results:
            if isinstance(result, Exception):
                task_logger.warning(f"Error in section crawl: {result}")
            elif isinstance(result, list):
                all_links.update(result)
        
        # Deduplicate and limit
        unique_links = list(all_links)[:limit]
        task_logger.info(f"Found {len(unique_links)} unique article links for {source.name}")
        
        return unique_links

    async def _fetch_section_links(self, section_url: str, visited_urls: Set[str]) -> List[str]:
        """
        Fetch links from a section page.
        
        Args:
            section_url: The section page URL
            visited_urls: Set of already visited URLs to avoid duplicates
            
        Returns:
            List of article URLs found on the section page
        """
        # Skip if already visited
        if section_url in visited_urls:
            return []
        
        visited_urls.add(section_url)
        html = await self._fetch_with_throttling(section_url)
        
        if not html:
            return []
        
        return await self._extract_links_from_html(html, section_url)

    async def crawl_source(self, source: NewsSource, location_query: str, 
                          max_articles: int = 15) -> List[NewsArticle]:
        """
        Crawl a single news source to extract articles.
        
        Args:
            source: The news source to crawl
            location_query: The location these articles are related to
            max_articles: Maximum number of articles to extract
            
        Returns:
            List of extracted articles
        """
        task_logger = logger.with_context(task="crawl_source", source=source.name)
        task_logger.info(f"Crawling source: {source.name}")
        
        # Update agent status to show currently processing source
        await metrics_collector.update_agent_status(
            "crawling", 
            "Crawling Source", 
            {"source": source.name}
        )
        
        start_time = time.time()
        
        # Discover links first
        article_links = await self.discover_links_from_source(source)
        
        if not article_links:
            task_logger.warning(f"No article links discovered for {source.name}")
            return []
        
        # Process articles in parallel with a semaphore to control concurrency
        semaphore = asyncio.Semaphore(MAX_CONNECTIONS_PER_HOST)
        
        async def process_with_semaphore(url: str) -> Optional[NewsArticle]:
            async with semaphore:
                return await self._process_article(url, source.name, location_query)
        
        # Create tasks for all articles
        tasks = [process_with_semaphore(link) for link in article_links[:max_articles]]
        
        # Process with progress tracking
        processed_articles: List[Optional[NewsArticle]] = []
        completed_tasks = 0
        total_tasks = len(tasks)
        
        # Simple progress update
        task_logger.info(f"Processing {total_tasks} articles from {source.name}")
        
        # Use gather with progress tracking
        for future in async_tqdm(asyncio.as_completed(tasks), 
                                total=total_tasks, 
                                desc=f"Crawling {source.name}"):
            article = await future
            if article:  # Only append successful articles
                processed_articles.append(article)
            
            completed_tasks += 1
            if completed_tasks % 5 == 0:  # Log progress every 5 articles
                success_rate = len(processed_articles) / completed_tasks if completed_tasks > 0 else 0
                task_logger.debug(f"Progress: {completed_tasks}/{total_tasks} articles, "
                                 f"{len(processed_articles)} successful ({success_rate:.0%})")
                
                # Update the funnel counts in real-time
                metrics_collector.increment_funnel_count("articles_fetched", len(processed_articles))
        
        elapsed = time.time() - start_time
        success_rate = len(processed_articles) / len(tasks) if tasks else 0
        
        task_logger.info(f"Finished crawling {source.name}: "
                       f"Extracted {len(processed_articles)}/{len(tasks)} articles "
                       f"({success_rate:.0%}) in {elapsed:.1f}s")
        
        return processed_articles

    async def crawl_sources(self, sources: List[NewsSource], location_query: str) -> List[NewsArticle]:
        """
        Crawl multiple news sources and extract articles.
        
        Args:
            sources: List of news sources to crawl
            location_query: The location these articles are related to
            
        Returns:
            Combined list of all extracted articles
        """
        task_logger = logger.with_context(task="crawl_sources", location=location_query)
        task_logger.info(f"Starting crawl for {len(sources)} sources for location '{location_query}'")
        
        # Update agent status
        await metrics_collector.update_agent_status(
            "crawling",
            "Starting Crawl",
            {"location": location_query, "sources": len(sources)}
        )
        
        start_time = time.time()
        all_articles: List[NewsArticle] = []
        
        try:
            # Process sources in parallel with a semaphore to control overall concurrency
            # We use a larger semaphore here as each source already has its own concurrency control
            source_semaphore = asyncio.Semaphore(min(10, len(sources)))
            
            async def crawl_with_semaphore(source: NewsSource) -> List[NewsArticle]:
                async with source_semaphore:
                    try:
                        return await self.crawl_source(source, location_query)
                    except Exception as e:
                        task_logger.error(f"Error crawling {source.name}: {e}", exc_info=True)
                        return []
            
            # Create tasks for all sources
            tasks = [crawl_with_semaphore(source) for source in sources]
            
            # Process with progress tracking
            for source_idx, future in enumerate(async_tqdm(asyncio.as_completed(tasks),
                                                        total=len(tasks),
                                                        desc="Crawling Sources")):
                source_articles = await future
                all_articles.extend(source_articles)
                
                # Log progress
                task_logger.info(f"Completed source {source_idx + 1}/{len(sources)}: "
                               f"Found {len(source_articles)} articles, "
                               f"Total so far: {len(all_articles)}")
                
                # Update funnel counts
                metrics_collector.increment_funnel_count("articles_validated", len(source_articles))
                
                # Brief delay between sources for politeness
                await asyncio.sleep(0.5)
            
            # Deduplicate based on URL
            unique_articles = {article.url: article for article in all_articles}
            final_articles = list(unique_articles.values())
            
            # Log completion
            elapsed = time.time() - start_time
            dedup_count = len(all_articles) - len(final_articles)
            
            task_logger.info(f"Finished crawling all sources for '{location_query}': "
                           f"Found {len(final_articles)} unique articles "
                           f"(removed {dedup_count} duplicates) in {elapsed:.1f}s")
            
            # Update agent status to show completion
            await metrics_collector.update_agent_status(
                "crawling",
                "Idle",
                {"completed": f"{len(final_articles)} articles"}
            )
            
            return final_articles
            
        except Exception as e:
            task_logger.error(f"Unexpected error during crawl_sources: {e}", exc_info=True)
            # Update agent status to show error
            await metrics_collector.update_agent_status(
                "crawling",
                "Error",
                {"error": str(e)}
            )
            return all_articles
        finally:
            # Ensure we always close the session
            await self.close()