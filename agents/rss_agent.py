"""
RSS feed discovery and processing agent for the news aggregation system.
"""
import asyncio
import logging
import time
from datetime import datetime, timezone, timedelta
import re
import hashlib
from typing import List, Dict, Any, Optional, Set, Tuple
from urllib.parse import urlparse, urljoin
import xml.etree.ElementTree as ET
from pathlib import Path
import json

import aiohttp
from bs4 import BeautifulSoup
from newspaper import Article as NewspaperArticle
from newspaper import Config as NewspaperConfig

from models.data_models import NewsSource, NewsArticle
from utils.logging_config import get_logger

logger = get_logger(__name__, {"component": "rss_agent"})

# Constants
DEFAULT_TIMEOUT = 30  # seconds
RSS_FEED_CACHE_DIR = "data/cache/rss_feeds"
DEFAULT_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
MAX_ARTICLES_PER_FEED = 50  # Max articles to process from a single feed

# Common RSS feed URL patterns
RSS_PATTERNS = [
    'rss', 'feed', 'feeds', 'atom.xml', 'rss.xml', 'feed.xml', 'atom', 'news.xml',
    'rss/index', 'index.rss', 'feeds/posts/default', 'rss2', 'atom2', 'feed/atom',
]


class RSSAgent:
    """
    Agent responsible for discovering and processing RSS feeds for news sources.
    """
    
    def __init__(self, cache_dir: str = RSS_FEED_CACHE_DIR):
        """
        Initialize the RSS Agent.
        
        Args:
            cache_dir: Directory to cache RSS feed data
        """
        self._session: Optional[aiohttp.ClientSession] = None
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Configure newspaper
        self.newspaper_config = NewspaperConfig()
        self.newspaper_config.browser_user_agent = DEFAULT_USER_AGENT
        self.newspaper_config.request_timeout = DEFAULT_TIMEOUT
        self.newspaper_config.fetch_images = False
        
        # Store feed metadata
        self.known_feeds: Dict[str, Dict] = {}
        self._load_feed_metadata()
        
        logger.info("RSSAgent initialized")

    async def _get_session(self) -> aiohttp.ClientSession:
        """Gets or creates a shared aiohttp session for all requests."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=DEFAULT_TIMEOUT),
                headers={
                    'User-Agent': DEFAULT_USER_AGENT,
                    'Accept': 'text/html,application/xhtml+xml,application/xml',
                }
            )
        return self._session

    async def close(self):
        """Close the HTTP session when done."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
            logger.debug("Closed aiohttp session")
            
    def _get_cache_path(self, feed_url: str) -> Path:
        """Generate a cache file path for a feed URL."""
        url_hash = hashlib.md5(feed_url.encode()).hexdigest()
        return self.cache_dir / f"{url_hash}.xml"
    
    def _load_feed_metadata(self):
        """Load cached feed metadata."""
        metadata_path = self.cache_dir / "feed_metadata.json"
        
        if metadata_path.exists():
            try:
                with open(metadata_path, 'r') as f:
                    self.known_feeds = json.load(f)
                logger.info(f"Loaded metadata for {len(self.known_feeds)} RSS feeds")
            except Exception as e:
                logger.error(f"Error loading feed metadata: {e}", exc_info=True)
                self.known_feeds = {}
    
    def _save_feed_metadata(self):
        """Save feed metadata to cache."""
        metadata_path = self.cache_dir / "feed_metadata.json"
        
        try:
            with open(metadata_path, 'w') as f:
                json.dump(self.known_feeds, f)
            logger.debug(f"Saved metadata for {len(self.known_feeds)} RSS feeds")
        except Exception as e:
            logger.error(f"Error saving feed metadata: {e}", exc_info=True)

    async def discover_feeds(self, source: NewsSource) -> List[str]:
        """
        Discover RSS feeds for a news source.
        
        Args:
            source: The news source to discover feeds for
            
        Returns:
            List of discovered feed URLs
        """
        task_logger = logger.with_context(task="discover_feeds", source=source.name)
        task_logger.info(f"Discovering RSS feeds for {source.name} ({source.url})")
        
        # Check if this source already has known feeds
        domain = urlparse(source.url).netloc
        known_feeds_for_domain = [url for url in self.known_feeds 
                               if urlparse(url).netloc == domain]
        
        if known_feeds_for_domain:
            task_logger.info(f"Found {len(known_feeds_for_domain)} known feeds for {source.name}")
            return known_feeds_for_domain
        
        # Fetch the source homepage to look for feed links
        session = await self._get_session()
        found_feeds = set()
        
        try:
            task_logger.debug(f"Fetching {source.url} to discover feeds")
            async with session.get(source.url) as response:
                if response.status != 200:
                    task_logger.warning(f"HTTP error {response.status} when fetching {source.url}")
                    return []
                
                html = await response.text()
                
                # Use BeautifulSoup to find feed links
                soup = BeautifulSoup(html, 'html.parser')
                
                # Look for standard RSS/Atom link elements
                for link in soup.find_all('link', rel=True, href=True):
                    if link['rel'] in ['alternate', 'feed'] or 'rss' in link['rel'] or 'atom' in link['rel']:
                        feed_url = urljoin(source.url, link['href'])
                        found_feeds.add(feed_url)
                        task_logger.debug(f"Found feed link in HTML: {feed_url}")
                
                # Look for a elements with feed-related href
                for a in soup.find_all('a', href=True):
                    href = a['href']
                    text = a.get_text().lower()
                    
                    # Check if the link or text indicates a feed
                    if ('rss' in href or 'feed' in href or 'atom' in href or
                        'rss' in text or 'feed' in text):
                        feed_url = urljoin(source.url, href)
                        found_feeds.add(feed_url)
                        task_logger.debug(f"Found potential feed link in anchor: {feed_url}")
            
            # If no feeds found, try common feed URL patterns
            if not found_feeds:
                base_url = source.url.rstrip('/')
                for pattern in RSS_PATTERNS:
                    feed_url = f"{base_url}/{pattern}"
                    found_feeds.add(feed_url)
                
                task_logger.debug(f"No feeds found in HTML, trying {len(found_feeds)} common patterns")
            
            # Validate each feed URL by checking MIME type and basic parsing
            valid_feeds = []
            for feed_url in found_feeds:
                try:
                    is_valid = await self._validate_feed(feed_url)
                    if is_valid:
                        valid_feeds.append(feed_url)
                        # Update known feeds metadata
                        self.known_feeds[feed_url] = {
                            'source_name': source.name,
                            'source_url': source.url,
                            'discovered_at': datetime.now(timezone.utc).isoformat(),
                            'last_fetch': None,
                        }
                except Exception as e:
                    task_logger.debug(f"Error validating feed {feed_url}: {e}")
            
            # Save updated feed metadata
            if valid_feeds:
                self._save_feed_metadata()
            
            task_logger.info(f"Discovered {len(valid_feeds)} valid RSS feeds for {source.name}")
            return valid_feeds
            
        except Exception as e:
            task_logger.error(f"Error discovering feeds for {source.name}: {e}", exc_info=True)
            return []

    async def _validate_feed(self, feed_url: str) -> bool:
        """
        Check if a URL is a valid RSS feed.
        
        Args:
            feed_url: The URL to validate
            
        Returns:
            True if the URL is a valid feed, False otherwise
        """
        session = await self._get_session()
        
        try:
            # Send a HEAD request first to check content type
            async with session.head(feed_url, allow_redirects=True) as head_resp:
                content_type = head_resp.headers.get('Content-Type', '')
                
                # If content type is not feed-related, might not be a feed
                if (not any(x in content_type.lower() for x in 
                          ['xml', 'rss', 'atom', 'feed'])):
                    # If HEAD doesn't indicate a feed, still try with GET since some servers
                    # don't correctly configure content type for feeds
                    pass
                else:
                    logger.debug(f"Feed {feed_url} has valid content type: {content_type}")
                    return True
            
            # Fetch a small part of the feed content to validate
            async with session.get(feed_url) as resp:
                if resp.status != 200:
                    return False
                
                # Read just enough to identify if it's a feed
                content = await resp.read(4096)  # First 4KB should be enough to identify
                
                # Look for common feed indicators in the XML
                text = content.decode('utf-8', errors='ignore').lower()
                
                is_feed = (
                    '<rss' in text or 
                    '<feed' in text or 
                    '<channel' in text or
                    'xmlns="http://www.w3.org/2005/Atom"' in text
                )
                
                if is_feed:
                    logger.debug(f"Validated feed: {feed_url}")
                    # Cache the feed content
                    cache_path = self._get_cache_path(feed_url)
                    async with session.get(feed_url) as full_resp:
                        if full_resp.status == 200:
                            full_content = await full_resp.text()
                            with open(cache_path, 'w', encoding='utf-8') as f:
                                f.write(full_content)
                    
                return is_feed
                
        except Exception as e:
            logger.debug(f"Error validating feed {feed_url}: {e}")
            return False

    async def fetch_feed(self, feed_url: str, last_fetch_date: Optional[datetime] = None) -> Tuple[List[Dict], datetime]:
        """
        Fetch and parse an RSS feed, extracting article data.
        
        Args:
            feed_url: The RSS feed URL
            last_fetch_date: Only return articles newer than this date
            
        Returns:
            Tuple of (list of article data, current fetch timestamp)
        """
        task_logger = logger.with_context(task="fetch_feed", feed_url=feed_url)
        task_logger.debug(f"Fetching feed: {feed_url}")
        
        current_fetch_time = datetime.now(timezone.utc)
        session = await self._get_session()
        articles = []
        
        try:
            async with session.get(feed_url) as response:
                if response.status != 200:
                    task_logger.warning(f"HTTP error {response.status} fetching feed {feed_url}")
                    return [], current_fetch_time
                
                feed_content = await response.text()
                
                # Cache the feed content
                cache_path = self._get_cache_path(feed_url)
                with open(cache_path, 'w', encoding='utf-8') as f:
                    f.write(feed_content)
                
                # Parse the feed
                feed_type = self._determine_feed_type(feed_content)
                
                if feed_type == 'rss':
                    articles = self._parse_rss_feed(feed_content, feed_url, last_fetch_date)
                elif feed_type == 'atom':
                    articles = self._parse_atom_feed(feed_content, feed_url, last_fetch_date)
                else:
                    task_logger.warning(f"Unknown feed type for {feed_url}")
                
                # Update metadata
                if feed_url in self.known_feeds:
                    self.known_feeds[feed_url]['last_fetch'] = current_fetch_time.isoformat()
                    self.known_feeds[feed_url]['last_article_count'] = len(articles)
                    self._save_feed_metadata()
                
                task_logger.info(f"Fetched {len(articles)} articles from {feed_url}")
                return articles, current_fetch_time
                
        except Exception as e:
            task_logger.error(f"Error fetching feed {feed_url}: {e}", exc_info=True)
            return [], current_fetch_time

    def _determine_feed_type(self, content: str) -> str:
        """
        Determine if a feed is RSS or Atom.
        
        Args:
            content: The feed content
            
        Returns:
            'rss', 'atom', or 'unknown'
        """
        content = content.lower()
        if '<rss' in content:
            return 'rss'
        elif '<feed' in content and 'xmlns="http://www.w3.org/2005/Atom"' in content:
            return 'atom'
        else:
            return 'unknown'

    def _parse_rss_feed(self, content: str, feed_url: str, last_fetch_date: Optional[datetime] = None) -> List[Dict]:
        """
        Parse an RSS feed and extract article data.
        
        Args:
            content: The feed content
            feed_url: The feed URL (for error reporting)
            last_fetch_date: Only return articles newer than this date
            
        Returns:
            List of article data dictionaries
        """
        articles = []
        try:
            # Parse XML
            root = ET.fromstring(content)
            
            # Find the channel element
            channel = root.find('./channel')
            if channel is None:
                logger.warning(f"No channel element found in RSS feed: {feed_url}")
                return []
            
            # Extract feed metadata
            feed_title = channel.findtext('./title', '')
            
            # Process each item
            items = channel.findall('./item')
            for item in items[:MAX_ARTICLES_PER_FEED]:
                try:
                    # Extract basic item data
                    title = item.findtext('./title', '')
                    link = item.findtext('./link', '')
                    description = item.findtext('./description', '')
                    
                    # Some feeds use content:encoded instead of description
                    if not description:
                        # Look for content in different namespaces
                        for elem in item.findall('.//*'):
                            if elem.tag.endswith('encoded') or elem.tag.endswith('content'):
                                description = elem.text or ''
                                break
                    
                    # Parse publication date
                    pub_date_str = item.findtext('./pubDate', '')
                    pub_date = self._parse_date(pub_date_str)
                    
                    # Skip if article is older than last fetch
                    if last_fetch_date and pub_date and pub_date < last_fetch_date:
                        continue
                    
                    if not link:
                        continue
                    
                    articles.append({
                        'url': link,
                        'title': title,
                        'source_name': feed_title,
                        'content_snippet': description,
                        'published_at': pub_date,
                        'fetch_full_text': True  # Flag to indicate if we need to fetch the full article text
                    })
                except Exception as e:
                    logger.debug(f"Error parsing RSS item: {e}")
                    continue
                    
        except ET.ParseError:
            logger.warning(f"XML parsing error in RSS feed: {feed_url}")
        except Exception as e:
            logger.error(f"Error parsing RSS feed {feed_url}: {e}", exc_info=True)
            
        return articles

    def _parse_atom_feed(self, content: str, feed_url: str, last_fetch_date: Optional[datetime] = None) -> List[Dict]:
        """
        Parse an Atom feed and extract article data.
        
        Args:
            content: The feed content
            feed_url: The feed URL (for error reporting)
            last_fetch_date: Only return articles newer than this date
            
        Returns:
            List of article data dictionaries
        """
        articles = []
        try:
            # Parse XML with namespace handling
            root = ET.fromstring(content)
            
            # Extract namespaces
            ns = {'atom': 'http://www.w3.org/2005/Atom'}
            
            # Extract feed metadata
            feed_title = root.findtext('./atom:title', '', ns)
            
            # Process each entry
            entries = root.findall('./atom:entry', ns)
            for entry in entries[:MAX_ARTICLES_PER_FEED]:
                try:
                    # Extract basic entry data
                    title = entry.findtext('./atom:title', '', ns)
                    
                    # Get link - Atom can have multiple links, look for the alternate one
                    link = ''
                    for link_elem in entry.findall('./atom:link', ns):
                        rel = link_elem.get('rel', 'alternate')
                        if rel == 'alternate':
                            link = link_elem.get('href', '')
                            break
                    
                    if not link:
                        # If no alternate link, use the first link
                        link_elem = entry.find('./atom:link', ns)
                        if link_elem is not None:
                            link = link_elem.get('href', '')
                    
                    # Extract content or summary
                    content_elem = entry.find('./atom:content', ns)
                    summary_elem = entry.find('./atom:summary', ns)
                    
                    description = ''
                    if content_elem is not None:
                        description = content_elem.text or ''
                    elif summary_elem is not None:
                        description = summary_elem.text or ''
                    
                    # Parse publication date
                    pub_date_str = entry.findtext('./atom:published', '', ns) or entry.findtext('./atom:updated', '', ns)
                    pub_date = self._parse_date(pub_date_str)
                    
                    # Skip if article is older than last fetch
                    if last_fetch_date and pub_date and pub_date < last_fetch_date:
                        continue
                    
                    if not link:
                        continue
                    
                    articles.append({
                        'url': link,
                        'title': title,
                        'source_name': feed_title,
                        'content_snippet': description,
                        'published_at': pub_date,
                        'fetch_full_text': True  # Flag to indicate if we need to fetch the full article text
                    })
                except Exception as e:
                    logger.debug(f"Error parsing Atom entry: {e}")
                    continue
                    
        except ET.ParseError:
            logger.warning(f"XML parsing error in Atom feed: {feed_url}")
        except Exception as e:
            logger.error(f"Error parsing Atom feed {feed_url}: {e}", exc_info=True)
            
        return articles

    def _parse_date(self, date_string: str) -> Optional[datetime]:
        """
        Parse a date string into a datetime object.
        Handles various date formats commonly found in feeds.
        
        Args:
            date_string: The date string to parse
            
        Returns:
            datetime object or None if parsing fails
        """
        if not date_string:
            return None
            
        date_formats = [
            '%a, %d %b %Y %H:%M:%S %z',  # RFC 822: 'Mon, 01 Jan 2023 12:30:00 +0000'
            '%a, %d %b %Y %H:%M:%S %Z',  # RFC 822 with timezone name: 'Mon, 01 Jan 2023 12:30:00 GMT'
            '%Y-%m-%dT%H:%M:%SZ',        # ISO 8601 UTC: '2023-01-01T12:30:00Z'
            '%Y-%m-%dT%H:%M:%S%z',       # ISO 8601 with timezone: '2023-01-01T12:30:00+00:00'
            '%Y-%m-%d %H:%M:%S',         # Simple datetime: '2023-01-01 12:30:00'
            '%Y-%m-%d',                  # Just date: '2023-01-01'
            '%a, %d %b %Y %H:%M:%S',     # Without timezone: 'Mon, 01 Jan 2023 12:30:00'
        ]
        
        # Try each format
        for fmt in date_formats:
            try:
                dt = datetime.strptime(date_string, fmt)
                # Make timezone-aware if naive
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue
                
        # If all formats fail, try a more flexible approach
        try:
            # Handle 'GMT' or other timezone names by replacing them
            date_string = re.sub(r'\s+(?:GMT|UTC|EST|PST)(?:\s+|$)', ' +0000 ', date_string)
            
            # Try various formats with dateutil parser if available
            try:
                from dateutil import parser as dateutil_parser
                dt = dateutil_parser.parse(date_string)
                # Make timezone-aware if naive
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ImportError:
                # If dateutil not available, we'll return None
                pass
                
        except Exception:
            pass
            
        logger.debug(f"Could not parse date string: {date_string}")
        return None

    async def fetch_article_content(self, article_data: Dict) -> Optional[NewsArticle]:
        """
        Fetch and extract full content for an article from feed data.
        
        Args:
            article_data: Article data from the feed
            
        Returns:
            NewsArticle object or None if extraction fails
        """
        url = article_data['url']
        task_logger = logger.with_context(task="fetch_article", url=url[-20:])
        task_logger.debug(f"Fetching article content: {url}")
        
        # Skip fetch if we already have sufficient content
        if article_data.get('content', '') and not article_data.get('fetch_full_text', True):
            # Use feed content directly
            try:
                return NewsArticle(
                    url=url,
                    title=article_data.get('title', ''),
                    source_name=article_data.get('source_name', ''),
                    content=article_data.get('content', ''),
                    published_at=article_data.get('published_at'),
                    fetched_at=datetime.now(timezone.utc)
                )
            except Exception as e:
                task_logger.warning(f"Error creating article from feed data: {e}")
                # Fall through to full fetch
        
        try:
            # Use newspaper3k to extract the full article
            article = NewspaperArticle(url, config=self.newspaper_config)
            
            # Run the download and parsing in a thread pool
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, article.download)
            await loop.run_in_executor(None, article.parse)
            
            if not article.title or not article.text:
                task_logger.warning(f"Failed to extract content from {url}")
                return None
            
            # Convert article date to timezone-aware
            pub_date = article.publish_date or article_data.get('published_at')
            if pub_date and pub_date.tzinfo is None:
                pub_date = pub_date.replace(tzinfo=timezone.utc)
            
            return NewsArticle(
                url=url,
                title=article.title,
                source_name=article_data.get('source_name', ''),
                content=article.text,
                published_at=pub_date,
                fetched_at=datetime.now(timezone.utc)
            )
            
        except Exception as e:
            task_logger.error(f"Error fetching article content for {url}: {e}")
            return None

    async def process_feeds_for_source(self, source: NewsSource, 
                                     location_query: str = None,
                                     max_age_days: int = 7) -> List[NewsArticle]:
        """
        Process all feeds for a source, fetching new articles.
        
        Args:
            source: The news source
            location_query: Optional location to associate with articles
            max_age_days: Maximum age of articles to fetch
            
        Returns:
            List of fetched articles
        """
        task_logger = logger.with_context(task="process_feeds", source=source.name)
        task_logger.info(f"Processing feeds for source: {source.name}")
        
        # First, discover feeds for this source
        feeds = await self.discover_feeds(source)
        
        if not feeds:
            task_logger.warning(f"No feeds found for {source.name}")
            return []
        
        articles = []
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=max_age_days)
        
        for feed_url in feeds:
            try:
                # Get last fetch date for this feed to avoid duplicates
                last_fetch_str = self.known_feeds.get(feed_url, {}).get('last_fetch')
                last_fetch_date = None
                if last_fetch_str:
                    try:
                        last_fetch_date = datetime.fromisoformat(last_fetch_str)
                        # Use the newer of last_fetch_date or cutoff_date
                        last_fetch_date = max(last_fetch_date, cutoff_date)
                    except ValueError:
                        last_fetch_date = cutoff_date
                else:
                    last_fetch_date = cutoff_date
                
                # Fetch feed
                feed_articles, fetch_time = await self.fetch_feed(feed_url, last_fetch_date)
                
                if not feed_articles:
                    task_logger.debug(f"No new articles in feed {feed_url}")
                    continue
                
                task_logger.info(f"Found {len(feed_articles)} new articles in feed {feed_url}")
                
                # Fetch full content for each article, with concurrency limit
                semaphore = asyncio.Semaphore(5)  # Limit concurrent requests
                
                async def fetch_with_semaphore(art_data):
                    async with semaphore:
                        return await self.fetch_article_content(art_data)
                
                # Create tasks for all articles
                tasks = []
                for art_data in feed_articles:
                    if location_query:
                        art_data['location_query'] = location_query
                    tasks.append(fetch_with_semaphore(art_data))
                
                # Execute all tasks and gather results
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Process results
                for result in results:
                    if isinstance(result, Exception):
                        task_logger.warning(f"Error fetching article: {result}")
                    elif result is not None:
                        # Add location if provided
                        if location_query and not result.location_query:
                            result.location_query = location_query
                        articles.append(result)
                
            except Exception as e:
                task_logger.error(f"Error processing feed {feed_url}: {e}", exc_info=True)
        
        task_logger.info(f"Successfully fetched {len(articles)} articles from feeds for {source.name}")
        return articles

    async def update_all_sources(self, sources: List[NewsSource], location_query: str = None) -> List[NewsArticle]:
        """
        Update all sources, fetching new articles from their feeds.
        
        Args:
            sources: List of news sources to update
            location_query: Optional location to associate with articles
            
        Returns:
            List of all new articles
        """
        task_logger = logger.with_context(task="update_all")
        task_logger.info(f"Updating {len(sources)} sources")
        
        all_articles = []
        
        for source in sources:
            try:
                source_articles = await self.process_feeds_for_source(source, location_query)
                all_articles.extend(source_articles)
                task_logger.info(f"Fetched {len(source_articles)} articles from {source.name}")
            except Exception as e:
                task_logger.error(f"Error updating source {source.name}: {e}", exc_info=True)
        
        task_logger.info(f"Finished updating all sources. Total new articles: {len(all_articles)}")
        return all_articles