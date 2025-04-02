import logging
import aiohttp
import asyncio
import time
import re
import hashlib
import xml.etree.ElementTree as ET
from typing import List, Dict, Any, Optional, Set, Tuple
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

from models.data_models import NewsSource
from agents.base_agent import BaseAgent, Task, TaskPriority

# Constants
DEFAULT_TIMEOUT = 20  # seconds
RSS_FEED_CACHE_DIR = "data/cache/rss_feeds"
DEFAULT_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
MAX_ARTICLES_PER_FEED = 30  # Max articles to process from a single feed

# Common RSS feed URL patterns
RSS_PATTERNS = [
    'rss', 'feed', 'feeds', 'atom.xml', 'rss.xml', 'feed.xml', 'atom', 'news.xml',
    'rss/index', 'index.rss', 'feeds/posts/default', 'rss2', 'atom2', 'feed/atom',
]

logger = logging.getLogger(__name__)

class RSSDiscoveryAgent(BaseAgent):
    """Micro-agent responsible for discovering and processing RSS feeds"""
    
    def __init__(self):
        super().__init__("rss_discovery")
        self._session: Optional[aiohttp.ClientSession] = None
        
        # Setup cache directory
        self.cache_dir = Path(RSS_FEED_CACHE_DIR)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Feed metadata cache
        self.known_feeds = {}
        
    async def _initialize_resources(self):
        """Initialize HTTP session and load metadata cache"""
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=DEFAULT_TIMEOUT),
            headers={
                'User-Agent': DEFAULT_USER_AGENT,
                'Accept': 'text/html,application/xhtml+xml,application/xml',
            }
        )
        # Load cached feed metadata
        self._load_feed_metadata()
        
    async def _cleanup_resources(self):
        """Close HTTP session and save metadata"""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
        
        # Save feed metadata to cache
        self._save_feed_metadata()
            
    async def _process_task_internal(self, task: Task) -> Any:
        """Process RSS discovery tasks"""
        task_type = task.task_type
        
        if task_type == "discover_feeds":
            return await self._discover_feeds(task)
        elif task_type == "fetch_feed":
            return await self._fetch_feed(task)
        else:
            raise ValueError(f"Unknown task type: {task_type}")
    
    def _get_cache_path(self, feed_url: str) -> Path:
        """Generate a cache file path for a feed URL"""
        url_hash = hashlib.md5(feed_url.encode()).hexdigest()
        return self.cache_dir / f"{url_hash}.xml"
    
    def _load_feed_metadata(self):
        """Load cached feed metadata"""
        metadata_file = self.cache_dir / "feed_metadata.json"
        if metadata_file.exists():
            try:
                import json
                with open(metadata_file, 'r') as f:
                    self.known_feeds = json.load(f)
                logger.debug(f"Loaded {len(self.known_feeds)} feed entries from metadata cache")
            except Exception as e:
                logger.warning(f"Failed to load feed metadata: {e}")
                self.known_feeds = {}
    
    def _save_feed_metadata(self):
        """Save feed metadata to cache"""
        metadata_file = self.cache_dir / "feed_metadata.json"
        try:
            import json
            with open(metadata_file, 'w') as f:
                json.dump(self.known_feeds, f)
            logger.debug(f"Saved {len(self.known_feeds)} feed entries to metadata cache")
        except Exception as e:
            logger.warning(f"Failed to save feed metadata: {e}")
    
    async def _discover_feeds(self, task: Task) -> List[str]:
        """Discover RSS feeds for a given source"""
        source = task.data.get("source")
        if not source:
            raise ValueError("Source is required for RSS feed discovery")
        
        # Get the URL
        url = source.get("url") if isinstance(source, dict) else source.url if hasattr(source, "url") else None
        name = source.get("name") if isinstance(source, dict) else source.name if hasattr(source, "name") else "Unknown"
        
        if not url:
            raise ValueError("URL is required for RSS feed discovery")
        
        await self.update_status("Discovering feeds", {"source": name, "url": url})
        
        discovered_feeds = []
        base_url = url
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        
        try:
            # 1. First check the homepage
            async with self._session.get(url, allow_redirects=True) as response:
                if response.status == 200:
                    content_type = response.headers.get('Content-Type', '')
                    if 'html' in content_type:
                        # Parse HTML and look for feed links
                        html = await response.text()
                        
                        # Search for RSS/Atom links
                        feed_links = set()
                        
                        # Extract links from <link> tags
                        link_matches = re.findall(r'<link[^>]+(?:atom|rss)[^>]+href=[\'"]([^\'"]+)[\'"]', html, re.IGNORECASE)
                        feed_links.update(link_matches)
                        
                        # Extract links from <a> tags with feed-related text
                        a_matches = re.findall(r'<a[^>]+href=[\'"]([^\'"]+)[\'"][^>]*>.*?(?:RSS|Feed|Atom).*?</a>', html, re.IGNORECASE)
                        feed_links.update(a_matches)
                        
                        # Process the feed links
                        for link in feed_links:
                            feed_url = urljoin(base_url, link)
                            discovered_feeds.append(feed_url)
                    
            # 2. Try common feed URL patterns if no feeds found
            if not discovered_feeds:
                for pattern in RSS_PATTERNS:
                    # Construct potential feed URLs
                    feed_url = urljoin(base_url, pattern)
                    
                    # Check if the feed URL works
                    if await self._validate_feed(feed_url):
                        discovered_feeds.append(feed_url)
                        break  # Found a valid feed
        
        except Exception as e:
            logger.warning(f"Error discovering feeds for {url}: {e}")
        
        # Update metadata for discovered feeds
        for feed_url in discovered_feeds:
            if feed_url not in self.known_feeds:
                self.known_feeds[feed_url] = {
                    "source_url": url,
                    "source_name": name,
                    "last_fetch": None,
                    "first_seen": datetime.now(timezone.utc).isoformat()
                }
        
        logger.info(f"Discovered {len(discovered_feeds)} RSS feeds for {name}")
        return discovered_feeds
    
    async def _validate_feed(self, feed_url: str) -> bool:
        """Validate if a URL is a valid RSS/Atom feed with enhanced content-type checking"""
        try:
            async with self._session.get(feed_url, timeout=10) as response:
                if response.status != 200:
                    return False
                
                # Check content type - RSS feeds should be XML
                content_type = response.headers.get('Content-Type', '').lower()
                is_valid_content_type = any(ct in content_type for ct in [
                    'xml', 'rss', 'atom', 'application/rss', 'application/atom', 
                    'application/xml', 'text/xml', 'application/rss+xml', 'application/atom+xml'
                ])
                
                # If content type doesn't look like XML, this likely isn't a feed
                if not is_valid_content_type and 'html' in content_type:
                    return False
                
                # Get the first chunk of content to validate
                content_sample = await response.content.read(4096)
                try:
                    content_text = content_sample.decode('utf-8', errors='ignore')
                    
                    # Quick check if it looks like XML before trying to parse
                    if not content_text.strip().startswith('<?xml') and not content_text.strip().startswith('<rss') and not content_text.strip().startswith('<feed'):
                        return False
                        
                    # Try to parse as XML to validate
                    root = ET.fromstring(content_text)
                    
                    # Check for RSS or Atom elements
                    is_rss = root.tag == 'rss' or root.find('./channel') is not None
                    is_atom = root.tag.endswith('feed') or root.find('.//{http://www.w3.org/2005/Atom}feed') is not None
                    
                    return is_rss or is_atom
                except ET.ParseError:
                    return False
        
        except Exception as e:
            logger.debug(f"Failed to validate feed {feed_url}: {e}")
            return False
    
    async def _fetch_feed(self, task: Task) -> Dict[str, Any]:
        """Fetch and parse an RSS feed"""
        feed_url = task.data.get("feed_url")
        if not feed_url:
            raise ValueError("feed_url is required for fetching RSS feed")
        
        # Get the last fetch time
        last_fetch_date = None
        metadata = self.known_feeds.get(feed_url, {})
        if metadata.get("last_fetch"):
            try:
                last_fetch_str = metadata["last_fetch"]
                last_fetch_date = datetime.fromisoformat(last_fetch_str)
            except ValueError:
                last_fetch_date = None
        
        await self.update_status("Fetching feed", {"url": feed_url})
        
        try:
            # Attempt to fetch from cache first if no recent fetch date specified
            cache_path = self._get_cache_path(feed_url)
            use_cache = False
            
            if cache_path.exists() and not last_fetch_date:
                # Use cache if it's not too old (less than 1 hour)
                mtime = datetime.fromtimestamp(cache_path.stat().st_mtime, tz=timezone.utc)
                if datetime.now(timezone.utc) - mtime < timedelta(hours=1):
                    use_cache = True
                    logger.debug(f"Using cached feed for {feed_url}")
                    with open(cache_path, 'r', encoding='utf-8') as f:
                        content = f.read()
            
            # Fetch fresh content if needed
            if not use_cache:
                async with self._session.get(feed_url) as response:
                    if response.status != 200:
                        return {
                            "feed_url": feed_url,
                            "success": False,
                            "error": f"HTTP error: {response.status}"
                        }
                    
                    content = await response.text()
                    
                    # Save to cache
                    with open(cache_path, 'w', encoding='utf-8') as f:
                        f.write(content)
            
            # Parse the feed content
            articles, pub_date = self._parse_feed_content(content, feed_url, last_fetch_date)
            
            # Update metadata
            self.known_feeds[feed_url] = {
                **metadata,
                "last_fetch": datetime.now(timezone.utc).isoformat(),
                "last_pub_date": pub_date.isoformat() if pub_date else None
            }
            
            return {
                "feed_url": feed_url,
                "success": True,
                "articles": articles,
                "updated_at": pub_date
            }
        
        except Exception as e:
            logger.error(f"Error fetching feed {feed_url}: {e}", exc_info=True)
            return {
                "feed_url": feed_url,
                "success": False,
                "error": str(e)
            }
    
    def _parse_feed_content(self, content: str, feed_url: str, 
                          last_fetch_date: Optional[datetime] = None) -> Tuple[List[Dict], Optional[datetime]]:
        """Parse RSS/Atom feed content into articles"""
        articles = []
        latest_pub_date = None
        
        try:
            root = ET.fromstring(content)
            
            # Determine feed type and parse accordingly
            is_atom = root.tag.endswith('feed')
            
            if is_atom:
                # Parse Atom feed
                for entry in root.findall('.//{http://www.w3.org/2005/Atom}entry'):
                    try:
                        # Get article data
                        title_el = entry.find('.//{http://www.w3.org/2005/Atom}title')
                        link_el = entry.find('.//{http://www.w3.org/2005/Atom}link[@rel="alternate"][@href]') or entry.find('.//{http://www.w3.org/2005/Atom}link[@href]')
                        pub_date_el = entry.find('.//{http://www.w3.org/2005/Atom}published') or entry.find('.//{http://www.w3.org/2005/Atom}updated')
                        
                        if title_el is None or link_el is None:
                            continue
                        
                        title = title_el.text or ""
                        url = link_el.get('href')
                        
                        # Parse the publish date
                        pub_date = None
                        if pub_date_el is not None and pub_date_el.text:
                            try:
                                pub_date = datetime.fromisoformat(pub_date_el.text.replace('Z', '+00:00'))
                                
                                # Update latest publication date
                                if latest_pub_date is None or pub_date > latest_pub_date:
                                    latest_pub_date = pub_date
                                
                                # Skip if older than last fetch
                                if last_fetch_date and pub_date <= last_fetch_date:
                                    continue
                                    
                            except ValueError:
                                pass
                        
                        # Add to articles
                        articles.append({
                            "title": title,
                            "url": url,
                            "published_at": pub_date.isoformat() if pub_date else None,
                        })
                    except Exception as e:
                        logger.warning(f"Error parsing Atom entry: {e}")
            else:
                # Parse RSS feed
                channel = root.find('.//channel') or root
                if channel is None:
                    return [], None
                
                for item in channel.findall('.//item'):
                    try:
                        title_el = item.find('./title')
                        link_el = item.find('./link')
                        pub_date_el = item.find('./pubDate')
                        
                        if title_el is None or link_el is None:
                            continue
                        
                        title = title_el.text or ""
                        url = link_el.text or ""
                        
                        # Parse the publish date
                        pub_date = None
                        if pub_date_el is not None and pub_date_el.text:
                            try:
                                from email.utils import parsedate_to_datetime
                                pub_date = parsedate_to_datetime(pub_date_el.text)
                                
                                # Update latest publication date
                                if latest_pub_date is None or pub_date > latest_pub_date:
                                    latest_pub_date = pub_date
                                
                                # Skip if older than last fetch
                                if last_fetch_date and pub_date <= last_fetch_date:
                                    continue
                                    
                            except ValueError:
                                pass
                        
                        # Add to articles
                        articles.append({
                            "title": title,
                            "url": url,
                            "published_at": pub_date.isoformat() if pub_date else None,
                        })
                    except Exception as e:
                        logger.warning(f"Error parsing RSS item: {e}")
            
            # Limit the number of articles
            articles = articles[:MAX_ARTICLES_PER_FEED]
            
        except ET.ParseError as e:
            logger.error(f"XML parsing error for feed {feed_url}: {e}")
        except Exception as e:
            logger.error(f"Error parsing feed {feed_url}: {e}", exc_info=True)
        
        return articles, latest_pub_date