#!/usr/bin/env python3
# news_headline_streamer.py
# A production-grade real-time news headline scraper

import asyncio
import aiohttp
import feedparser
import logging
import json
import time
import re
import os
import signal
import sys
from typing import Dict, List, Set, Optional, Any, Tuple
from datetime import datetime, timedelta
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
import newspaper
from newspaper import Article, Source
import nltk
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor
import hashlib
import argparse
import configparser
import random
from aiohttp import ClientTimeout
from collections import deque

# Initialize nltk data
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')

try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("news_scraper.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Default configuration
DEFAULT_CONFIG = {
    "general": {
        "update_interval": 60,  # seconds
        "max_sources": 50,
        "max_headlines_per_source": 25,
        "user_agent": "NewsHeadlineStreamer/1.0",
        "discovery_depth": 2,
        "request_timeout": 30,
        "max_concurrent_requests": 20,
        "source_refresh_interval": 3600,  # 1 hour
        "feed_discovery_keywords": "rss,feed,atom,xml,news,headlines"
    },
    "filtering": {
        "min_headline_length": 20,
        "max_headline_age": 3600,  # 1 hour in seconds
        "language": "en",
        "remove_duplicates": True
    },
    "storage": {
        "enable_cache": True,
        "cache_dir": ".cache",
        "cache_expiration": 86400  # 24 hours in seconds
    }
}

@dataclass
class NewsHeadline:
    """Represents a news headline with metadata."""
    title: str
    url: str
    source_name: str
    source_url: str
    category: Optional[str] = None
    location: Optional[str] = None
    published_date: Optional[datetime] = None
    summary: Optional[str] = None
    image_url: Optional[str] = None
    hash_id: Optional[str] = None

    def __post_init__(self):
        """Generate a hash ID for the headline if not provided."""
        if not self.hash_id:
            headline_data = f"{self.title}|{self.url}|{self.source_name}"
            self.hash_id = hashlib.md5(headline_data.encode()).hexdigest()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary, handling datetime serialization."""
        data = asdict(self)
        if self.published_date:
            data["published_date"] = self.published_date.isoformat()
        return data
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), ensure_ascii=False)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'NewsHeadline':
        """Create a NewsHeadline from a dictionary."""
        if "published_date" in data and data["published_date"]:
            if isinstance(data["published_date"], str):
                data["published_date"] = datetime.fromisoformat(data["published_date"])
        return cls(**data)

class Cache:
    """Simple cache mechanism for headlines and sources."""
    
    def __init__(self, config: Dict[str, Any]):
        self.enabled = config["storage"]["enable_cache"]
        self.cache_dir = config["storage"]["cache_dir"]
        self.expiration = config["storage"]["cache_expiration"]
        
        if self.enabled:
            os.makedirs(self.cache_dir, exist_ok=True)
            self.headlines_cache = os.path.join(self.cache_dir, "headlines.json")
            self.sources_cache = os.path.join(self.cache_dir, "sources.json")
            self.seen_headlines = set()
            self._load_seen_headlines()
    
    def _load_seen_headlines(self):
        """Load previously seen headlines from cache."""
        if not self.enabled:
            return
        
        try:
            if os.path.exists(self.headlines_cache):
                with open(self.headlines_cache, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                    # Only load non-expired headlines
                    cutoff = time.time() - self.expiration
                    for item in cache_data.get("headlines", []):
                        if item.get("timestamp", 0) > cutoff:
                            self.seen_headlines.add(item.get("hash_id"))
                logger.info(f"Loaded {len(self.seen_headlines)} headlines from cache")
        except Exception as e:
            logger.error(f"Error loading headlines cache: {e}")
    
    def save_seen_headlines(self):
        """Save seen headlines to cache."""
        if not self.enabled:
            return
        
        try:
            cache_data = {
                "headlines": [
                    {"hash_id": h_id, "timestamp": time.time()}
                    for h_id in self.seen_headlines
                ]
            }
            with open(self.headlines_cache, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f)
            logger.debug(f"Saved {len(self.seen_headlines)} headlines to cache")
        except Exception as e:
            logger.error(f"Error saving headlines cache: {e}")
    
    def is_headline_seen(self, headline: NewsHeadline) -> bool:
        """Check if a headline has been seen before."""
        if not self.enabled:
            return False
        return headline.hash_id in self.seen_headlines
    
    def mark_headline_seen(self, headline: NewsHeadline):
        """Mark a headline as seen."""
        if not self.enabled:
            return
        self.seen_headlines.add(headline.hash_id)
    
    def save_sources(self, sources: List[Dict[str, Any]]):
        """Save discovered sources to cache."""
        if not self.enabled:
            return
        
        try:
            with open(self.sources_cache, 'w', encoding='utf-8') as f:
                json.dump({"sources": sources, "timestamp": time.time()}, f)
            logger.debug(f"Saved {len(sources)} sources to cache")
        except Exception as e:
            logger.error(f"Error saving sources cache: {e}")
    
    def load_sources(self) -> List[Dict[str, Any]]:
        """Load sources from cache if not expired."""
        if not self.enabled or not os.path.exists(self.sources_cache):
            return []
        
        try:
            with open(self.sources_cache, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
                
                # Check if cache is expired
                if time.time() - cache_data.get("timestamp", 0) > self.expiration:
                    return []
                
                return cache_data.get("sources", [])
        except Exception as e:
            logger.error(f"Error loading sources cache: {e}")
            return []

class NewsSourceDiscoverer:
    """Discovers news sources dynamically."""
    
    def __init__(self, config: Dict[str, Any], cache: Cache):
        self.config = config
        self.cache = cache
        self.discovery_depth = config["general"]["discovery_depth"]
        self.max_sources = config["general"]["max_sources"]
        self.request_timeout = ClientTimeout(total=config["general"]["request_timeout"])
        self.user_agent = config["general"]["user_agent"]
        self.max_concurrent = config["general"]["max_concurrent_requests"]
        self.known_sources = set()
        self.feed_keywords = config["general"]["feed_discovery_keywords"].split(',')
        
        # Starting points for news source discovery
        self.seed_urls = [
            "https://blog.feedspot.com/news_rss_feeds/",
            "https://blog.feedspot.com/world_news_rss_feeds/",
            "https://www.google.com/search?q=top+news+websites",
            "https://www.google.com/search?q=news+rss+feeds",
            "https://news.google.com/",
            "https://news.yahoo.com/",
            "https://www.reddit.com/r/news/",
            "https://www.reddit.com/r/worldnews/"
        ]

    async def discover_sources(self, categories: Optional[List[str]] = None, 
                               locations: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Discover news sources dynamically based on categories and locations.
        Returns a list of source information including URL and feed URL.
        """
        logger.info(f"Starting news source discovery for categories={categories}, locations={locations}")
        
        # Try to load sources from cache first
        cached_sources = self.cache.load_sources()
        if cached_sources:
            logger.info(f"Loaded {len(cached_sources)} sources from cache")
            return cached_sources
        
        discovered_sources = []
        session_timeout = ClientTimeout(total=self.config["general"]["request_timeout"])
        
        # Create a custom session with proper headers
        async with aiohttp.ClientSession(timeout=session_timeout, 
                                         headers={"User-Agent": self.user_agent}) as session:
            # Start with seed URLs
            seed_urls = self.seed_urls.copy()
            
            # Add category/location specific seed URLs if provided
            if categories:
                for category in categories:
                    seed_urls.append(f"https://www.google.com/search?q={category}+news+rss+feeds")
                    seed_urls.append(f"https://blog.feedspot.com/{category}_rss_feeds")
            
            if locations:
                for location in locations:
                    seed_urls.append(f"https://www.google.com/search?q={location}+news+rss+feeds")
                    seed_urls.append(f"https://blog.feedspot.com/{location}_news_rss_feeds")
            
            # Process seed URLs to find potential news sources
            potential_sources = set()
            tasks = [self._process_seed_url(session, url) for url in seed_urls]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, list):
                    potential_sources.update(result)
            
            # Limit the number of potential sources to process
            potential_sources = list(potential_sources)[:self.max_sources * 2]
            
            # For each potential source, try to find RSS/Atom feeds
            semaphore = asyncio.Semaphore(self.max_concurrent)
            tasks = [self._find_feeds(session, url, semaphore) for url in potential_sources]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, dict) and result.get("feed_url"):
                    discovered_sources.append(result)
                    if len(discovered_sources) >= self.max_sources:
                        break
            
            # If we haven't reached max_sources yet, try direct newspaper analysis
            if len(discovered_sources) < self.max_sources:
                executor = ThreadPoolExecutor(max_workers=min(10, self.max_sources - len(discovered_sources)))
                loop = asyncio.get_event_loop()
                
                def analyze_with_newspaper(url):
                    try:
                        source = newspaper.build(url, memoize_articles=False, fetch_images=False,
                                                language=self.config["filtering"]["language"])
                        if source.size() > 0:
                            feed_urls = source.feed_urls()
                            return {
                                "name": source.brand or urlparse(url).netloc,
                                "url": url,
                                "feed_url": feed_urls[0] if feed_urls else None,
                                "category": None,
                                "location": None
                            }
                    except Exception as e:
                        logger.debug(f"Error analyzing {url} with newspaper: {e}")
                    return None
                
                remaining_sources = [url for url in potential_sources if url not in [s["url"] for s in discovered_sources]]
                newspaper_tasks = [loop.run_in_executor(executor, analyze_with_newspaper, url) 
                                  for url in remaining_sources[:self.max_sources]]
                newspaper_results = await asyncio.gather(*newspaper_tasks)
                
                for result in newspaper_results:
                    if result and result["feed_url"] and result not in discovered_sources:
                        discovered_sources.append(result)
                        if len(discovered_sources) >= self.max_sources:
                            break
        
        # Filter and categorize sources if needed
        if categories or locations:
            discovered_sources = await self._filter_sources_by_relevance(discovered_sources, categories, locations)
        
        logger.info(f"Discovered {len(discovered_sources)} news sources")
        
        # Save sources to cache
        self.cache.save_sources(discovered_sources)
        
        return discovered_sources

    async def _process_seed_url(self, session: aiohttp.ClientSession, url: str) -> List[str]:
        """Process a seed URL to extract potential news source URLs."""
        potential_sources = []
        try:
            async with session.get(url, allow_redirects=True) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Extract all links
                    for link in soup.find_all('a', href=True):
                        href = link['href']
                        
                        # Convert relative URLs to absolute
                        if not href.startswith(('http://', 'https://')):
                            href = urljoin(url, href)
                        
                        # Filter out non-news sites and common non-news domains
                        parsed_url = urlparse(href)
                        domain = parsed_url.netloc
                        
                        if (
                            domain and
                            not domain.endswith(('.gov', '.edu', '.mil')) and
                            'news' in domain or 'media' in domain or any(
                                term in href.lower() for term in 
                                ['news', 'article', 'press', 'media', 'feed', 'rss']
                            )
                        ):
                            # Normalize URL to base domain
                            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                            if base_url not in potential_sources and base_url not in self.known_sources:
                                potential_sources.append(base_url)
                                self.known_sources.add(base_url)
        except Exception as e:
            logger.debug(f"Error processing seed URL {url}: {e}")
        
        return potential_sources

    async def _find_feeds(self, session: aiohttp.ClientSession, url: str, semaphore: asyncio.Semaphore) -> Dict[str, Any]:
        """Find RSS/Atom feeds for a potential news source."""
        async with semaphore:
            try:
                async with session.get(url, allow_redirects=True) as response:
                    if response.status != 200:
                        return {}
                    
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Get site title
                    title = soup.title.text if soup.title else urlparse(url).netloc
                    
                    # Check for RSS/Atom link tags
                    feed_url = None
                    for link in soup.find_all('link', type=re.compile(r'(rss|atom|xml)|(application/rss\+xml)|(application/atom\+xml)')):
                        if 'href' in link.attrs:
                            feed_url = urljoin(url, link['href'])
                            break
                    
                    # If no link tags found, try common feed paths
                    if not feed_url:
                        common_feed_paths = [
                            '/feed', '/rss', '/feed/rss', '/rss.xml', '/atom.xml', 
                            '/feed/atom', '/feeds', '/news/rss', '/rss/news', '/feed.xml',
                            '/index.xml', '/news/feed', '/feed/rss/news'
                        ]
                        
                        for path in common_feed_paths:
                            potential_feed = urljoin(url, path)
                            try:
                                async with session.get(potential_feed, timeout=ClientTimeout(total=5)) as feed_response:
                                    if feed_response.status == 200:
                                        content_type = feed_response.headers.get('Content-Type', '')
                                        if any(t in content_type.lower() for t in ['xml', 'rss', 'atom']):
                                            feed_content = await feed_response.text()
                                            if '<rss' in feed_content or '<feed' in feed_content:
                                                feed_url = potential_feed
                                                break
                            except:
                                continue
                    
                    # If still no feed found, look for links with feed keywords
                    if not feed_url:
                        for anchor in soup.find_all('a', href=True):
                            href = anchor['href']
                            if any(keyword in href.lower() or keyword in anchor.text.lower() for keyword in self.feed_keywords):
                                feed_url = urljoin(url, href)
                                # Verify it's a valid feed
                                try:
                                    async with session.get(feed_url, timeout=ClientTimeout(total=5)) as feed_response:
                                        if feed_response.status == 200:
                                            content_type = feed_response.headers.get('Content-Type', '')
                                            feed_content = await feed_response.text()
                                            if not ('<rss' in feed_content or '<feed' in feed_content):
                                                feed_url = None
                                except:
                                    feed_url = None
                                
                                if feed_url:
                                    break
                    
                    # Extract categories and location if possible
                    categories = []
                    locations = []
                    
                    # Look for category and location indicators in meta tags, nav elements, etc.
                    for meta in soup.find_all('meta', property=['article:section', 'category', 'og:section']):
                        if 'content' in meta.attrs:
                            categories.append(meta['content'].lower())
                    
                    # Look for navigation elements that might indicate categories
                    for nav in soup.find_all(['nav', 'ul']):
                        if 'class' in nav.attrs and any(c in ' '.join(nav['class']).lower() for c in ['category', 'categories', 'menu', 'main-menu']):
                            for link in nav.find_all('a'):
                                if link.text.strip():
                                    categories.append(link.text.strip().lower())
                    
                    return {
                        "name": title,
                        "url": url,
                        "feed_url": feed_url,
                        "category": categories[:5] if categories else None,
                        "location": locations[:3] if locations else None
                    }
            except Exception as e:
                logger.debug(f"Error finding feeds for {url}: {e}")
            
            return {}

    async def _filter_sources_by_relevance(self, sources: List[Dict[str, Any]], 
                                         categories: Optional[List[str]] = None,
                                         locations: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Filter sources by relevance to categories and locations."""
        if not categories and not locations:
            return sources
        
        relevant_sources = []
        
        for source in sources:
            relevance_score = 0
            
            # If source has a feed URL, try to parse it to check content relevance
            if source.get("feed_url"):
                try:
                    feed = feedparser.parse(source["feed_url"])
                    
                    # Check entries for relevance to categories and locations
                    feed_text = ""
                    for entry in feed.entries[:10]:  # Check first 10 entries
                        feed_text += f"{entry.get('title', '')} {entry.get('summary', '')}"
                    
                    # Score based on categories
                    if categories:
                        for category in categories:
                            if category.lower() in feed_text.lower():
                                relevance_score += 2
                            if source.get("category") and any(category.lower() in cat.lower() for cat in source["category"]):
                                relevance_score += 3
                    
                    # Score based on locations
                    if locations:
                        for location in locations:
                            if location.lower() in feed_text.lower():
                                relevance_score += 2
                            if source.get("location") and any(location.lower() in loc.lower() for loc in source["location"]):
                                relevance_score += 3
                except Exception as e:
                    logger.debug(f"Error parsing feed {source.get('feed_url')}: {e}")
            
            # Also check the main URL for relevance
            try:
                # Check if the source URL contains category or location indicators
                url_lower = source["url"].lower()
                
                if categories:
                    for category in categories:
                        if category.lower() in url_lower:
                            relevance_score += 1
                
                if locations:
                    for location in locations:
                        if location.lower() in url_lower:
                            relevance_score += 1
            except:
                pass
            
            # Add sources with positive relevance score
            if relevance_score > 0:
                source["relevance_score"] = relevance_score
                relevant_sources.append(source)
            
        # Sort by relevance score and return top sources
        return sorted(relevant_sources, key=lambda x: x.get("relevance_score", 0), reverse=True)

class NewsHeadlineStreamer:
    """Main class for streaming news headlines in real-time."""
    
    def __init__(self, config_file: Optional[str] = None):
        self.config = self._load_config(config_file)
        self.cache = Cache(self.config)
        self.source_discoverer = NewsSourceDiscoverer(self.config, self.cache)
        self.update_interval = self.config["general"]["update_interval"]
        self.max_headlines_per_source = self.config["general"]["max_headlines_per_source"]
        self.user_agent = self.config["general"]["user_agent"]
        self.source_refresh_interval = self.config["general"]["source_refresh_interval"]
        self.min_headline_length = self.config["filtering"]["min_headline_length"]
        self.max_headline_age = self.config["filtering"]["max_headline_age"]
        self.language = self.config["filtering"]["language"]
        self.remove_duplicates = self.config["filtering"]["remove_duplicates"]
        self.max_concurrent = self.config["general"]["max_concurrent_requests"]
        self.sources = []
        self.last_source_refresh = 0
        self.running = False
        self.seen_headlines = deque(maxlen=10000)  # Store recent headlines to avoid duplicates
        
        # Signal handling
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _load_config(self, config_file: Optional[str]) -> Dict[str, Any]:
        """Load configuration from file or use defaults."""
        config = DEFAULT_CONFIG.copy()
        
        if config_file and os.path.exists(config_file):
            try:
                parser = configparser.ConfigParser()
                parser.read(config_file)
                
                for section in parser.sections():
                    if section in config:
                        for key, value in parser.items(section):
                            # Convert types appropriately
                            if key in config[section]:
                                if isinstance(config[section][key], bool):
                                    config[section][key] = parser.getboolean(section, key)
                                elif isinstance(config[section][key], int):
                                    config[section][key] = parser.getint(section, key)
                                elif isinstance(config[section][key], float):
                                    config[section][key] = parser.getfloat(section, key)
                                else:
                                    config[section][key] = value
                
                logger.info(f"Loaded configuration from {config_file}")
            except Exception as e:
                logger.error(f"Error loading configuration: {e}. Using defaults.")
        
        return config

    def _signal_handler(self, sig, frame):
        """Handle termination signals gracefully."""
        logger.info("Received termination signal. Shutting down...")
        self.running = False
        self.cache.save_seen_headlines()

    async def start(self, categories: Optional[List[str]] = None, 
                  locations: Optional[List[str]] = None,
                  callback=None):
        """
        Start streaming headlines in real-time.
        
        Args:
            categories: Optional list of categories to filter by
            locations: Optional list of locations to filter by
            callback: Optional callback function that receives each headline
                      If not provided, headlines are printed to stdout
        """
        self.running = True
        self.categories = categories
        self.locations = locations
        self.callback = callback
        
        logger.info(f"Starting news headline streamer with categories={categories}, locations={locations}")
        
        # Discover sources initially
        self.sources = await self.source_discoverer.discover_sources(categories, locations)
        self.last_source_refresh = time.time()
        
        if not self.sources:
            logger.error("No news sources found. Exiting.")
            return
        
        logger.info(f"Found {len(self.sources)} news sources. Starting headline streaming...")
        
        # Main streaming loop
        while self.running:
            try:
                # Check if we need to refresh sources
                current_time = time.time()
                if current_time - self.last_source_refresh > self.source_refresh_interval:
                    logger.info("Refreshing news sources...")
                    self.sources = await self.source_discoverer.discover_sources(categories, locations)
                    self.last_source_refresh = current_time
                
                # Create a session for this iteration
                async with aiohttp.ClientSession(
                    headers={"User-Agent": self.user_agent},
                    timeout=ClientTimeout(total=self.config["general"]["request_timeout"])
                ) as session:
                    # Fetch headlines from all sources concurrently
                    semaphore = asyncio.Semaphore(self.max_concurrent)
                    tasks = [self._fetch_source_headlines(session, source, semaphore) 
                             for source in self.sources]
                    headlines_lists = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Process results, filtering out exceptions
                    all_headlines = []
                    for result in headlines_lists:
                        if isinstance(result, list):
                            all_headlines.extend(result)
                    
                    # Filter, deduplicate, and process the headlines
                    processed_headlines = await self._process_headlines(all_headlines, categories, locations)
                    
                    # Output headlines
                    for headline in processed_headlines:
                        if self.callback:
                            self.callback(headline)
                        else:
                            print(f"{headline.source_name}: {headline.title}")
                            print(f"  URL: {headline.url}")
                            if headline.published_date:
                                print(f"  Published: {headline.published_date.isoformat()}")
                            print(f"  Category: {headline.category or 'Unknown'}")
                            print(f"  Location: {headline.location or 'Unknown'}")
                            if headline.summary:
                                print(f"  Summary: {headline.summary}")
                            print("")
                
                # Save cache periodically
                self.cache.save_seen_headlines()
                
                # Sleep before next update
                await asyncio.sleep(self.update_interval)
            
            except asyncio.CancelledError:
                logger.info("Streaming cancelled.")
                self.running = False
                break
            except Exception as e:
                logger.error(f"Error in main streaming loop: {e}")
                # Sleep a bit before retrying to avoid rapid failure loops
                await asyncio.sleep(5)
        
        logger.info("Headline streaming stopped.")

    async def _fetch_source_headlines(self, session: aiohttp.ClientSession, 
                                    source: Dict[str, Any],
                                    semaphore: asyncio.Semaphore) -> List[NewsHeadline]:
        """Fetch headlines from a specific news source."""
        async with semaphore:
            headlines = []
            
            if not source.get("feed_url"):
                return headlines
            
            try:
                # For RSS/Atom feeds, use feedparser
                async with session.get(source["feed_url"], allow_redirects=True) as response:
                    if response.status != 200:
                        return headlines
                    
                    content = await response.text()
                    feed = feedparser.parse(content)
                    
                    for entry in feed.entries[:self.max_headlines_per_source]:
                        try:
                            # Extract basic headline info
                            title = entry.get("title", "").strip()
                            if not title or len(title) < self.min_headline_length:
                                continue
                            
                            url = entry.get("link", "")
                            if not url:
                                continue
                            
                            # Get published date
                            published_date = None
                            if "published_parsed" in entry and entry.published_parsed:
                                published_date = datetime(*entry.published_parsed[:6])
                            elif "updated_parsed" in entry and entry.updated_parsed:
                                published_date = datetime(*entry.updated_parsed[:6])
                            
                            # Skip old headlines
                            if published_date and (datetime.now() - published_date).total_seconds() > self.max_headline_age:
                                continue
                            
                            # Extract category and location if available
                            category = None
                            if "tags" in entry:
                                categories = [tag.get("term", "") for tag in entry.tags if "term" in tag]
                                if categories:
                                    category = categories[0]
                            elif source.get("category"):
                                category = source["category"][0] if isinstance(source["category"], list) else source["category"]
                            
                            location = source.get("location", None)
                            if isinstance(location, list) and location:
                                location = location[0]
                            
                            # Extract summary
                            summary = entry.get("summary", "")
                            if not summary and "description" in entry:
                                summary = entry["description"]
                            
                            if summary:
                                # Clean up summary (remove HTML, limit length)
                                soup = BeautifulSoup(summary, "html.parser")
                                summary = soup.get_text()[:200] + "..." if len(soup.get_text()) > 200 else soup.get_text()
                            
                            # Get image URL if available
                            image_url = None
                            if "media_content" in entry:
                                for media in entry.media_content:
                                    if "url" in media and media.get("medium", "") == "image":
                                        image_url = media["url"]
                                        break
                            if not image_url and "links" in entry:
                                for link in entry.links:
                                    if link.get("type", "").startswith("image/"):
                                        image_url = link.get("href")
                                        break
                            
                            # Create headline object
                            headline = NewsHeadline(
                                title=title,
                                url=url,
                                source_name=source.get("name", urlparse(source["url"]).netloc),
                                source_url=source["url"],
                                category=category,
                                location=location,
                                published_date=published_date,
                                summary=summary,
                                image_url=image_url
                            )
                            
                            headlines.append(headline)
                        except Exception as e:
                            logger.debug(f"Error processing feed entry: {e}")
            
            except Exception as e:
                logger.debug(f"Error fetching headlines from {source.get('name', source.get('url', 'unknown'))}: {e}")
            
            return headlines

    async def _process_headlines(self, headlines: List[NewsHeadline], 
                              categories: Optional[List[str]] = None,
                              locations: Optional[List[str]] = None) -> List[NewsHeadline]:
        """Process and filter headlines based on criteria."""
        filtered_headlines = []
        
        for headline in headlines:
            # Skip seen headlines if duplicate removal is enabled
            if self.remove_duplicates:
                if headline.hash_id in self.seen_headlines or self.cache.is_headline_seen(headline):
                    continue
                self.seen_headlines.append(headline.hash_id)
                self.cache.mark_headline_seen(headline)
            
            # Filter by category if specified
            if categories and headline.category:
                if not any(category.lower() in headline.category.lower() for category in categories):
                    # Also check title for category mentions
                    if not any(category.lower() in headline.title.lower() for category in categories):
                        continue
            
            # Filter by location if specified
            if locations and headline.location:
                if not any(location.lower() in headline.location.lower() for location in locations):
                    # Also check title for location mentions
                    if not any(location.lower() in headline.title.lower() for location in locations):
                        continue
            
            filtered_headlines.append(headline)
        
        return filtered_headlines

    def stop(self):
        """Stop the headline streaming."""
        self.running = False
        logger.info("Stopping headline streaming...")

class NewsHeadlineAPI:
    """Simple API wrapper for the news headline streamer."""
    
    def __init__(self, config_file: Optional[str] = None):
        self.streamer = NewsHeadlineStreamer(config_file)
        self.headlines_queue = asyncio.Queue()
        self._stream_task = None
    
    async def start_streaming(self, categories: Optional[List[str]] = None, 
                             locations: Optional[List[str]] = None) -> None:
        """Start streaming headlines with the given filters."""
        if self._stream_task:
            # Already streaming, stop first
            self.stop_streaming()
        
        # Use a callback to put headlines into the queue
        def on_headline(headline):
            self.headlines_queue.put_nowait(headline)
        
        # Start the streaming in a background task
        self._stream_task = asyncio.create_task(
            self.streamer.start(categories, locations, callback=on_headline)
        )
    
    def stop_streaming(self) -> None:
        """Stop the headline streaming."""
        if self._stream_task:
            self.streamer.stop()
            self._stream_task.cancel()
            self._stream_task = None
    
    async def get_next_headline(self) -> NewsHeadline:
        """Get the next headline from the queue."""
        return await self.headlines_queue.get()
    
    async def get_headlines(self, max_count: int = 10, timeout: int = 5) -> List[NewsHeadline]:
        """
        Get up to max_count headlines, waiting up to timeout seconds.
        Returns whatever headlines are available within the timeout period.
        """
        headlines = []
        try:
            # Wait for the first headline with timeout
            headline = await asyncio.wait_for(self.headlines_queue.get(), timeout)
            headlines.append(headline)
            
            # Get remaining headlines without waiting
            while len(headlines) < max_count and not self.headlines_queue.empty():
                headline = self.headlines_queue.get_nowait()
                headlines.append(headline)
        except asyncio.TimeoutError:
            # No headlines available within timeout
            pass
        
        return headlines

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Real-time News Headline Streamer")
    parser.add_argument("--config", "-c", type=str, help="Path to configuration file")
    parser.add_argument("--categories", "-cat", type=str, help="Comma-separated list of categories to filter by")
    parser.add_argument("--locations", "-loc", type=str, help="Comma-separated list of locations to filter by")
    parser.add_argument("--interval", "-i", type=int, help="Update interval in seconds")
    parser.add_argument("--format", "-f", choices=["text", "json"], default="text", 
                        help="Output format (text or json)")
    return parser.parse_args()

async def main():
    """Main entry point for the command-line interface."""
    args = parse_args()
    
    # Parse categories and locations
    categories = args.categories.split(",") if args.categories else None
    locations = args.locations.split(",") if args.locations else None
    
    # Create streamer
    streamer = NewsHeadlineStreamer(args.config)
    
    # Override interval if specified
    if args.interval:
        streamer.update_interval = args.interval
    
    # Define callback based on format
    if args.format == "json":
        def callback(headline):
            print(headline.to_json())
    else:
        callback = None  # Use default text output
    
    # Start streaming
    await streamer.start(categories, locations, callback)

if __name__ == "__main__":
    asyncio.run(main())