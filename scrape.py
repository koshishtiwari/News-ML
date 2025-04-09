#!/usr/bin/env python3
# news_headline_extractor.py - Real-Time News Headline Identifier and Extractor

import os
import time
import json
import logging
import asyncio
import feedparser
import requests
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Set, Tuple, Optional, Any
from dataclasses import dataclass, asdict
from urllib.parse import urlparse
import xml
import re
from google import genai
from google.genai.types import GenerateContentConfig, Part

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("news_extractor.log")
    ]
)
logger = logging.getLogger(__name__)

# Constants
DEFAULT_SCAN_INTERVAL = 60  # seconds
MAX_HEADLINES_PER_BATCH = 100
RSS_DISCOVERY_TIMEOUT = 10  # seconds
MAX_FEED_SOURCES = 50
GEMINI_MODEL = "gemini-2.0-flash-001"

@dataclass
class NewsHeadline:
    """Data class to store news headline information"""
    id: str
    title: str
    link: str
    source: str
    category: str
    published: str
    summary: str
    timestamp: float
    content: Optional[str] = None
    sentiment: Optional[str] = None
    keywords: Optional[List[str]] = None
    language: Optional[str] = None
    
    def __post_init__(self):
        # Generate a unique ID if not provided
        if not self.id:
            unique_parts = f"{self.source}:{self.title}:{self.published}"
            self.id = str(hash(unique_parts))
        
        # Set timestamp if not provided
        if not self.timestamp:
            self.timestamp = time.time()

class FeedDiscoverer:
    """Discovers RSS feeds from various sources dynamically"""
    
    def __init__(self):
        self.known_feeds: Set[str] = set()
        self.feed_directory_urls = [
            "https://rss.feedspot.com/news_rss_feeds/",
            "https://rss.feedspot.com/world_news_rss_feeds/",
            "https://rss.feedspot.com/technology_news_rss_feeds/",
            "https://rss.feedspot.com/business_news_rss_feeds/",
            "https://rss.feedspot.com/science_news_rss_feeds/",
            "https://rss.feedspot.com/sports_news_rss_feeds/",
        ]
        self.common_feed_paths = [
            "/feed/", 
            "/rss/", 
            "/feeds/posts/default",
            "/atom.xml", 
            "/rss.xml", 
            "/feed.xml",
            "/index.xml"
        ]
    
    async def discover_feeds(self) -> Set[str]:
        """Main method to discover RSS feeds from multiple sources"""
        logger.info("Starting feed discovery process")
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            
            # Search feed directories
            for directory_url in self.feed_directory_urls:
                futures.append(
                    executor.submit(self._extract_feeds_from_directory, directory_url)
                )
            
            # Get popular news sites and try common feed paths
            news_sites = self._get_popular_news_sites()
            for site in news_sites:
                futures.append(
                    executor.submit(self._find_feeds_for_site, site)
                )
            
            # Gather all results
            for future in futures:
                try:
                    feeds = future.result()
                    self.known_feeds.update(feeds)
                except Exception as e:
                    logger.error(f"Error in feed discovery: {e}")
        
        # Limit the number of feeds to avoid overwhelming the system
        limited_feeds = list(self.known_feeds)[:MAX_FEED_SOURCES]
        logger.info(f"Discovered {len(limited_feeds)} RSS feeds")
        return set(limited_feeds)
    
    def _extract_feeds_from_directory(self, directory_url: str) -> Set[str]:
        """Extract RSS feed URLs from a feed directory website"""
        feeds = set()
        try:
            response = requests.get(directory_url, timeout=RSS_DISCOVERY_TIMEOUT)
            if response.status_code == 200:
                # Look for feed URLs in the HTML content
                content = response.text
                # Simple regex pattern to find RSS feed URLs
                feed_urls = re.findall(r'https?://[^\s"\']+\.(rss|xml|atom|feed)', content)
                for url in feed_urls:
                    if isinstance(url, tuple):
                        url = url[0]
                    feeds.add(url)
        except Exception as e:
            logger.error(f"Error extracting feeds from directory {directory_url}: {e}")
        return feeds
    
    def _get_popular_news_sites(self) -> List[str]:
        """Return a list of popular news sites to check for RSS feeds"""
        return [
            "https://www.bbc.com",
            "https://www.cnn.com",
            "https://www.reuters.com",
            "https://www.nytimes.com",
            "https://www.theguardian.com",
            "https://www.washingtonpost.com",
            "https://www.aljazeera.com",
            "https://www.forbes.com",
            "https://www.bloomberg.com",
            "https://www.wsj.com",
            "https://apnews.com",
            "https://news.yahoo.com",
            "https://www.cnbc.com"
        ]
    
    def _find_feeds_for_site(self, site_url: str) -> Set[str]:
        """Try to find RSS feeds for a given website by checking common paths"""
        feeds = set()
        parsed_url = urlparse(site_url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        
        # First check if the site has a meta link pointing to the feed
        try:
            response = requests.get(site_url, timeout=RSS_DISCOVERY_TIMEOUT)
            if response.status_code == 200:
                content = response.text
                # Look for RSS/Atom link in the HTML head
                feed_links = re.findall(r'<link[^>]*type=["\']application\/(rss|atom)\+xml["\'][^>]*href=["\']([^"\']+)["\']', content)
                for link_type, link_url in feed_links:
                    if link_url.startswith('http'):
                        feeds.add(link_url)
                    else:
                        feeds.add(f"{base_url.rstrip('/')}/{link_url.lstrip('/')}")
        except Exception as e:
            logger.warning(f"Error checking site {site_url} for feed links: {e}")
        
        # Try common feed paths
        for path in self.common_feed_paths:
            feed_url = f"{base_url}{path}"
            try:
                response = requests.get(feed_url, timeout=RSS_DISCOVERY_TIMEOUT)
                if response.status_code == 200:
                    # Very simple check if it looks like a feed
                    content = response.text.lower()
                    if '<rss' in content or '<feed' in content or '<channel' in content:
                        feeds.add(feed_url)
            except Exception:
                # Just skip if we can't access this path
                pass
        
        return feeds

class HeadlineExtractor:
    """Extracts headlines from RSS feeds in real-time"""
    
    def __init__(self, gemini_client: genai.Client):
        self.feed_discoverer = FeedDiscoverer()
        self.feeds: Set[str] = set()
        self.last_scan_times: Dict[str, float] = {}
        self.seen_headlines: Set[str] = set()
        self.gemini_client = gemini_client
    
    async def start(self, scan_interval: int = DEFAULT_SCAN_INTERVAL):
        """Start the headline extraction process"""
        logger.info("Starting headline extractor")
        
        # Initial feed discovery
        self.feeds = await self.feed_discoverer.discover_feeds()
        
        # Initial scan of all feeds
        initial_headlines = await self._scan_all_feeds()
        if initial_headlines:
            await self._process_headlines(initial_headlines)
        
        # Continuous monitoring loop
        try:
            while True:
                headlines = await self._scan_all_feeds()
                if headlines:
                    await self._process_headlines(headlines)
                
                # Periodically rediscover feeds to find new ones
                if time.time() % (3600 * 24) < scan_interval:  # Once per day
                    new_feeds = await self.feed_discoverer.discover_feeds()
                    self.feeds.update(new_feeds)
                
                await asyncio.sleep(scan_interval)
        except KeyboardInterrupt:
            logger.info("Stopping headline extractor")
            return
        except Exception as e:
            logger.error(f"Error in headline extractor: {e}")
            raise
    
    async def _scan_all_feeds(self) -> List[NewsHeadline]:
        """Scan all feeds for new headlines"""
        logger.info(f"Scanning {len(self.feeds)} feeds for headlines")
        
        all_headlines = []
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [
                executor.submit(self._scan_feed, feed_url)
                for feed_url in self.feeds
            ]
            
            for future in futures:
                try:
                    headlines = future.result()
                    if headlines:
                        all_headlines.extend(headlines)
                except Exception as e:
                    logger.error(f"Error scanning feed: {e}")
        
        # Remove duplicate headlines
        unique_headlines = {}
        for headline in all_headlines:
            if headline.id not in unique_headlines:
                unique_headlines[headline.id] = headline
        
        return list(unique_headlines.values())
    
    def _scan_feed(self, feed_url: str) -> List[NewsHeadline]:
        """Scan a single feed for new headlines"""
        headlines = []
        
        try:
            # Get the last scan time for this feed, default to 24 hours ago
            last_scan = self.last_scan_times.get(feed_url, time.time() - 86400)
            
            feed = feedparser.parse(feed_url)
            if not feed.entries:
                return []
            
            # Update last scan time
            self.last_scan_times[feed_url] = time.time()
            
            # Extract source name from feed
            source_name = self._extract_source_name(feed, feed_url)
            
            # Process each entry in the feed
            for entry in feed.entries:
                # Skip previously seen headlines
                entry_id = entry.get('id', entry.get('link', ''))
                if entry_id in self.seen_headlines:
                    continue
                
                # Parse the publish date
                published = self._parse_date(entry)
                
                # Skip old headlines
                if published:
                    published_time = datetime.strptime(published, "%Y-%m-%d %H:%M:%S")
                    if (datetime.now() - published_time) > timedelta(days=1):
                        continue
                
                # Create a headline object
                headline = NewsHeadline(
                    id=entry_id,
                    title=entry.get('title', ''),
                    link=entry.get('link', ''),
                    source=source_name,
                    category='',  # To be filled by classifier
                    published=published,
                    summary=self._clean_text(entry.get('summary', '')),
                    timestamp=time.time(),
                    content=self._extract_content(entry)
                )
                
                headlines.append(headline)
                self.seen_headlines.add(entry_id)
        
        except Exception as e:
            logger.error(f"Error parsing feed {feed_url}: {e}")
        
        return headlines
    
    def _extract_source_name(self, feed, feed_url: str) -> str:
        """Extract the source name from a feed"""
        if hasattr(feed, 'feed') and hasattr(feed.feed, 'title'):
            return feed.feed.title
        else:
            parsed_url = urlparse(feed_url)
            return parsed_url.netloc
    
    def _parse_date(self, entry) -> str:
        """Parse and standardize the publication date"""
        date_fields = ['published', 'pubDate', 'updated', 'created', 'date']
        
        for field in date_fields:
            if hasattr(entry, field):
                try:
                    date_str = getattr(entry, field)
                    # Try to parse with feedparser's internal date parser
                    if hasattr(entry, f"{field}_parsed") and getattr(entry, f"{field}_parsed"):
                        parsed = getattr(entry, f"{field}_parsed")
                        dt = datetime(
                            parsed[0], parsed[1], parsed[2],
                            parsed[3], parsed[4], parsed[5]
                        )
                        return dt.strftime("%Y-%m-%d %H:%M:%S")
                    
                    # Try common date formats
                    for fmt in [
                        "%a, %d %b %Y %H:%M:%S %z",
                        "%Y-%m-%dT%H:%M:%S%z",
                        "%Y-%m-%d %H:%M:%S",
                        "%Y-%m-%dT%H:%M:%SZ"
                    ]:
                        try:
                            dt = datetime.strptime(date_str, fmt)
                            return dt.strftime("%Y-%m-%d %H:%M:%S")
                        except ValueError:
                            continue
                except Exception:
                    pass
        
        # If we couldn't parse the date, return the current time
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    def _clean_text(self, text: str) -> str:
        """Clean HTML and XML tags from text"""
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', ' ', text)
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        return text
    
    def _extract_content(self, entry) -> Optional[str]:
        """Extract the full content if available"""
        if hasattr(entry, 'content'):
            for content in entry.content:
                if content.get('type', '') == 'text/html':
                    return self._clean_text(content.value)
        
        if hasattr(entry, 'content_encoded'):
            return self._clean_text(entry.content_encoded)
        
        # If no full content, use the summary
        return None
    
    async def _process_headlines(self, headlines: List[NewsHeadline]) -> None:
        """Process a batch of headlines"""
        if not headlines:
            return
        
        logger.info(f"Processing {len(headlines)} new headlines")
        
        # Limit the number of headlines per batch to avoid overwhelming the AI
        if len(headlines) > MAX_HEADLINES_PER_BATCH:
            headlines = headlines[:MAX_HEADLINES_PER_BATCH]
        
        # Categorize and enhance headlines using Gemini AI
        enhanced_headlines = []
        
        for headline in headlines:
            try:
                # Categorize the headline
                category = await self._categorize_headline(headline)
                headline.category = category
                
                # Extract keywords and sentiment using Gemini
                enhancements = await self._enhance_headline(headline)
                for key, value in enhancements.items():
                    setattr(headline, key, value)
                
                enhanced_headlines.append(headline)
            except Exception as e:
                logger.error(f"Error processing headline: {e}")
        
        # Output the processed headlines (in a real system, you might save to a database)
        self._output_headlines(enhanced_headlines)
    
    async def _categorize_headline(self, headline: NewsHeadline) -> str:
        """Categorize a headline using Gemini"""
        try:
            # Combine title and summary for better categorization
            text = f"Title: {headline.title}\nSummary: {headline.summary}"
            
            prompt = f"""
            Categorize the following news headline into EXACTLY ONE of these categories:
            - POLITICS
            - BUSINESS
            - TECHNOLOGY
            - HEALTH
            - SCIENCE
            - SPORTS
            - ENTERTAINMENT
            - WORLD
            - ENVIRONMENT
            - EDUCATION
            
            Return ONLY the category name, nothing else.
            
            Headline: {text}
            """
            
            response = self.gemini_client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=GenerateContentConfig(
                    temperature=0.0,
                    max_output_tokens=10
                )
            )
            
            category = response.text.strip()
            if category not in [
                "POLITICS", "BUSINESS", "TECHNOLOGY", "HEALTH", 
                "SCIENCE", "SPORTS", "ENTERTAINMENT", "WORLD",
                "ENVIRONMENT", "EDUCATION"
            ]:
                category = "WORLD"  # Default category
            
            return category
            
        except Exception as e:
            logger.error(f"Error categorizing headline: {e}")
            return "WORLD"  # Default category
    
    async def _enhance_headline(self, headline: NewsHeadline) -> Dict[str, Any]:
        """Enhance headline with keywords, sentiment analysis, and language detection"""
        try:
            text = f"Title: {headline.title}\nSummary: {headline.summary}"
            if headline.content:
                text += f"\nContent: {headline.content[:500]}"  # Limit content length
            
            prompt = f"""
            Analyze the following news article:
            
            {text}
            
            Return a JSON object with these fields:
            1. "keywords": An array of 3-5 important keywords from the text
            2. "sentiment": Overall sentiment ("positive", "neutral", or "negative")
            3. "language": The language of the text (ISO 639-1 code)
            
            Return ONLY the JSON object, no other text.
            """
            
            response = self.gemini_client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=GenerateContentConfig(
                    temperature=0.1,
                    max_output_tokens=150
                )
            )
            
            # Extract JSON from response
            response_text = response.text.strip()
            # Remove code block markers if present
            if response_text.startswith("```json"):
                response_text = response_text[7:-3] if response_text.endswith("```") else response_text[7:]
            elif response_text.startswith("```"):
                response_text = response_text[3:-3] if response_text.endswith("```") else response_text[3:]
            
            enhancements = json.loads(response_text)
            return enhancements
            
        except Exception as e:
            logger.error(f"Error enhancing headline: {e}")
            # Return default values
            return {
                "keywords": [],
                "sentiment": "neutral",
                "language": "en"
            }
    
    def _output_headlines(self, headlines: List[NewsHeadline]) -> None:
        """Output processed headlines (can be modified to save to database, API, etc.)"""
        df = pd.DataFrame([asdict(h) for h in headlines])
        
        # Group by category
        for category, group in df.groupby('category'):
            logger.info(f"=== {category} HEADLINES ({len(group)}) ===")
            for _, row in group.iterrows():
                logger.info(f"[{row['source']}] {row['title']}")
                logger.info(f"  Link: {row['link']}")
                logger.info(f"  Published: {row['published']}")
                logger.info(f"  Keywords: {', '.join(row['keywords'] or [])}")
                logger.info(f"  Sentiment: {row['sentiment']}")
                logger.info("---")

class NewsStreamer:
    """Main class that coordinates the real-time news streaming process"""
    
    def __init__(self, gemini_api_key: str = None):
        """Initialize the news streamer"""
        # Initialize Gemini API client
        if not gemini_api_key:
            gemini_api_key = os.environ.get("GEMINI_API_KEY")
            if not gemini_api_key:
                raise ValueError("Gemini API key is required either in constructor or as GEMINI_API_KEY environment variable")
        
        self.gemini_client = genai.Client(api_key=gemini_api_key)
        
        # Initialize the headline extractor
        self.headline_extractor = HeadlineExtractor(self.gemini_client)
    
    async def start(self, scan_interval: int = DEFAULT_SCAN_INTERVAL):
        """Start the news streaming process"""
        logger.info("Starting real-time news streaming")
        await self.headline_extractor.start(scan_interval)

async def main():
    """Main function to run the news streamer"""
    # Get API key from environment variable
    gemini_api_key = os.environ.get("GEMINI_API_KEY")
    if not gemini_api_key:
        logger.error("GEMINI_API_KEY environment variable is not set")
        print("Please set the GEMINI_API_KEY environment variable")
        return
    
    # Create and start the news streamer
    streamer = NewsStreamer(gemini_api_key)
    await streamer.start()

if __name__ == "__main__":
    asyncio.run(main())