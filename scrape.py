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
import sys
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

class UserPreferences:
    """Stores user preferences for news categories and locations"""
    
    # Available categories for news
    AVAILABLE_CATEGORIES = [
        "POLITICS",
        "BUSINESS",
        "TECHNOLOGY",
        "HEALTH",
        "SCIENCE",
        "SPORTS",
        "ENTERTAINMENT",
        "WORLD",
        "ENVIRONMENT",
        "EDUCATION"
    ]
    
    # Common locations/regions for news
    AVAILABLE_LOCATIONS = [
        "GLOBAL",
        "NORTH_AMERICA",
        "EUROPE",
        "ASIA",
        "MIDDLE_EAST",
        "AFRICA",
        "SOUTH_AMERICA",
        "AUSTRALIA",
        "UK",
        "USA"
    ]
    
    @staticmethod
    def get_user_preferences():
        """Get user preferences for news categories and locations"""
        print("\n===== NEWS HEADLINE EXTRACTOR =====")
        print("\nWhat categories of news are you interested in?")
        
        # Display available categories
        for i, category in enumerate(UserPreferences.AVAILABLE_CATEGORIES, 1):
            print(f"{i}. {category}")
        
        # Get user input for categories
        print("\nEnter the numbers for categories you want (comma-separated, e.g. '1,3,5'), or")
        print("press Enter for all categories:")
        category_input = input("> ").strip()
        
        selected_categories = []
        if category_input:
            try:
                indices = [int(idx.strip()) - 1 for idx in category_input.split(",")]
                selected_categories = [UserPreferences.AVAILABLE_CATEGORIES[i] for i in indices if 0 <= i < len(UserPreferences.AVAILABLE_CATEGORIES)]
            except (ValueError, IndexError):
                print("Invalid input. Using all categories.")
                selected_categories = UserPreferences.AVAILABLE_CATEGORIES
        else:
            selected_categories = UserPreferences.AVAILABLE_CATEGORIES
        
        # Display available locations
        print("\nWhat locations/regions are you interested in?")
        for i, location in enumerate(UserPreferences.AVAILABLE_LOCATIONS, 1):
            print(f"{i}. {location}")
        
        # Get user input for locations
        print("\nEnter the numbers for locations you want (comma-separated, e.g. '1,3,5'), or")
        print("press Enter for global news:")
        location_input = input("> ").strip()
        
        selected_locations = []
        if location_input:
            try:
                indices = [int(idx.strip()) - 1 for idx in location_input.split(",")]
                selected_locations = [UserPreferences.AVAILABLE_LOCATIONS[i] for i in indices if 0 <= i < len(UserPreferences.AVAILABLE_LOCATIONS)]
            except (ValueError, IndexError):
                print("Invalid input. Using global news.")
                selected_locations = ["GLOBAL"]
        else:
            selected_locations = ["GLOBAL"]
        
        print(f"\nSelected categories: {', '.join(selected_categories)}")
        print(f"Selected locations: {', '.join(selected_locations)}")
        print("\nStarting news headline extraction...\n")
        
        return {
            "categories": selected_categories,
            "locations": selected_locations
        }


class FeedDiscoverer:
    """Discovers RSS feeds from various sources dynamically"""
    
    def __init__(self, user_preferences):
        self.known_feeds: Set[str] = set()
        self.user_preferences = user_preferences
        
        # Map categories to feed directory URLs
        category_to_feed_map = {
            "POLITICS": "https://rss.feedspot.com/politics_news_rss_feeds/",
            "BUSINESS": "https://rss.feedspot.com/business_news_rss_feeds/",
            "TECHNOLOGY": "https://rss.feedspot.com/technology_news_rss_feeds/",
            "HEALTH": "https://rss.feedspot.com/health_news_rss_feeds/",
            "SCIENCE": "https://rss.feedspot.com/science_news_rss_feeds/",
            "SPORTS": "https://rss.feedspot.com/sports_news_rss_feeds/",
            "ENTERTAINMENT": "https://rss.feedspot.com/entertainment_news_rss_feeds/",
            "WORLD": "https://rss.feedspot.com/world_news_rss_feeds/",
            "ENVIRONMENT": "https://rss.feedspot.com/environment_news_rss_feeds/",
            "EDUCATION": "https://rss.feedspot.com/education_news_rss_feeds/"
        }
        
        # Map locations to regional feed directory URLs
        location_to_feed_map = {
            "GLOBAL": "https://rss.feedspot.com/news_rss_feeds/",
            "NORTH_AMERICA": "https://rss.feedspot.com/usa_news_rss_feeds/",
            "EUROPE": "https://rss.feedspot.com/european_news_rss_feeds/",
            "ASIA": "https://rss.feedspot.com/asian_news_rss_feeds/",
            "MIDDLE_EAST": "https://rss.feedspot.com/middle_east_news_rss_feeds/",
            "AFRICA": "https://rss.feedspot.com/africa_news_rss_feeds/",
            "SOUTH_AMERICA": "https://rss.feedspot.com/south_america_news_rss_feeds/",
            "AUSTRALIA": "https://rss.feedspot.com/australia_news_rss_feeds/",
            "UK": "https://rss.feedspot.com/uk_news_rss_feeds/",
            "USA": "https://rss.feedspot.com/usa_news_rss_feeds/"
        }
        
        # Build feed directory URLs based on user preferences
        self.feed_directory_urls = []
        
        # Add category-specific feed directories
        for category in self.user_preferences["categories"]:
            if category in category_to_feed_map:
                self.feed_directory_urls.append(category_to_feed_map[category])
        
        # Add location-specific feed directories
        for location in self.user_preferences["locations"]:
            if location in location_to_feed_map:
                self.feed_directory_urls.append(location_to_feed_map[location])
        
        # If no valid preferences were found, use default global feeds
        if not self.feed_directory_urls:
            self.feed_directory_urls = ["https://rss.feedspot.com/news_rss_feeds/"]
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
        
        # Limit the number of headlines per batch to avoid overwhelming the system
        if len(headlines) > MAX_HEADLINES_PER_BATCH:
            headlines = headlines[:MAX_HEADLINES_PER_BATCH]
        
        # First, try to categorize headlines using keyword matching to reduce API calls
        categorized_headlines = []
        
        for headline in headlines:
            # First try rule-based categorization to avoid using API
            category = self._rule_based_categorization(headline)
            
            # If rule-based categorization is confident, use it
            if category != "UNKNOWN":
                headline.category = category
                # Extract basic keywords without using API
                headline.keywords = self._extract_basic_keywords(headline)
                headline.language = self._detect_language(headline)
                categorized_headlines.append(headline)
            else:
                # Only use Gemini for headlines that couldn't be categorized by rules
                try:
                    # Categorize the headline with Gemini
                    category = await self._categorize_headline(headline)
                    headline.category = category
                    
                    # Extract keywords using Gemini
                    headline.keywords = await self._extract_keywords(headline)
                    headline.language = "en"  # Default to English
                    
                    categorized_headlines.append(headline)
                except Exception as e:
                    logger.error(f"Error processing headline: {e}")
        
        # Output the processed headlines (in a real system, you might save to a database)
        self._output_headlines(categorized_headlines)
    
    def _rule_based_categorization(self, headline: NewsHeadline) -> str:
        """
        Categorize a headline using rule-based approach to avoid API calls
        Returns "UNKNOWN" if not confident, otherwise returns a category
        """
        text = f"{headline.title} {headline.summary}".lower()
        
        # Define keyword sets for each category
        category_keywords = {
            "POLITICS": {
                "politic", "president", "congress", "senate", "parliament", "election", 
                "vote", "democrat", "republican", "government", "minister", "policy",
                "campaign", "candidate", "bill", "legislation", "lawmaker", "governor"
            },
            "BUSINESS": {
                "business", "economy", "market", "stock", "trade", "company", "corporate",
                "investor", "finance", "economic", "ceo", "startup", "investment", "profit",
                "revenue", "industry", "commercial", "bank", "financial", "dollar", "euro"
            },
            "TECHNOLOGY": {
                "tech", "technology", "software", "app", "digital", "cyber", "internet",
                "computer", "device", "ai", "artificial intelligence", "robot", "machine learning",
                "data", "smartphone", "gadget", "innovation", "programming", "algorithm",
                "blockchain", "crypto", "virtual reality", "vr", "augmented reality", "ar"
            },
            "HEALTH": {
                "health", "medical", "hospital", "doctor", "patient", "disease", "covid",
                "virus", "treatment", "medicine", "vaccine", "healthcare", "drug", "clinical",
                "therapy", "mental health", "wellness", "diagnosis", "cancer", "pandemic"
            },
            "SCIENCE": {
                "science", "research", "study", "scientist", "discovery", "space", "nasa",
                "physics", "chemistry", "biology", "experiment", "laboratory", "theory",
                "astronomy", "planet", "moon", "solar", "quantum", "gene", "fossil", "species"
            },
            "SPORTS": {
                "sport", "game", "team", "player", "win", "lose", "score", "championship",
                "tournament", "match", "athlete", "coach", "soccer", "football", "basketball",
                "baseball", "tennis", "golf", "olympic", "league", "nfl", "nba", "mlb", "nhl"
            },
            "ENTERTAINMENT": {
                "entertainment", "movie", "film", "music", "actor", "actress", "celebrity",
                "star", "hollywood", "tv", "television", "show", "series", "concert", "award",
                "album", "singer", "director", "performance", "streaming", "netflix"
            },
            "WORLD": {
                "world", "international", "global", "country", "nation", "foreign", "europe",
                "asia", "africa", "america", "australia", "middle east", "united nations",
                "treaty", "embassy", "diplomat", "war", "refugee", "crisis", "peace"
            },
            "ENVIRONMENT": {
                "environment", "climate", "weather", "pollution", "carbon", "green", "clean",
                "renewable", "sustainable", "ecology", "conservation", "biodiversity", "recycle",
                "waste", "emission", "solar power", "wind power", "fossil fuel", "ocean"
            },
            "EDUCATION": {
                "education", "school", "student", "teacher", "college", "university", "campus",
                "academic", "learn", "study", "degree", "classroom", "curriculum", "professor",
                "graduate", "undergraduate", "scholar", "research", "tuition", "course"
            }
        }
        
        # Count keyword matches for each category
        category_scores = {category: 0 for category in category_keywords}
        
        for category, keywords in category_keywords.items():
            for keyword in keywords:
                if keyword in text:
                    category_scores[category] += 1
        
        # Get the category with the highest score
        best_category = max(category_scores.items(), key=lambda x: x[1])
        
        # Only return a category if we have a minimum number of keyword matches
        # and there's a clear winner (to avoid ambiguous cases)
        if best_category[1] >= 2:
            runner_up = sorted(category_scores.items(), key=lambda x: x[1], reverse=True)[1]
            # If the best category has significantly more matches than the runner-up
            if best_category[1] > runner_up[1] + 1:
                return best_category[0]
        
        return "UNKNOWN"
    
    def _extract_basic_keywords(self, headline: NewsHeadline) -> List[str]:
        """Extract basic keywords without using API"""
        text = f"{headline.title} {headline.summary}".lower()
        
        # Remove common stopwords
        stopwords = {
            "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", 
            "any", "are", "as", "at", "be", "because", "been", "before", "being", "below", 
            "between", "both", "but", "by", "could", "did", "do", "does", "doing", "down", 
            "during", "each", "few", "for", "from", "further", "had", "has", "have", "having", 
            "he", "her", "here", "hers", "herself", "him", "himself", "his", "how", "i", 
            "if", "in", "into", "is", "it", "its", "itself", "just", "me", "more", "most", 
            "my", "myself", "no", "nor", "not", "now", "of", "off", "on", "once", "only", 
            "or", "other", "our", "ours", "ourselves", "out", "over", "own", "same", "she", 
            "should", "so", "some", "such", "than", "that", "the", "their", "theirs", "them", 
            "themselves", "then", "there", "these", "they", "this", "those", "through", "to", 
            "too", "under", "until", "up", "very", "was", "we", "were", "what", "when", 
            "where", "which", "while", "who", "whom", "why", "will", "with", "would", "you", 
            "your", "yours", "yourself", "yourselves"
        }
        
        # Split into words, remove punctuation and stopwords
        words = re.findall(r'\b\w+\b', text)
        words = [word for word in words if word not in stopwords and len(word) > 3]
        
        # Count word frequencies
        word_count = {}
        for word in words:
            word_count[word] = word_count.get(word, 0) + 1
        
        # Get most frequent words as keywords
        keywords = sorted(word_count.items(), key=lambda x: x[1], reverse=True)
        return [word for word, count in keywords[:5]]
    
    def _detect_language(self, headline: NewsHeadline) -> str:
        """Basic language detection without API"""
        # This is a very simple language detection that defaults to English
        # In a production system, you'd use a proper language detection library
        text = f"{headline.title} {headline.summary}"
        
        # Very simplified language detection based on common words
        english_markers = ["the", "and", "in", "to", "of", "a", "is", "for", "on", "that"]
        spanish_markers = ["el", "la", "en", "de", "y", "es", "para", "un", "una", "por"]
        french_markers = ["le", "la", "en", "de", "et", "est", "pour", "un", "une", "que"]
        german_markers = ["der", "die", "das", "und", "in", "zu", "von", "mit", "auf", "ist"]
        
        # Count occurrences of marker words
        text_lower = text.lower()
        counts = {
            "en": sum(1 for word in english_markers if f" {word} " in f" {text_lower} "),
            "es": sum(1 for word in spanish_markers if f" {word} " in f" {text_lower} "),
            "fr": sum(1 for word in french_markers if f" {word} " in f" {text_lower} "),
            "de": sum(1 for word in german_markers if f" {word} " in f" {text_lower} ")
        }
        
        # Get language with highest marker count
        best_match = max(counts.items(), key=lambda x: x[1])
        if best_match[1] > 0:
            return best_match[0]
        
        # Default to English if no markers found
        return "en"
    
    async def _categorize_headline(self, headline: NewsHeadline) -> str:
        """Categorize a headline using Gemini (only for ambiguous cases)"""
        try:
            # Combine title and summary for better categorization
            text = f"Title: {headline.title}\nSummary: {headline.summary}"
            
            # Only include user's selected categories
            categories = self.headline_extractor.user_preferences["categories"]
            categories_text = "\n".join([f"- {category}" for category in categories])
            
            prompt = f"""
            Categorize the following news headline into EXACTLY ONE of these categories:
            {categories_text}
            
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
            if category not in self.headline_extractor.user_preferences["categories"]:
                # If the category is not in the user's preferences, use default
                category = self.headline_extractor.user_preferences["categories"][0]
            
            return category
            
        except Exception as e:
            logger.error(f"Error categorizing headline: {e}")
            # Return the first category in user preferences as default
            return self.headline_extractor.user_preferences["categories"][0]
    
    async def _extract_keywords(self, headline: NewsHeadline) -> List[str]:
        """Extract keywords using Gemini (only for headlines that need API)"""
        try:
            text = f"Title: {headline.title}\nSummary: {headline.summary}"
            if headline.content:
                text += f"\nContent: {headline.content[:300]}"  # Limit content length more aggressively
            
            prompt = f"""
            Extract 3-5 important keywords from the following news article:
            
            {text}
            
            Return ONLY a comma-separated list of keywords, nothing else.
            """
            
            response = self.gemini_client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=GenerateContentConfig(
                    temperature=0.1,
                    max_output_tokens=50
                )
            )
            
            # Parse comma-separated keywords
            keywords_text = response.text.strip()
            keywords = [k.strip() for k in keywords_text.split(",")]
            return keywords[:5]  # Limit to 5 keywords max
            
        except Exception as e:
            logger.error(f"Error extracting keywords: {e}")
            return self._extract_basic_keywords(headline)  # Fall back to basic method
    
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
                logger.info("---")

class NewsStreamer:
    """Main class that coordinates the real-time news streaming process"""
    
    def __init__(self, gemini_api_key: str = None, user_preferences: dict = None):
        """Initialize the news streamer"""
        # Initialize Gemini API client
        if not gemini_api_key:
            gemini_api_key = os.environ.get("GEMINI_API_KEY")
            if not gemini_api_key:
                raise ValueError("Gemini API key is required either in constructor or as GEMINI_API_KEY environment variable")
        
        self.gemini_client = genai.Client(api_key=gemini_api_key)
        
        # Get user preferences if not provided
        if user_preferences is None:
            self.user_preferences = UserPreferences.get_user_preferences()
        else:
            self.user_preferences = user_preferences
        
        # Initialize the headline extractor with user preferences
        self.headline_extractor = HeadlineExtractor(self.gemini_client)
        self.headline_extractor.user_preferences = self.user_preferences
    
    async def start(self, scan_interval: int = DEFAULT_SCAN_INTERVAL):
        """Start the news streaming process"""
        logger.info("Starting real-time news streaming")
        
        # Initialize feed discoverer with user preferences
        self.headline_extractor.feed_discoverer = FeedDiscoverer(self.user_preferences)
        
        # Start the headline extraction process
        await self.headline_extractor.start(scan_interval)

async def main():
    """Main function to run the news streamer"""
    # Get API key from environment variable
    gemini_api_key = os.environ.get("GEMINI_API_KEY")
    if not gemini_api_key:
        logger.error("GEMINI_API_KEY environment variable is not set")
        print("Please set the GEMINI_API_KEY environment variable")
        return
    
    try:
        # Create and start the news streamer
        streamer = NewsStreamer(gemini_api_key)
        await streamer.start()
    except KeyboardInterrupt:
        print("\nStopping news headline extractor. Goodbye!")
    except Exception as e:
        logger.error(f"Error running news streamer: {e}")
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    asyncio.run(main())