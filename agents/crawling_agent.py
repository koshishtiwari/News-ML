# This is a placeholder file
import logging
import asyncio
from typing import List, Optional, Dict, Any
from urllib.parse import urlparse
from datetime import datetime, timezone
import aiohttp # newspaper might need it, or for direct fetching if needed

# Use newspaper3k as requested
import newspaper
from newspaper import Article as NewspaperArticle
from newspaper import Config as NewspaperConfig

from tqdm.asyncio import tqdm as async_tqdm # Use external tqdm

# Assuming models are in ../models/
from models.data_models import NewsSource, NewsArticle

logger = logging.getLogger(__name__)

# Define boilerplate/error patterns to check against extracted text
BOILERPLATE_STRINGS = [
    "javascript is required",
    "enable javascript",
    "browser doesn't support",
    "please enable cookies",
    "verify you are human",
    "site doesn't work properly without javascript",
    # Add more common patterns if observed
]

class NewsCrawlingAgent:
    """Agent responsible for crawling news using newspaper3k."""

    def __init__(self):
        self.newspaper_config = NewspaperConfig()
        self.newspaper_config.browser_user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36'
        self.newspaper_config.request_timeout = 20 # Increased timeout slightly
        self.newspaper_config.fetch_images = False
        self.newspaper_config.memoize_articles = False
        self.newspaper_config.verbose = False # Keep newspaper logs quiet unless debugging
        logger.debug("NewsCrawlingAgent initialized with newspaper3k config.")
        # Session management could be added if direct aiohttp calls are needed later
        # self._session = None

    def _is_valid_content(self, text: Optional[str]) -> bool:
        """Checks if the extracted text seems like valid content vs boilerplate/error."""
        if not text or len(text.strip()) < 100: # Arbitrary minimum length
            return False
        text_lower = text.lower()
        for pattern in BOILERPLATE_STRINGS:
            if pattern in text_lower:
                return False
        # Add more sophisticated checks if needed (e.g., keyword density, sentence structure)
        return True

    async def _fetch_and_parse_article(self, url: str, source_name: str, location_query: str) -> Optional[NewsArticle]:
        """Fetches and parses a single article using newspaper3k."""
        logger.debug(f"Processing article URL: {url}")
        try:
            loop = asyncio.get_running_loop()
            article_obj: Optional[NewspaperArticle] = await loop.run_in_executor(
                None, # Use default thread pool executor
                self._sync_download_and_parse,
                url
            )

            if not article_obj or not article_obj.is_parsed:
                logger.warning(f"newspaper3k failed to download or parse: {url}")
                return None

            # *** Crucial Validation Step ***
            if not self._is_valid_content(article_obj.text):
                 logger.warning(f"Discarding article - content seems invalid/boilerplate: {url}")
                 return None

            # Proceed only if content looks valid
            title = article_obj.title
            if not title:
                 logger.warning(f"Discarding article - missing title: {url}")
                 return None

            publish_dt = None
            if article_obj.publish_date:
                 pub_date = article_obj.publish_date
                 # Ensure timezone-aware UTC
                 if pub_date.tzinfo is None:
                     publish_dt = pub_date.replace(tzinfo=timezone.utc)
                 else:
                     publish_dt = pub_date.astimezone(timezone.utc)

            return NewsArticle(
                url=url,
                title=title,
                source_name=source_name,
                content=article_obj.text, # Store the validated text
                published_at=publish_dt,
                location_query=location_query,
                fetched_at=datetime.now(timezone.utc)
            )

        except Exception as e:
            # Catch potential errors during download/parse or validation
            logger.error(f"Error processing article {url} with newspaper3k: {e}", exc_info=False) # Keep exc_info=False unless debugging newspaper itself
            return None

    def _sync_download_and_parse(self, url: str) -> Optional[NewspaperArticle]:
        """Synchronous helper for newspaper3k download and parse."""
        try:
            article = NewspaperArticle(url, config=self.newspaper_config)
            article.download()
            article.parse()
            return article
        except Exception as e:
            # Log specific newspaper errors if needed, but mainly handled in async wrapper
            logger.debug(f"Newspaper3k sync download/parse error for {url}: {e}")
            return None

    async def discover_links_from_source(self, source: NewsSource, limit: int = 15) -> List[str]:
        """Discover article links using newspaper3k's build function."""
        logger.info(f"Discovering links for {source.name} ({source.url}) via newspaper3k build...")
        links = set()
        try:
            loop = asyncio.get_running_loop()
            news_source: Optional[newspaper.Source] = await loop.run_in_executor(
                 None,
                 lambda: newspaper.build(source.url, config=self.newspaper_config, memoize_articles=False)
            )

            if not news_source:
                 logger.warning(f"newspaper.build returned None for {source.url}")
                 return []

            logger.debug(f"Source built. Found {len(news_source.articles)} potential articles in newspaper object.")

            source_domain = urlparse(source.url).netloc.lower()
            for article in news_source.articles:
                try:
                    url = article.url
                    if not url or not url.startswith(('http://', 'https://')): continue

                    # Basic domain check + allow subdomains
                    link_domain = urlparse(url).netloc.lower()
                    if link_domain == source_domain or link_domain.endswith('.' + source_domain):
                        links.add(url)

                except Exception as e:
                     logger.warning(f"Error processing URL from newspaper build {article.url}: {e}")

            found_count = len(links)
            logger.info(f"Found {found_count} validated links via newspaper3k build for {source.name}.")
            # Apply limit after validation
            return list(links)[:limit]

        except Exception as e:
             # Log errors from newspaper.build() itself, often network/DNS related
             logger.error(f"Failed during newspaper.build() for {source.url}: {e}", exc_info=False) # Keep exc_info=False unless debugging network
             return []

    async def crawl_source(self, source: NewsSource, location_query: str) -> List[NewsArticle]:
        """Crawl a news source for articles using newspaper3k, with validation."""
        logger.info(f"--- Crawling source: {source.name} ---")
        # Discover links first
        article_links = await self.discover_links_from_source(source) # Use default limit

        if not article_links:
            logger.warning(f"No article links discovered for {source.name}.")
            return []

        logger.info(f"Attempting to fetch and parse {len(article_links)} articles from {source.name} using newspaper3k...")

        tasks = [self._fetch_and_parse_article(link, source.name, location_query) for link in article_links]
        processed_articles: List[NewsArticle] = []

        # Process concurrently with progress bar
        for future in async_tqdm(asyncio.as_completed(tasks), total=len(tasks), desc=f"Parsing {source.name[:20]}"):
            result: Optional[NewsArticle] = await future
            if result: # Only append if parsing and validation succeeded
                processed_articles.append(result)

        logger.info(f"--- Finished crawling {source.name}. Successfully extracted and validated {len(processed_articles)} articles. ---")
        return processed_articles

    async def crawl_sources(self, sources: List[NewsSource], location_query: str) -> List[NewsArticle]:
        """Crawl multiple news sources sequentially using newspaper3k."""
        logger.info(f"Starting crawl for {len(sources)} sources for location '{location_query}'...")
        all_articles: List[NewsArticle] = []

        for source in async_tqdm(sources, desc="Crawling Sources"):
            try:
                articles = await self.crawl_source(source, location_query)
                all_articles.extend(articles)
                await asyncio.sleep(1) # Politeness delay between sources
            except Exception as e:
                logger.error(f"Unexpected error during crawl_source for {source.name}: {e}", exc_info=True)

        logger.info(f"Finished crawling all sources. Total articles fetched and validated: {len(all_articles)}.")
        # Deduplicate based on URL (although less likely with sequential source crawl)
        unique_articles = {article.url: article for article in all_articles}
        final_list = list(unique_articles.values())
        if len(final_list) < len(all_articles):
            logger.info(f"Removed {len(all_articles) - len(final_list)} duplicate articles based on URL.")

        return final_list