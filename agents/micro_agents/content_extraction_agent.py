import logging
import asyncio
import aiohttp
import time
import re
import hashlib
from pathlib import Path
from typing import Dict, Any, List, Optional
from urllib.parse import urlparse, urljoin
from datetime import datetime, timezone

from newspaper import Article as NewspaperArticle
from newspaper import Config as NewspaperConfig
from bs4 import BeautifulSoup

from models.data_models import NewsArticle
from agents.base_agent import BaseAgent, Task, TaskPriority

# Constants
DEFAULT_TIMEOUT = 30  # seconds
PAGE_CACHE_DIR = "data/cache/pages"
DEFAULT_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
MAX_CONTENT_LENGTH = 50000  # Characters

logger = logging.getLogger(__name__)

class ContentExtractionAgent(BaseAgent):
    """Micro-agent responsible for fetching and extracting content from web pages"""
    
    def __init__(self):
        super().__init__("content_extraction")
        self._session: Optional[aiohttp.ClientSession] = None
        
        # Setup cache directory
        self.cache_dir = Path(PAGE_CACHE_DIR)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Configure newspaper
        self.newspaper_config = NewspaperConfig()
        self.newspaper_config.browser_user_agent = DEFAULT_USER_AGENT
        self.newspaper_config.request_timeout = DEFAULT_TIMEOUT
        self.newspaper_config.fetch_images = False
        self.newspaper_config.memoize_articles = False
        
    async def _initialize_resources(self):
        """Initialize HTTP session"""
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=DEFAULT_TIMEOUT),
            headers={
                'User-Agent': DEFAULT_USER_AGENT,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            }
        )
        
    async def _cleanup_resources(self):
        """Close HTTP session"""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
            
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
        """Generate a cache file path for a URL"""
        url_hash = hashlib.md5(url.encode()).hexdigest()
        return self.cache_dir / url_hash[:2] / f"{url_hash}.html"
            
    async def _extract_article(self, task: Task) -> Dict[str, Any]:
        """Extract article content from a URL"""
        article_data = task.data.get("article")
        if not article_data:
            raise ValueError("Article data is required for extraction")
            
        # Get the URL
        url = article_data.get("url")
        title = article_data.get("title")
        
        if not url:
            raise ValueError("URL is required for article extraction")
            
        await self.update_status("Extracting content", {"url": url})
        
        try:
            # Check the cache first
            cache_path = self._get_cache_path(url)
            html_content = None
            
            # Create parent directories if they don't exist
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Try to load from cache
            if cache_path.exists():
                try:
                    with open(cache_path, 'r', encoding='utf-8') as f:
                        html_content = f.read()
                    logger.debug(f"Loaded content from cache for {url}")
                except Exception as e:
                    logger.warning(f"Failed to read cache for {url}: {e}")
            
            # Fetch the page if not in cache
            if not html_content:
                async with self._session.get(url, allow_redirects=True) as response:
                    if response.status != 200:
                        return {
                            "url": url,
                            "success": False,
                            "error": f"HTTP error: {response.status}"
                        }
                    
                    # Check content type to make sure it's HTML
                    content_type = response.headers.get('Content-Type', '').lower()
                    if not 'html' in content_type:
                        return {
                            "url": url,
                            "success": False,
                            "error": f"Not HTML content: {content_type}"
                        }
                    
                    # Get the content
                    html_content = await response.text()
                    
                    # Save to cache
                    try:
                        with open(cache_path, 'w', encoding='utf-8') as f:
                            f.write(html_content)
                    except Exception as e:
                        logger.warning(f"Failed to cache content for {url}: {e}")
            
            # Extract article using newspaper3k
            article = NewspaperArticle(url, config=self.newspaper_config)
            article.download(input_html=html_content)
            article.parse()
            
            # Get published date
            publish_date = None
            if article.publish_date:
                publish_date = article.publish_date
                
                # Ensure timezone awareness
                if publish_date.tzinfo is None:
                    publish_date = publish_date.replace(tzinfo=timezone.utc)
            
            # Create the article object
            extracted_article = {
                "url": url,
                "title": article.title or title,  # Use provided title if extraction fails
                "content": article.text[:MAX_CONTENT_LENGTH] if article.text else None,
                "summary": None,  # Will be filled by analysis agent
                "published_at": publish_date.isoformat() if publish_date else None,
                "source_name": urlparse(url).netloc,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "success": True
            }
            
            # Try to extract source name from meta tags if not found by newspaper3k
            if not article.meta_data.get('og:site_name') and html_content:
                try:
                    soup = BeautifulSoup(html_content, 'html.parser')
                    
                    # Try common meta tags for site name
                    site_name_meta = soup.find('meta', property='og:site_name') or \
                                    soup.find('meta', {'name': 'application-name'}) or \
                                    soup.find('meta', {'name': 'publisher'})
                    
                    if site_name_meta and site_name_meta.get('content'):
                        extracted_article["source_name"] = site_name_meta.get('content')
                except Exception as e:
                    logger.debug(f"Error extracting site name from meta tags: {e}")
            
            return extracted_article
            
        except Exception as e:
            logger.error(f"Error extracting article from {url}: {e}", exc_info=True)
            return {
                "url": url,
                "success": False,
                "error": str(e)
            }
            
    async def _extract_article_batch(self, task: Task) -> List[Dict[str, Any]]:
        """Extract content from a batch of articles"""
        articles_data = task.data.get("articles", [])
        if not articles_data:
            return []
            
        await self.update_status("Extracting batch", {"count": len(articles_data)})
        
        # Process concurrently with rate limiting
        results = []
        semaphore = asyncio.Semaphore(5)  # Limit to 5 concurrent extractions
        
        async def process_single_article(article_data):
            async with semaphore:
                # Create a subtask for each article
                subtask = Task(
                    task_id=f"extract_{len(results)}",
                    task_type="extract_article",
                    data={"article": article_data}
                )
                return await self._extract_article(subtask)
        
        # Process all articles concurrently
        tasks = [process_single_article(article) for article in articles_data]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions
        valid_results = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Error in batch extraction: {result}")
            else:
                valid_results.append(result)
                
        return valid_results
        
    @staticmethod
    def article_dict_to_model(article_dict: Dict[str, Any], location_query: str = None) -> NewsArticle:
        """Convert article dictionary to NewsArticle model"""
        # Parse date strings to datetime objects
        published_at = None
        if article_dict.get('published_at'):
            try:
                published_at = datetime.fromisoformat(article_dict['published_at'])
            except ValueError:
                pass
                
        fetched_at = None
        if article_dict.get('fetched_at'):
            try:
                fetched_at = datetime.fromisoformat(article_dict['fetched_at'])
            except ValueError:
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
            location_query=location_query,
            category=article_dict.get('category'),
            importance=article_dict.get('importance', 'Medium')
        )