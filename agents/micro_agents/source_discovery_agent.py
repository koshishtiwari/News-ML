import logging
import aiohttp
import asyncio
import json
import re
import time
from typing import List, Dict, Any, Optional, Set
from urllib.parse import urlparse

from models.data_models import NewsSource
from llm_providers.base import LLMProvider
from ..base_agent import BaseAgent, Task, TaskPriority

logger = logging.getLogger(__name__)

class SourceDiscoveryAgent(BaseAgent):
    """Micro-agent responsible for discovering news sources for a location"""
    
    def __init__(self, llm_provider: LLMProvider):
        super().__init__("source_discovery")
        self.llm_provider = llm_provider
        self._session: Optional[aiohttp.ClientSession] = None
        self._source_quality_cache = {}  # Cache source quality assessments
        
    async def _initialize_resources(self):
        """Initialize the HTTP session"""
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
        )
        
    async def _cleanup_resources(self):
        """Close the HTTP session"""
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
        else:
            raise ValueError(f"Unknown task type: {task_type}")
            
    async def _discover_sources(self, task: Task) -> List[NewsSource]:
        """Discover news sources for a location using fallback sources"""
        location = task.data.get("location")
        if not location:
            raise ValueError("Location is required for source discovery")
            
        limit = task.data.get("limit", 10)
        
        await self.update_status("Discovering sources", {"location": location, "limit": limit})
        
        # Use fallback sources to generate potential sources
        sources = await self._generate_source_candidates(location, limit)
        logger.info(f"Generated {len(sources)} source candidates for {location}")
        
        return sources
        
    async def _verify_source(self, task: Task) -> Dict[str, Any]:
        """Verify a news source URL works and extract metadata"""
        source = task.data.get("source")
        if not source:
            raise ValueError("Source is required for verification")
            
        url = source.get("url") if isinstance(source, dict) else source.url if hasattr(source, "url") else None
        if not url:
            raise ValueError("URL is required for source verification")
            
        await self.update_status("Verifying source", {"url": url})
        
        try:
            # Check URL accessibility and get basic metadata
            async with self._session.get(url, allow_redirects=True, timeout=15) as response:
                if response.status != 200:
                    return {
                        "url": url,
                        "verified": False,
                        "status_code": response.status,
                        "reason": f"HTTP error: {response.status} {response.reason}"
                    }
                
                content_type = response.headers.get("Content-Type", "")
                if not content_type.startswith("text/html"):
                    return {
                        "url": url,
                        "verified": False,
                        "reason": f"Not HTML content: {content_type}"
                    }
                
                # Get the page content
                html_content = await response.text()
                
                # Extract basic metadata
                title = self._extract_title(html_content) or urlparse(url).netloc
                
                # Return verification result with metadata
                return {
                    "url": url,
                    "verified": True,
                    "title": title,
                    "content_type": content_type,
                    "final_url": str(response.url),  # In case of redirects
                    "html_length": len(html_content)
                }
                
        except asyncio.TimeoutError:
            return {
                "url": url,
                "verified": False,
                "reason": "Connection timeout"
            }
        except Exception as e:
            return {
                "url": url,
                "verified": False,
                "reason": f"Error: {str(e)}"
            }
    
    async def _generate_source_candidates(self, location: str, limit: int = 10) -> List[NewsSource]:
        """Use fallback sources to generate potential news sources for a location"""
        logger.info(f"Starting source candidate generation for location: {location}")
        
        # Skip LLM entirely and just use fallback sources
        # This is a more reliable approach until LLM issues are resolved
        logger.info(f"Using reliable fallback sources for {location}")
        return self._create_fallback_sources(location)
        
    def _create_fallback_sources(self, location: str) -> List[NewsSource]:
        """Create a fallback list of news sources for when LLM fails"""
        fallback_sources = []
        location_lower = location.lower()
        
        # Expanded fallback sources with reliable news outlets
        if "nyc" in location_lower or "new york" in location_lower:
            fallback_sources = [
                NewsSource(name="New York Times", url="https://www.nytimes.com/", location_type="National/Local", reliability_score=0.9),
                NewsSource(name="New York Post", url="https://nypost.com/", location_type="Local", reliability_score=0.7),
                NewsSource(name="NY1", url="https://www.ny1.com/", location_type="Local", reliability_score=0.8),
                NewsSource(name="Gothamist", url="https://gothamist.com/", location_type="Local", reliability_score=0.8),
                NewsSource(name="amNY", url="https://www.amny.com/", location_type="Local", reliability_score=0.7)
            ]
        elif "la" in location_lower or "los angeles" in location_lower:
            fallback_sources = [
                NewsSource(name="Los Angeles Times", url="https://www.latimes.com/", location_type="National/Local", reliability_score=0.9),
                NewsSource(name="LA Daily News", url="https://www.dailynews.com/", location_type="Local", reliability_score=0.8),
                NewsSource(name="LAist", url="https://laist.com/", location_type="Local", reliability_score=0.8)
            ]
        elif "chicago" in location_lower:
            fallback_sources = [
                NewsSource(name="Chicago Tribune", url="https://www.chicagotribune.com/", location_type="National/Local", reliability_score=0.9),
                NewsSource(name="Chicago Sun-Times", url="https://chicago.suntimes.com/", location_type="Local", reliability_score=0.8),
                NewsSource(name="Block Club Chicago", url="https://blockclubchicago.org/", location_type="Local", reliability_score=0.8)
            ]
        elif "london" in location_lower:
            fallback_sources = [
                NewsSource(name="BBC London", url="https://www.bbc.co.uk/news/england/london", location_type="Local", reliability_score=0.9),
                NewsSource(name="Evening Standard", url="https://www.standard.co.uk/", location_type="Local", reliability_score=0.8),
                NewsSource(name="The Guardian London", url="https://www.theguardian.com/uk/london", location_type="Local", reliability_score=0.9)
            ]
        elif "tokyo" in location_lower:
            fallback_sources = [
                NewsSource(name="The Japan Times", url="https://www.japantimes.co.jp/", location_type="National/Local", reliability_score=0.8),
                NewsSource(name="Tokyo Weekender", url="https://www.tokyoweekender.com/", location_type="Local", reliability_score=0.7),
                NewsSource(name="NHK World", url="https://www3.nhk.or.jp/nhkworld/", location_type="National", reliability_score=0.9)
            ]
        else:
            # Generic national sources for any location
            fallback_sources = [
                NewsSource(name="CNN", url="https://www.cnn.com/", location_type="National", reliability_score=0.8),
                NewsSource(name="BBC News", url="https://www.bbc.com/news", location_type="International", reliability_score=0.9),
                NewsSource(name="Reuters", url="https://www.reuters.com/", location_type="International", reliability_score=0.9),
                NewsSource(name="Associated Press", url="https://apnews.com/", location_type="International", reliability_score=0.9),
                NewsSource(name="The New York Times", url="https://www.nytimes.com/", location_type="National", reliability_score=0.9)
            ]
            
        logger.info(f"Created {len(fallback_sources)} fallback sources for {location}")
        return fallback_sources
    
    @staticmethod
    def _extract_title(html_content: str) -> Optional[str]:
        """Extract the title from HTML content"""
        title_match = re.search(r'<title[^>]*>(.*?)</title>', html_content, re.IGNORECASE | re.DOTALL)
        if title_match:
            return title_match.group(1).strip()
        return None