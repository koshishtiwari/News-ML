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
from .base_agent import BaseAgent, Task, TaskPriority

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
        """Discover news sources for a location using LLM"""
        location = task.data.get("location")
        if not location:
            raise ValueError("Location is required for source discovery")
            
        limit = task.data.get("limit", 10)
        
        await self.update_status("Discovering sources", {"location": location, "limit": limit})
        
        # Use LLM to generate potential sources
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
        """Use LLM to generate potential news sources for a location"""
        # Prompt the LLM to generate source suggestions in JSON format
        prompt = f"""
        I need to find reliable news sources for {location}. Please provide a list of {limit} news sources 
        with the following details:
        - News source name
        - Homepage URL (full URL starting with https://)
        - Type of news coverage (Local, National/Local, National, or International)
        
        Your response should be a valid JSON array with objects containing 'name', 'url', and 'location_type' keys.
        Only include sources you're confident exist and are reliable.
        """
        
        result = await self.llm_provider.generate(prompt)
        
        # Try to extract JSON from response
        sources = []
        try:
            # Find JSON array in the response using regex
            json_match = re.search(r'\[\s*\{.*\}\s*\]', result, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
                sources_data = json.loads(json_str)
                
                # Convert to NewsSource objects
                for source_data in sources_data:
                    try:
                        source = NewsSource(
                            name=source_data.get("name", "Unknown"),
                            url=source_data.get("url", ""),
                            location_type=source_data.get("location_type", "Local"),
                            reliability_score=0.5  # Default score until verified
                        )
                        sources.append(source)
                    except Exception as e:
                        logger.warning(f"Failed to parse source: {source_data}. Error: {e}")
            else:
                logger.warning(f"No valid JSON found in LLM response for {location}")
                # Try to extract data using regex as fallback
                urls = re.findall(r'https?://[^\s\'"]+', result)
                names = re.findall(r'(?:^|\n)(?:- |• |• )?([A-Z][A-Za-z\s]{2,50})(?:\s*[-:–]|$)', result, re.MULTILINE)
                
                # Create sources from extracted data
                for i, url in enumerate(urls[:limit]):
                    name = names[i] if i < len(names) else f"News Source {i+1}"
                    try:
                        source = NewsSource(
                            name=name,
                            url=url,
                            location_type="Unknown",
                            reliability_score=0.3  # Lower score for regex-extracted sources
                        )
                        sources.append(source)
                    except Exception as e:
                        logger.warning(f"Failed to create source from URL {url}. Error: {e}")
                        
        except Exception as e:
            logger.error(f"Error parsing LLM response for {location}: {e}", exc_info=True)
            
        return sources
    
    @staticmethod
    def _extract_title(html_content: str) -> Optional[str]:
        """Extract the title from HTML content"""
        title_match = re.search(r'<title[^>]*>(.*?)</title>', html_content, re.IGNORECASE | re.DOTALL)
        if title_match:
            return title_match.group(1).strip()
        return None