# discovery_agent.py
import logging
import json
import re
import asyncio
from typing import List, Optional, Dict, Tuple, Any
import aiohttp # Needed for URL verification
import time

# Assuming models are in ../models/
from models.data_models import NewsSource
# Assuming LLM provider base/interface is in ../llm_providers/
from llm_providers.base import LLMProvider
from agents.base_agent import BaseAgent  # Import BaseAgent

logger = logging.getLogger(__name__)

class NewsSourceDiscoveryAgent(BaseAgent):
    """Agent responsible for discovering and verifying news sources."""

    def __init__(self, llm_provider: LLMProvider, session: Optional[aiohttp.ClientSession] = None):
        super().__init__("source_discovery")  # Initialize the BaseAgent
        self.llm_provider = llm_provider
        # Use a shared session if provided for efficiency, or manage internally
        self._session = session
        self._created_session = False # Flag if we created the session internally
        
        # Source quality assessment metrics
        self.source_blacklist = set()  # Known unreliable sources
        self.quality_cache = {}  # Cache for source quality assessments
        self.quality_cache_expiry = 86400  # 24 hours in seconds
        
        logger.debug("NewsSourceDiscoveryAgent initialized.")

    async def _get_session(self) -> aiohttp.ClientSession:
        """Gets or creates an aiohttp session."""
        if self._session is None or self._session.closed:
            logger.debug("Creating internal aiohttp session for DiscoveryAgent.")
            # Consider adding connector limits if making many requests
            connector = aiohttp.TCPConnector(limit_per_host=5)
            self._session = aiohttp.ClientSession(connector=connector) 
            self._created_session = True
        return self._session

    async def close_internal_session(self):
        """Closes the internally created aiohttp session if it exists and was created here."""
        await self._update_status_async("Closing session")  # Using async version
        if self._created_session and self._session and not self._session.closed:
            await self._session.close()
            self._session = None
            self._created_session = False
            logger.debug("Closed internal DiscoveryAgent aiohttp session.")

    async def _verify_source_url(self, url: str, timeout: int = 15) -> Dict[str, Any]:
        """
        Checks if a URL is reachable and collects metadata for quality assessment.
        Returns a dict with verification results and metadata.
        """
        await self._update_status_async("Verifying URL", {"url": url})
        result = {
            "verified": False,
            "status_code": None,
            "response_time_ms": 0,
            "content_type": None,
            "server": None,
            "error": None,
            "https": url.startswith("https://")
        }
        
        if not url or not url.startswith(('http://', 'https://')):
            logger.warning(f"Invalid URL format for verification: {url}")
            result["error"] = "invalid_url_format"
            return result
            
        try:
            session = await self._get_session()
            start_time = time.time()
            
            # First try a HEAD request for efficiency
            async with session.head(url, timeout=timeout, allow_redirects=True, ssl=False) as response:
                result["status_code"] = response.status
                result["response_time_ms"] = int((time.time() - start_time) * 1000)
                result["content_type"] = response.headers.get('Content-Type', '')
                result["server"] = response.headers.get('Server', '')
                
                # Consider any 2xx or 3xx status as potentially valid/reachable
                result["verified"] = 200 <= response.status < 400
                
                # If HEAD didn't work well, try GET for more detailed assessment
                if not result["verified"] or not result["content_type"]:
                    # Some servers don't handle HEAD correctly
                    logger.debug(f"HEAD request failed or incomplete for {url}, trying GET")
                    async with session.get(url, timeout=timeout, allow_redirects=True, ssl=False) as get_response:
                        result["status_code"] = get_response.status
                        result["response_time_ms"] = int((time.time() - start_time) * 1000)
                        result["content_type"] = get_response.headers.get('Content-Type', '')
                        result["server"] = get_response.headers.get('Server', '')
                        result["verified"] = 200 <= get_response.status < 400
                        
                        # Check if it looks like a news page
                        if result["verified"] and result["content_type"] and "text/html" in result["content_type"]:
                            # Read a small sample of content to detect news characteristics
                            sample = await get_response.read(10000)  # Read first 10KB
                            sample_text = sample.decode('utf-8', errors='ignore').lower()
                            
                            # Check for keywords that suggest a news site
                            news_indicators = ["news", "article", "story", "headline", "report", "journalist", "editor"]
                            result["news_indicators"] = sum(1 for word in news_indicators if word in sample_text)
                        
            if not result["verified"]:
                logger.warning(f"URL verification failed for {url} (Status: {result['status_code']})")
            else:
                logger.debug(f"URL verification success for {url} (Status: {result['status_code']})")
                
        except asyncio.TimeoutError:
            logger.warning(f"URL verification timed out ({timeout}s) for {url}")
            result["error"] = "timeout"
        except aiohttp.ClientConnectorError as e:
            logger.warning(f"URL verification connection error for {url}: {e}")
            result["error"] = "connection_error"
        except aiohttp.InvalidURL:
            logger.warning(f"Invalid URL encountered during verification: {url}")
            result["error"] = "invalid_url"
        except aiohttp.ClientError as e:
            logger.warning(f"URL verification client error for {url}: {e}")
            result["error"] = "client_error"
        except Exception as e:
            # Catch broader exceptions during verification
            logger.error(f"Unexpected error during URL verification for {url}: {e}", exc_info=True)
            result["error"] = "unexpected_error"
            
        return result

    async def _assess_source_quality(self, source: NewsSource, verification_data: Dict[str, Any]) -> float:
        """
        Assess the quality of a news source based on multiple factors.
        Returns a quality score between 0.0 and 1.0.
        """
        # Check if we have a cached assessment that's still valid
        cache_key = source.url
        now = time.time()
        if cache_key in self.quality_cache:
            cached_score, timestamp = self.quality_cache[cache_key]
            if now - timestamp < self.quality_cache_expiry:
                return cached_score
        
        await self._update_status_async("Assessing source quality", {"source": source.name})
        
        # Start with the baseline reliability score from the LLM
        quality_score = source.reliability_score
        
        # Adjust based on verification results
        if not verification_data["verified"]:
            # Severely penalize sources that can't be reached
            quality_score *= 0.3
        else:
            # Performance and security factors
            if verification_data.get("https", False):
                quality_score *= 1.1  # Bonus for HTTPS
            
            # Response time affects user experience
            response_time = verification_data.get("response_time_ms", 5000)
            if response_time < 500:
                quality_score *= 1.05  # Fast sites get a small bonus
            elif response_time > 3000:
                quality_score *= 0.95  # Slow sites get a small penalty
                
            # Check for news indicators in content
            news_indicators = verification_data.get("news_indicators", 0)
            if news_indicators >= 3:
                quality_score *= 1.1  # Content looks like news
            elif news_indicators == 0:
                quality_score *= 0.8  # Might not be a news site
        
        # Check for domain-specific factors that impact credibility
        domain_quality = await self._evaluate_domain_credibility(source.url)
        quality_score = (quality_score + domain_quality) / 2
        
        # Ensure score stays in valid range
        final_score = max(0.1, min(1.0, quality_score))
        
        # Cache the result
        self.quality_cache[cache_key] = (final_score, now)
        
        return final_score
        
    async def _evaluate_domain_credibility(self, url: str) -> float:
        """
        Evaluate domain credibility using patterns and third-party data if available.
        Returns a score between 0.0 and 1.0.
        """
        # Extract domain from URL
        domain_match = re.search(r'https?://(?:www\.)?([^/]+)', url)
        if not domain_match:
            return 0.5  # Default for unrecognized format
            
        domain = domain_match.group(1).lower()
        
        # Check blacklist
        if domain in self.source_blacklist:
            return 0.1
            
        # Domain age factors - we could query WHOIS data here
        # Domain authority factors - could integrate with third-party API
        
        # Search for known credible TLDs
        credible_tlds = ['.gov', '.edu', '.org']
        for tld in credible_tlds:
            if domain.endswith(tld):
                return 0.9
                
        # Check for suspicious patterns
        suspicious_patterns = ['news24', 'dailynews', '-news', 
                              'breaking', 'viral', 'trending']
        for pattern in suspicious_patterns:
            if pattern in domain:
                return 0.6  # Slightly lower score for potentially clickbait domains
                
        # Default credibility score
        return 0.75

    async def discover_sources(self, location: str, limit: int = 10) -> List[NewsSource]:
        """Discover top news sources via LLM and verify their URLs."""
        await self._update_status_async("Discovering sources", {"location": location, "limit": limit})
        logger.info(f"Discovering top {limit} sources for '{location}'...")
        
        # Prompt definition with focus on reliability and diversity
        prompt = f"""
        I need to find the top {limit} reputable local news sources for {location}.
        Focus on identifying sources that are:
        1. Reliable with accurate reporting
        2. Accessible (functioning websites)
        3. Include a mix of established and independent journalism
        4. Represent diverse perspectives when possible
        
        For each source, provide:
        1. Name: Common name (e.g., "The Example Times").
        2. URL: The EXACT official homepage URL (must be valid HTTP/HTTPS). Double-check accuracy.
        3. Location Type: "Local" if focused on {location} specifically, "Regional" if covering a broader area including {location}, or "National/Local" if a national outlet with dedicated {location} coverage.
        4. Reliability Score: Estimate from 0.0 to 1.0 based on reputation for factual reporting.
        5. Brief Description: In 2-3 sentences, explain what makes this a reliable source for {location} news.

        Format ONLY as a valid JSON array of objects with keys: "name", "url", "location_type", "reliability_score", "description".
        Example: [{{"name": "Kathmandu Post", "url": "https://kathmandupost.com", "location_type": "Local", "reliability_score": 0.9, "description": "Leading English daily with strong local reporting. Known for factual coverage and investigative journalism."}}]
        Do not include any text before or after the JSON array.
        """
        system_prompt = """
        You are an expert media researcher who specializes in identifying reliable news sources.
        Prioritize sources with:
        - Strong factual reporting history
        - Local expertise for the specified location
        - Transparency in reporting and corrections
        - Functional websites that are regularly updated
        
        Respond ONLY with a valid JSON array. Ensure URLs are the correct, official homepage addresses.
        """

        llm_response = await self.llm_provider.generate(prompt, system_prompt)
        potential_sources: List[Tuple[NewsSource, Dict]] = []
        verified_sources: List[NewsSource] = []

        if not llm_response:
            logger.error("LLM provider returned no response for source discovery.")
            await self.close_internal_session()
            return []

        # --- Parse LLM Response ---
        try:
            # Robust JSON extraction
            json_match = re.search(r'\[\s*\{.*?\}\s*\]', llm_response, re.DOTALL)
            if json_match:
                sources_data = json.loads(json_match.group(0))
            else:
                logger.warning("Could not find clear JSON array in LLM response for sources. Trying full parse.")
                sources_data = json.loads(llm_response.strip())

            if not isinstance(sources_data, list):
                raise ValueError("LLM response was not a JSON list.")

            # Initial parsing and basic validation
            for item in sources_data:
                if isinstance(item, dict) and all(k in item for k in ["name", "url", "reliability_score"]):
                    url = str(item["url"]).strip()
                    if not url:
                        logger.warning(f"Skipping source '{item.get('name')}' due to empty URL.")
                        continue
                    try:
                        score = float(item["reliability_score"])
                        # Store description if available
                        description = str(item.get("description", ""))
                        
                        source = NewsSource(
                            name=str(item["name"]),
                            url=url,
                            location_type=str(item.get("location_type", "Unknown")),
                            reliability_score=max(0.0, min(1.0, score)),
                            description=description
                        )
                        
                        # Add to potential sources for verification
                        potential_sources.append(source)
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid reliability score for '{item.get('name')}'. Skipping.")
                else:
                    logger.warning(f"Skipping invalid source entry format from LLM: {item}")

        except (json.JSONDecodeError, ValueError) as e:
            logger.error(f"Failed parsing LLM source response: {e}. Raw:\n{llm_response[:500]}...")
        except Exception as e:
            logger.error(f"Unexpected error parsing sources: {e}", exc_info=True)

        if not potential_sources:
            logger.warning(f"LLM response processed, but no valid source structures found for '{location}'.")
            await self.close_internal_session()
            return []

        logger.info(f"Parsed {len(potential_sources)} potential sources from LLM. Verifying URLs...")

        # --- Verify URLs and Assess Quality Concurrently ---
        semaphore = asyncio.Semaphore(10)  # Limit to 10 concurrent checks
        assessment_tasks = []

        async def verify_and_assess(source: NewsSource):
            async with semaphore:
                # Get verification data
                verification_data = await self._verify_source_url(source.url)
                
                # Assess quality based on verification and other factors
                quality_score = await self._assess_source_quality(source, verification_data)
                
                # Update the source with the new quality assessment
                source.reliability_score = quality_score
                
                return source, verification_data["verified"], quality_score

        for source in potential_sources:
            assessment_tasks.append(verify_and_assess(source))

        assessed_sources = []
        for future in asyncio.as_completed(assessment_tasks):
            try:
                source, is_verified, quality_score = await future
                if is_verified:
                    assessed_sources.append((source, quality_score))
                    logger.info(f"Source '{source.name}' verified with quality score: {quality_score:.2f}")
                else:
                    logger.debug(f"Discarding source '{source.name}' due to failed URL verification.")
            except Exception as e:
                logger.error(f"Assessment task failed unexpectedly: {e}")

        # Sort by quality score and take top sources
        assessed_sources.sort(key=lambda x: x[1], reverse=True)
        verified_sources = [source for source, _ in assessed_sources[:limit]]

        logger.info(f"Discovery complete for '{location}'. Found {len(verified_sources)} quality-assessed sources (from {len(potential_sources)} potential).")
        # Close session only if created internally
        await self.close_internal_session()
        return verified_sources