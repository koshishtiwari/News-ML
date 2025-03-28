import logging
import json
import re
import asyncio
from typing import List, Optional
import aiohttp # Needed for URL verification

# Assuming models are in ../models/
from models.data_models import NewsSource
# Assuming LLM provider base/interface is in ../llm_providers/
from llm_providers.base import LLMProvider

logger = logging.getLogger(__name__)

class NewsSourceDiscoveryAgent:
    """Agent responsible for discovering and verifying news sources."""

    def __init__(self, llm_provider: LLMProvider, session: Optional[aiohttp.ClientSession] = None):
        self.llm_provider = llm_provider
        # Use a shared session if provided for efficiency, or manage internally
        self._session = session
        self._created_session = False # Flag if we created the session internally
        logger.debug("NewsSourceDiscoveryAgent initialized.")

    async def _get_session(self) -> aiohttp.ClientSession:
        """Gets or creates an aiohttp session."""
        if self._session is None or self._session.closed:
            logger.debug("Creating internal aiohttp session for DiscoveryAgent.")
            # Consider adding connector limits if making many requests
            # connector = aiohttp.TCPConnector(limit_per_host=5)
            self._session = aiohttp.ClientSession() # Add connector=connector if needed
            self._created_session = True
        return self._session

    async def close_internal_session(self):
        """Closes the internally created aiohttp session if it exists and was created here."""
        if self._created_session and self._session and not self._session.closed:
            await self._session.close()
            self._session = None
            self._created_session = False
            logger.debug("Closed internal DiscoveryAgent aiohttp session.")

    async def _verify_source_url(self, url: str, timeout: int = 15) -> bool:
        """Quickly checks if a URL seems reachable using a HEAD request."""
        if not url or not url.startswith(('http://', 'https://')):
            logger.warning(f"Invalid URL format for verification: {url}")
            return False
        try:
            session = await self._get_session()
            # Use HEAD, allow redirects, increased timeout, disable SSL verify carefully
            # For production, configure SSL context properly instead of ssl=False
            async with session.head(url, timeout=timeout, allow_redirects=True, ssl=False) as response:
                # Consider any 2xx or 3xx status as potentially valid/reachable
                is_ok = 200 <= response.status < 400
                if not is_ok:
                     logger.warning(f"URL verification failed for {url} (Status: {response.status})")
                else:
                     logger.debug(f"URL verification success for {url} (Status: {response.status})")
                return is_ok
        except asyncio.TimeoutError:
            logger.warning(f"URL verification timed out ({timeout}s) for {url}")
            return False
        except aiohttp.ClientConnectorError as e:
            logger.warning(f"URL verification connection error for {url}: {e}")
            return False
        except aiohttp.InvalidURL:
             logger.warning(f"Invalid URL encountered during verification: {url}")
             return False
        except aiohttp.ClientError as e:
            logger.warning(f"URL verification client error for {url}: {e}")
            return False
        except Exception as e:
            # Catch broader exceptions during verification
            logger.error(f"Unexpected error during URL verification for {url}: {e}", exc_info=False)
            return False

    async def discover_sources(self, location: str, limit: int = 10) -> List[NewsSource]:
        """Discover top news sources via LLM and verify their URLs."""
        logger.info(f"Discovering top {limit} sources for '{location}'...")
        # Prompt definition (ensure it requests accurate URLs)
        prompt = f"""
        I need to find the top {limit} reputable local news sources for {location}.
        Provide the EXACT official homepage URL for each source.
        For each source, provide:
        1. Name: Common name (e.g., "The Example Times").
        2. URL: The EXACT official homepage URL (must be valid HTTP/HTTPS). Double-check accuracy.
        3. Location Type: "Local" or "National/Local".
        4. Reliability Score: Estimate from 0.0 to 1.0.

        Format ONLY as a valid JSON array of objects. Keys: "name", "url", "location_type", "reliability_score".
        Example: [{{"name": "Kathmandu Post", "url": "https://kathmandupost.com", "location_type": "Local", "reliability_score": 0.9}}]
        Do not include any text before or after the JSON array.
        """
        system_prompt = "Respond ONLY with a valid JSON array. Ensure URLs are the correct, official homepage addresses."


        llm_response = await self.llm_provider.generate(prompt, system_prompt)
        potential_sources: List[NewsSource] = []
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
            for item in sources_data: # Process all returned, then limit after verification
                if isinstance(item, dict) and all(k in item for k in ["name", "url", "location_type", "reliability_score"]):
                    url = str(item["url"]).strip()
                    if not url:
                         logger.warning(f"Skipping source '{item.get('name')}' due to empty URL.")
                         continue
                    try:
                        score = float(item["reliability_score"])
                        potential_sources.append(NewsSource(
                            name=str(item["name"]),
                            url=url, # Store stripped URL
                            location_type=str(item["location_type"]),
                            reliability_score=max(0.0, min(1.0, score))
                        ))
                    except (ValueError, TypeError):
                         logger.warning(f"Invalid reliability score for '{item.get('name')}'. Skipping.")
                else:
                     logger.warning(f"Skipping invalid source entry format from LLM: {item}")

        except (json.JSONDecodeError, ValueError) as e:
            logger.error(f"Failed parsing LLM source response: {e}. Raw:\n{llm_response[:500]}...")
        except Exception as e:
             logger.error(f"Unexpected error parsing sources: {e}", exc_info=True)
        # Continue to verification even if parsing had minor issues with some entries

        if not potential_sources:
             logger.warning(f"LLM response processed, but no valid source structures found for '{location}'.")
             await self.close_internal_session()
             return []

        logger.info(f"Parsed {len(potential_sources)} potential sources from LLM. Verifying URLs...")

        # --- Verify URLs Concurrently ---
        # Limit the number of concurrent verification tasks if needed
        semaphore = asyncio.Semaphore(10) # Limit to 10 concurrent checks
        verification_tasks = []
        async def verify_with_semaphore(source: NewsSource):
             async with semaphore:
                 is_valid = await self._verify_source_url(source.url)
                 return source, is_valid

        for source in potential_sources:
             verification_tasks.append(verify_with_semaphore(source))

        # Use tqdm for progress if desired
        # processed_count = 0
        # for future in async_tqdm(asyncio.as_completed(verification_tasks), total=len(verification_tasks), desc="Verifying Sources"):
        for future in asyncio.as_completed(verification_tasks):
            try:
                source, is_valid = await future
                # processed_count += 1
                if is_valid:
                    verified_sources.append(source)
                    if len(verified_sources) >= limit: # Stop once limit is reached
                         logger.info(f"Reached verification limit ({limit}).")
                         # Cancel remaining tasks (optional, helps release resources faster)
                         # for task in verification_tasks:
                         #    if not task.done(): task.cancel()
                         break
                else:
                    # Verification returned False (already logged in _verify_source_url)
                    logger.debug(f"Discarding source '{source.name}' due to failed URL verification.")
            except Exception as e:
                 # Should ideally not happen if gather used return_exceptions=True, but good practice
                 logger.error(f"Verification task failed unexpectedly: {e}")


        logger.info(f"Discovery complete for '{location}'. Verified {len(verified_sources)} sources (limit: {limit}).")
        # Close session only if created internally
        await self.close_internal_session()
        return verified_sources # Return the limited, verified list