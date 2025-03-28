# This is a placeholder file
import logging
import json
import re
import asyncio
from typing import List

# Assuming models are in ../models/
from models.data_models import NewsArticle
# Assuming LLM provider base/interface is in ../llm_providers/
from llm_providers.base import LLMProvider
# Import metrics collector for real-time updates
from monitor.metrics import metrics_collector

from tqdm.asyncio import tqdm as async_tqdm

logger = logging.getLogger(__name__)

class NewsAnalysisAgent:
    """Agent responsible for analyzing pre-extracted article content using an LLM."""

    def __init__(self, llm_provider: LLMProvider):
        self.llm_provider = llm_provider
        logger.debug("NewsAnalysisAgent initialized.")

    async def analyze_article(self, article: NewsArticle) -> NewsArticle:
        """Analyze content to add summary, importance, and category using LLM."""
        logger.debug(f"Analyzing article content: '{article.title[:60]}...' ({article.url})")
        
        # Update agent status to show current article being processed
        await metrics_collector.update_agent_status(
            "analysis",
            "Analyzing",
            {"title": article.title[:40], "url": article.url}
        )

        # Content validation happened during crawling, assume article.content is good here
        if not article.content:
            logger.warning(f"Article '{article.title}' reached analysis with no content. Skipping LLM analysis.")
            article.summary = "Content unavailable."
            article.importance = "Medium"
            article.category = "Unknown"
            # Send real-time update even for skipped articles
            await metrics_collector.update_article(article)
            return article

        # Prepare snippet for LLM
        content_snippet = article.content[:3500].strip() # Slightly larger snippet if desired
        if len(article.content) > 3500: content_snippet += "..."

        # ... (Keep prompt and system_prompt as defined before) ...
        prompt = f"""
        Analyze the following news article content:

        Title: {article.title}
        Source: {article.source_name}
        Content Snippet (first ~3500 chars):
        ---
        {content_snippet}
        ---

        Perform the following analysis:
        1. Summary: Provide a brief, neutral, 1-2 sentence summary of the main point.
        2. Importance: Assign an importance rating based on likely impact/urgency for a general audience interested in the source's region. Choose ONE: Critical, High, Medium, Low.
        3. Category: Identify the primary news category. Choose ONE from: Politics, Business, Technology, Sports, Entertainment, Health, Science, World News, Local News, Crime, Other.

        Format your response ONLY as a valid JSON object with EXACTLY these keys: "summary", "importance", "category".
        Example: {{"summary": "The city council debated the new zoning proposal.", "importance": "Medium", "category": "Local News"}}
        Do not include any text before or after the JSON object. Ensure the importance and category values are only from the provided options.
        """
        system_prompt = "You are a news analyst. Extract summary, importance, and category from the provided text. Respond ONLY with the specified JSON object."


        llm_response = await self.llm_provider.generate(prompt, system_prompt)
        if not llm_response:
             logger.error(f"LLM generated no response for analysis of {article.url}. Using defaults.")
             article.summary = "Summary generation failed (LLM error)."
             article.importance = "Medium"
             article.category = "Unknown"
             # Send real-time update even for failed articles
             await metrics_collector.update_article(article)
             return article

        try:
            # Robust JSON extraction
            json_match = re.search(r'\{\s*".*?":.*?\s*\}', llm_response, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
                analysis = json.loads(json_str)
            else:
                 logger.warning(f"Could not find JSON object in analysis response for {article.url}. Trying full parse.")
                 analysis = json.loads(llm_response.strip()) # Fallback

            # Validate and assign, providing defaults
            valid_importance = ["Critical", "High", "Medium", "Low"]
            valid_categories = ["Politics", "Business", "Technology", "Sports", "Entertainment", "Health", "Science", "World News", "Local News", "Crime", "Other"]

            article.summary = str(analysis.get('summary', 'Summary generation failed (parsing).')).strip()
            # Use assignment expression for cleaner check
            article.importance = str(imp) if (imp := analysis.get('importance')) and imp in valid_importance else "Medium"
            article.category = str(cat) if (cat := analysis.get('category')) and cat in valid_categories else "Other"

            logger.debug(f"  -> Analysis complete for {article.url}: Importance={article.importance}, Category={article.category}")
            
            # Send real-time update for the analyzed article
            await metrics_collector.update_article(article)
            
            return article

        except json.JSONDecodeError as e:
            logger.error(f"Error parsing analysis JSON for {article.url}: {e}. Raw response:\n{llm_response[:500]}...")
            # Assign defaults on error
            article.summary = "Summary generation failed (JSON error)."
            article.importance = "Medium"
            article.category = "Unknown"
            # Send real-time update even for failed articles
            await metrics_collector.update_article(article)
            return article
        except Exception as e:
            logger.error(f"Unexpected error during LLM analysis for {article.url}: {e}", exc_info=True)
            # Assign defaults on error
            article.summary = "Summary generation failed (unexpected error)."
            article.importance = "Medium"
            article.category = "Unknown"
            # Send real-time update even for failed articles
            await metrics_collector.update_article(article)
            return article

    async def analyze_articles(self, articles: List[NewsArticle]) -> List[NewsArticle]:
        """Analyze multiple articles using the LLM concurrently."""
        if not articles: return []
        logger.info(f"Analyzing {len(articles)} articles using LLM...")

        tasks = [self.analyze_article(article) for article in articles]
        analyzed_results = []

        # Process concurrently with progress bar
        for future in async_tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Analyzing Articles"):
             result = await future
             # We always append the article, even if analysis failed, as defaults are set
             analyzed_results.append(result)

        # Reset agent status when all done
        await metrics_collector.update_agent_status("analysis", "Idle")
        
        logger.info(f"Finished LLM analysis for {len(analyzed_results)} articles.")
        return analyzed_results