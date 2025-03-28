# This is a placeholder file
import logging
import json
from typing import List, Dict
from datetime import datetime
from datetime import timezone
import pytz # Keep for timezone handling

# Assuming models are in ../models/
from models.data_models import NewsArticle
# Assuming LLM provider base/interface is in ../llm_providers/
from llm_providers.base import LLMProvider
# Import config for timezone
import config

logger = logging.getLogger(__name__)

class NewsOrganizationAgent:
    """Agent responsible for organizing news and generating presentation text."""

    def __init__(self, llm_provider: LLMProvider):
         self.llm_provider = llm_provider
         logger.debug("NewsOrganizationAgent initialized.")

    async def _generate_location_summary_llm(self, location: str, articles: List[NewsArticle]) -> str:
        """Generates a brief summary of the location's news using the LLM."""
        logger.info(f"Generating LLM summary for location '{location}'...")
        if not articles: return "No articles available to generate a summary."

        # Sort articles by importance/recency to get top ones
        importance_order = {"Critical": 0, "High": 1, "Medium": 2, "Low": 3}
        articles.sort(key=lambda a: (importance_order.get(a.importance, 4), -a.fetched_at.timestamp()))

        top_articles_info = []
        for article in articles[:5]: # Consider top 5 for summary context
            if article.importance in ["Critical", "High"]:
                 top_articles_info.append({"title": article.title, "summary": article.summary})

        prompt = f"""
        Based on recently fetched news articles for {location}:
        - Total articles analyzed: {len(articles)}
        - Top Headlines (up to 5, Critical/High importance):
        {json.dumps(top_articles_info, indent=2)}

        Generate a very brief (2-3 sentences) narrative summary of the most significant news themes currently affecting {location}. Be neutral and objective.
        """
        system_prompt = f"You are a news summarization agent. Synthesize the provided news data into a concise overview for {location}."

        summary = await self.llm_provider.generate(prompt, system_prompt)

        if summary:
             logger.info("LLM Location summary generated successfully.")
             return summary.strip()
        else:
             logger.error("LLM failed to generate location summary.")
             return "Automated summary generation failed."

    def _format_article_presentation(self, article: NewsArticle) -> str:
        """Formats a single article for the text presentation."""
        try:
            pub_time_str = article.published_at.strftime('%Y-%m-%d %H:%M') if article.published_at else "No date"
            fetch_time_str = article.fetched_at.strftime('%Y-%m-%d %H:%M UTC')
        except AttributeError: # Handle case where datetime might somehow be invalid
            pub_time_str = "Invalid date"
            fetch_time_str = "Invalid date"

        lines = [
            f"### {article.title or 'No Title'}",
            f"* Source: {article.source_name or 'N/A'} | Importance: {article.importance or 'N/A'} | Category: {article.category or 'N/A'}",
            f"* Published: {pub_time_str} | Fetched: {fetch_time_str}",
            f"* Summary: {article.summary or '(No summary generated)'}",
            f"* URL: {article.url}"
        ]
        return "\n".join(lines)

    async def generate_presentation(self, location: str, articles: List[NewsArticle]) -> str:
        """Generates the final news presentation string."""
        logger.info(f"Generating presentation for {len(articles)} articles for '{location}'...")
        if not articles:
            logger.warning(f"No articles provided to generate_presentation for {location}.")
            return f"No news articles were processed or available for {location}."

        # Sort articles by importance and then by fetch time (most recent first)
        importance_order = {"Critical": 0, "High": 1, "Medium": 2, "Low": 3}
        articles.sort(key=lambda a: (importance_order.get(a.importance, 4), -(a.fetched_at.timestamp() if a.fetched_at else 0)))

        # Get current time in configured timezone
        try:
            local_tz = pytz.timezone(config.DEFAULT_TIMEZONE)
            now = datetime.now(local_tz)
            report_time_str = now.strftime("%Y-%m-%d %H:%M:%S %Z")
        except pytz.UnknownTimeZoneError:
            logger.warning(f"Timezone '{config.DEFAULT_TIMEZONE}' not found. Using UTC.")
            now = datetime.now(timezone.utc)
            report_time_str = now.strftime("%Y-%m-%d %H:%M:%S UTC")

        # Generate location summary using LLM
        location_summary = await self._generate_location_summary_llm(location, articles)

        # Build presentation string
        presentation_lines = [
            f"--- News Report for {location} ---",
            f"Generated on: {report_time_str}",
            f"Total Articles Presented: {len(articles)}",
            "------------------------------------",
            "",
            f"## Location Summary",
            location_summary,
            "",
            "------------------------------------",
            "",
            "## Articles (Sorted by Importance & Recency)",
            ""
        ]

        if not articles:
             presentation_lines.append("(No articles found or processed successfully)")
        else:
            for article in articles:
                presentation_lines.append(self._format_article_presentation(article))
                presentation_lines.append("") # Add space between articles

        presentation_lines.append("--- End of Report ---")
        logger.info("Presentation generation complete.")
        return "\n".join(presentation_lines)