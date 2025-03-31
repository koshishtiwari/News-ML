import logging
import asyncio
from typing import Dict, Any, List, Optional, Union
from datetime import datetime

from models.data_models import NewsArticle, ImportanceLevel
from llm_providers.base import LLMProvider
from agents.base_agent import BaseAgent, Task, TaskPriority

logger = logging.getLogger(__name__)

class ContentAnalysisAgent(BaseAgent):
    """Micro-agent responsible for analyzing article content using LLM"""
    
    def __init__(self, llm_provider: LLMProvider):
        super().__init__("content_analysis")
        self.llm_provider = llm_provider
        self.analysis_semaphore = asyncio.Semaphore(3)  # Limit concurrent LLM calls
        self._categories = set()  # Track categories for consistency
        
    async def _initialize_resources(self):
        """Initialize any resources needed"""
        # Nothing to initialize for this agent
        pass
        
    async def _cleanup_resources(self):
        """Cleanup any resources"""
        # Nothing to clean up for this agent
        pass
        
    async def _process_task_internal(self, task: Task) -> Any:
        """Process content analysis tasks"""
        task_type = task.task_type
        
        if task_type == "analyze_article":
            return await self._analyze_article(task)
        elif task_type == "analyze_batch":
            return await self._analyze_batch(task)
        elif task_type == "summarize_location":
            return await self._summarize_location(task)
        else:
            raise ValueError(f"Unknown task type: {task_type}")
    
    async def _analyze_article(self, task: Task) -> Dict[str, Any]:
        """Analyze an article's content to generate summary, importance, and category"""
        article_data = task.data.get("article") 
        if not article_data:
            raise ValueError("Article data is required for analysis")
            
        # Get the article content
        url = article_data.get("url")
        title = article_data.get("title") 
        content = article_data.get("content")
        
        if not url:
            raise ValueError("URL is required for article analysis")
            
        if not content:
            logger.warning(f"Article has no content to analyze: {url}")
            return {
                **article_data,
                "summary": "Content unavailable for analysis.",
                "importance": ImportanceLevel.MEDIUM.value,
                "category": "Unknown"
            }
            
        # Truncate content for LLM if it's too long
        content_for_analysis = content[:4000]  # Limit to 4000 chars for LLM
        
        await self.update_status("Analyzing", {"url": url, "title": title[:30] if title else "Unknown"})
        
        try:
            # Use semaphore to limit concurrent LLM calls
            async with self.analysis_semaphore:
                # Generate analysis using LLM
                analysis = await self._generate_content_analysis(title, content_for_analysis)
                
                # Merge analysis results with original article data
                return {
                    **article_data,
                    "summary": analysis.get("summary"),
                    "importance": analysis.get("importance", ImportanceLevel.MEDIUM.value),
                    "category": analysis.get("category", "General")
                }
                
        except Exception as e:
            logger.error(f"Error analyzing article {url}: {e}", exc_info=True)
            return {
                **article_data,
                "summary": f"Analysis error: {str(e)}",
                "importance": ImportanceLevel.MEDIUM.value,
                "category": "Error"
            }
    
    async def _analyze_batch(self, task: Task) -> List[Dict[str, Any]]:
        """Analyze a batch of articles"""
        articles_data = task.data.get("articles", [])
        if not articles_data:
            return []
            
        await self.update_status("Analyzing batch", {"count": len(articles_data)})
        
        # Process articles with prioritization
        prioritized_articles = self._prioritize_articles_for_analysis(articles_data)
        
        # Process up to 5 articles at a time
        batch_size = 5
        results = []
        
        for i in range(0, len(prioritized_articles), batch_size):
            batch = prioritized_articles[i:i+batch_size]
            batch_tasks = []
            
            for article_data in batch:
                subtask = Task(
                    task_id=f"analyze_{len(results)}",
                    task_type="analyze_article",
                    data={"article": article_data}
                )
                batch_tasks.append(self._analyze_article(subtask))
            
            # Process batch concurrently
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            # Filter out exceptions
            for result in batch_results:
                if isinstance(result, Exception):
                    logger.error(f"Error in batch analysis: {result}")
                else:
                    results.append(result)
            
            # Update status
            progress = min(len(results) + len(batch), len(articles_data))
            await self.update_status(f"Analyzed {progress}/{len(articles_data)}")
                    
        return results
    
    async def _summarize_location(self, task: Task) -> str:
        """Generate a summary for a location based on analyzed articles"""
        location = task.data.get("location")
        articles = task.data.get("articles", [])
        
        if not location:
            raise ValueError("Location is required for location summary")
            
        if not articles:
            return f"No articles available to summarize for {location}."
            
        await self.update_status("Summarizing location", {"location": location, "article_count": len(articles)})
        
        # Sort the articles by importance and recency
        importance_order = {"Critical": 0, "High": 1, "Medium": 2, "Low": 3}
        sorted_articles = sorted(
            articles,
            key=lambda a: (
                importance_order.get(a.get("importance", "Medium"), 4),
                # Sort by published_at if available, otherwise by order in list
                -1 * (a.get("published_at_timestamp", 0) or -1 * articles.index(a))
            )
        )
        
        # Take the top 5 most important articles for the summary context
        top_articles = []
        for article in sorted_articles[:5]:
            top_articles.append({
                "title": article.get("title", "Untitled"),
                "summary": article.get("summary", "No summary available"),
                "importance": article.get("importance", "Medium"),
                "category": article.get("category", "General")
            })
            
        # Get category distribution
        categories = {}
        for article in articles:
            category = article.get("category", "General")
            if category in categories:
                categories[category] += 1
            else:
                categories[category] = 1
                
        # Sort categories by frequency
        sorted_categories = sorted(categories.items(), key=lambda x: x[1], reverse=True)
        category_text = ", ".join([f"{cat} ({count})" for cat, count in sorted_categories[:5]])
        
        # Generate the location summary with LLM
        prompt = f"""
        Based on the following news articles about {location}, generate a concise summary (3-5 sentences) of the current situation.
        
        Top articles:
        {self._format_articles_for_prompt(top_articles)}
        
        Category distribution: {category_text}
        
        Summary:
        """
        
        try:
            summary = await self.llm_provider.generate(prompt)
            return summary.strip()
        except Exception as e:
            logger.error(f"Error generating location summary for {location}: {e}", exc_info=True)
            return f"Unable to generate a summary for {location} due to an error."
    
    async def _generate_content_analysis(self, title: str, content: str) -> Dict[str, Any]:
        """Use LLM to analyze content and extract summary, importance, and category"""
        prompt = f"""
        Analyze the following news article and provide:
        1. A concise summary (1-2 sentences)
        2. The importance level (Critical, High, Medium, or Low)
        3. A single category (e.g., Politics, Economy, Technology, Health, etc.)
        
        Title: {title}
        
        Content:
        {content}
        
        Format your response as valid JSON with the following keys: summary, importance, category.
        Example: {{"summary": "...", "importance": "High", "category": "Politics"}}
        """
        
        try:
            response = await self.llm_provider.generate(prompt)
            
            # Extract JSON from response
            import json
            import re
            
            # Find JSON pattern in the response
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
                analysis = json.loads(json_str)
                
                # Validate importance level
                valid_importance_levels = [level.value for level in ImportanceLevel]
                if analysis.get("importance") not in valid_importance_levels:
                    analysis["importance"] = "Medium"  # Default to Medium
                    
                # Add category to tracked categories
                if "category" in analysis:
                    self._categories.add(analysis["category"])
                    
                return analysis
            else:
                # Fallback to regex extraction
                summary = re.search(r'summary["\s:]+([^"}.]+)', response, re.IGNORECASE)
                importance = re.search(r'importance["\s:]+([^"}.]+)', response, re.IGNORECASE)
                category = re.search(r'category["\s:]+([^"}.]+)', response, re.IGNORECASE)
                
                return {
                    "summary": summary.group(1).strip() if summary else "Summary unavailable.",
                    "importance": importance.group(1).strip() if importance else "Medium",
                    "category": category.group(1).strip() if category else "General"
                }
                
        except Exception as e:
            logger.error(f"Error generating content analysis: {e}", exc_info=True)
            return {
                "summary": "Analysis failed due to an error.",
                "importance": "Medium",
                "category": "Error"
            }
    
    def _prioritize_articles_for_analysis(self, articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Prioritize articles for analysis based on various factors"""
        # Simple prioritization for now - newer articles first
        try:
            # Try to parse published_at dates and add them as timestamps for sorting
            for article in articles:
                if article.get("published_at"):
                    try:
                        dt = datetime.fromisoformat(article["published_at"].replace('Z', '+00:00'))
                        article["published_at_timestamp"] = dt.timestamp()
                    except (ValueError, TypeError):
                        article["published_at_timestamp"] = 0
                else:
                    article["published_at_timestamp"] = 0
                    
            # Sort by publish date (newest first)
            return sorted(articles, key=lambda x: -1 * x.get("published_at_timestamp", 0))
        except Exception as e:
            logger.warning(f"Error in article prioritization: {e}")
            return articles  # Return original order if prioritization fails
    
    @staticmethod
    def _format_articles_for_prompt(articles: List[Dict[str, Any]]) -> str:
        """Format article data for inclusion in a prompt"""
        formatted = []
        for i, article in enumerate(articles, 1):
            formatted.append(f"{i}. {article['title']} ({article['importance']} importance, {article['category']})")
            formatted.append(f"   Summary: {article['summary']}")
            formatted.append("")
            
        return "\n".join(formatted)