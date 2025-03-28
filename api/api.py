import logging
from fastapi import FastAPI, HTTPException, Query, Depends
from typing import List, Dict, Any, Optional
import asyncio
from models.data_models import NewsArticle
from agents.storage_agent import NewsStorageAgent
from models.vector_store import ArticleVectorStore
import config

from utils.logging_config import get_logger

logger = get_logger(__name__, {"component": "api"})

# Create FastAPI app instance
app = FastAPI(
    title="News Archive API",
    description="API for accessing the news article database",
    version="1.0.0"
)

# Storage agent instance
storage_agent = None
vector_store = None

async def get_storage_agent() -> NewsStorageAgent:
    """Dependency to get or create the storage agent."""
    global storage_agent
    if storage_agent is None:
        storage_agent = NewsStorageAgent(db_path=config.DATABASE_PATH)
        # Allow time for initialization
        await asyncio.sleep(0.1)
    return storage_agent

async def get_vector_store() -> ArticleVectorStore:
    """Dependency to get or create the vector store."""
    global vector_store
    if vector_store is None:
        vector_store = ArticleVectorStore()
        # Allow time for initialization
        await asyncio.sleep(0.1)
    return vector_store

@app.get("/api/articles/location/{location}", response_model=List[Dict[str, Any]])
async def get_articles_by_location(
    location: str, 
    limit: int = Query(20, ge=1, le=100),
    storage: NewsStorageAgent = Depends(get_storage_agent)
):
    """Get articles for a specific location."""
    try:
        articles = await storage.get_articles_by_location(location, limit)
        return [article.to_dict() for article in articles]
    except Exception as e:
        logger.error(f"Error retrieving articles for location '{location}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/api/articles/search", response_model=List[Dict[str, Any]])
async def search_articles(
    keyword: str = Query(..., min_length=2),
    limit: int = Query(20, ge=1, le=100),
    storage: NewsStorageAgent = Depends(get_storage_agent)
):
    """Search for articles by keyword."""
    try:
        articles = await storage.search_articles_by_keyword(keyword, limit)
        return [article.to_dict() for article in articles]
    except Exception as e:
        logger.error(f"Error searching articles with keyword '{keyword}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/api/search/semantic", response_model=List[Dict[str, Any]])
async def semantic_search(
    query: str = Query(..., min_length=3),
    limit: int = Query(10, ge=1, le=50),
    vector_store: ArticleVectorStore = Depends(get_vector_store),
    storage: NewsStorageAgent = Depends(get_storage_agent)
):
    """Search for articles semantically similar to the query."""
    try:
        # Search using vector store
        similar_articles = await vector_store.search_by_text(query, limit)
        
        if not similar_articles:
            return []
        
        # Fetch full article data for results
        urls = [article['url'] for article in similar_articles]
        articles = await storage.get_articles_by_urls(urls)
        
        # Create result with similarity scores
        url_to_score = {article['url']: article['similarity_score'] for article in similar_articles}
        result = []
        for article in articles:
            article_dict = article.to_dict()
            article_dict['similarity_score'] = url_to_score.get(article.url, 0)
            result.append(article_dict)
            
        # Sort by similarity score
        result.sort(key=lambda x: x['similarity_score'], reverse=True)
        return result
        
    except Exception as e:
        logger.error(f"Error during semantic search for '{query}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Semantic search error: {str(e)}")

@app.get("/api/articles/similar/{article_url:path}", response_model=List[Dict[str, Any]])
async def get_similar_articles(
    article_url: str,
    limit: int = Query(5, ge=1, le=20),
    vector_store: ArticleVectorStore = Depends(get_vector_store),
    storage: NewsStorageAgent = Depends(get_storage_agent)
):
    """Get articles similar to a specific article."""
    try:
        # Get the source article
        article = await storage.get_article_by_url(article_url)
        if not article:
            raise HTTPException(status_code=404, detail="Article not found")
            
        # Get similar articles
        similar_articles = await vector_store.get_similar_articles(article, limit)
        
        if not similar_articles:
            return []
            
        # Fetch full article data for results
        urls = [a['url'] for a in similar_articles]
        articles = await storage.get_articles_by_urls(urls)
        
        # Create result with similarity scores
        url_to_score = {a['url']: a['similarity_score'] for a in similar_articles}
        result = []
        for article in articles:
            article_dict = article.to_dict()
            article_dict['similarity_score'] = url_to_score.get(article.url, 0)
            result.append(article_dict)
            
        # Sort by similarity score
        result.sort(key=lambda x: x['similarity_score'], reverse=True)
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error finding similar articles for '{article_url}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error finding similar articles: {str(e)}")

@app.get("/api/categories", response_model=Dict[str, int])
async def get_article_categories(
    storage: NewsStorageAgent = Depends(get_storage_agent)
):
    """Get count of articles by category."""
    try:
        return await storage.get_category_counts()
    except Exception as e:
        logger.error(f"Error retrieving article categories: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/api/articles/recent", response_model=List[Dict[str, Any]])
async def get_recent_articles(
    limit: int = Query(20, ge=1, le=100),
    storage: NewsStorageAgent = Depends(get_storage_agent)
):
    """Get most recently added articles."""
    try:
        articles = await storage.get_recent_articles(limit)
        return [article.to_dict() for article in articles]
    except Exception as e:
        logger.error(f"Error retrieving recent articles: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/api/locations", response_model=List[str])
async def get_available_locations(
    storage: NewsStorageAgent = Depends(get_storage_agent)
):
    """Get list of locations with articles."""
    try:
        return await storage.get_available_locations()
    except Exception as e:
        logger.error(f"Error retrieving available locations: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/api/stats", response_model=Dict[str, Any])
async def get_database_stats(storage: NewsStorageAgent = Depends(get_storage_agent)):
    """Get statistics about the article database."""
    try:
        return await storage.get_article_stats()
    except Exception as e:
        logger.error(f"Error retrieving database statistics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.delete("/api/maintenance/cleanup")
async def cleanup_old_articles(
    days_to_keep: int = Query(30, ge=7, le=365),
    storage: NewsStorageAgent = Depends(get_storage_agent)
):
    """Admin endpoint to remove old articles."""
    try:
        deleted_count = await storage.cleanup_old_articles(days_to_keep)
        return {"status": "success", "deleted_count": deleted_count}
    except Exception as e:
        logger.error(f"Error during cleanup of old articles: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Cleanup error: {str(e)}")

@app.post("/api/vector/index")
async def index_articles(
    limit: int = Query(1000, ge=100, le=10000),
    storage: NewsStorageAgent = Depends(get_storage_agent),
    vector_store: ArticleVectorStore = Depends(get_vector_store)
):
    """Index articles in the vector store."""
    try:
        # Get articles to index
        articles = await storage.get_recent_articles(limit)
        
        if not articles:
            return {"status": "success", "indexed_count": 0, "message": "No articles to index"}
            
        # Add to vector store
        await vector_store.add_articles(articles)
        
        return {
            "status": "success",
            "indexed_count": len(articles),
            "message": f"Successfully indexed {len(articles)} articles"
        }
    except Exception as e:
        logger.error(f"Error indexing articles: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Indexing error: {str(e)}")

# Add health check endpoint
@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "ok", "version": "1.0.0"}