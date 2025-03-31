import logging
import asyncio
import aiosqlite
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
import warnings

# Assuming models are in ../models/
from models.data_models import NewsArticle

logger = logging.getLogger(__name__)

class NewsStorageAgent:
    """
    LEGACY: Handles persistence of news data in an SQLite database with async support.
    
    This class is maintained for backward compatibility.
    Please use the micro-agents and task manager for new development.
    """

    def __init__(self, db_path: str):
        self.db_path = Path(db_path).resolve()
        self._ensure_db_directory()
        # Initialize the database synchronously on startup
        asyncio.create_task(self._initialize_db_async())
        self._db: Optional[aiosqlite.Connection] = None
        logger.info(f"Storage agent initialized with database at: {self.db_path}")
        
        warnings.warn(
            "NewsStorageAgent is deprecated. Use the new micro-agent architecture instead.",
            DeprecationWarning,
            stacklevel=2
        )

    def _ensure_db_directory(self):
        """Creates the directory for the database file if it doesn't exist."""
        try:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logger.error(f"Failed to create database directory {self.db_path.parent}: {e}", exc_info=True)
            raise

    async def _get_db(self) -> aiosqlite.Connection:
        """Gets a connection to the SQLite database asynchronously."""
        if self._db is None or self._db.closed:
            try:
                self._db = await aiosqlite.connect(self.db_path)
                # Enable foreign keys and set pragmas for better performance
                await self._db.execute("PRAGMA foreign_keys = ON")
                await self._db.execute("PRAGMA journal_mode = WAL")
                await self._db.execute("PRAGMA synchronous = NORMAL")
            except Exception as e:
                logger.error(f"Database connection error: {e}", exc_info=True)
                raise
        return self._db

    async def _initialize_db_async(self):
        """Creates the articles table and indexes if they don't already exist."""
        db = await self._get_db()
        try:
            # Create articles table
            await db.execute('''
                CREATE TABLE IF NOT EXISTS articles (
                    url TEXT PRIMARY KEY,
                    title TEXT,
                    source_name TEXT,
                    content TEXT,
                    summary TEXT,
                    published_at TIMESTAMP,
                    fetched_at TIMESTAMP NOT NULL,
                    location_query TEXT,
                    category TEXT,
                    importance TEXT
                )
            ''')
            
            # Create indexes
            await db.execute('CREATE INDEX IF NOT EXISTS idx_articles_location ON articles(location_query)')
            await db.execute('CREATE INDEX IF NOT EXISTS idx_articles_source ON articles(source_name)')
            await db.execute('CREATE INDEX IF NOT EXISTS idx_articles_published ON articles(published_at)')
            await db.execute('CREATE INDEX IF NOT EXISTS idx_articles_fetched ON articles(fetched_at)')
            await db.execute('CREATE INDEX IF NOT EXISTS idx_articles_category ON articles(category)')
            await db.execute('CREATE INDEX IF NOT EXISTS idx_articles_importance ON articles(importance)')
            
            # Create sources table
            await db.execute('''
                CREATE TABLE IF NOT EXISTS sources (
                    url TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    location_type TEXT,
                    reliability_score REAL DEFAULT 0.5
                )
            ''')
            
            await db.commit()
            logger.info("Database schema initialized")
        except Exception as e:
            logger.error(f"Failed to initialize database schema: {e}", exc_info=True)
            raise

    async def store_article(self, article: NewsArticle) -> bool:
        """Store a single article in the database."""
        if not article.url:
            logger.warning("Attempted to store article with no URL")
            return False
            
        try:
            db = await self._get_db()
            
            # Convert datetime objects to ISO format strings
            published_at = article.published_at.isoformat() if article.published_at else None
            fetched_at = article.fetched_at.isoformat() if article.fetched_at else datetime.now(timezone.utc).isoformat()
            
            await db.execute('''
                INSERT OR REPLACE INTO articles 
                (url, title, source_name, content, summary, published_at, fetched_at, location_query, category, importance)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                article.url,
                article.title,
                article.source_name,
                article.content,
                article.summary,
                published_at,
                fetched_at,
                article.location_query,
                article.category,
                article.importance
            ))
            
            await db.commit()
            logger.debug(f"Stored article: {article.title} ({article.url})")
            return True
            
        except Exception as e:
            logger.error(f"Error storing article {article.url}: {e}", exc_info=True)
            return False

    async def store_articles(self, articles: List[NewsArticle]) -> int:
        """Store multiple articles in the database."""
        if not articles:
            return 0
            
        success_count = 0
        for article in articles:
            if await self.store_article(article):
                success_count += 1
                
        logger.info(f"Stored {success_count}/{len(articles)} articles to database")
        return success_count

    async def get_articles_for_location(self, location: str, limit: int = 50) -> List[NewsArticle]:
        """Retrieve articles for a specific location."""
        try:
            db = await self._get_db()
            
            # Get the most recent articles for this location
            async with db.execute('''
                SELECT url, title, source_name, content, summary, published_at, fetched_at, 
                       location_query, category, importance
                FROM articles
                WHERE location_query = ?
                ORDER BY 
                    CASE importance
                        WHEN 'Critical' THEN 1
                        WHEN 'High' THEN 2
                        WHEN 'Medium' THEN 3
                        WHEN 'Low' THEN 4
                        ELSE 5
                    END,
                    COALESCE(published_at, fetched_at) DESC
                LIMIT ?
            ''', (location, limit)) as cursor:
                
                articles = []
                async for row in cursor:
                    # Convert ISO format strings back to datetime objects
                    published_at = None
                    if row[5]:  # published_at
                        try:
                            published_at = datetime.fromisoformat(row[5])
                        except ValueError:
                            pass
                            
                    fetched_at = None
                    if row[6]:  # fetched_at
                        try:
                            fetched_at = datetime.fromisoformat(row[6])
                        except ValueError:
                            fetched_at = datetime.now(timezone.utc)
                    
                    # Create NewsArticle object
                    article = NewsArticle(
                        url=row[0],
                        title=row[1],
                        source_name=row[2],
                        content=row[3],
                        summary=row[4],
                        published_at=published_at,
                        fetched_at=fetched_at,
                        location_query=row[7],
                        category=row[8],
                        importance=row[9]
                    )
                    
                    articles.append(article)
                    
            logger.info(f"Retrieved {len(articles)} articles for location '{location}'")
            return articles
            
        except Exception as e:
            logger.error(f"Error retrieving articles for location '{location}': {e}", exc_info=True)
            return []

    async def get_recent_articles(self, limit: int = 30) -> List[NewsArticle]:
        """Retrieve the most recent articles across all locations."""
        try:
            db = await self._get_db()
            
            # Get the most recent articles
            async with db.execute('''
                SELECT url, title, source_name, content, summary, published_at, fetched_at, 
                       location_query, category, importance
                FROM articles
                ORDER BY 
                    CASE importance
                        WHEN 'Critical' THEN 1
                        WHEN 'High' THEN 2
                        WHEN 'Medium' THEN 3
                        WHEN 'Low' THEN 4
                        ELSE 5
                    END,
                    COALESCE(published_at, fetched_at) DESC
                LIMIT ?
            ''', (limit,)) as cursor:
                
                articles = []
                async for row in cursor:
                    # Convert ISO format strings back to datetime objects
                    published_at = None
                    if row[5]:  # published_at
                        try:
                            published_at = datetime.fromisoformat(row[5])
                        except ValueError:
                            pass
                            
                    fetched_at = None
                    if row[6]:  # fetched_at
                        try:
                            fetched_at = datetime.fromisoformat(row[6])
                        except ValueError:
                            fetched_at = datetime.now(timezone.utc)
                    
                    # Create NewsArticle object
                    article = NewsArticle(
                        url=row[0],
                        title=row[1],
                        source_name=row[2],
                        content=row[3],
                        summary=row[4],
                        published_at=published_at,
                        fetched_at=fetched_at,
                        location_query=row[7],
                        category=row[8],
                        importance=row[9]
                    )
                    
                    articles.append(article)
                    
            logger.info(f"Retrieved {len(articles)} recent articles")
            return articles
            
        except Exception as e:
            logger.error(f"Error retrieving recent articles: {e}", exc_info=True)
            return []

    async def close(self):
        """Close the database connection."""
        if self._db:
            try:
                await self._db.close()
                self._db = None
                logger.debug("Database connection closed")
            except Exception as e:
                logger.error(f"Error closing database: {e}", exc_info=True)