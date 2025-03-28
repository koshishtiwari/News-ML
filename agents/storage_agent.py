import logging
import asyncio
import aiosqlite
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone

# Assuming models are in ../models/
from models.data_models import NewsArticle

logger = logging.getLogger(__name__)

class NewsStorageAgent:
    """Handles persistence of news data in an SQLite database with async support."""

    def __init__(self, db_path: str):
        self.db_path = Path(db_path).resolve()
        self._ensure_db_directory()
        # Initialize the database synchronously on startup
        asyncio.create_task(self._initialize_db_async())
        self._db: Optional[aiosqlite.Connection] = None
        logger.info(f"Storage agent initialized with database at: {self.db_path}")

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
                # Using aiosqlite for async database operations
                self._db = await aiosqlite.connect(
                    self.db_path,
                    isolation_level=None  # Enable autocommit mode
                )
                # Enable foreign keys and configure connection
                await self._db.execute("PRAGMA foreign_keys = ON")
                # Enable row factory to access columns by name
                self._db.row_factory = aiosqlite.Row
            except Exception as e:
                logger.error(f"Failed to connect to database {self.db_path}: {e}", exc_info=True)
                raise
        return self._db

    async def _initialize_db_async(self):
        """Creates the articles table and indexes if they don't already exist."""
        schema = """
            CREATE TABLE IF NOT EXISTS articles (
                url TEXT PRIMARY KEY,
                title TEXT,
                source_name TEXT,
                content TEXT,
                summary TEXT,
                published_at TIMESTAMP, -- Store as TEXT ISO8601 recommended for SQLite
                fetched_at TIMESTAMP NOT NULL, -- Store as TEXT ISO8601 recommended
                location_query TEXT,
                category TEXT,
                importance TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_articles_fetched_at ON articles (fetched_at);
            CREATE INDEX IF NOT EXISTS idx_articles_location ON articles (location_query);
            CREATE INDEX IF NOT EXISTS idx_articles_published_at ON articles (published_at);
            
            -- Add new table for tracking article metrics
            CREATE TABLE IF NOT EXISTS article_metrics (
                url TEXT PRIMARY KEY,
                view_count INTEGER DEFAULT 0,
                ratings_sum INTEGER DEFAULT 0,
                ratings_count INTEGER DEFAULT 0,
                FOREIGN KEY (url) REFERENCES articles(url) ON DELETE CASCADE
            );
        """
        try:
            db = await self._get_db()
            await db.executescript(schema)
            logger.debug("Database schema initialized or verified.")
        except Exception as e:
            logger.error(f"Database schema initialization failed: {e}", exc_info=True)
            raise

    async def close(self):
        """Close the database connection when done."""
        if self._db is not None and not self._db.closed:
            await self._db.close()
            self._db = None
            logger.debug("Database connection closed.")

    async def save_or_update_articles(self, articles: List[NewsArticle]) -> int:
        """
        Saves or updates a batch of articles in the database using UPSERT.
        Returns the number of articles successfully saved.
        """
        if not articles:
            logger.debug("No articles provided to save_or_update_articles.")
            return 0

        sql = """
            INSERT INTO articles (
                url, title, source_name, content, summary, published_at,
                fetched_at, location_query, category, importance
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET
                title=excluded.title,
                source_name=excluded.source_name,
                content=excluded.content,
                summary=excluded.summary,
                published_at=excluded.published_at,
                fetched_at=excluded.fetched_at,
                location_query=excluded.location_query,
                category=excluded.category,
                importance=excluded.importance
            WHERE excluded.fetched_at > articles.fetched_at; -- Only update if newer data
        """
        # Convert articles to tuples for executemany, handling datetimes
        data_to_insert = []
        for article in articles:
            published_at_iso = article.published_at.isoformat() if article.published_at else None
            fetched_at_iso = article.fetched_at.isoformat()  # Should always exist

            data_to_insert.append((
                article.url, article.title, article.source_name, article.content,
                article.summary, published_at_iso, fetched_at_iso,
                article.location_query, article.category, article.importance
            ))

        try:
            db = await self._get_db()
            async with db.execute('BEGIN TRANSACTION'):
                # Use executemany for batch operations
                await db.executemany(sql, data_to_insert)
                
                # Also insert into metrics table for new articles
                metrics_sql = """
                    INSERT OR IGNORE INTO article_metrics (url)
                    VALUES (?)
                """
                await db.executemany(metrics_sql, [(article.url,) for article in articles])
                
            logger.info(f"Successfully processed (saved/updated) {len(data_to_insert)} articles in database.")
            return len(data_to_insert)
        except Exception as e:
            logger.error(f"Database operation failed during save_or_update_articles: {e}", exc_info=True)
            return 0

    async def get_articles_by_location(self, location: str, limit: int = 50) -> List[NewsArticle]:
        """Retrieves recent articles matching a location query from the database."""
        sql = """
            SELECT * FROM articles
            WHERE location_query = ?
            ORDER BY fetched_at DESC
            LIMIT ?
        """
        articles = []
        try:
            db = await self._get_db()
            async with db.execute(sql, (location, limit)) as cursor:
                rows = await cursor.fetchall()

                for row in rows:
                    try:
                        # Convert ISO strings back to timezone-aware datetimes (assuming UTC)
                        published_at = datetime.fromisoformat(p_at).replace(tzinfo=timezone.utc) if (p_at := row['published_at']) else None
                        fetched_at = datetime.fromisoformat(f_at).replace(tzinfo=timezone.utc) if (f_at := row['fetched_at']) else None # Should exist

                        articles.append(NewsArticle(
                            url=row['url'],
                            title=row['title'],
                            source_name=row['source_name'],
                            content=row['content'],
                            summary=row['summary'],
                            published_at=published_at,
                            fetched_at=fetched_at,
                            location_query=row['location_query'],
                            category=row['category'],
                            importance=row['importance']
                        ))
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Skipping article row due to data conversion error (URL: {row.get('url', 'N/A')}): {e}")

            logger.info(f"Retrieved {len(articles)} articles for location '{location}' from DB.")
            return articles
        except Exception as e:
            logger.error(f"Failed to retrieve articles for location '{location}': {e}", exc_info=True)
            return []
            
    async def search_articles_by_keyword(self, keyword: str, limit: int = 50) -> List[NewsArticle]:
        """Search for articles containing a specific keyword in title or content."""
        search_term = f"%{keyword}%"
        sql = """
            SELECT * FROM articles 
            WHERE title LIKE ? OR content LIKE ?
            ORDER BY fetched_at DESC
            LIMIT ?
        """
        
        articles = []
        try:
            db = await self._get_db()
            async with db.execute(sql, (search_term, search_term, limit)) as cursor:
                rows = await cursor.fetchall()
                
                for row in rows:
                    try:
                        # Convert ISO strings back to timezone-aware datetimes
                        published_at = datetime.fromisoformat(p_at).replace(tzinfo=timezone.utc) if (p_at := row['published_at']) else None
                        fetched_at = datetime.fromisoformat(f_at).replace(tzinfo=timezone.utc) if (f_at := row['fetched_at']) else None
                        
                        articles.append(NewsArticle(
                            url=row['url'],
                            title=row['title'],
                            source_name=row['source_name'],
                            content=row['content'],
                            summary=row['summary'],
                            published_at=published_at,
                            fetched_at=fetched_at,
                            location_query=row['location_query'],
                            category=row['category'],
                            importance=row['importance']
                        ))
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Skipping article row due to data conversion error (URL: {row.get('url', 'N/A')}): {e}")
                        
            logger.info(f"Found {len(articles)} articles containing keyword '{keyword}'")
            return articles
        except Exception as e:
            logger.error(f"Failed to search articles by keyword '{keyword}': {e}", exc_info=True)
            return []
    
    async def get_article_stats(self) -> Dict[str, Any]:
        """Get statistics about the articles database."""
        try:
            db = await self._get_db()
            stats = {}
            
            # Total article count
            async with db.execute("SELECT COUNT(*) FROM articles") as cursor:
                stats["total_articles"] = (await cursor.fetchone())[0]
                
            # Articles per category
            categories = {}
            async with db.execute("SELECT category, COUNT(*) FROM articles GROUP BY category") as cursor:
                async for row in cursor:
                    categories[row[0] or "Uncategorized"] = row[1]
            stats["articles_by_category"] = categories
            
            # Articles per importance level
            importance = {}
            async with db.execute("SELECT importance, COUNT(*) FROM articles GROUP BY importance") as cursor:
                async for row in cursor:
                    importance[row[0] or "Unrated"] = row[1]
            stats["articles_by_importance"] = importance
            
            # Most frequent locations
            locations = []
            async with db.execute(
                "SELECT location_query, COUNT(*) as count FROM articles GROUP BY location_query ORDER BY count DESC LIMIT 10"
            ) as cursor:
                async for row in cursor:
                    if row[0]:  # Avoid None values
                        locations.append({"location": row[0], "count": row[1]})
            stats["top_locations"] = locations
            
            return stats
        except Exception as e:
            logger.error(f"Failed to gather article statistics: {e}", exc_info=True)
            return {"error": str(e)}

    async def cleanup_old_articles(self, days_to_keep: int = 30) -> int:
        """Remove articles older than the specified number of days."""
        try:
            from datetime import timedelta
            
            cutoff_date = (datetime.now(timezone.utc) - timedelta(days=days_to_keep)).isoformat()
            
            db = await self._get_db()
            async with db.execute("BEGIN TRANSACTION"):
                async with db.execute(
                    "DELETE FROM articles WHERE fetched_at < ?", 
                    (cutoff_date,)
                ) as cursor:
                    deleted_count = cursor.rowcount
            
            logger.info(f"Removed {deleted_count} articles older than {days_to_keep} days")
            return deleted_count
        except Exception as e:
            logger.error(f"Failed to clean up old articles: {e}", exc_info=True)
            return 0