# This is a placeholder file
import logging
import sqlite3
from pathlib import Path
from typing import List, Optional
from datetime import datetime, timezone

# Assuming models are in ../models/
from models.data_models import NewsArticle

logger = logging.getLogger(__name__)

class NewsStorageAgent:
    """Handles persistence of news data in an SQLite database."""

    def __init__(self, db_path: str):
        self.db_path = Path(db_path).resolve()
        self._ensure_db_directory()
        self._initialize_db()
        logger.info(f"Storage agent initialized with database at: {self.db_path}")

    def _ensure_db_directory(self):
        """Creates the directory for the database file if it doesn't exist."""
        try:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logger.error(f"Failed to create database directory {self.db_path.parent}: {e}", exc_info=True)
            raise

    def _get_connection(self) -> sqlite3.Connection:
        """Establishes a connection to the SQLite database."""
        try:
            # PARSE_DECLTYPES allows SQLite to recognize TIMESTAMP type affinity
            # PARSE_COLNAMES helps if using 'timestamp as "ts [timestamp]"' syntax (not used here)
            return sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES)
        except sqlite3.Error as e:
            logger.error(f"Failed to connect to database {self.db_path}: {e}", exc_info=True)
            raise

    def _initialize_db(self):
        """Creates the articles table if it doesn't already exist."""
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
        """
        try:
            with self._get_connection() as conn:
                conn.executescript(schema)
            logger.debug("Database schema initialized or verified.")
        except sqlite3.Error as e:
            logger.error(f"Database schema initialization failed: {e}", exc_info=True)
            raise

    async def save_or_update_articles(self, articles: List[NewsArticle]):
        """
        Saves or updates a batch of articles in the database using UPSERT.
        Runs synchronously in the calling thread (consider asyncio.to_thread for heavy loads).
        """
        if not articles:
            logger.debug("No articles provided to save_or_update_articles.")
            return

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
             fetched_at_iso = article.fetched_at.isoformat() # Should always exist

             data_to_insert.append((
                 article.url, article.title, article.source_name, article.content,
                 article.summary, published_at_iso, fetched_at_iso,
                 article.location_query, article.category, article.importance
             ))

        try:
            with self._get_connection() as conn:
                conn.executemany(sql, data_to_insert)
            logger.info(f"Successfully processed (saved/updated) {len(data_to_insert)} articles in database.")
        except sqlite3.Error as e:
            logger.error(f"Database batch save/update failed: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Unexpected error during database save: {e}", exc_info=True)


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
            with self._get_connection() as conn:
                 conn.row_factory = sqlite3.Row # Access columns by name
                 cursor = conn.execute(sql, (location, limit))
                 rows = cursor.fetchall()

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
        except sqlite3.Error as e:
             logger.error(f"Failed to retrieve articles for location '{location}': {e}", exc_info=True)
        return articles