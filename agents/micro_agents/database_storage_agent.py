import logging
import asyncio
import aiosqlite
import json
import time
import hashlib
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Set, Union
from datetime import datetime, timezone, timedelta
from functools import lru_cache

from models.data_models import NewsArticle, NewsSource
from agents.base_agent import BaseAgent, Task, TaskPriority
from monitor.metrics import metrics_collector

logger = logging.getLogger(__name__)

# Constants
DEFAULT_CONNECTION_TIMEOUT = 30  # seconds
DEFAULT_COMMAND_TIMEOUT = 60  # seconds
MAX_BATCH_SIZE = 100
DEFAULT_CACHE_TTL = 300  # 5 minutes in seconds

class DatabaseStorageAgent(BaseAgent):
    """
    Micro-agent responsible for efficient database storage and retrieval operations
    with connection pooling, query optimization, and caching.
    """
    
    def __init__(self, db_path: str, max_connections: int = 5):
        super().__init__("database_storage")
        self.db_path = Path(db_path).resolve()
        self._ensure_db_directory()
        
        # Connection pool settings
        self.max_connections = max_connections
        self._connection_pool = asyncio.Queue(maxsize=max_connections)
        self._active_connections = 0
        self._pool_lock = asyncio.Lock()
        
        # Query execution stats
        self.query_stats = {
            "total_queries": 0,
            "slow_queries": 0,
            "errors": 0,
            "total_execution_time": 0.0,
            "articles_stored": 0,
            "articles_retrieved": 0,
        }
        
        # Caching
        self._query_cache = {}
        self._cache_timestamps = {}
        
        # Batch operation tracking
        self._pending_writes = []
        self._write_batch_lock = asyncio.Lock()
        self._write_flush_task = None
        
        logger.info(f"DatabaseStorageAgent initialized with database at: {self.db_path}")
        
    async def _initialize_resources(self):
        """Initialize the database and connection pool"""
        try:
            # Create the initial database schema
            await self._initialize_db_schema()
            
            # Initialize the connection pool
            for _ in range(self.max_connections):
                conn = await self._create_connection()
                await self._connection_pool.put(conn)
                
            # Start the background task for flushing batched writes
            self._write_flush_task = asyncio.create_task(self._periodic_flush_writes())
            
            logger.info(f"Database connection pool initialized with {self.max_connections} connections")
            
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}", exc_info=True)
            raise
        
    async def _cleanup_resources(self):
        """Close all database connections"""
        # Cancel the flush task if running
        if self._write_flush_task:
            self._write_flush_task.cancel()
            try:
                await self._write_flush_task
            except asyncio.CancelledError:
                pass
        
        # Flush any pending writes
        if self._pending_writes:
            await self._flush_pending_writes()
            
        # Close all connections in the pool
        while not self._connection_pool.empty():
            conn = await self._connection_pool.get()
            await conn.close()
            
        logger.info("Database connection pool closed")
        
        # Log query statistics
        if self.query_stats["total_queries"] > 0:
            avg_time = self.query_stats["total_execution_time"] / self.query_stats["total_queries"]
            logger.info(
                f"Database query statistics: "
                f"{self.query_stats['total_queries']} queries, "
                f"{self.query_stats['slow_queries']} slow queries, "
                f"{self.query_stats['errors']} errors, "
                f"avg time: {avg_time:.4f}s"
            )
            
    async def _process_task_internal(self, task: Task) -> Any:
        """Process storage-related tasks"""
        task_type = task.task_type
        
        if task_type == "store_article":
            return await self._store_article(task)
        elif task_type == "store_articles":
            return await self._store_articles(task)
        elif task_type == "get_articles_by_location":
            return await self._get_articles_by_location(task)
        elif task_type == "get_recent_articles":
            return await self._get_recent_articles(task)
        elif task_type == "get_article_by_url":
            return await self._get_article_by_url(task)
        elif task_type == "search_articles":
            return await self._search_articles(task)
        elif task_type == "store_source":
            return await self._store_source(task)
        elif task_type == "get_sources_by_location":
            return await self._get_sources_by_location(task)
        elif task_type == "get_database_stats":
            return await self._get_database_stats(task)
        elif task_type == "cleanup_old_articles":
            return await self._cleanup_old_articles(task)
        elif task_type == "store_fallback_source":
            return await self._store_fallback_source(task)
        elif task_type == "get_fallback_sources_by_location":
            return await self._get_fallback_sources_by_location(task)
        elif task_type == "update_fallback_source_metrics":
            return await self._update_fallback_source_metrics(task)
        else:
            raise ValueError(f"Unknown task type: {task_type}")
            
    def _ensure_db_directory(self):
        """Creates the directory for the database file if it doesn't exist."""
        try:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logger.error(f"Failed to create database directory {self.db_path.parent}: {e}", exc_info=True)
            raise
            
    async def _create_connection(self) -> aiosqlite.Connection:
        """Create a new database connection with optimized settings"""
        try:
            # Connect to the database
            conn = await aiosqlite.connect(
                self.db_path,
                timeout=DEFAULT_CONNECTION_TIMEOUT
            )
            
            # Configure connection for best performance
            await conn.execute("PRAGMA foreign_keys = ON")
            await conn.execute("PRAGMA journal_mode = WAL")
            await conn.execute("PRAGMA synchronous = NORMAL")
            await conn.execute("PRAGMA cache_size = 10000")  # Use 10MB of RAM for cache
            await conn.execute("PRAGMA temp_store = MEMORY")
            
            # Configure row factory to return dict-like objects
            conn.row_factory = aiosqlite.Row
            
            async with self._pool_lock:
                self._active_connections += 1
                
            return conn
            
        except Exception as e:
            logger.error(f"Failed to create database connection: {e}", exc_info=True)
            raise
            
    async def _get_connection(self) -> aiosqlite.Connection:
        """Get a connection from the pool or create a new one if needed"""
        try:
            # Try to get from the pool first
            return await asyncio.wait_for(
                self._connection_pool.get(),
                timeout=DEFAULT_CONNECTION_TIMEOUT
            )
        except asyncio.TimeoutError:
            # If pool is exhausted, create a new connection temporarily
            logger.warning("Connection pool exhausted, creating temporary connection")
            return await self._create_connection()
            
    async def _release_connection(self, conn: aiosqlite.Connection, is_from_pool: bool = True):
        """Release a connection back to the pool or close it if temporary"""
        if conn.closed:
            if is_from_pool:
                # Replace the closed connection in the pool
                new_conn = await self._create_connection()
                await self._connection_pool.put(new_conn)
            return
            
        if is_from_pool:
            # Return to the pool
            await self._connection_pool.put(conn)
        else:
            # Close temporary connection
            await conn.close()
            async with self._pool_lock:
                self._active_connections -= 1
                
    async def _execute_query(
        self, 
        query: str, 
        params: Optional[tuple] = None, 
        fetch_mode: str = 'all',
        timeout: float = DEFAULT_COMMAND_TIMEOUT
    ) -> Union[List[Dict[str, Any]], Dict[str, Any], int, None]:
        """
        Execute a database query with timeout, monitoring, and error handling
        
        Args:
            query: SQL query to execute
            params: Query parameters
            fetch_mode: One of 'all', 'one', 'scalar', or None (for execute)
            timeout: Query timeout in seconds
            
        Returns:
            Query results based on fetch_mode
        """
        start_time = time.monotonic()
        conn = None
        is_from_pool = True
        results = None
        
        try:
            # Get a connection from the pool
            conn = await self._get_connection()
            
            # Execute the query with timeout
            async with conn.execute(query, params or ()) as cursor:
                if fetch_mode == 'all':
                    rows = await cursor.fetchall()
                    results = [dict(row) for row in rows]
                elif fetch_mode == 'one':
                    row = await cursor.fetchone()
                    results = dict(row) if row else None
                elif fetch_mode == 'scalar':
                    row = await cursor.fetchone()
                    results = row[0] if row else None
                else:  # execute only
                    results = cursor.rowcount
                    # Commit changes for write operations
                    await conn.commit()
                    
        except aiosqlite.OperationalError as e:
            # Handle database locked errors by waiting and retrying once
            if "database is locked" in str(e):
                logger.warning(f"Database locked, retrying: {query[:100]}...")
                # Release the current connection
                if conn:
                    await self._release_connection(conn, is_from_pool)
                # Wait briefly and retry with a new connection
                await asyncio.sleep(0.5)
                conn = await self._create_connection()
                is_from_pool = False
                
                # Try once more
                async with conn.execute(query, params or ()) as cursor:
                    if fetch_mode == 'all':
                        rows = await cursor.fetchall()
                        results = [dict(row) for row in rows]
                    elif fetch_mode == 'one':
                        row = await cursor.fetchone()
                        results = dict(row) if row else None
                    elif fetch_mode == 'scalar':
                        row = await cursor.fetchone()
                        results = row[0] if row else None
                    else:  # execute only
                        results = cursor.rowcount
                        await conn.commit()
            else:
                self.query_stats["errors"] += 1
                raise
                
        except Exception as e:
            self.query_stats["errors"] += 1
            logger.error(f"Database error executing '{query[:100]}...': {e}", exc_info=True)
            raise
            
        finally:
            # Release the connection back to the pool
            if conn:
                await self._release_connection(conn, is_from_pool)
                
            # Update query statistics
            execution_time = time.monotonic() - start_time
            self.query_stats["total_queries"] += 1
            self.query_stats["total_execution_time"] += execution_time
            
            # Log slow queries
            if execution_time > 1.0:  # More than 1 second
                self.query_stats["slow_queries"] += 1
                logger.warning(f"Slow query ({execution_time:.2f}s): {query[:100]}...")
                
        return results
        
    async def _initialize_db_schema(self):
        """Create the database schema with all required tables and indexes"""
        logger.info("Initializing database schema")
        
        # Get a dedicated connection for schema initialization
        conn = await self._create_connection()
        
        try:
            # Articles table
            await conn.execute('''
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
                    importance TEXT,
                    extraction_method TEXT,
                    content_hash TEXT,
                    metadata TEXT
                )
            ''')
            
            # Create optimized indexes
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_articles_location ON articles(location_query)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_articles_source ON articles(source_name)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_articles_published ON articles(published_at)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_articles_fetched ON articles(fetched_at)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_articles_category ON articles(category)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_articles_importance ON articles(importance)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_articles_content_hash ON articles(content_hash)')
            
            # News sources table with enhanced fields
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS sources (
                    url TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    location_type TEXT,
                    reliability_score REAL DEFAULT 0.5,
                    last_fetch_time TIMESTAMP,
                    fetch_frequency INTEGER DEFAULT 3600,
                    active BOOLEAN DEFAULT 1,
                    rss_feed_url TEXT,
                    locations TEXT,
                    metadata TEXT
                )
            ''')
            
            # Create indexes for sources
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_sources_name ON sources(name)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_sources_active ON sources(active)')
            
            # Location-source mapping table for faster lookups
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS location_sources (
                    location TEXT NOT NULL,
                    source_url TEXT NOT NULL,
                    relevance_score REAL DEFAULT 1.0,
                    last_updated TIMESTAMP,
                    PRIMARY KEY (location, source_url),
                    FOREIGN KEY (source_url) REFERENCES sources(url) ON DELETE CASCADE
                )
            ''')
            
            # Full-text search for articles
            await conn.execute('''
                CREATE VIRTUAL TABLE IF NOT EXISTS articles_fts USING fts5(
                    title, content, summary, category,
                    content='articles',
                    content_rowid='rowid'
                )
            ''')
            
            # Create fallback sources table for dynamic source management
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS fallback_sources (
                    url TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    location_type TEXT,
                    reliability_score REAL DEFAULT 0.5,
                    location_pattern TEXT NOT NULL,
                    priority INTEGER DEFAULT 5,
                    last_verified TIMESTAMP,
                    verified_count INTEGER DEFAULT 0,
                    added_at TIMESTAMP
                )
            ''')
            
            # Create index for fallback sources
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_fallback_location_pattern ON fallback_sources(location_pattern)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_fallback_priority ON fallback_sources(priority)')
            
            # Triggers to keep FTS index updated
            await conn.execute('''
                CREATE TRIGGER IF NOT EXISTS articles_ai AFTER INSERT ON articles BEGIN
                    INSERT INTO articles_fts(rowid, title, content, summary, category)
                    VALUES (new.rowid, new.title, new.content, new.summary, new.category);
                END;
            ''')
            
            await conn.execute('''
                CREATE TRIGGER IF NOT EXISTS articles_ad AFTER DELETE ON articles BEGIN
                    INSERT INTO articles_fts(articles_fts, rowid, title, content, summary, category)
                    VALUES('delete', old.rowid, old.title, old.content, old.summary, old.category);
                END;
            ''')
            
            await conn.execute('''
                CREATE TRIGGER IF NOT EXISTS articles_au AFTER UPDATE ON articles BEGIN
                    INSERT INTO articles_fts(articles_fts, rowid, title, content, summary, category)
                    VALUES('delete', old.rowid, old.title, old.content, old.summary, old.category);
                    INSERT INTO articles_fts(rowid, title, content, summary, category)
                    VALUES (new.rowid, new.title, new.content, new.summary, new.category);
                END;
            ''')
            
            # Cache/stats table for quick-access statistics
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS db_stats (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TIMESTAMP
                )
            ''')
            
            await conn.commit()
            logger.info("Database schema initialization complete")
            
        except Exception as e:
            logger.error(f"Error initializing database schema: {e}", exc_info=True)
            raise
        finally:
            await conn.close()
            
    async def _store_article(self, task: Task) -> bool:
        """Store a single article in the database with deduplication"""
        article_data = task.data.get("article")
        if not article_data:
            raise ValueError("Article data is required")
            
        article = (
            article_data if isinstance(article_data, NewsArticle) 
            else NewsArticle(**article_data)
        )
        
        if not article.url:
            logger.warning("Attempted to store article with no URL")
            return False
            
        # Generate content hash for deduplication
        content_hash = None
        if article.content:
            content_hash = hashlib.md5(article.content.encode('utf-8')).hexdigest()
            
        # Prepare metadata
        metadata = {
            "extraction_method": article_data.get("extraction_method"),
            "extraction_latency": article_data.get("extraction_latency"),
            "final_url": article_data.get("final_url"),
        }
            
        # Add to pending writes batch
        async with self._write_batch_lock:
            self._pending_writes.append({
                "type": "article",
                "data": {
                    "url": article.url,
                    "title": article.title,
                    "source_name": article.source_name,
                    "content": article.content,
                    "summary": article.summary,
                    "published_at": article.published_at.isoformat() if article.published_at else None,
                    "fetched_at": article.fetched_at.isoformat() if article.fetched_at else datetime.now(timezone.utc).isoformat(),
                    "location_query": article.location_query,
                    "category": article.category,
                    "importance": article.importance,
                    "content_hash": content_hash,
                    "extraction_method": article_data.get("extraction_method"),
                    "metadata": json.dumps(metadata) if metadata else None
                }
            })
            
            # If batch reached threshold, process immediately
            if len(self._pending_writes) >= MAX_BATCH_SIZE:
                asyncio.create_task(self._flush_pending_writes())
                
        return True
            
    async def _store_articles(self, task: Task) -> int:
        """Store multiple articles in the database efficiently"""
        articles_data = task.data.get("articles", [])
        if not articles_data:
            return 0
            
        # Process each article
        success_count = 0
        
        for article_data in articles_data:
            # Create a subtask for each article
            subtask = Task(
                task_id=f"store_{success_count}",
                task_type="store_article",
                data={"article": article_data}
            )
            
            if await self._store_article(subtask):
                success_count += 1
                
        # Force flush writes
        await self._flush_pending_writes()
                
        return success_count
            
    async def _get_articles_by_location(self, task: Task) -> List[NewsArticle]:
        """Retrieve articles for a location with caching and pagination"""
        location = task.data.get("location")
        if not location:
            raise ValueError("Location is required")
            
        limit = task.data.get("limit", 50)
        offset = task.data.get("offset", 0)
        include_content = task.data.get("include_content", True)
        
        # Check cache for this query
        cache_key = f"location_{location}_{limit}_{offset}_{include_content}"
        cached_result = self._get_from_cache(cache_key)
        if cached_result:
            self.query_stats["articles_retrieved"] += len(cached_result)
            return cached_result
            
        # Build appropriate query based on content inclusion
        if include_content:
            query = '''
                SELECT url, title, source_name, content, summary, published_at, fetched_at, 
                       location_query, category, importance, metadata
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
                LIMIT ? OFFSET ?
            '''
        else:
            # Exclude content for faster queries when not needed
            query = '''
                SELECT url, title, source_name, NULL as content, summary, published_at, fetched_at, 
                       location_query, category, importance, metadata
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
                LIMIT ? OFFSET ?
            '''
            
        try:
            rows = await self._execute_query(query, (location, limit, offset), 'all')
            
            articles = []
            for row in rows:
                # Parse date strings to datetime objects
                published_at = self._parse_iso_datetime(row.get('published_at'))
                fetched_at = self._parse_iso_datetime(row.get('fetched_at')) or datetime.now(timezone.utc)
                
                # Parse metadata if available
                metadata = {}
                if row.get('metadata'):
                    try:
                        metadata = json.loads(row.get('metadata'))
                    except json.JSONDecodeError:
                        pass
                
                # Create NewsArticle object
                article = NewsArticle(
                    url=row.get('url'),
                    title=row.get('title'),
                    source_name=row.get('source_name'),
                    content=row.get('content'),
                    summary=row.get('summary'),
                    published_at=published_at,
                    fetched_at=fetched_at,
                    location_query=row.get('location_query'),
                    category=row.get('category'),
                    importance=row.get('importance')
                )
                
                articles.append(article)
                
            # Update metrics
            self.query_stats["articles_retrieved"] += len(articles)
            
            # Cache the results
            self._add_to_cache(cache_key, articles)
                
            return articles
            
        except Exception as e:
            logger.error(f"Error retrieving articles for location '{location}': {e}", exc_info=True)
            return []
            
    async def _get_recent_articles(self, task: Task) -> List[NewsArticle]:
        """Retrieve the most recent articles across all locations"""
        limit = task.data.get("limit", 30)
        include_content = task.data.get("include_content", True)
        category = task.data.get("category")
        
        # Check cache for this query
        cache_key = f"recent_{limit}_{include_content}_{category}"
        cached_result = self._get_from_cache(cache_key)
        if cached_result:
            return cached_result
            
        # Build query with optional category filter
        where_clause = "WHERE 1=1"
        params = []
        
        if category:
            where_clause += " AND category = ?"
            params.append(category)
            
        # Build appropriate query based on content inclusion
        content_field = "content" if include_content else "NULL as content"
            
        query = f'''
            SELECT url, title, source_name, {content_field}, summary, published_at, fetched_at, 
                   location_query, category, importance
            FROM articles
            {where_clause}
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
        '''
        
        params.append(limit)
            
        try:
            rows = await self._execute_query(query, tuple(params), 'all')
            
            articles = []
            for row in rows:
                # Parse date strings to datetime objects
                published_at = self._parse_iso_datetime(row.get('published_at'))
                fetched_at = self._parse_iso_datetime(row.get('fetched_at')) or datetime.now(timezone.utc)
                
                # Create NewsArticle object
                article = NewsArticle(
                    url=row.get('url'),
                    title=row.get('title'),
                    source_name=row.get('source_name'),
                    content=row.get('content'),
                    summary=row.get('summary'),
                    published_at=published_at,
                    fetched_at=fetched_at,
                    location_query=row.get('location_query'),
                    category=row.get('category'),
                    importance=row.get('importance')
                )
                
                articles.append(article)
                
            # Cache the results
            self._add_to_cache(cache_key, articles)
                
            return articles
            
        except Exception as e:
            logger.error(f"Error retrieving recent articles: {e}", exc_info=True)
            return []
            
    async def _get_article_by_url(self, task: Task) -> Optional[NewsArticle]:
        """Retrieve a specific article by URL"""
        url = task.data.get("url")
        if not url:
            raise ValueError("URL is required")
            
        # Check cache first
        cache_key = f"article_{hashlib.md5(url.encode()).hexdigest()}"
        cached_result = self._get_from_cache(cache_key)
        if cached_result:
            # Return first item since it's a single article
            return cached_result[0] if cached_result else None
            
        query = '''
            SELECT url, title, source_name, content, summary, published_at, fetched_at, 
                   location_query, category, importance, metadata
            FROM articles
            WHERE url = ?
        '''
            
        try:
            row = await self._execute_query(query, (url,), 'one')
            
            if not row:
                return None
                
            # Parse date strings to datetime objects
            published_at = self._parse_iso_datetime(row.get('published_at'))
            fetched_at = self._parse_iso_datetime(row.get('fetched_at')) or datetime.now(timezone.utc)
            
            # Create NewsArticle object
            article = NewsArticle(
                url=row.get('url'),
                title=row.get('title'),
                source_name=row.get('source_name'),
                content=row.get('content'),
                summary=row.get('summary'),
                published_at=published_at,
                fetched_at=fetched_at,
                location_query=row.get('location_query'),
                category=row.get('category'),
                importance=row.get('importance')
            )
            
            # Cache the article
            self._add_to_cache(cache_key, [article])
            
            return article
            
        except Exception as e:
            logger.error(f"Error retrieving article by URL '{url}': {e}", exc_info=True)
            return None
            
    async def _search_articles(self, task: Task) -> List[NewsArticle]:
        """Search for articles using full-text search"""
        query_text = task.data.get("query")
        if not query_text:
            raise ValueError("Search query is required")
            
        limit = task.data.get("limit", 30)
        include_content = task.data.get("include_content", False)
        
        # Check cache
        cache_key = f"search_{hashlib.md5(query_text.encode()).hexdigest()}_{limit}_{include_content}"
        cached_result = self._get_from_cache(cache_key)
        if cached_result:
            return cached_result
            
        # Build appropriate query based on content inclusion
        content_field = "a.content" if include_content else "NULL as content"
            
        query = f'''
            SELECT a.url, a.title, a.source_name, {content_field}, a.summary, a.published_at, a.fetched_at, 
                   a.location_query, a.category, a.importance,
                   rank
            FROM articles_fts fts
            JOIN articles a ON fts.rowid = a.rowid
            WHERE articles_fts MATCH ?
            ORDER BY rank, 
                CASE a.importance
                    WHEN 'Critical' THEN 1
                    WHEN 'High' THEN 2
                    WHEN 'Medium' THEN 3
                    WHEN 'Low' THEN 4
                    ELSE 5
                END,
                COALESCE(a.published_at, a.fetched_at) DESC
            LIMIT ?
        '''
            
        try:
            # Prepare the FTS query
            fts_query = self._prepare_fts_query(query_text)
            
            rows = await self._execute_query(query, (fts_query, limit), 'all')
            
            articles = []
            for row in rows:
                # Parse date strings to datetime objects
                published_at = self._parse_iso_datetime(row.get('published_at'))
                fetched_at = self._parse_iso_datetime(row.get('fetched_at')) or datetime.now(timezone.utc)
                
                # Create NewsArticle object
                article = NewsArticle(
                    url=row.get('url'),
                    title=row.get('title'),
                    source_name=row.get('source_name'),
                    content=row.get('content'),
                    summary=row.get('summary'),
                    published_at=published_at,
                    fetched_at=fetched_at,
                    location_query=row.get('location_query'),
                    category=row.get('category'),
                    importance=row.get('importance')
                )
                
                articles.append(article)
                
            # Cache the results
            self._add_to_cache(cache_key, articles)
                
            return articles
            
        except Exception as e:
            logger.error(f"Error searching for articles with query '{query_text}': {e}", exc_info=True)
            return []
            
    def _prepare_fts_query(self, query_text: str) -> str:
        """Prepare a query string for FTS search with smart handling of phrases and wildcards"""
        # Split into words while preserving quoted phrases
        terms = []
        current_term = []
        in_quotes = False
        
        for char in query_text:
            if char == '"':
                in_quotes = not in_quotes
                current_term.append(char)
            elif char.isspace() and not in_quotes:
                if current_term:
                    terms.append(''.join(current_term))
                    current_term = []
            else:
                current_term.append(char)
                
        if current_term:
            terms.append(''.join(current_term))
            
        # Process terms for FTS
        processed_terms = []
        
        for term in terms:
            # If it's a quoted phrase, leave it as is
            if term.startswith('"') and term.endswith('"'):
                processed_terms.append(term)
            # Otherwise add wildcard for partial matching
            elif len(term) > 3:  # Only add wildcards to longer terms
                processed_terms.append(f"{term}*")
            else:
                processed_terms.append(term)
                
        return ' '.join(processed_terms)
            
    async def _store_source(self, task: Task) -> bool:
        """Store a news source in the database"""
        source_data = task.data.get("source")
        if not source_data:
            raise ValueError("Source data is required")
            
        source = (
            source_data if isinstance(source_data, NewsSource) 
            else NewsSource(**source_data)
        )
        
        if not source.url:
            logger.warning("Attempted to store source with no URL")
            return False
            
        # Store locations as JSON array
        locations = task.data.get("locations", [])
        if not isinstance(locations, list):
            locations = [locations] if locations else []
            
        # Prepare metadata
        metadata = task.data.get("metadata", {})
        if not isinstance(metadata, dict):
            metadata = {}
            
        # Update source with current timestamp
        now = datetime.now(timezone.utc).isoformat()
            
        query = '''
            INSERT OR REPLACE INTO sources 
            (url, name, location_type, reliability_score, last_fetch_time,
             active, rss_feed_url, locations, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        try:
            await self._execute_query(
                query, 
                (
                    source.url,
                    source.name,
                    source.location_type,
                    source.reliability_score,
                    now,
                    True,
                    source.rss_feed_url,
                    json.dumps(locations),
                    json.dumps(metadata) if metadata else None
                ),
                None  # Execute only
            )
            
            # Also add location mappings if provided
            if locations:
                for location in locations:
                    loc_query = '''
                        INSERT OR REPLACE INTO location_sources
                        (location, source_url, relevance_score, last_updated)
                        VALUES (?, ?, ?, ?)
                    '''
                    
                    # Get relevance score from metadata or default to 1.0
                    relevance = metadata.get("relevance", {}).get(location, 1.0)
                    
                    await self._execute_query(
                        loc_query,
                        (location, source.url, relevance, now),
                        None
                    )
            
            # Invalidate relevant cache entries
            self._invalidate_cache_pattern("sources_")
            
            return True
            
        except Exception as e:
            logger.error(f"Error storing source {source.url}: {e}", exc_info=True)
            return False
            
    async def _get_sources_by_location(self, task: Task) -> List[NewsSource]:
        """Get news sources for a specific location"""
        location = task.data.get("location")
        if not location:
            raise ValueError("Location is required")
            
        limit = task.data.get("limit", 20)
        
        # Check cache
        cache_key = f"sources_{location}_{limit}"
        cached_result = self._get_from_cache(cache_key)
        if cached_result:
            return cached_result
            
        # First try the location_sources mapping table
        query = '''
            SELECT s.url, s.name, s.location_type, s.reliability_score, 
                   s.rss_feed_url, s.metadata, ls.relevance_score
            FROM sources s
            JOIN location_sources ls ON s.url = ls.source_url
            WHERE ls.location = ? AND s.active = 1
            ORDER BY ls.relevance_score DESC, s.reliability_score DESC
            LIMIT ?
        '''
            
        try:
            rows = await self._execute_query(query, (location, limit), 'all')
            
            # If no results from mapping, try sources with location in their locations field
            if not rows:
                logger.debug(f"No mapped sources for {location}, trying locations field")
                query = '''
                    SELECT url, name, location_type, reliability_score, 
                           rss_feed_url, metadata, 1.0 as relevance_score
                    FROM sources
                    WHERE json_extract(locations, '$') LIKE ? AND active = 1
                    ORDER BY reliability_score DESC
                    LIMIT ?
                '''
                
                rows = await self._execute_query(query, (f'%{location}%', limit), 'all')
            
            sources = []
            for row in rows:
                # Parse metadata
                metadata = {}
                if row.get('metadata'):
                    try:
                        metadata = json.loads(row.get('metadata'))
                    except json.JSONDecodeError:
                        pass
                
                # Create NewsSource object
                source = NewsSource(
                    url=row.get('url'),
                    name=row.get('name'),
                    location_type=row.get('location_type', 'Unknown'),
                    reliability_score=float(row.get('reliability_score', 0.5)),
                    rss_feed_url=row.get('rss_feed_url')
                )
                
                sources.append(source)
                
            # Cache the results
            self._add_to_cache(cache_key, sources)
                
            return sources
            
        except Exception as e:
            logger.error(f"Error retrieving sources for location '{location}': {e}", exc_info=True)
            return []
            
    async def _get_database_stats(self, task: Task) -> Dict[str, Any]:
        """Get detailed database statistics"""
        # Check cache first (stats can be slightly stale)
        cache_key = "database_stats"
        cached_stats = self._get_from_cache(cache_key, ttl=300)  # 5 minute cache
        if cached_stats:
            return cached_stats[0]  # Return as dict, not list
            
        stats = {}
        
        try:
            # Get article counts
            article_count = await self._execute_query(
                "SELECT COUNT(*) FROM articles", 
                fetch_mode='scalar'
            )
            stats["total_articles"] = article_count
            
            # Get source counts
            source_count = await self._execute_query(
                "SELECT COUNT(*) FROM sources WHERE active = 1", 
                fetch_mode='scalar'
            )
            stats["active_sources"] = source_count
            
            # Get category distribution
            categories = await self._execute_query(
                """
                SELECT category, COUNT(*) as count
                FROM articles
                WHERE category IS NOT NULL
                GROUP BY category
                ORDER BY count DESC
                LIMIT 10
                """,
                fetch_mode='all'
            )
            stats["categories"] = {row['category']: row['count'] for row in categories}
            
            # Get location distribution
            locations = await self._execute_query(
                """
                SELECT location_query, COUNT(*) as count
                FROM articles
                WHERE location_query IS NOT NULL
                GROUP BY location_query
                ORDER BY count DESC
                LIMIT 15
                """,
                fetch_mode='all'
            )
            stats["locations"] = {row['location_query']: row['count'] for row in locations}
            
            # Get date range
            date_range = await self._execute_query(
                """
                SELECT 
                    MIN(published_at) as oldest,
                    MAX(published_at) as newest
                FROM articles
                WHERE published_at IS NOT NULL
                """,
                fetch_mode='one'
            )
            stats["date_range"] = {
                "oldest": date_range['oldest'] if date_range else None,
                "newest": date_range['newest'] if date_range else None
            }
            
            # Get importance distribution
            importance = await self._execute_query(
                """
                SELECT importance, COUNT(*) as count
                FROM articles
                WHERE importance IS NOT NULL
                GROUP BY importance
                ORDER BY 
                    CASE importance
                        WHEN 'Critical' THEN 1
                        WHEN 'High' THEN 2
                        WHEN 'Medium' THEN 3
                        WHEN 'Low' THEN 4
                        ELSE 5
                    END
                """,
                fetch_mode='all'
            )
            stats["importance"] = {row['importance']: row['count'] for row in importance}
            
            # Get database file size
            stats["database_size_bytes"] = self.db_path.stat().st_size
            stats["database_size_mb"] = round(stats["database_size_bytes"] / (1024 * 1024), 2)
            
            # Add timestamp
            stats["generated_at"] = datetime.now(timezone.utc).isoformat()
            
            # Cache the statistics
            self._add_to_cache(cache_key, [stats])
            
            return stats
            
        except Exception as e:
            logger.error(f"Error retrieving database statistics: {e}", exc_info=True)
            return {"error": str(e)}
            
    async def _cleanup_old_articles(self, task: Task) -> Dict[str, Any]:
        """Remove old articles to prevent unlimited database growth"""
        days_to_keep = task.data.get("days_to_keep", 30)
        if days_to_keep < 7:  # Safety check
            days_to_keep = 7
            
        # Calculate cutoff date
        cutoff_date = (datetime.now(timezone.utc) - timedelta(days=days_to_keep)).isoformat()
        
        try:
            # Count articles to delete
            to_delete_count = await self._execute_query(
                """
                SELECT COUNT(*) 
                FROM articles 
                WHERE fetched_at < ? AND importance NOT IN ('Critical', 'High')
                """,
                (cutoff_date,),
                'scalar'
            )
            
            # Proceed with deletion if there are articles to delete
            if to_delete_count > 0:
                deleted_count = await self._execute_query(
                    """
                    DELETE FROM articles 
                    WHERE fetched_at < ? AND importance NOT IN ('Critical', 'High')
                    """,
                    (cutoff_date,),
                    None  # Execute only
                )
                
                logger.info(f"Deleted {deleted_count} old articles (older than {days_to_keep} days)")
                
                # Vacuum the database to reclaim space
                await self._execute_query("VACUUM", fetch_mode=None)
                
                # Invalidate all article-related caches
                self._invalidate_cache_pattern("location_")
                self._invalidate_cache_pattern("recent_")
                self._invalidate_cache_pattern("search_")
                self._invalidate_cache_pattern("article_")
                self._invalidate_cache_pattern("database_stats")
                
                return {
                    "success": True,
                    "deleted_count": deleted_count,
                    "days_kept": days_to_keep
                }
                
            else:
                logger.info(f"No articles to delete (older than {days_to_keep} days)")
                return {
                    "success": True,
                    "deleted_count": 0,
                    "days_kept": days_to_keep
                }
                
        except Exception as e:
            logger.error(f"Error cleaning up old articles: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }
            
    async def _periodic_flush_writes(self):
        """Background task to periodically flush batched writes"""
        try:
            while True:
                await asyncio.sleep(5)  # Check every 5 seconds
                
                if self._pending_writes:
                    await self._flush_pending_writes()
                    
        except asyncio.CancelledError:
            # Final flush on cancellation
            if self._pending_writes:
                await self._flush_pending_writes()
            raise
            
    async def _flush_pending_writes(self):
        """Flush all pending writes to the database in efficient batches"""
        async with self._write_batch_lock:
            if not self._pending_writes:
                return
                
            pending_writes = self._pending_writes.copy()
            self._pending_writes.clear()
            
        if not pending_writes:
            return
            
        # Group by type for efficient bulk operations
        articles = []
        sources = []
        
        for item in pending_writes:
            item_type = item.get("type")
            item_data = item.get("data")
            
            if item_type == "article":
                articles.append(item_data)
            elif item_type == "source":
                sources.append(item_data)
                
        # Process articles in bulk
        if articles:
            try:
                conn = await self._get_connection()
                
                try:
                    # Prepare bulk insert statement
                    query = '''
                        INSERT OR REPLACE INTO articles
                        (url, title, source_name, content, summary, published_at, fetched_at,
                         location_query, category, importance, content_hash, extraction_method, metadata)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    '''
                    
                    # Execute in chunks to avoid parameter limits
                    chunk_size = 50
                    for i in range(0, len(articles), chunk_size):
                        chunk = articles[i:i+chunk_size]
                        values = []
                        
                        for article in chunk:
                            values.append((
                                article.get("url"),
                                article.get("title"),
                                article.get("source_name"),
                                article.get("content"),
                                article.get("summary"),
                                article.get("published_at"),
                                article.get("fetched_at", datetime.now(timezone.utc).isoformat()),
                                article.get("location_query"),
                                article.get("category"),
                                article.get("importance"),
                                article.get("content_hash"),
                                article.get("extraction_method"),
                                article.get("metadata")
                            ))
                            
                        async with conn.executemany(query, values):
                            pass
                            
                    await conn.commit()
                    self.query_stats["articles_stored"] += len(articles)
                    
                    # Invalidate related caches
                    self._invalidate_cache_pattern("location_")
                    self._invalidate_cache_pattern("recent_")
                    
                except Exception as e:
                    logger.error(f"Error during bulk article insertion: {e}", exc_info=True)
                    await conn.rollback()
                finally:
                    await self._release_connection(conn)
                    
            except Exception as e:
                logger.error(f"Failed to get connection for bulk article insertion: {e}", exc_info=True)
                
        # Process sources in bulk (similar approach)
        if sources:
            # Similar implementation for sources bulk insert
            pass
            
    def _add_to_cache(self, key: str, value: Any, ttl: int = DEFAULT_CACHE_TTL):
        """Add an item to the cache with expiration"""
        expires_at = time.monotonic() + ttl
        self._query_cache[key] = value
        self._cache_timestamps[key] = expires_at
        
    def _get_from_cache(self, key: str, ttl: int = None) -> Optional[Any]:
        """Get an item from the cache, checking expiration"""
        if key in self._query_cache and key in self._cache_timestamps:
            # Check if expired
            if ttl is not None:
                expires_at = time.monotonic() - DEFAULT_CACHE_TTL + ttl
            else:
                expires_at = self._cache_timestamps[key]
                
            if time.monotonic() < expires_at:
                return self._query_cache[key]
                
            # Expired, remove from cache
            del self._query_cache[key]
            del self._cache_timestamps[key]
            
        return None
        
    def _invalidate_cache_pattern(self, pattern: str):
        """Invalidate all cache entries that start with the given pattern"""
        keys_to_remove = []
        
        for key in self._query_cache.keys():
            if key.startswith(pattern):
                keys_to_remove.append(key)
                
        for key in keys_to_remove:
            if key in self._query_cache:
                del self._query_cache[key]
            if key in self._cache_timestamps:
                del self._cache_timestamps[key]
                
    @staticmethod
    def _parse_iso_datetime(datetime_str: Optional[str]) -> Optional[datetime]:
        """Parse ISO format datetime string to datetime object with error handling"""
        if not datetime_str:
            return None
            
        try:
            # Handle Z suffix by replacing with +00:00
            dt_str = datetime_str.replace('Z', '+00:00')
            return datetime.fromisoformat(dt_str)
        except (ValueError, AttributeError):
            return None
            
    async def _store_fallback_source(self, task: Task) -> bool:
        """Store a news source in the fallback sources database"""
        source_data = task.data.get("source")
        if not source_data:
            raise ValueError("Source data is required")
            
        source = (
            source_data if isinstance(source_data, NewsSource) 
            else NewsSource(**source_data)
        )
        
        location_pattern = task.data.get("location_pattern", "")
        if not location_pattern:
            raise ValueError("Location pattern is required for fallback sources")
            
        if not source.url:
            logger.warning("Attempted to store fallback source with no URL")
            return False
            
        # Update source with current timestamp
        now = datetime.now(timezone.utc).isoformat()
            
        query = '''
            INSERT OR REPLACE INTO fallback_sources 
            (url, name, location_type, reliability_score, location_pattern,
             priority, last_verified, verified_count, added_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        try:
            # Use priority from task or default to 10
            priority = task.data.get("priority", 10)
            
            # Get verified count or default to 1
            verified_count = task.data.get("verified_count", 1)
            
            await self._execute_query(
                query, 
                (
                    source.url,
                    source.name,
                    source.location_type,
                    source.reliability_score,
                    location_pattern,
                    priority,
                    now,
                    verified_count,
                    now
                ),
                None  # Execute only
            )
            
            # Invalidate relevant cache entries
            self._invalidate_cache_pattern("fallback_sources_")
            
            return True
            
        except Exception as e:
            logger.error(f"Error storing fallback source {source.url}: {e}", exc_info=True)
            return False
            
    async def _get_fallback_sources_by_location(self, task: Task) -> List[NewsSource]:
        """Get fallback news sources for a specific location pattern"""
        location = task.data.get("location", "")
        if not location:
            raise ValueError("Location is required")
            
        limit = task.data.get("limit", 10)
        
        # Check cache
        location_lower = location.lower()
        cache_key = f"fallback_sources_{location_lower}_{limit}"
        cached_result = self._get_from_cache(cache_key)
        if cached_result:
            return cached_result
            
        # First try exact match on location pattern
        query = '''
            SELECT url, name, location_type, reliability_score, location_pattern,
                   priority, verified_count
            FROM fallback_sources
            WHERE location_pattern = ?
            ORDER BY priority ASC, verified_count DESC, reliability_score DESC
            LIMIT ?
        '''
        
        try:
            rows = await self._execute_query(query, (location_lower, limit), 'all')
            
            # If no exact matches, try substring matches
            if not rows:
                # Create a list of terms to check for partial matches
                location_terms = location_lower.split()
                
                # If we have multiple terms, try searching for each term
                if len(location_terms) > 1:
                    placeholders = ', '.join(['?'] * len(location_terms))
                    query = f'''
                        SELECT url, name, location_type, reliability_score, location_pattern,
                               priority, verified_count
                        FROM fallback_sources
                        WHERE location_pattern IN ({placeholders})
                        ORDER BY priority ASC, verified_count DESC, reliability_score DESC
                        LIMIT ?
                    '''
                    # Add limit as the last parameter
                    params = tuple(location_terms) + (limit,)
                    rows = await self._execute_query(query, params, 'all')
                
                # If still no results, try LIKE queries for partial matches
                if not rows:
                    like_params = []
                    like_clauses = []
                    
                    # Try pattern matching with longer terms first
                    for term in location_terms:
                        if len(term) > 3:  # Only use longer terms
                            like_clauses.append("location_pattern LIKE ?")
                            like_params.append(f"%{term}%")
                    
                    if like_clauses:
                        query = f'''
                            SELECT url, name, location_type, reliability_score, location_pattern,
                                   priority, verified_count
                            FROM fallback_sources
                            WHERE {" OR ".join(like_clauses)}
                            ORDER BY priority ASC, verified_count DESC, reliability_score DESC
                            LIMIT ?
                        '''
                        # Add limit as the last parameter
                        like_params.append(limit)
                        rows = await self._execute_query(query, tuple(like_params), 'all')
            
            # If still no results, get generic fallback sources (location_pattern = 'global')
            if not rows:
                query = '''
                    SELECT url, name, location_type, reliability_score, location_pattern,
                           priority, verified_count 
                    FROM fallback_sources
                    WHERE location_pattern = 'global'
                    ORDER BY priority ASC, verified_count DESC, reliability_score DESC
                    LIMIT ?
                '''
                rows = await self._execute_query(query, (limit,), 'all')
            
            # If all else fails, get top sources regardless of location
            if not rows:
                query = '''
                    SELECT url, name, location_type, reliability_score, location_pattern,
                           priority, verified_count
                    FROM fallback_sources
                    ORDER BY verified_count DESC, reliability_score DESC
                    LIMIT ?
                '''
                rows = await self._execute_query(query, (limit,), 'all')
            
            # Convert rows to NewsSource objects
            sources = []
            for row in rows:
                source = NewsSource(
                    url=row.get('url'),
                    name=row.get('name'),
                    location_type=row.get('location_type', 'Unknown'),
                    reliability_score=float(row.get('reliability_score', 0.5))
                )
                sources.append(source)
                
            # Cache the results
            self._add_to_cache(cache_key, sources)
                
            return sources
            
        except Exception as e:
            logger.error(f"Error retrieving fallback sources for location '{location}': {e}", exc_info=True)
            return []
    
    async def _update_fallback_source_metrics(self, task: Task) -> bool:
        """Update metrics for a fallback source when it's successfully verified"""
        url = task.data.get("url")
        if not url:
            raise ValueError("URL is required")
        
        try:
            query = '''
                UPDATE fallback_sources
                SET verified_count = verified_count + 1,
                    last_verified = ?
                WHERE url = ?
            '''
            
            now = datetime.now(timezone.utc).isoformat()
            await self._execute_query(query, (now, url), None)
            
            # Invalidate cache
            self._invalidate_cache_pattern("fallback_sources_")
            
            return True
        except Exception as e:
            logger.error(f"Error updating fallback source metrics for {url}: {e}", exc_info=True)
            return False