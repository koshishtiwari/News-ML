"""
Vector store implementation for semantic search and similarity matching of articles.
This enables finding semantically similar articles and performing natural language queries beyond keyword matching.
"""
import os
import logging
import numpy as np
import sqlite3
from typing import List, Dict, Any, Optional, Tuple
import json
from datetime import datetime
import time

# For embedding generation
from sentence_transformers import SentenceTransformer

# Import project modules
from models.data_models import NewsArticle
from utils.logging_config import get_logger

# Constants
DEFAULT_VECTOR_DB_PATH = "data/vector_store.db"
DEFAULT_MODEL_NAME = "all-MiniLM-L6-v2"  # Small but effective embedding model
VECTOR_DIMENSION = 384  # Dimension of the embedding vectors from the model

# Setup logger
logger = get_logger(__name__, {"component": "vector_store"})


class ArticleVectorStore:
    """
    A vector store for news articles that enables semantic search capabilities.
    Uses sentence-transformers for embedding generation and SQLite for storage.
    """

    def __init__(self, db_path: str = DEFAULT_VECTOR_DB_PATH, model_name: str = DEFAULT_MODEL_NAME):
        """
        Initialize the vector store.
        
        Args:
            db_path: Path to the SQLite database file
            model_name: Name of the sentence-transformer model to use
        """
        self.db_path = db_path
        self.model_name = model_name
        self._embedding_model = None  # Lazy-loaded
        
        # Create the database directory if it doesn't exist
        os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)
        
        # Initialize the database
        self._init_db()
        
        logger.info(f"ArticleVectorStore initialized with model {model_name}")
    
    @property
    def embedding_model(self) -> SentenceTransformer:
        """Lazy-load the embedding model when first needed."""
        if self._embedding_model is None:
            start_time = time.time()
            logger.info(f"Loading embedding model: {self.model_name}")
            
            # Load the model for generating embeddings
            self._embedding_model = SentenceTransformer(self.model_name)
            
            elapsed = time.time() - start_time
            logger.info(f"Embedding model loaded in {elapsed:.2f} seconds")
        
        return self._embedding_model
    
    def _init_db(self):
        """Initialize the vector database schema."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Create the articles table if it doesn't exist
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS article_vectors (
                    url TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    category TEXT,
                    location TEXT,
                    published_at TEXT,
                    vector BLOB NOT NULL,
                    metadata TEXT,
                    created_at TEXT NOT NULL
                )
                ''')
                
                # Create an index on category and location for faster filtering
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_category ON article_vectors(category)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_location ON article_vectors(location)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_source ON article_vectors(source_name)')
                
                conn.commit()
                logger.debug("Vector database schema initialized")
                
        except Exception as e:
            logger.error(f"Error initializing vector database: {e}", exc_info=True)
            raise
    
    def generate_embedding(self, article: NewsArticle) -> np.ndarray:
        """
        Generate an embedding vector for an article.
        
        Args:
            article: The news article to embed
            
        Returns:
            A numpy array containing the article's embedding vector
        """
        # Combine title and content for a more comprehensive embedding
        # The title is repeated to give it more weight in the embedding
        text_to_embed = f"{article.title} {article.title} {article.content[:5000]}"
        
        # Generate the embedding
        vector = self.embedding_model.encode(text_to_embed, show_progress_bar=False)
        return vector
    
    def add_article(self, article: NewsArticle, replace_existing: bool = False) -> bool:
        """
        Add an article to the vector store.
        
        Args:
            article: The news article to add
            replace_existing: Whether to replace if article already exists
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Check if the article already exists
            if not replace_existing:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute('SELECT 1 FROM article_vectors WHERE url = ?', (article.url,))
                    if cursor.fetchone():
                        logger.debug(f"Article already exists in vector store: {article.url}")
                        return False
            
            # Generate the embedding vector
            vector = self.generate_embedding(article)
            
            # Prepare metadata
            metadata = {
                "summary": getattr(article, "summary", None),
                "keywords": getattr(article, "keywords", []),
                "sentiment": getattr(article, "sentiment", None),
            }
            
            # Convert vector to binary blob
            vector_bytes = vector.tobytes()
            
            # Store in database
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                INSERT OR REPLACE INTO article_vectors 
                (url, title, source_name, category, location, published_at, vector, metadata, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    article.url,
                    article.title,
                    article.source_name,
                    getattr(article, "category", None),
                    article.location_query,
                    article.published_at.isoformat() if article.published_at else None,
                    vector_bytes,
                    json.dumps(metadata),
                    datetime.now().isoformat()
                ))
                conn.commit()
            
            logger.debug(f"Added article to vector store: {article.url}")
            return True
            
        except Exception as e:
            logger.error(f"Error adding article to vector store: {e}", exc_info=True)
            return False
    
    def bulk_add_articles(self, articles: List[NewsArticle], replace_existing: bool = False) -> int:
        """
        Add multiple articles to the vector store in bulk.
        
        Args:
            articles: List of articles to add
            replace_existing: Whether to replace if articles already exist
            
        Returns:
            Number of articles successfully added
        """
        added_count = 0
        start_time = time.time()
        
        logger.info(f"Adding {len(articles)} articles to vector store")
        
        try:
            # Process each article
            for i, article in enumerate(articles):
                if self.add_article(article, replace_existing):
                    added_count += 1
                
                # Log progress for large batches
                if (i + 1) % 50 == 0 or i + 1 == len(articles):
                    elapsed = time.time() - start_time
                    logger.info(f"Processed {i+1}/{len(articles)} articles in {elapsed:.2f}s - {added_count} added")
            
            return added_count
            
        except Exception as e:
            logger.error(f"Error in bulk_add_articles: {e}", exc_info=True)
            return added_count
    
    def _vector_from_bytes(self, vector_bytes: bytes) -> np.ndarray:
        """Convert binary blob back to numpy array."""
        return np.frombuffer(vector_bytes, dtype=np.float32)
    
    def search_similar(self, article_url: str, limit: int = 5,
                      category_filter: Optional[str] = None,
                      location_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Find articles similar to a given article by URL.
        
        Args:
            article_url: URL of the source article
            limit: Maximum number of similar articles to return
            category_filter: Optional category to filter results
            location_filter: Optional location to filter results
            
        Returns:
            List of similar articles with similarity scores
        """
        try:
            # Get the vector for the source article
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute('SELECT vector FROM article_vectors WHERE url = ?', (article_url,))
                result = cursor.fetchone()
                
                if not result:
                    logger.warning(f"Article not found in vector store: {article_url}")
                    return []
                
                source_vector = self._vector_from_bytes(result['vector'])
                
                # Construct the SQL query with optional filters
                sql = '''
                SELECT url, title, source_name, category, location, published_at, metadata
                FROM article_vectors
                WHERE url != ?
                '''
                params = [article_url]
                
                if category_filter:
                    sql += ' AND category = ?'
                    params.append(category_filter)
                
                if location_filter:
                    sql += ' AND location = ?'
                    params.append(location_filter)
                
                # Get all candidate articles
                cursor.execute(sql, params)
                candidates = cursor.fetchall()
                
                if not candidates:
                    logger.debug(f"No candidate articles found for similarity search")
                    return []
                
                # Calculate cosine similarities
                results = []
                for row in candidates:
                    candidate_vector = self._vector_from_bytes(row['vector'])
                    
                    # Calculate cosine similarity (normalized dot product)
                    similarity = np.dot(source_vector, candidate_vector) / (
                        np.linalg.norm(source_vector) * np.linalg.norm(candidate_vector)
                    )
                    
                    # Add to results if similarity is above threshold
                    if similarity > 0.5:  # Minimum similarity threshold
                        results.append({
                            'url': row['url'],
                            'title': row['title'],
                            'source_name': row['source_name'],
                            'category': row['category'],
                            'location': row['location'],
                            'published_at': row['published_at'],
                            'similarity': float(similarity),
                            'metadata': json.loads(row['metadata']) if row['metadata'] else {}
                        })
                
                # Sort by similarity (highest first) and limit results
                results.sort(key=lambda x: x['similarity'], reverse=True)
                return results[:limit]
                
        except Exception as e:
            logger.error(f"Error in search_similar: {e}", exc_info=True)
            return []
    
    def semantic_search(self, query: str, limit: int = 10,
                       category_filter: Optional[str] = None,
                       location_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Perform semantic search using natural language query.
        
        Args:
            query: Natural language search query
            limit: Maximum number of results to return
            category_filter: Optional category to filter results
            location_filter: Optional location to filter results
            
        Returns:
            List of matching articles with relevance scores
        """
        try:
            # Generate embedding for the query
            query_vector = self.embedding_model.encode(query, show_progress_bar=False)
            
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                # Construct the SQL query with optional filters
                sql = '''
                SELECT url, title, source_name, category, location, published_at, vector, metadata
                FROM article_vectors
                '''
                params = []
                
                where_clauses = []
                
                if category_filter:
                    where_clauses.append('category = ?')
                    params.append(category_filter)
                
                if location_filter:
                    where_clauses.append('location = ?')
                    params.append(location_filter)
                
                if where_clauses:
                    sql += ' WHERE ' + ' AND '.join(where_clauses)
                
                # Get all candidate articles
                cursor.execute(sql, params)
                candidates = cursor.fetchall()
                
                if not candidates:
                    logger.debug(f"No candidate articles found for semantic search")
                    return []
                
                # Calculate relevance scores
                results = []
                for row in candidates:
                    article_vector = self._vector_from_bytes(row['vector'])
                    
                    # Calculate cosine similarity
                    similarity = np.dot(query_vector, article_vector) / (
                        np.linalg.norm(query_vector) * np.linalg.norm(article_vector)
                    )
                    
                    # Add to results if relevance is reasonable
                    if similarity > 0.4:  # Minimum relevance threshold
                        results.append({
                            'url': row['url'],
                            'title': row['title'],
                            'source_name': row['source_name'],
                            'category': row['category'],
                            'location': row['location'],
                            'published_at': row['published_at'],
                            'relevance': float(similarity),
                            'metadata': json.loads(row['metadata']) if row['metadata'] else {}
                        })
                
                # Sort by relevance (highest first) and limit results
                results.sort(key=lambda x: x['relevance'], reverse=True)
                return results[:limit]
                
        except Exception as e:
            logger.error(f"Error in semantic_search: {e}", exc_info=True)
            return []
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the vector store.
        
        Returns:
            Dictionary with statistics (count, categories, locations, etc.)
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Get total count
                cursor.execute('SELECT COUNT(*) FROM article_vectors')
                total_count = cursor.fetchone()[0]
                
                # Get unique categories
                cursor.execute('SELECT DISTINCT category FROM article_vectors WHERE category IS NOT NULL')
                categories = [row[0] for row in cursor.fetchall()]
                
                # Get unique locations
                cursor.execute('SELECT DISTINCT location FROM article_vectors WHERE location IS NOT NULL')
                locations = [row[0] for row in cursor.fetchall()]
                
                # Get unique sources
                cursor.execute('SELECT DISTINCT source_name FROM article_vectors')
                sources = [row[0] for row in cursor.fetchall()]
                
                # Get date range
                cursor.execute('''
                SELECT MIN(published_at), MAX(published_at)
                FROM article_vectors
                WHERE published_at IS NOT NULL
                ''')
                date_range = cursor.fetchone()
                
                return {
                    'total_articles': total_count,
                    'categories': categories,
                    'locations': locations,
                    'sources': sources,
                    'oldest_article': date_range[0] if date_range else None,
                    'newest_article': date_range[1] if date_range else None
                }
                
        except Exception as e:
            logger.error(f"Error getting vector store stats: {e}", exc_info=True)
            return {
                'total_articles': 0,
                'categories': [],
                'locations': [],
                'sources': [],
                'oldest_article': None,
                'newest_article': None
            }


class VectorStoreManager:
    """
    Singleton manager for accessing the article vector store.
    This ensures we only load the embedding model once across the application.
    """
    _instance = None
    
    @classmethod
    def get_instance(cls, db_path: str = DEFAULT_VECTOR_DB_PATH, 
                    model_name: str = DEFAULT_MODEL_NAME) -> ArticleVectorStore:
        """Get or create the vector store instance."""
        if cls._instance is None:
            cls._instance = ArticleVectorStore(db_path, model_name)
        return cls._instance