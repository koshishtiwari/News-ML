from datetime import datetime, timezone
from typing import Optional, List, Dict, ClassVar
from pydantic import BaseModel, Field, HttpUrl, validator
from enum import Enum


class ImportanceLevel(str, Enum):
    """Enum for article importance levels"""
    CRITICAL = "Critical"
    HIGH = "High"
    MEDIUM = "Medium"
    LOW = "Low"


class LocationType(str, Enum):
    """Enum for news source location types"""
    LOCAL = "Local"
    NATIONAL_LOCAL = "National/Local"
    NATIONAL = "National"
    INTERNATIONAL = "International"


class NewsArticleBase(BaseModel):
    """Base model for news articles with validation"""
    url: str  # Make URL primary identifier
    title: Optional[str] = None
    source_name: Optional[str] = None
    content: Optional[str] = None  # Store the main extracted text
    summary: Optional[str] = None  # LLM generated
    published_at: Optional[datetime] = None  # Use timezone-aware datetime objects
    fetched_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    location_query: Optional[str] = None
    category: Optional[str] = None  # LLM generated
    importance: Optional[str] = None  # LLM generated: Critical, High, Medium, Low

    class Config:
        # Allow creation from dataclass objects
        from_attributes = True
        # Allow assignments to frozen instances (equivalent to @dataclass(frozen=False))
        validate_assignment = True
        # Validate URLs
        json_encoders = {
            datetime: lambda dt: dt.isoformat(),
        }

    @validator('importance')
    def validate_importance(cls, v):
        """Validate that importance is one of the allowed values"""
        if v is None:
            return v
        if v not in [level.value for level in ImportanceLevel]:
            raise ValueError(f"Importance must be one of: {', '.join([level.value for level in ImportanceLevel])}")
        return v

    @validator('url')
    def validate_url(cls, v):
        """Basic URL validation"""
        if not (v.startswith('http://') or v.startswith('https://')):
            raise ValueError("URL must start with http:// or https://")
        return v

    def to_dict(self) -> Dict:
        """Convert model to a dictionary, handling datetime objects"""
        return self.model_dump(
            exclude_none=True,
            mode='json',
        )


# For backwards compatibility with existing code using dataclass
class NewsArticle(NewsArticleBase):
    """Pydantic model for news articles that maintains compatibility"""
    pass


class NewsSourceBase(BaseModel):
    """Base model for news sources with validation"""
    name: str
    url: str  # Homepage URL, validated
    location_type: str  # e.g., Local, National/Local
    reliability_score: float = Field(default=0.8, ge=0.0, le=1.0)
    rss_feed_url: Optional[str] = None  # For RSS implementation

    @validator('location_type')
    def validate_location_type(cls, v):
        """Validate that location_type is one of the allowed values"""
        if v not in [loc_type.value for loc_type in LocationType]:
            # Be lenient with existing data, but log warning
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Non-standard location_type: {v}. Allowed: {', '.join([lt.value for lt in LocationType])}")
        return v

    @validator('url', 'rss_feed_url')
    def validate_url(cls, v):
        """Basic URL validation if value is provided"""
        if v is not None and not (v.startswith('http://') or v.startswith('https://')):
            raise ValueError("URL must start with http:// or https://")
        return v

    class Config:
        # Allow creation from dataclass objects
        from_attributes = True
        # Validate URLs
        json_encoders = {
            datetime: lambda dt: dt.isoformat(),
        }


# For backwards compatibility with existing code using dataclass
class NewsSource(NewsSourceBase):
    """Pydantic model for news sources that maintains compatibility"""
    pass