from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, List, Optional, TYPE_CHECKING

from pydantic import BaseModel, Field, field_validator

from app.text_utils import strip_html_tags

if TYPE_CHECKING:
    pass


logger = logging.getLogger(__name__)


class Security(BaseModel):
    """Represents a security/ticker symbol from the API.
    
    Note: The API may send securities in various formats (dict, str, etc.),
    so this model is used for structured data when available.
    """
    symbol: Optional[str] = None
    exchange: Optional[str] = None
    primary: Optional[bool] = None


class Content(BaseModel):
    """News content data from Benzinga API.
    
    The securities field accepts List[Any] to handle API flexibility:
    - Can be objects with {symbol, exchange, primary}
    - Can be plain strings
    - Can be other formats
    """
    id: Optional[int] = None
    revision_id: Optional[int] = None
    type: Optional[str] = None
    title: Optional[str] = None
    body: Optional[str] = None
    authors: List[str] = Field(default_factory=list)
    teaser: Optional[str] = None
    url: Optional[str] = None
    channels: List[str] = Field(default_factory=list)
    securities: List[Any] = Field(default_factory=list)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    @field_validator('securities', 'authors', 'channels', mode='before')
    @classmethod
    def ensure_list_fields(cls, v):
        """Ensure list fields are always lists, even if API sends null or non-list."""
        if v is None:
            return []
        if not isinstance(v, list):
            return [v]
        return v


class DataMessage(BaseModel):
    action: Optional[str] = None
    id: int
    timestamp: datetime
    content: Content


class StreamMessage(BaseModel):
    api_version: Optional[str] = None
    kind: Optional[str] = None
    data: DataMessage


class OutputRecord(BaseModel):
    """Output record written to NDJSON files/S3.
    
    Each record represents a single news item with an associated ticker.
    If a news item has multiple tickers, multiple records will be generated.
    
    Optimized for Knowledge Base with LLM-generated summaries:
    - action: Article action (Created, Updated, etc.)
    - content: Claude-generated 200-word summary
    - Compact, focused format for embeddings
    """
    timestamp: int  # Epoch milliseconds
    ticker: str
    news_id: int
    action: str  # "Created", "Updated", etc.
    title: Optional[str] = None
    content: Optional[str] = None  # LLM-generated summary (~200 words)
    authors: List[str] = Field(default_factory=list)
    url: Optional[str] = None
    channels: List[str] = Field(default_factory=list)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_ndjson(self) -> str:
        """Serialize to NDJSON format (JSON + newline)."""
        return self.model_dump_json() + "\n"


def _extract_symbol(security: Any) -> Optional[str]:
    """Extract symbol from a security entry (can be dict, string, or object).
    
    Args:
        security: Can be a dict with 'symbol' key, a string, or a Security object
        
    Returns:
        The extracted symbol string, or None if not found
    """
    if isinstance(security, dict):
        symbol = security.get('symbol')
        if symbol and isinstance(symbol, str):
            return symbol
    elif isinstance(security, str):
        # API may send plain string tickers
        return security.strip() if security.strip() else None
    elif hasattr(security, 'symbol'):
        # Security object or similar
        symbol = getattr(security, 'symbol', None)
        if symbol and isinstance(symbol, str):
            return symbol
    return None


def extract_all_outputs(
    msg: StreamMessage,
    summary: str,
) -> List[OutputRecord]:
    """Extract multiple output records from a stream message (one per ticker).
    
    This is the RECOMMENDED approach as it ensures that queries for any
    ticker mentioned in the news will return the article.
    
    For multi-ticker articles, the summary is generated once and reused
    for all ticker records (efficient and consistent).
    
    Args:
        msg: The incoming stream message
        summary: The pre-generated summary for the article's content
        
    Returns:
        List of OutputRecord, one for each valid ticker found
    """
    # Extract all valid tickers
    securities = msg.data.content.securities or []
    tickers = [sym for sec in securities if (sym := _extract_symbol(sec))]
    
    if not tickers:
        return []
    
    # Create one record per ticker with the same summary
    records = [
        OutputRecord(
            timestamp=int(msg.data.timestamp.timestamp() * 1000),
            ticker=ticker,
            news_id=msg.data.id,
            action=msg.data.action or "Created",
            title=msg.data.content.title,
            content=summary,
            authors=msg.data.content.authors or [],
            url=msg.data.content.url,
            channels=msg.data.content.channels or [],
            created_at=msg.data.content.created_at,
            updated_at=msg.data.content.updated_at,
        )
        for ticker in tickers
    ]
    
    if len(records) > 1:
        logger.debug("multi-ticker-article news_id=%s tickers=%d", msg.data.id, len(records))
    
    return records

