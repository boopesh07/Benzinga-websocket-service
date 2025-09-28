from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class Security(BaseModel):
    symbol: Optional[str] = None
    exchange: Optional[str] = None
    primary: Optional[bool] = None


class Content(BaseModel):
    id: Optional[int] = None
    revision_id: Optional[int] = None
    type: Optional[str] = None
    title: Optional[str] = None
    body: Optional[str] = None
    authors: List[str] = Field(default_factory=list)
    teaser: Optional[str] = None
    url: Optional[str] = None
    channels: List[str] = Field(default_factory=list)
    securities: List[Security] = Field(default_factory=list)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


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
    timestamp: datetime
    ticker: str
    news_id: int
    title: Optional[str] = None
    body: Optional[str] = None
    authors: List[str] = Field(default_factory=list)
    url: Optional[str] = None
    teaser: Optional[str] = None
    channels: List[str] = Field(default_factory=list)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_ndjson(self) -> str:
        return self.model_dump_json() + "\n"


def try_extract_output(msg: StreamMessage) -> Optional[OutputRecord]:
    securities = msg.data.content.securities or []
    if not securities:
        return None
    symbol = next((s.symbol for s in securities if s and s.symbol), None)
    if not symbol:
        return None
    return OutputRecord(
        timestamp=msg.data.timestamp,
        ticker=symbol,
        news_id=msg.data.id,
        title=msg.data.content.title,
        body=msg.data.content.body,
        authors=msg.data.content.authors,
        url=msg.data.content.url,
        teaser=msg.data.content.teaser,
        channels=msg.data.content.channels,
        created_at=msg.data.content.created_at,
        updated_at=msg.data.content.updated_at,
    )

