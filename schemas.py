from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class PublishRequest(BaseModel):
    key: Optional[str] = None
    value: Any
    headers: Dict[str, Any] = Field(default_factory=dict)


class SubscribeRequest(BaseModel):
    topic: str
    consumer_id: Optional[str] = None
    group_id: Optional[str] = None
    from_offset: Optional[int] = None


class CommitRequest(BaseModel):
    topic: str
    offset: int
    consumer_id: Optional[str] = None
    group_id: Optional[str] = None


class GossipPushPullRequest(BaseModel):
    from_id: str
    from_addr: str
    membership: List[Dict[str, Any]] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ReplicateRequest(BaseModel):
    offset: int
    key: Optional[str] = None
    value: Any
    headers: Dict[str, Any] = Field(default_factory=dict)
