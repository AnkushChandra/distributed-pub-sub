from __future__ import annotations
from typing import Optional, Dict, Any, List, Tuple
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
import asyncio, time
import aiohttp
import time  
import storage as st
import config as cfg
from gossip import build_state, GossipService, GossipState


app = FastAPI(title="Mini Pub/Sub (SQLite) + Gossip")
USER_MESSAGES_TOPIC = "user-messages"


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

def _now_ms() -> int:
    return int(time.time() * 1000)

class ChatMessageIn(BaseModel):
    user: str
    text: str

class ChatMessageOut(BaseModel):
    offset: int
    user: str
    text: str
    ts_ms: int

class TopicNotifier:
    def __init__(self, topic: str):
        self.topic = topic
        self.cond = asyncio.Condition()

    async def notify(self):
        async with self.cond:
            self.cond.notify_all()

    async def wait(self, timeout: float) -> bool:
        async with self.cond:
            try:
                await asyncio.wait_for(self.cond.wait(), timeout=timeout)
                return True
            except asyncio.TimeoutError:
                return False


_notifiers: Dict[str, TopicNotifier] = {}
_group_leases: Dict[Tuple[str, str], float] = {}
LEASE_TTL_SEC = 10.0


def _notifier(topic: str) -> TopicNotifier:
    if topic not in _notifiers:
        st.init_topic(topic)  # ensures topic_seq row
        _notifiers[topic] = TopicNotifier(topic)
    return _notifiers[topic]

def _enforce_group_singleton(topic: str, group_id: Optional[str]):
    if not group_id:
        return
    now = time.time()
    k = (topic, group_id)
    if _group_leases.get(k, 0.0) > now:
        raise HTTPException(status_code=409, detail="Group already has an active consumer")
    _group_leases[k] = now + LEASE_TTL_SEC

def _committed(topic: str, consumer_id: Optional[str], group_id: Optional[str]) -> int:
    return st.get_commit(topic, consumer_id, group_id)

def _commit(topic: str, consumer_id: Optional[str], group_id: Optional[str], offset: int) -> None:
    st.set_commit(topic, consumer_id, group_id, offset)
    if group_id:
        _group_leases[(topic, group_id)] = time.time() + LEASE_TTL_SEC

_gossip_state: GossipState = build_state(cfg.BROKER_ID, cfg.BROKER_ADDR)
_http_session: aiohttp.ClientSession | None = None
_gossip: GossipService | None = None


@app.on_event("startup")
async def _startup():
    """
    On startup we only start gossip for membership.
    No user messages are sent via gossip.
    """
    global _http_session, _gossip
    _http_session = aiohttp.ClientSession()
    _gossip = GossipService(_gossip_state, _http_session)
    await _gossip.start(cfg.SEED_PEERS)


@app.on_event("shutdown")
async def _shutdown():
    global _http_session, _gossip
    if _gossip:
        await _gossip.stop()
    if _http_session:
        await _http_session.close()

@app.get("/cluster")
def cluster():
    return {
        "self": {"id": _gossip_state.self_id, "addr": _gossip_state.self_addr},
        "members": [
            {
                "node_id": m.node_id,
                "addr": m.addr,
                "status": m.status,
                "incarnation": m.incarnation,
                "last_seen": m.last_seen,
            }
            for m in sorted(_gossip_state.members.values(), key=lambda x: x.node_id)
        ],
    }


@app.post("/_gossip/pushpull")
async def gossip_pushpull(payload: Dict[str, Any]):
    remote = payload.get("membership", [])
    _gossip_state.merge_remote_view(remote)
    return {"membership": _gossip_state.serialize_view()}



@app.post("/topics/{topic}/publish")
async def publish(topic: str, req: PublishRequest):
    off = st.append_record(topic, req.key, req.value, req.headers)
    await _notifier(topic).notify()
    return {"topic": topic, "offset": off}


@app.post("/subscribe")
async def subscribe(sub: SubscribeRequest):
    _enforce_group_singleton(sub.topic, sub.group_id)
    committed = _committed(sub.topic, sub.consumer_id, sub.group_id)
    start = sub.from_offset if sub.from_offset is not None else (committed + 1)
    return {"topic": sub.topic, "start_offset": max(start, 0), "committed": committed}


@app.get("/poll")
async def poll(
    topic: str = Query(...),
    consumer_id: Optional[str] = Query(None),
    group_id: Optional[str] = Query(None),
    from_offset: Optional[int] = Query(None),
    max_records: int = Query(100, ge=1, le=1000),
    timeout_ms: int = Query(15000, ge=0, le=60000),
):
    committed = _committed(topic, consumer_id, group_id)
    start = from_offset if from_offset is not None else (committed + 1)
    start = max(start, 0)

    # fast path
    recs = st.poll_records(topic, start, max_records)
    if recs:
        return {
            "topic": topic,
            "records": recs,
            "next_offset": start + len(recs),
            "committed": committed,
        }

    # long-poll
    if timeout_ms > 0:
        waited = await _notifier(topic).wait(timeout_ms / 1000.0)
        if waited:
            recs = st.poll_records(topic, start, max_records)

    return {
        "topic": topic,
        "records": recs or [],
        "next_offset": start + len(recs or []),
        "committed": committed,
    }


@app.post("/commit")
async def commit(c: CommitRequest):
    if c.offset < -1:
        raise HTTPException(status_code=400, detail="invalid offset")
    _commit(c.topic, c.consumer_id, c.group_id, c.offset)
    return {"topic": c.topic, "committed": c.offset}


@app.get("/metrics")
def metrics():
    return {
        "leases": {f"{k[0]}|{k[1]}": exp for k, exp in _group_leases.items()},
    }

# topic used just for storing chat messages in SQLite
USER_MESSAGES_TOPIC = "user-messages"


class ChatMessageIn(BaseModel):
    user: str
    text: str


class ChatMessageOut(BaseModel):
    offset: int
    user: str
    text: str
    ts_ms: int


def _now_ms() -> int:
    return int(time.time() * 1000)


@app.post("/chat/send")
async def chat_send(msg: ChatMessageIn):
    value = {
        "user": msg.user,
        "text": msg.text,
        "ts_ms": _now_ms(),
    }
    off = st.append_record(USER_MESSAGES_TOPIC, key=None, value=value, headers={})
    await _notifier(USER_MESSAGES_TOPIC).notify()
    return {"topic": USER_MESSAGES_TOPIC, "offset": off}


@app.get("/chat/history", response_model=List[ChatMessageOut])
async def chat_history(
    from_offset: int = Query(0, ge=0),
    max_records: int = Query(50, ge=1, le=500),
):
    recs = st.poll_records(USER_MESSAGES_TOPIC, from_offset, max_records)
    out: List[ChatMessageOut] = []
    for r in recs:
        v = r["value"]
        out.append(
            ChatMessageOut(
                offset=r["offset"],
                user=v.get("user", ""),
                text=v.get("text", ""),
                ts_ms=v.get("ts_ms", 0),
            )
        )
    return out

