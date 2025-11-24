# main.py
from typing import Optional, Dict, Any, List, Tuple
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
import asyncio, time

import storage as st

app = FastAPI(title="Mini Pub/Sub (SQLite)")

# --------- models ----------
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

# --------- in-proc coordination (for long-poll) ----------
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

# --------- endpoints ----------
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

    # fast path: try immediate read
    recs = st.poll_records(topic, start, max_records)
    if recs:
        return {"topic": topic, "records": recs, "next_offset": start + len(recs), "committed": committed}

    # long-poll until timeout or a publish arrives
    if timeout_ms > 0:
        waited = await _notifier(topic).wait(timeout_ms / 1000.0)
        if waited:
            recs = st.poll_records(topic, start, max_records)

    return {"topic": topic, "records": recs or [], "next_offset": start + len(recs or []), "committed": committed}

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
