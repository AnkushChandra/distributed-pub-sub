# main.py
from typing import Optional, Dict, Any, Tuple
from fastapi import FastAPI, HTTPException, Query, status
from pydantic import BaseModel, Field
import asyncio, time, os, threading
import httpx

import storage as st
from election import (
    start_election_service,
    stop_election_service,
    is_cluster_leader,
    get_leader_id,
)

# -------------------- Broker identity & API peers --------------------
# This node's ID (must match bully_impl.CONFIG ids)
BROKER_ID = int(os.getenv("BROKER_ID", "1"))

# Map broker id -> API base URL (used for routing/proxying)
# You can override with JSON in BROKER_API_PEERS='{"1":"http://127.0.0.1:8081",...}'
DEFAULT_API = {
    1: "http://127.0.0.1:8081",
    2: "http://127.0.0.1:8082",
    3: "http://127.0.0.1:8083",
    4: "http://127.0.0.1:8084",
    5: "http://127.0.0.1:8085",
}
import json as _json
_api_env = os.getenv("BROKER_API_PEERS")
BROKER_API: Dict[int, str] = DEFAULT_API if not _api_env else {int(k): v for k, v in _json.loads(_api_env).items()}

# -------------------- FastAPI app --------------------
app = FastAPI(title="Mini Pub/Sub (SQLite + Bully + Routed Reads)")

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
        st.init_topic(topic)  # ensures topic_seq row exists if local owner uses it
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

# -------------------- Topic ownership/placement --------------------
# For now: single owner per topic (no replication/partitions)
# Authoritative on the LEADER; followers cache via /metadata.
TOPIC_METADATA_FILE = st.DATA_DIR / "topics.json"
_topic_meta_lock = threading.RLock()

def _load_persisted_metadata() -> Dict[str, int]:
    if not TOPIC_METADATA_FILE.exists():
        return {}
    try:
        text = TOPIC_METADATA_FILE.read_text()
        data = _json.loads(text)
        return {str(k): int(v) for k, v in data.items()}
    except Exception as e:
        print(f"[P{BROKER_ID}] failed to load topic metadata: {e}")
        return {}

def _persist_metadata_snapshot(snapshot: Dict[str, int]):
    with _topic_meta_lock:
        try:
            tmp_path = TOPIC_METADATA_FILE.with_suffix(".tmp")
            tmp_path.write_text(_json.dumps(snapshot, sort_keys=True))
            tmp_path.replace(TOPIC_METADATA_FILE)
        except Exception as e:
            print(f"[P{BROKER_ID}] failed to persist topic metadata: {e}")

_boot_meta = _load_persisted_metadata()
TOPIC_OWNER: Dict[str, int] = dict(_boot_meta)          # leader: topic -> owner_id
OWNER_CACHE: Dict[str, int] = dict(_boot_meta)          # follower cache of topic -> owner_id
OWNER_CACHE_TTL_SEC = 10.0
_owner_cache_expiry: float = 0.0

def _set_topic_owner(topic: str, owner_id: int):
    TOPIC_OWNER[topic] = owner_id
    _persist_metadata_snapshot(TOPIC_OWNER)

def _get_leader_api() -> str:
    leader_id = get_leader_id()
    if leader_id is None or leader_id not in BROKER_API:
        raise HTTPException(status_code=503, detail="No leader available")
    return BROKER_API[leader_id]

async def _refresh_owner_cache_if_needed():
    global _owner_cache_expiry
    if is_cluster_leader():
        # Leader doesn't use cache; it is the source of truth
        return
    now = time.time()
    if now < _owner_cache_expiry:
        return
    # fetch from leader
    leader_api = _get_leader_api()
    url = f"{leader_api}/metadata"
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            r = await client.get(url)
            r.raise_for_status()
            meta = r.json()
            topics = meta.get("topics", {})
            OWNER_CACHE.clear()
            for t, owner in topics.items():
                OWNER_CACHE[t] = int(owner)
            # small TTL
            _owner_cache_expiry = time.time() + OWNER_CACHE_TTL_SEC
    except Exception as e:
        # don't blow up the request; we'll retry next time
        print(f"[P{BROKER_ID}] failed to refresh metadata from leader: {e}")

def _local_is_owner(topic: str) -> bool:
    if is_cluster_leader():
        owner = TOPIC_OWNER.get(topic)
    else:
        owner = OWNER_CACHE.get(topic)
    return owner == BROKER_ID

def _owner_for(topic: str) -> Optional[int]:
    if is_cluster_leader():
        return TOPIC_OWNER.get(topic)
    return OWNER_CACHE.get(topic)

# -------------------- Lifecycle hooks --------------------
@app.on_event("startup")
async def _on_startup():
    start_election_service()

@app.on_event("shutdown")
async def _on_shutdown():
    stop_election_service()

# -------------------- Admin & metadata --------------------
@app.get("/admin/leader")
def admin_leader():
    return {
        "broker_id": BROKER_ID,
        "this_broker_is_leader": is_cluster_leader(),
        "leader_id": get_leader_id(),
    }

@app.get("/metadata")
def metadata():
    """
    Returns placement info. On the leader: authoritative topic->owner map.
    On followers: responds with whatever it currently knows (may be stale).
    """
    if is_cluster_leader():
        return {"leader_id": get_leader_id(), "topics": TOPIC_OWNER}
    else:
        return {"leader_id": get_leader_id(), "topics": OWNER_CACHE}

@app.post("/admin/topics")
def admin_create_or_assign_topic(
    name: str,
    owner_id: Optional[int] = None,
):
    """
    Leader-only:
      - create a topic and assign an owner (broker id).
      - if owner_id is omitted, default to leader itself.
    """
    if not is_cluster_leader():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"error": "NOT_LEADER", "leader_id": get_leader_id()},
        )
    if owner_id is None:
        owner_id = get_leader_id()
    if owner_id not in BROKER_API:
        raise HTTPException(status_code=400, detail=f"Unknown owner_id {owner_id}")

    _set_topic_owner(name, int(owner_id))
    # If this node is the owner, ensure local topic exists now
    if owner_id == BROKER_ID:
        st.init_topic(name)

    return {"status": "ok", "topic": name, "owner_id": owner_id, "leader_id": get_leader_id()}

# -------------------- Routing helpers (subscribe/poll proxy) --------------------
async def _proxy_get(base: str, path: str, params: Dict[str, Any]) -> Any:
    url = f"{base}{path}"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(url, params=params)
    except httpx.RequestError as e:
        # couldn't reach owner (connection refused, timeout, DNS, etc.)
        raise HTTPException(status_code=502, detail={
            "error": "UPSTREAM_UNREACHABLE",
            "upstream": url,
            "reason": str(e),
        })
    # pass through upstream non-2xx as-is (not a 500)
    ct = (r.headers.get("content-type") or "").lower()
    body = None
    if "application/json" in ct:
        try:
            body = r.json()
        except Exception:
            body = {"raw": r.text}
    else:
        body = {"raw": r.text}
    if r.status_code >= 400:
        raise HTTPException(status_code=r.status_code, detail={
            "error": "UPSTREAM_ERROR",
            "upstream": url,
            "status": r.status_code,
            "body": body,
        })
    return body

async def _proxy_post(base: str, path: str, body: Dict[str, Any]) -> Any:
    url = f"{base}{path}"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(url, json=body)
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail={
            "error": "UPSTREAM_UNREACHABLE",
            "upstream": url,
            "reason": str(e),
        })
    ct = (r.headers.get("content-type") or "").lower()
    resp = None
    if "application/json" in ct:
        try:
            resp = r.json()
        except Exception:
            resp = {"raw": r.text}
    else:
        resp = {"raw": r.text}
    if r.status_code >= 400:
        raise HTTPException(status_code=r.status_code, detail={
            "error": "UPSTREAM_ERROR",
            "upstream": url,
            "status": r.status_code,
            "body": resp,
        })
    return resp

def _owner_base(owner_id: int) -> str:
    base = BROKER_API.get(owner_id)
    if not base:
        raise HTTPException(status_code=503, detail=f"No API base for owner {owner_id}")
    return base

# -------------------- Pub/Sub endpoints --------------------
@app.post("/topics/{topic}/publish")
async def publish(topic: str, req: PublishRequest):
    await _refresh_owner_cache_if_needed()
    owner = _owner_for(topic)
    if owner is None:
        raise HTTPException(status_code=404, detail="Topic not found (no owner)")

    if owner != BROKER_ID:
        base = _owner_base(owner)
        print(f"[P{BROKER_ID}] routing publish for topic={topic} to P{owner}")
        return await _proxy_post(base, f"/topics/{topic}/publish", req.model_dump())
    off = st.append_record(topic, req.key, req.value, req.headers)
    await _notifier(topic).notify()
    return {"topic": topic, "offset": off}

@app.post("/subscribe")
async def subscribe(sub: SubscribeRequest):
    # If this node doesn't own the topic, route to owner transparently
    await _refresh_owner_cache_if_needed()
    owner = _owner_for(sub.topic)
    if owner is None:
        # Topic may not be created yet
        raise HTTPException(status_code=404, detail="Topic not found (no owner)")

    if owner != BROKER_ID:
        base = _owner_base(owner)
        print(f"[P{BROKER_ID}] routing subscribe for topic={sub.topic} to P{owner} (messages coming from {owner})")
        return await _proxy_post(base, "/subscribe", sub.model_dump())

    # Local owner path
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
    # Route to owner if needed
    await _refresh_owner_cache_if_needed()
    owner = _owner_for(topic)
    if owner is None:
        raise HTTPException(status_code=404, detail="Topic not found (no owner)")

    if owner != BROKER_ID:
        base = _owner_base(owner)
        print(f"[P{BROKER_ID}] routing poll for topic={topic} to P{owner} (messages coming from {owner})")
        return await _proxy_get(
            base,
            "/poll",
            {
                "topic": topic,
                "consumer_id": consumer_id,
                "group_id": group_id,
                "from_offset": from_offset,
                "max_records": max_records,
                "timeout_ms": timeout_ms,
            },
        )

    # Local owner fast path
    committed = _committed(topic, consumer_id, group_id)
    start = from_offset if from_offset is not None else (committed + 1)
    start = max(start, 0)

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
        "broker_id": BROKER_ID,
        "leases": {f"{k[0]}|{k[1]}": exp for k, exp in _group_leases.items()},
        "owned_topics": [t for t, owner in TOPIC_OWNER.items() if owner == BROKER_ID] if is_cluster_leader() else [],
    }
