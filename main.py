def _gossip_payload() -> Dict[str, Any]:
    return {
        "metadata": _metadata_snapshot(),
    }

def _handle_gossip_payload(payload: Dict[str, Any]) -> None:
    if not payload:
        return
    meta = payload.get("metadata")
    if meta:
        _maybe_update_metadata_from_remote(meta)
# main.py
from typing import Optional, Dict, Any, Tuple, List
from fastapi import FastAPI, HTTPException, Query, status
from pydantic import BaseModel, Field
import asyncio, time, os, threading, random
import httpx, aiohttp

import gossip
import replication

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

class GossipPushPullRequest(BaseModel):
    from_id: str
    from_addr: str
    membership: List[Dict[str, Any]] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)

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
Placement = Dict[str, Any]
# Authoritative on the LEADER; followers cache via /metadata.
TOPIC_METADATA_FILE = st.DATA_DIR / "topics.json"
_topic_meta_lock = threading.RLock()
_metadata_version: int = 0

def _normalize_placement(entry: Any) -> Placement:
    owner = None
    followers: List[int] = []
    if isinstance(entry, dict):
        owner = entry.get("owner")
        followers = entry.get("followers", [])
    elif isinstance(entry, list) and entry:
        owner = entry[0]
        followers = entry[1:]
    else:
        owner = entry
        followers = []
    try:
        owner_int = int(owner)
    except (TypeError, ValueError):
        owner_int = None
    follower_ints: List[int] = []
    for f in followers:
        try:
            follower_ints.append(int(f))
        except (TypeError, ValueError):
            continue
    deduped = sorted(dict.fromkeys(follower_ints))
    return {"owner": owner_int, "followers": deduped}

def _clone_placements(src: Dict[str, Placement]) -> Dict[str, Placement]:
    return {
        str(topic): {
            "owner": placement.get("owner"),
            "followers": list(placement.get("followers", [])),
        }
        for topic, placement in src.items()
    }

def _load_persisted_metadata() -> Tuple[int, Dict[str, Placement]]:
    if not TOPIC_METADATA_FILE.exists():
        return 0, {}
    try:
        text = TOPIC_METADATA_FILE.read_text()
        data = _json.loads(text)
        if isinstance(data, dict) and "topics" in data:
            topics_raw = data.get("topics", {})
            version = int(data.get("version", 0))
        else:
            topics_raw = data if isinstance(data, dict) else {}
            version = 0
        placements = {str(k): _normalize_placement(v) for k, v in (topics_raw or {}).items()}
        return version, placements
    except Exception as e:
        print(f"[P{BROKER_ID}] failed to load topic metadata: {e}")
    return 0, {}

def _persist_metadata_snapshot(snapshot: Dict[str, Placement], version: int):
    with _topic_meta_lock:
        try:
            payload = {"version": version, "topics": snapshot}
            tmp_path = TOPIC_METADATA_FILE.with_suffix(".tmp")
            tmp_path.write_text(_json.dumps(payload, sort_keys=True))
            tmp_path.replace(TOPIC_METADATA_FILE)
        except Exception as e:
            print(f"[P{BROKER_ID}] failed to persist topic metadata: {e}")

TOPIC_PLACEMENTS: Dict[str, Placement] = {}
PLACEMENT_CACHE: Dict[str, Placement] = {}
PLACEMENT_CACHE_TTL_SEC = 10.0
_placement_cache_expiry: float = 0.0
_gossip_state: Optional[gossip.GossipState] = None
_gossip_service: Optional[gossip.GossipService] = None
_gossip_http: Optional[aiohttp.ClientSession] = None
_replication_task: Optional[asyncio.Task] = None
_failover_task: Optional[asyncio.Task] = None
_metadata_sync_task: Optional[asyncio.Task] = None

def _self_api_base() -> str:
    base = BROKER_API.get(BROKER_ID)
    if not base:
        raise RuntimeError(f"No API base configured for broker {BROKER_ID}")
    return base

def _apply_metadata_snapshot(snapshot: Dict[str, Placement], version: int, *, persist: bool):
    global _metadata_version, _placement_cache_expiry
    with _topic_meta_lock:
        TOPIC_PLACEMENTS.clear()
        TOPIC_PLACEMENTS.update(_clone_placements(snapshot))
        PLACEMENT_CACHE.clear()
        PLACEMENT_CACHE.update(_clone_placements(snapshot))
        _metadata_version = version
        if persist:
            _persist_metadata_snapshot(_clone_placements(TOPIC_PLACEMENTS), _metadata_version)
    _placement_cache_expiry = time.time() + PLACEMENT_CACHE_TTL_SEC

def _metadata_snapshot() -> Dict[str, Any]:
    with _topic_meta_lock:
        return {"version": _metadata_version, "topics": _clone_placements(TOPIC_PLACEMENTS)}

def _placement_source() -> Dict[str, Placement]:
    return TOPIC_PLACEMENTS if is_cluster_leader() else PLACEMENT_CACHE

def _owner_for(topic: str) -> Optional[int]:
    placement = _placement_source().get(topic)
    if not placement:
        return None
    owner = placement.get("owner")
    try:
        return int(owner) if owner is not None else None
    except (TypeError, ValueError):
        return None

def _followers_for(topic: str, *, leader_view: bool = False) -> List[int]:
    src = TOPIC_PLACEMENTS if leader_view else _placement_source()
    placement = src.get(topic)
    if not placement:
        return []
    followers = placement.get("followers", [])
    return list(followers) if isinstance(followers, list) else []

def _maybe_update_metadata(snapshot: Dict[str, Placement], version: int, *, persist: bool):
    global _metadata_version
    with _topic_meta_lock:
        current = _metadata_version
    if version <= current:
        return
    _apply_metadata_snapshot(snapshot, version, persist=persist)

def _maybe_update_metadata_from_remote(meta: Dict[str, Any]):
    if not meta:
        return
    topics = meta.get("topics")
    version = meta.get("version")
    if topics is None or version is None:
        return
    try:
        version_int = int(version)
        snapshot = {str(k): _normalize_placement(v) for k, v in topics.items()}
    except Exception:
        return
    _maybe_update_metadata(snapshot, version_int, persist=True)

def _set_topic_placement(topic: str, owner_id: int, followers: Optional[List[int]] = None):
    followers_list = sorted(dict.fromkeys(int(f) for f in (followers or []) if f is not None))
    placement = {"owner": int(owner_id), "followers": followers_list}
    with _topic_meta_lock:
        snapshot = _clone_placements(TOPIC_PLACEMENTS)
        snapshot[topic] = placement
        version = _metadata_version + 1
    _apply_metadata_snapshot(snapshot, version, persist=True)

def _set_topic_owner(topic: str, owner_id: int):
    followers = _followers_for(topic, leader_view=True)
    _set_topic_placement(topic, owner_id, followers)

_boot_version, _boot_meta = _load_persisted_metadata()
_apply_metadata_snapshot(_boot_meta, _boot_version, persist=False)

def _peer_map() -> Dict[str, str]:
    return {str(bid): base for bid, base in BROKER_API.items() if bid != BROKER_ID}

def _alive_brokers() -> List[int]:
    if _gossip_state is None:
        return [BROKER_ID]
    alive: List[int] = []
    for member in _gossip_state.members.values():
        if member.status != "alive":
            continue
        try:
            bid = int(member.node_id)
        except ValueError:
            continue
        if bid in BROKER_API:
            alive.append(bid)
    if not alive:
        alive.append(BROKER_ID)
    return alive

def _select_followers(owner_id: int, replication_factor: int) -> List[int]:
    desired = max(0, replication_factor - 1)
    if desired == 0:
        return []
    candidates = [bid for bid in _alive_brokers() if bid != owner_id]
    if not candidates:
        return []
    if len(candidates) <= desired:
        return sorted(candidates)
    return sorted(random.sample(candidates, desired))

def _broker_is_alive(bid: int) -> bool:
    if _gossip_state is None:
        return True
    member = _gossip_state.members.get(str(bid))
    if member is None:
        return False
    return member.status == "alive"

def _maybe_promote_topic_owner(topic: str) -> None:
    if not is_cluster_leader():
        return
    placement = TOPIC_PLACEMENTS.get(topic)
    if not placement:
        return
    owner = placement.get("owner")
    if owner is None or _broker_is_alive(int(owner)):
        return
    followers = list(placement.get("followers", []))
    for candidate in followers:
        if _broker_is_alive(candidate):
            new_followers = [f for f in followers if f != candidate]
            new_followers.append(int(owner))
            _set_topic_placement(topic, candidate, new_followers)
            if candidate == BROKER_ID:
                st.init_topic(topic)
            print(f"[P{BROKER_ID}] promoted topic={topic} owner -> P{candidate}")
            return

def _get_leader_api() -> str:
    leader_id = get_leader_id()
    if leader_id is None or leader_id not in BROKER_API:
        raise HTTPException(status_code=503, detail="No leader available")
    return BROKER_API[leader_id]

async def _refresh_owner_cache_if_needed():
    global _placement_cache_expiry
    if is_cluster_leader():
        return
    now = time.time()
    if now < _placement_cache_expiry:
        return
    leader_api = _get_leader_api()
    url = f"{leader_api}/metadata"
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            r = await client.get(url)
            r.raise_for_status()
            meta = r.json()
    except Exception as e:
        print(f"[P{BROKER_ID}] failed to refresh metadata from leader: {e}")
        return
    topics = meta.get("topics", {})
    version = meta.get("version")
    try:
        version_int = int(version)
    except (TypeError, ValueError):
        version_int = _metadata_version + 1
    snapshot = {str(topic): _normalize_placement(entry) for topic, entry in topics.items()}
    _maybe_update_metadata(snapshot, version_int, persist=True)
    _placement_cache_expiry = time.time() + PLACEMENT_CACHE_TTL_SEC

def _local_is_owner(topic: str) -> bool:
    owner = _owner_for(topic)
    return owner == BROKER_ID

# -------------------- Lifecycle hooks --------------------
@app.on_event("startup")
async def _on_startup():
    start_election_service()
    await _start_gossip()
    replication.setup(
        broker_id=BROKER_ID,
        broker_api=BROKER_API,
        placement_getter=lambda: _metadata_snapshot()["topics"],
        refresh_fn=_refresh_owner_cache_if_needed,
    )
    await _start_replication()
    await _start_failover_monitor()
    await _start_metadata_sync()

@app.on_event("shutdown")
async def _on_shutdown():
    await _stop_metadata_sync()
    await _stop_failover_monitor()
    await _stop_replication()
    await _stop_gossip()
    stop_election_service()

async def _start_gossip():
    global _gossip_state, _gossip_service, _gossip_http
    if _gossip_state is not None:
        return
    state = gossip.GossipState(str(BROKER_ID), _self_api_base())
    http = aiohttp.ClientSession()
    service = gossip.GossipService(
        state,
        http,
        payload_fn=_gossip_payload,
        on_payload_fn=_handle_gossip_payload,
    )
    await service.start(_peer_map())
    _gossip_state = state
    _gossip_service = service
    _gossip_http = http

async def _stop_gossip():
    global _gossip_state, _gossip_service, _gossip_http
    service = _gossip_service
    if service is not None:
        await service.stop()
    if _gossip_http is not None:
        await _gossip_http.close()
    _gossip_http = None
    _gossip_service = None
    _gossip_state = None

async def _start_replication():
    global _replication_task
    if _replication_task is not None:
        return
    _replication_task = asyncio.create_task(replication.follower_replication_loop())

async def _stop_replication():
    global _replication_task
    task = _replication_task
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    _replication_task = None

async def _start_metadata_sync():
    global _metadata_sync_task
    if _metadata_sync_task is not None:
        return
    _metadata_sync_task = asyncio.create_task(_metadata_sync_loop())

async def _stop_metadata_sync():
    global _metadata_sync_task
    task = _metadata_sync_task
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    _metadata_sync_task = None

async def _metadata_sync_loop():
    while True:
        try:
            if not is_cluster_leader():
                await _refresh_owner_cache_if_needed()
            await asyncio.sleep(2.0)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[P{BROKER_ID}] metadata sync error: {e}")
            await asyncio.sleep(2.0)

async def _start_failover_monitor():
    global _failover_task
    if _failover_task is not None:
        return
    _failover_task = asyncio.create_task(_failover_loop())

async def _stop_failover_monitor():
    global _failover_task
    task = _failover_task
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    _failover_task = None

async def _failover_loop():
    while True:
        try:
            if is_cluster_leader():
                topics = list(TOPIC_PLACEMENTS.keys())
                for topic in topics:
                    _maybe_promote_topic_owner(topic)
            await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[P{BROKER_ID}] failover monitor error: {e}")
            await asyncio.sleep(1.0)

# -------------------- Admin & metadata --------------------
@app.get("/admin/leader")
def admin_leader():
    return {
        "broker_id": BROKER_ID,
        "this_broker_is_leader": is_cluster_leader(),
        "leader_id": get_leader_id(),
    }

@app.get("/metadata")
async def metadata():
    if not is_cluster_leader():
        await _refresh_owner_cache_if_needed()
    meta = _metadata_snapshot()
    topics_out: Dict[str, List[int]] = {}
    for topic, placement in meta.get("topics", {}).items():
        owner = placement.get("owner")
        followers = placement.get("followers", [])
        entries = []
        if owner is not None:
            entries.append(int(owner))
        entries.extend(int(f) for f in followers)
        topics_out[topic] = entries
    return {
        "leader_id": get_leader_id(),
        "version": meta.get("version", 0),
        "topics": topics_out,
    }

@app.post("/admin/topics")
async def admin_create_or_assign_topic(
    name: str,
    owner_id: Optional[int] = None,
    rf: int = 3,
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
    rf = max(1, int(rf))
    if owner_id is None:
        candidates = _alive_brokers()
        if not candidates:
            raise HTTPException(status_code=503, detail="No brokers available for topic placement")
        owner_id = random.choice(candidates)
    if owner_id not in BROKER_API:
        raise HTTPException(status_code=400, detail=f"Unknown owner_id {owner_id}")

    _set_topic_owner(name, int(owner_id))
    followers = _select_followers(int(owner_id), rf)
    _set_topic_placement(name, int(owner_id), followers)
    # If this node is the owner, ensure local topic exists now
    if owner_id == BROKER_ID:
        st.init_topic(name)
    return {"status": "ok", "topic": name, "owner_id": owner_id, "leader_id": get_leader_id()}

@app.post("/_gossip/pushpull")
async def gossip_pushpull(req: GossipPushPullRequest):
    if _gossip_state is not None:
        _gossip_state.merge_remote_view(req.membership)
    _maybe_update_metadata_from_remote(req.metadata)
    membership = _gossip_state.serialize_view() if _gossip_state else []
    return {
        "membership": membership,
        "metadata": _metadata_snapshot(),
    }

# -------------------- Replication internals --------------------

@app.get("/internal/topics/{topic}/fetch")
async def internal_fetch(topic: str, offset: int = 0, max_batch: int = 100):
    owner = _owner_for(topic)
    if owner != BROKER_ID:
        raise HTTPException(status_code=409, detail="NOT_OWNER")
    st.init_topic(topic)
    records = st.poll_records(topic, max(0, offset), max_batch)
    return {"records": records}

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
    owned = []
    if is_cluster_leader():
        for topic, placement in TOPIC_PLACEMENTS.items():
            if placement.get("owner") == BROKER_ID:
                owned.append(topic)
    return {
        "broker_id": BROKER_ID,
        "leases": {f"{k[0]}|{k[1]}": exp for k, exp in _group_leases.items()},
        "owned_topics": owned,
    }
