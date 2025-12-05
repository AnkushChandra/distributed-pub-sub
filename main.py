"""FastAPI application entrypoint for the distributed pub/sub service."""
from typing import Any, Dict, List, Optional, Tuple
import random, threading, time
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import storage as st, replication
from config import BROKER_API, BROKER_ID, MIN_SYNC_FOLLOWERS, peer_map, self_api_base
from discovery_client import register_with_discovery, seed_local_endpoint
from election import get_leader_id, is_cluster_leader, start_election_service, stop_election_service
from lifecycle import configure_lifecycle, get_gossip_state, lifecycle_shutdown, lifecycle_startup
from metadata_store import (TOPIC_PLACEMENTS, cache_is_fresh, followers_for, isr_for, mark_cache_fresh,
    maybe_update_metadata, maybe_update_metadata_from_remote, metadata_snapshot, metadata_version,
    normalize_placement, owner_for, set_topic_isr, set_topic_placement)
from admin_routes import build_admin_router
from client_routes import build_client_router
from internal_routes import build_internal_router
from notifier import leases_snapshot

app = FastAPI(title="Mini Pub/Sub")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
_lifecycle_configured = False
_topic_owner_offsets: Dict[str, int] = {}
_last_isr_status: Dict[str, Dict[str, Any]] = {}
_isr_lock = threading.Lock()

seed_local_endpoint()

def _gossip_payload() -> Dict[str, Any]:
    return {"metadata": metadata_snapshot()}

def _handle_gossip_payload(payload: Dict[str, Any]) -> None:
    if payload and payload.get("metadata"):
        if maybe_update_metadata_from_remote(payload["metadata"]):
            mark_cache_fresh(time.time())

def _alive_brokers() -> List[int]:
    state = get_gossip_state()
    if not state:
        return [BROKER_ID]
    alive = [int(m.node_id) for m in state.members.values() 
             if m.status == "alive" and m.node_id.isdigit() and int(m.node_id) in BROKER_API]
    return alive or [BROKER_ID]

def _select_followers(owner_id: int, rf: int) -> List[int]:
    desired = max(0, rf - 1)
    candidates = [b for b in _alive_brokers() if b != owner_id]
    return sorted(random.sample(candidates, min(desired, len(candidates)))) if candidates else []

def _broker_is_alive(bid: int) -> bool:
    state = get_gossip_state()
    return state.members.get(str(bid), type('', (), {'status': 'alive'})()).status == "alive" if state else True

def _fetch_offset(bid: int, topic: str) -> Optional[int]:
    if bid == BROKER_ID:
        st.init_topic(topic)
        return st.latest_offset(topic)
    base = BROKER_API.get(bid)
    if not base:
        return None
    try:
        r = httpx.get(f"{base}/internal/topics/{topic}/status", timeout=1.0)
        return int(r.json().get("latest_offset", -1)) if r.status_code == 200 else None
    except:
        return None

def _all_offsets(topic: str) -> Dict[int, int]:
    st.init_topic(topic)
    offsets = {BROKER_ID: st.latest_offset(topic)}
    with httpx.Client(timeout=1.0) as c:
        for bid, base in BROKER_API.items():
            if bid != BROKER_ID:
                try:
                    offsets[bid] = int(c.get(f"{base}/internal/topics/{topic}/status").json().get("latest_offset", -1))
                except:
                    pass
    return offsets

def _sync_isr(topic: str) -> None:
    if not is_cluster_leader():
        return
    p = TOPIC_PLACEMENTS.get(topic)
    if not p or not p.get("owner"):
        return
    owner_id = int(p["owner"])
    owner_off = _fetch_offset(owner_id, topic)
    if owner_off is None:
        return
    _topic_owner_offsets[topic] = owner_off
    
    followers = [int(f) for f in p.get("followers", []) if str(f).isdigit()]
    
    # Allow some lag tolerance - followers within 10 messages are considered in-sync
    # This prevents thrashing under high write load
    LAG_TOLERANCE = 10
    isr = [owner_id]
    for f in followers:
        f_off = _fetch_offset(f, topic)
        if f_off is not None and (owner_off - f_off) <= LAG_TOLERANCE:
            isr.append(f)
    
    # Add new followers if needed
    target = max(len(followers), MIN_SYNC_FOLLOWERS)
    pool = [b for b in _alive_brokers() if b != owner_id and b not in followers]
    random.shuffle(pool)
    while len(followers) < target and pool:
        followers.append(pool.pop())
    
    if isr != isr_for(topic, leader_view=True) or sorted(followers) != sorted(p.get("followers", [])):
        set_topic_placement(topic, owner_id, followers, isr)

def _promote_owner(topic: str) -> None:
    if not is_cluster_leader():
        return
    p = TOPIC_PLACEMENTS.get(topic)
    if not p or not p.get("owner"):
        return
    owner_id = int(p["owner"])
    if _fetch_offset(owner_id, topic) is not None:
        _topic_owner_offsets[topic] = _fetch_offset(owner_id, topic)
        return
    
    followers = [int(f) for f in p.get("followers", []) if str(f).isdigit()]
    if not followers:
        return
    isr = [m for m in isr_for(topic, leader_view=True) if m != owner_id]
    candidates = isr or [f for f in followers if f != owner_id]
    if not candidates:
        return
    
    offsets = _all_offsets(topic)
    target = max(_topic_owner_offsets.get(topic, -1), max(offsets.values(), default=-1))
    best = max(((c, offsets.get(c) or _fetch_offset(c, topic) or -1) for c in candidates), key=lambda x: x[1], default=(None, -1))
    if best[0] is None:
        return
    
    new_followers = [f for f in followers if f != best[0]] + [owner_id]
    set_topic_placement(topic, best[0], new_followers)
    if best[0] == BROKER_ID:
        st.init_topic(topic)
    print(f"[P{BROKER_ID}] promoted {topic} owner -> P{best[0]} (offset {best[1]}, target {target})")

def _owner_base(owner_id: int) -> str:
    base = BROKER_API.get(int(owner_id))
    if not base:
        raise HTTPException(status_code=502, detail=f"Unknown broker {owner_id}")
    return base

async def _refresh_cache(*, force: bool = False):
    if not force and cache_is_fresh(time.time()):
        return
    if is_cluster_leader():
        # When becoming leader, adopt the HIGHEST version from any peer
        # This prevents stale data after a leader comes back from failure
        best = None
        async with httpx.AsyncClient(timeout=3.0) as c:
            for bid, base in BROKER_API.items():
                if bid == BROKER_ID:
                    continue
                try:
                    r = await c.get(f"{base}/metadata")
                    body = r.json()
                    v, t = body.get("version"), body.get("topics")
                    if t and v:
                        v_int = int(v)
                        if not best or v_int > best[0]:
                            best = (v_int, {str(k): normalize_placement(e) for k, e in t.items()})
                except:
                    pass
        # CRITICAL: When force=True (becoming leader), adopt peer metadata even if
        # local version appears higher. Local data may be stale after a restart.
        if best:
            local_v = metadata_version()
            if best[0] > local_v:
                maybe_update_metadata(best[1], best[0], persist=True)
            elif force:
                # Force mode: adopt peer data with bumped version to override stale local
                maybe_update_metadata(best[1], local_v + 1, persist=True)
                print(f"[P{BROKER_ID}] adopted peer metadata (was stale), new version {local_v + 1}")
        mark_cache_fresh(time.time())
        return
    
    leader_id = get_leader_id()
    if not leader_id or leader_id not in BROKER_API:
        return
    try:
        async with httpx.AsyncClient(timeout=3.0) as c:
            r = await c.get(f"{BROKER_API[leader_id]}/metadata")
            meta = r.json()
        v = int(meta.get("version", metadata_version() + 1))
        snap = {str(k): normalize_placement(e) for k, e in meta.get("topics", {}).items()}
        if maybe_update_metadata(snap, v, persist=True):
            mark_cache_fresh(time.time())
    except Exception as e:
        print(f"[P{BROKER_ID}] metadata refresh failed: {e}")

async def _replicate_to_isr(topic: str, offset: int, key, value, headers):
    p = TOPIC_PLACEMENTS.get(topic)
    if not p:
        return {"acks": {}, "required_sync": 0, "success_count": 0, "quorum_met": True}
    
    # Replicate to ALL followers, not just ISR - this ensures followers can catch up
    raw_followers = p.get("followers", [])
    followers = []
    for f in raw_followers:
        try:
            fid = int(f)
            if fid != BROKER_ID:
                followers.append(fid)
        except (TypeError, ValueError):
            continue
    
    isr_members = [m for m in isr_for(topic, leader_view=True) if m != BROKER_ID]
    
    # Required acks only from ISR members
    required = min(MIN_SYNC_FOLLOWERS, len(isr_members))
    
    # Push to ALL followers so they can catch up
    if followers:
        results = await replication.replicate_to_followers(topic=topic, offset=offset, key=key, 
                                                            value=value, headers=headers, follower_ids=followers)
    else:
        results = {}
    
    # Only count ISR members for quorum
    isr_success = sum(1 for f in isr_members if results.get(f))
    
    status = {"timestamp": time.time(), "acks": results, "required_sync": required, 
              "success_count": isr_success, "quorum_met": isr_success >= required or required == 0}
    with _isr_lock:
        _last_isr_status[topic] = status
    return status

def _isr_snapshot() -> Dict[str, Any]:
    with _isr_lock:
        last = dict(_last_isr_status)
    snap = {}
    for topic, p in TOPIC_PLACEMENTS.items():
        owner, followers, isr = p.get("owner"), p.get("followers", []), p.get("isr", [])
        members = ([int(owner)] if owner else []) + [int(f) for f in followers]
        snap[topic] = {"owner": owner, "followers": followers, "isr": isr,
                       "alive": {m: _broker_is_alive(m) for m in members}, "min_sync_followers": MIN_SYNC_FOLLOWERS}
        if owner == BROKER_ID and topic in last:
            snap[topic]["last_publish"] = last[topic]
    return snap

def _configure_once():
    global _lifecycle_configured
    if _lifecycle_configured:
        return
    configure_lifecycle(broker_id=BROKER_ID, broker_api=BROKER_API, peer_map_fn=peer_map,
        self_api_base_fn=self_api_base, gossip_payload_fn=_gossip_payload,
        handle_gossip_payload_fn=_handle_gossip_payload,
        placement_getter=lambda: metadata_snapshot()["topics"], refresh_owner_cache_fn=_refresh_cache,
        is_leader_fn=is_cluster_leader, topics_provider_fn=lambda: list(TOPIC_PLACEMENTS.keys()),
        promote_topic_fn=_promote_owner, sync_isr_fn=_sync_isr)
    _lifecycle_configured = True

@app.on_event("startup")
async def _startup():
    await register_with_discovery()
    start_election_service()
    _configure_once()
    await lifecycle_startup()
    await _refresh_cache(force=True)

@app.on_event("shutdown")
async def _shutdown():
    await lifecycle_shutdown()
    stop_election_service()

app.include_router(build_admin_router(refresh_owner_cache_fn=_refresh_cache, alive_brokers_fn=_alive_brokers,
                                       select_followers_fn=_select_followers, isr_status_fn=_isr_snapshot))
app.include_router(build_client_router(broker_id=BROKER_ID, owner_for_fn=owner_for, refresh_owner_cache_fn=_refresh_cache,
    owner_base_fn=_owner_base, append_record_fn=st.append_record, poll_records_fn=st.poll_records,
    isr_members_fn=lambda t: isr_for(t, leader_view=True), replicate_to_isr_fn=_replicate_to_isr,
    followers_for_fn=followers_for, broker_base_fn=_owner_base))
app.include_router(build_internal_router(owner_for_fn=owner_for, broker_id=BROKER_ID))

@app.get("/metrics")
def metrics():
    owned = [t for t, p in TOPIC_PLACEMENTS.items() if p.get("owner") == BROKER_ID] if is_cluster_leader() else []
    return {"broker_id": BROKER_ID, "leases": leases_snapshot(), "owned_topics": owned}

@app.get("/stats")
def stats():
    """Live algorithm performance metrics."""
    from election import _node as election_node
    from replication import get_replication_metrics
    
    # Bully/Election stats
    election_stats = {}
    if election_node:
        with election_node.state_lock:
            election_stats = {
                "current_leader": election_node.coordinator_id,
                "is_leader": election_node.coordinator_id == BROKER_ID,
                "in_election": election_node.in_election,
                "last_heartbeat": election_node.last_heartbeat,
                "peers_count": len(election_node.peers),
                "higher_ids": election_node.higher_ids(),
            }
        # Add bully metrics
        election_stats["metrics"] = election_node.get_metrics()
    
    # Gossip stats
    gossip_stats = {}
    state = get_gossip_state()
    if state:
        members = state.members
        gossip_stats = {
            "total_members": len(members),
            "alive": sum(1 for m in members.values() if m.status == "alive"),
            "suspect": sum(1 for m in members.values() if m.status == "suspect"),
            "dead": sum(1 for m in members.values() if m.status == "dead"),
            "members": {mid: {"status": m.status, "addr": m.addr, "incarnation": m.incarnation, 
                             "last_seen": m.last_seen} for mid, m in members.items()},
            "interval_sec": state.interval_sec,
            "fanout": state.fanout,
            "metrics": state.metrics,
        }
    
    # Replication stats
    replication_stats = {
        "topics_count": len(TOPIC_PLACEMENTS),
        "topics": {}
    }
    for topic, p in TOPIC_PLACEMENTS.items():
        owner = p.get("owner")
        followers = p.get("followers", [])
        isr = p.get("isr", [])
        is_owner = owner == BROKER_ID
        
        topic_stats = {
            "owner": owner,
            "is_owner": is_owner,
            "followers": followers,
            "isr": isr,
            "isr_count": len(isr),
            "replica_count": 1 + len(followers),
        }
        
        # Add last publish info if we're the owner
        with _isr_lock:
            if topic in _last_isr_status:
                last = _last_isr_status[topic]
                topic_stats["last_replication"] = {
                    "timestamp": last.get("timestamp"),
                    "acks": last.get("acks", {}),
                    "success_count": last.get("success_count", 0),
                    "quorum_met": last.get("quorum_met", True),
                }
        
        replication_stats["topics"][topic] = topic_stats
    
    # Add replication metrics
    replication_stats["metrics"] = get_replication_metrics()
    
    return {
        "broker_id": BROKER_ID,
        "timestamp": time.time(),
        "election": election_stats,
        "gossip": gossip_stats,
        "replication": replication_stats,
    }
