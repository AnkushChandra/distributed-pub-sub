"""FastAPI application entrypoint for the distributed pub/sub service."""

from typing import Any, Dict, List, Optional, Tuple

import asyncio
import random
import threading
import time

import httpx
from fastapi import FastAPI, HTTPException

import storage as st
import replication
from config import BROKER_API, BROKER_ID, MIN_SYNC_FOLLOWERS, peer_map, self_api_base
from discovery_client import register_with_discovery, seed_local_endpoint
from election import (
    get_leader_id,
    is_cluster_leader,
    start_election_service,
    stop_election_service,
)
from lifecycle import (
    configure_lifecycle,
    get_gossip_state,
    lifecycle_shutdown,
    lifecycle_startup,
)
from metadata_store import (
    TOPIC_PLACEMENTS,
    cache_is_fresh,
    followers_for,
    isr_for,
    mark_cache_fresh,
    maybe_update_metadata,
    maybe_update_metadata_from_remote,
    metadata_snapshot,
    metadata_version,
    normalize_placement,
    owner_for,
    set_topic_isr,
    set_topic_placement,
)
from admin_routes import build_admin_router
from client_routes import build_client_router
from internal_routes import build_internal_router
from notifier import (
    commit as notifier_commit,
    committed as notifier_committed,
    enforce_group_singleton,
    get_notifier,
    leases_snapshot,
)


app = FastAPI(title="Mini Pub/Sub (SQLite + Bully + Routed Reads)")

_lifecycle_configured = False
_topic_owner_offsets: Dict[str, int] = {}
_last_isr_status: Dict[str, Dict[str, Any]] = {}
_isr_status_lock = threading.Lock()

seed_local_endpoint()


def _gossip_payload() -> Dict[str, Any]:
    return {
        "metadata": metadata_snapshot(),
    }


def _handle_gossip_payload(payload: Dict[str, Any]) -> None:
    if not payload:
        return
    meta = payload.get("metadata")
    if meta:
        updated = maybe_update_metadata_from_remote(meta)
        if updated:
            mark_cache_fresh(time.time())

def _notifier(topic: str):
    return get_notifier(topic)


def _enforce_group_singleton(topic: str, group_id: Optional[str]) -> None:
    if not group_id:
        return
    try:
        enforce_group_singleton(topic, group_id)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


def _committed(topic: str, consumer_id: Optional[str], group_id: Optional[str]) -> int:
    return notifier_committed(topic, consumer_id, group_id)


def _commit(topic: str, consumer_id: Optional[str], group_id: Optional[str], offset: int) -> None:
    notifier_commit(topic, consumer_id, group_id, offset)

# lifecycle state is kept in lifecycle.py

def _alive_brokers() -> List[int]:
    state = get_gossip_state()
    if state is None:
        return [BROKER_ID]
    alive: List[int] = []
    for member in state.members.values():
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
    state = get_gossip_state()
    if state is None:
        return True
    member = state.members.get(str(bid))
    if member is None:
        return False
    return member.status == "alive"

def _local_topic_offset(topic: str) -> int:
    st.init_topic(topic)
    return st.latest_offset(topic)


def _fetch_topic_offset(bid: int, topic: str) -> Optional[int]:
    if bid == BROKER_ID:
        return _local_topic_offset(topic)
    base = BROKER_API.get(bid)
    if not base:
        return None
    url = f"{base}/internal/topics/{topic}/status"
    try:
        resp = httpx.get(url, timeout=1.0)
        resp.raise_for_status()
        body = resp.json()
        latest = body.get("latest_offset", -1)
        return int(latest)
    except Exception:
        return None


def _topic_offsets_snapshot(topic: str) -> Dict[int, int]:
    offsets: Dict[int, int] = {}
    local = _local_topic_offset(topic)
    offsets[BROKER_ID] = local
    with httpx.Client(timeout=1.0) as client:
        for bid, base in BROKER_API.items():
            if bid == BROKER_ID:
                continue
            url = f"{base}/internal/topics/{topic}/status"
            try:
                resp = client.get(url)
                resp.raise_for_status()
                latest = int(resp.json().get("latest_offset", -1))
            except Exception:
                continue
            offsets[bid] = latest
    return offsets


def _record_owner_offset(topic: str, owner_id: int) -> None:
    latest = _fetch_topic_offset(owner_id, topic)
    if latest is not None:
        _topic_owner_offsets[topic] = latest


def _current_owner_offset(topic: str, owner_id: int) -> Optional[int]:
    if owner_id == BROKER_ID:
        latest = _local_topic_offset(topic)
    else:
        latest = _fetch_topic_offset(owner_id, topic)
    if latest is not None:
        _topic_owner_offsets[topic] = latest
    return latest


def _record_committed_offset(topic: str, offset: int) -> None:
    current = _topic_owner_offsets.get(topic, -1)
    if offset > current:
        _topic_owner_offsets[topic] = offset


def _sync_isr_membership(topic: str) -> None:
    if not is_cluster_leader():
        return
    placement = TOPIC_PLACEMENTS.get(topic)
    if not placement:
        return
    owner = placement.get("owner")
    if owner is None:
        return
    owner_id = int(owner)
    owner_offset = _current_owner_offset(topic, owner_id)
    if owner_offset is None:
        return
    current_isr = isr_for(topic, leader_view=True)
    followers_raw = list(placement.get("followers", []))
    followers_full = list(followers_raw)
    followers_alive: List[int] = []
    for follower in followers_raw:
        try:
            fid = int(follower)
        except (TypeError, ValueError):
            continue
        followers_alive.append(fid)

    desired_isr: List[int] = [owner_id]
    for fid in followers_alive:
        follower_offset = _fetch_topic_offset(fid, topic)
        if follower_offset is None:
            continue
        if follower_offset >= owner_offset:
            desired_isr.append(fid)

    target_followers = max(len(followers_raw), MIN_SYNC_FOLLOWERS)
    candidate_pool = [
        bid
        for bid in _alive_brokers()
        if bid != owner_id and bid not in followers_alive
    ]
    random.shuffle(candidate_pool)
    replacements_added = False
    while len(followers_alive) < target_followers and candidate_pool:
        new_follower = candidate_pool.pop()
        followers_alive.append(new_follower)
        if new_follower not in followers_full:
            followers_full.append(new_follower)
        replacements_added = True

    followers_changed = replacements_added or (
        sorted(followers_full) != sorted(followers_raw)
    )

    if desired_isr == current_isr and not followers_changed:
        return

    set_topic_placement(topic, owner_id, followers_full, desired_isr)


def _maybe_promote_topic_owner(topic: str) -> None:
    if not is_cluster_leader():
        return
    placement = TOPIC_PLACEMENTS.get(topic)
    if not placement:
        return
    owner = placement.get("owner")
    if owner is None:
        return
    owner_id = int(owner)
    owner_offset = _fetch_topic_offset(owner_id, topic)
    if owner_offset is not None:
        _topic_owner_offsets[topic] = owner_offset
        return
    # Owner is unreachable - need to promote a follower
    followers = list(placement.get("followers", []))
    if not followers:
        return
    # Try ISR members first, but fall back to any follower if ISR is empty/stale
    isr_members = [mid for mid in isr_for(topic, leader_view=True) if mid != owner_id]
    candidates = isr_members if isr_members else [f for f in followers if f != owner_id]
    if not candidates:
        return
    offsets = _topic_offsets_snapshot(topic)
    required = _topic_owner_offsets.get(topic, -1)
    current_max = max(offsets.values(), default=-1)
    target_offset = max(required, current_max)
    # Sort candidates by offset descending to pick the most caught-up one
    candidate_offsets = []
    for candidate in candidates:
        candidate_offset = offsets.get(candidate)
        if candidate_offset is None:
            candidate_offset = _fetch_topic_offset(candidate, topic)
        if candidate_offset is not None:
            candidate_offsets.append((candidate, candidate_offset))
    if not candidate_offsets:
        return
    # Pick the candidate with highest offset
    candidate_offsets.sort(key=lambda x: x[1], reverse=True)
    best_candidate, best_offset = candidate_offsets[0]
    if best_offset < target_offset:
        # No candidate is caught up enough, but if owner is dead we must promote anyway
        print(
            f"[P{BROKER_ID}] WARNING: promoting topic={topic} owner -> P{best_candidate} "
            f"with offset {best_offset} < target {target_offset} (data loss possible)"
        )
    new_followers = [f for f in followers if f != best_candidate]
    new_followers.append(owner_id)
    set_topic_placement(topic, best_candidate, new_followers)
    if best_candidate == BROKER_ID:
        st.init_topic(topic)
    print(
        f"[P{BROKER_ID}] promoted topic={topic} owner -> P{best_candidate} "
        f"(offset {best_offset}, target {target_offset})"
    )

def _owner_api_base(owner_id: int) -> str:
    base = BROKER_API.get(int(owner_id))
    if base is None:
        raise HTTPException(status_code=502, detail=f"Unknown owner broker {owner_id}")
    return base

def _get_leader_api() -> str:
    leader_id = get_leader_id()
    if leader_id is None or leader_id not in BROKER_API:
        raise HTTPException(status_code=503, detail="No leader available")
    return BROKER_API[leader_id]

async def _refresh_owner_cache_if_needed(*, force: bool = False):
    now = time.time()
    if not force and cache_is_fresh(now):
        return
    if is_cluster_leader():
        updated = await _pull_best_metadata_from_peers()
        if updated:
            mark_cache_fresh(time.time())
        return
    try:
        leader_api = _get_leader_api()
    except HTTPException as exc:
        if exc.status_code == 503:
            # No leader yet; skip forced refresh and let sync loop retry.
            return
        raise
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
        version_int = metadata_version() + 1
    snapshot = {str(topic): normalize_placement(entry) for topic, entry in topics.items()}
    updated = maybe_update_metadata(snapshot, version_int, persist=True)
    if updated:
        mark_cache_fresh(time.time())


async def _pull_best_metadata_from_peers() -> bool:
    """When we are (or become) leader, adopt the freshest snapshot we can find."""
    best: Optional[Tuple[int, Dict[str, Placement]]] = None
    current_version = metadata_version()
    async with httpx.AsyncClient(timeout=3.0) as client:
        for bid, base in BROKER_API.items():
            if bid == BROKER_ID:
                continue
            try:
                resp = await client.get(f"{base}/metadata")
                resp.raise_for_status()
                body = resp.json()
            except Exception:
                continue
            version = body.get("version")
            topics = body.get("topics")
            if topics is None or version is None:
                continue
            try:
                version_int = int(version)
            except (TypeError, ValueError):
                continue
            if version_int <= current_version:
                continue
            snapshot = {str(topic): normalize_placement(entry) for topic, entry in topics.items()}
            if best is None or version_int > best[0]:
                best = (version_int, snapshot)
    if not best:
        return False
    version_int, snapshot = best
    return maybe_update_metadata(snapshot, version_int, persist=True)

def _local_is_owner(topic: str) -> bool:
    owner = owner_for(topic)
    return owner == BROKER_ID

def _topics_iter() -> List[str]:
    return list(TOPIC_PLACEMENTS.keys())


def _isr_members(topic: str) -> List[int]:
    return isr_for(topic, leader_view=True)


async def _replicate_to_isr(topic: str, offset: int, key, value, headers):
    placement = TOPIC_PLACEMENTS.get(topic)
    if not placement:
        return {
            "acks": {},
            "required_sync": 0,
            "success_count": 0,
            "quorum_met": True,
        }
    isr_members = isr_for(topic, leader_view=True)
    follower_targets = [mid for mid in isr_members if mid != BROKER_ID]
    required_sync = min(MIN_SYNC_FOLLOWERS, len(follower_targets))
    results = await replication.replicate_to_followers(
        topic=topic,
        offset=offset,
        key=key,
        value=value,
        headers=headers,
        follower_ids=follower_targets,
    )
    failed = [fid for fid in follower_targets if not results.get(fid, False)]
    if failed:
        updated = [mid for mid in isr_members if mid not in failed]
        if updated:
            set_topic_isr(topic, updated)
    success_count = len([fid for fid in follower_targets if results.get(fid, False)])
    quorum_met = success_count >= required_sync
    pending = [fid for fid in follower_targets if not results.get(fid, False)]
    status_payload = {
        "timestamp": time.time(),
        "owner_id": BROKER_ID,
        "topic": topic,
        "acks": results,
        "pending": pending,
        "required_sync": required_sync,
        "success_count": success_count,
        "quorum_met": quorum_met,
        "targets": follower_targets,
    }
    with _isr_status_lock:
        _last_isr_status[topic] = status_payload
    return status_payload


def _isr_status_snapshot() -> Dict[str, Any]:
    snapshot: Dict[str, Any] = {}
    with _isr_status_lock:
        last_status = {topic: dict(data) for topic, data in _last_isr_status.items()}
    for topic, placement in TOPIC_PLACEMENTS.items():
        owner = placement.get("owner")
        followers = placement.get("followers", [])
        isr_members = placement.get("isr", [])
        members = []
        if owner is not None:
            members.append(int(owner))
        members.extend(int(f) for f in followers)
        alive_view = {mid: _broker_is_alive(mid) for mid in members}
        topic_status = {
            "owner": owner,
            "followers": followers,
            "isr": isr_members,
            "alive": alive_view,
            "min_sync_followers": MIN_SYNC_FOLLOWERS,
        }
        if owner == BROKER_ID and topic in last_status:
            topic_status["last_publish"] = last_status[topic]
        snapshot[topic] = topic_status
    return snapshot


def _configure_lifecycle_once():
    global _lifecycle_configured
    if _lifecycle_configured:
        return
    configure_lifecycle(
        broker_id=BROKER_ID,
        broker_api=BROKER_API,
        peer_map_fn=peer_map,
        self_api_base_fn=self_api_base,
        gossip_payload_fn=_gossip_payload,
        handle_gossip_payload_fn=_handle_gossip_payload,
        placement_getter=lambda: metadata_snapshot()["topics"],
        refresh_owner_cache_fn=_refresh_owner_cache_if_needed,
        is_leader_fn=is_cluster_leader,
        topics_provider_fn=_topics_iter,
        promote_topic_fn=_maybe_promote_topic_owner,
        sync_isr_fn=_sync_isr_membership,
    )
    _lifecycle_configured = True


# -------------------- Lifecycle hooks --------------------


@app.on_event("startup")
async def _on_startup():
    await register_with_discovery()
    start_election_service()
    _configure_lifecycle_once()
    await lifecycle_startup()
    await _refresh_owner_cache_if_needed(force=True)


@app.on_event("shutdown")
async def _on_shutdown():
    await lifecycle_shutdown()
    stop_election_service()

# -------------------- Admin & metadata --------------------
app.include_router(
    build_admin_router(
        refresh_owner_cache_fn=_refresh_owner_cache_if_needed,
        alive_brokers_fn=_alive_brokers,
        select_followers_fn=_select_followers,
        isr_status_fn=_isr_status_snapshot,
    )
)
def _broker_api_base(broker_id_target: int) -> str:
    base = BROKER_API.get(int(broker_id_target))
    if base is None:
        raise HTTPException(status_code=502, detail=f"Unknown broker {broker_id_target}")
    return base

app.include_router(
    build_client_router(
        broker_id=BROKER_ID,
        owner_for_fn=owner_for,
        refresh_owner_cache_fn=_refresh_owner_cache_if_needed,
        owner_base_fn=_owner_api_base,
        append_record_fn=st.append_record,
        poll_records_fn=st.poll_records,
        isr_members_fn=_isr_members,
        replicate_to_isr_fn=_replicate_to_isr,
        followers_for_fn=followers_for,
        broker_base_fn=_broker_api_base,
    )
)
app.include_router(
    build_internal_router(
        owner_for_fn=owner_for,
        broker_id=BROKER_ID,
    )
)

@app.get("/metrics")
def metrics():
    owned = []
    if is_cluster_leader():
        for topic, placement in TOPIC_PLACEMENTS.items():
            if placement.get("owner") == BROKER_ID:
                owned.append(topic)
    return {
        "broker_id": BROKER_ID,
        "leases": leases_snapshot(),
        "owned_topics": owned,
    }
