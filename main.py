"""FastAPI application entrypoint for the distributed pub/sub service."""

from typing import Any, Dict, List, Optional, Tuple

import asyncio
import random
import threading
import time

import httpx
from fastapi import FastAPI, HTTPException

import storage as st
from config import BROKER_API, BROKER_ID, peer_map, self_api_base
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
    mark_cache_fresh,
    maybe_update_metadata,
    maybe_update_metadata_from_remote,
    metadata_snapshot,
    metadata_version,
    normalize_placement,
    owner_for,
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
            set_topic_placement(topic, candidate, new_followers)
            if candidate == BROKER_ID:
                st.init_topic(topic)
            print(f"[P{BROKER_ID}] promoted topic={topic} owner -> P{candidate}")
            return

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
    if is_cluster_leader():
        return
    now = time.time()
    if not force and cache_is_fresh(now):
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

def _local_is_owner(topic: str) -> bool:
    owner = owner_for(topic)
    return owner == BROKER_ID

def _topics_iter() -> List[str]:
    return list(TOPIC_PLACEMENTS.keys())


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
    )
    _lifecycle_configured = True


# -------------------- Lifecycle hooks --------------------


@app.on_event("startup")
async def _on_startup():
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
    )
)
app.include_router(
    build_client_router(
        broker_id=BROKER_ID,
        owner_for_fn=owner_for,
        refresh_owner_cache_fn=_refresh_owner_cache_if_needed,
        owner_base_fn=_owner_api_base,
        append_record_fn=st.append_record,
        poll_records_fn=st.poll_records,
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
