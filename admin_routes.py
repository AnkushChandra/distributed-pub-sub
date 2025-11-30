from __future__ import annotations

import random
from typing import Any, Awaitable, Callable, Dict, List, Optional

from fastapi import APIRouter, HTTPException, status

import storage as st
from config import BROKER_API, BROKER_ID
from election import get_leader_id, is_cluster_leader
from lifecycle import get_gossip_state
from metadata_store import (
    maybe_update_metadata_from_remote,
    metadata_snapshot,
    set_topic_owner,
    set_topic_placement,
)
from schemas import GossipPushPullRequest

AsyncRefreshFn = Callable[[], Awaitable[None]]
AliveFn = Callable[[], List[int]]
SelectFollowersFn = Callable[[int, int], List[int]]
IsrStatusFn = Callable[[], Dict[str, Any]]


def build_admin_router(
    *,
    refresh_owner_cache_fn: AsyncRefreshFn,
    alive_brokers_fn: AliveFn,
    select_followers_fn: SelectFollowersFn,
    isr_status_fn: IsrStatusFn,
):
    router = APIRouter()

    @router.get("/admin/leader")
    def admin_leader():
        return {
            "broker_id": BROKER_ID,
            "this_broker_is_leader": is_cluster_leader(),
            "leader_id": get_leader_id(),
        }

    @router.get("/metadata")
    async def metadata():
        if not is_cluster_leader():
            await refresh_owner_cache_fn(force=True)
        meta = metadata_snapshot()
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

    @router.get("/admin/isr")
    async def isr_status():
        if not is_cluster_leader():
            await refresh_owner_cache_fn(force=True)
        snapshot = isr_status_fn()
        return {
            "leader_id": get_leader_id(),
            "topics": snapshot,
        }

    @router.post("/admin/topics")
    async def admin_create_or_assign_topic(
        name: str,
        owner_id: Optional[int] = None,
        rf: int = 3,
    ):
        if not is_cluster_leader():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"error": "NOT_LEADER", "leader_id": get_leader_id()},
            )
        rf = max(1, int(rf))
        if owner_id is None:
            candidates = alive_brokers_fn()
            if not candidates:
                raise HTTPException(status_code=503, detail="No brokers available for topic placement")
            owner_id = random.choice(candidates)
        if owner_id not in BROKER_API:
            raise HTTPException(status_code=400, detail=f"Unknown owner_id {owner_id}")

        followers = select_followers_fn(int(owner_id), rf)
        set_topic_placement(name, int(owner_id), followers)
        if owner_id == BROKER_ID:
            st.init_topic(name)
        return {
            "status": "ok",
            "topic": name,
            "owner_id": owner_id,
            "leader_id": get_leader_id(),
        }

    @router.post("/_gossip/pushpull")
    async def gossip_pushpull(req: GossipPushPullRequest):
        state = get_gossip_state()
        if state is not None:
            state.merge_remote_view(req.membership)
        maybe_update_metadata_from_remote(req.metadata)
        membership = state.serialize_view() if state else []
        return {
            "membership": membership,
            "metadata": metadata_snapshot(),
        }

    return router


def _select_followers(owner_id: int, replication_factor: int, alive_fn) -> list[int]:
    desired = max(0, replication_factor - 1)
    candidates = [bid for bid in alive_fn() if bid != owner_id]
    if len(candidates) <= desired:
        return sorted(candidates)
    return sorted(random.sample(candidates, desired))
