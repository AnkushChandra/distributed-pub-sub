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
    topic_exists,
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

        if topic_exists(name, leader_view=True):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"error": "TOPIC_EXISTS", "topic": name},
            )

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

    @router.post("/admin/topics/{topic}/replicas")
    async def add_replica(topic: str, broker_id_to_add: int):
        """Add a replica to an existing topic."""
        if not is_cluster_leader():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"error": "NOT_LEADER", "leader_id": get_leader_id()},
            )
        if not topic_exists(topic, leader_view=True):
            raise HTTPException(status_code=404, detail=f"Topic '{topic}' not found")
        if broker_id_to_add not in BROKER_API:
            raise HTTPException(status_code=400, detail=f"Unknown broker_id {broker_id_to_add}")
        
        meta = metadata_snapshot()
        placement = meta.get("topics", {}).get(topic, {})
        owner = placement.get("owner")
        followers = list(placement.get("followers", []))
        
        if broker_id_to_add == owner:
            raise HTTPException(status_code=400, detail=f"Broker {broker_id_to_add} is already the owner")
        if broker_id_to_add in followers:
            raise HTTPException(status_code=400, detail=f"Broker {broker_id_to_add} is already a replica")
        
        followers.append(broker_id_to_add)
        set_topic_placement(topic, owner, followers)
        
        return {
            "status": "ok",
            "topic": topic,
            "owner": owner,
            "followers": followers,
            "replica_count": len(followers) + 1,
        }

    @router.delete("/admin/topics/{topic}/replicas/{broker_id_to_remove}")
    async def remove_replica(topic: str, broker_id_to_remove: int):
        """Remove a replica from a topic."""
        if not is_cluster_leader():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"error": "NOT_LEADER", "leader_id": get_leader_id()},
            )
        if not topic_exists(topic, leader_view=True):
            raise HTTPException(status_code=404, detail=f"Topic '{topic}' not found")
        
        meta = metadata_snapshot()
        placement = meta.get("topics", {}).get(topic, {})
        owner = placement.get("owner")
        followers = list(placement.get("followers", []))
        
        if broker_id_to_remove == owner:
            raise HTTPException(status_code=400, detail="Cannot remove the owner. Reassign ownership first.")
        if broker_id_to_remove not in followers:
            raise HTTPException(status_code=400, detail=f"Broker {broker_id_to_remove} is not a replica")
        
        followers.remove(broker_id_to_remove)
        set_topic_placement(topic, owner, followers)
        
        return {
            "status": "ok",
            "topic": topic,
            "owner": owner,
            "followers": followers,
            "replica_count": len(followers) + 1,
        }

    @router.get("/admin/topics/{topic}")
    async def get_topic_info(topic: str):
        """Get detailed info about a topic."""
        if not is_cluster_leader():
            await refresh_owner_cache_fn(force=True)
        if not topic_exists(topic, leader_view=is_cluster_leader()):
            raise HTTPException(status_code=404, detail=f"Topic '{topic}' not found")
        
        meta = metadata_snapshot()
        placement = meta.get("topics", {}).get(topic, {})
        owner = placement.get("owner")
        followers = placement.get("followers", [])
        isr = placement.get("isr", [])
        
        return {
            "topic": topic,
            "owner": owner,
            "followers": followers,
            "isr": isr,
            "replica_count": len(followers) + 1,
            "alive_brokers": alive_brokers_fn(),
        }

    @router.post("/_gossip/pushpull")
    async def gossip_pushpull(req: GossipPushPullRequest):
        state = get_gossip_state()
        if state is not None:
            # Record that sender has info about us (for propagation tracking)
            if req.membership:
                state.record_propagation_ack(req.from_id, req.membership)
            state.merge_remote_view(req.membership)
        maybe_update_metadata_from_remote(req.metadata)
        membership = state.serialize_view() if state else []
        return {
            "from_id": state.self_id if state else None,
            "membership": membership,
            "metadata": metadata_snapshot(),
        }

    return router
