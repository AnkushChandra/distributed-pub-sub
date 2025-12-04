from fastapi import APIRouter, HTTPException, Query

import storage as st
from discovery_client import apply_discovery_update
from notifier import get_notifier
from schemas import DiscoveryUpdateRequest, ReplicateRequest


def build_internal_router(*, owner_for_fn, broker_id: int) -> APIRouter:
    router = APIRouter()

    @router.get("/internal/topics/{topic}/fetch")
    async def internal_fetch(
        topic: str,
        offset: int = 0,
        max_batch: int = 100,
        include_commits: bool = Query(False),
    ):
        owner = owner_for_fn(topic)
        if owner != broker_id:
            raise HTTPException(status_code=409, detail="NOT_OWNER")
        st.init_topic(topic)
        records = st.poll_records(topic, max(0, offset), max_batch)
        response = {"records": records}
        if include_commits:
            response["commits"] = st.commit_entries_for_topic(topic)
        return response

    @router.get("/internal/topics/{topic}/status")
    async def topic_status(topic: str):
        st.init_topic(topic)
        latest = st.latest_offset(topic)
        return {"latest_offset": latest}

    @router.post("/internal/topics/{topic}/replicate")
    async def replicate_record_endpoint(topic: str, req: ReplicateRequest):
        owner = owner_for_fn(topic)
        if owner == broker_id:
            raise HTTPException(status_code=409, detail="NOT_FOLLOWER")
        if req.offset < 0:
            raise HTTPException(status_code=400, detail="invalid offset")
        st.replicate_record(topic, req.offset, req.key, req.value, req.headers)
        await get_notifier(topic).notify()
        return {"status": "ok", "offset": req.offset}

    @router.post("/internal/discovery/update")
    async def discovery_update_endpoint(req: DiscoveryUpdateRequest):
        apply_discovery_update(req.brokers)
        return {"status": "ok", "brokers": len(req.brokers)}

    return router
