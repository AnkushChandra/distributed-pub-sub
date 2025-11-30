from fastapi import APIRouter, HTTPException, Query

import storage as st


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

    return router
