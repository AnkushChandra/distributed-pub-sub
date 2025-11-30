from typing import Any, Dict, Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

from notifier import commit as notifier_commit
from notifier import committed as notifier_committed
from notifier import enforce_group_singleton, get_notifier
from schemas import CommitRequest, PublishRequest, SubscribeRequest


def build_client_router(
    *,
    broker_id: int,
    owner_for_fn,
    refresh_owner_cache_fn,
    owner_base_fn,
    append_record_fn,
    poll_records_fn,
):
    router = APIRouter()

    async def _proxy_get(base: str, path: str, params: Dict[str, Any]) -> Any:
        url = f"{base}{path}"
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                r = await client.get(url, params=params)
        except httpx.RequestError as e:
            raise HTTPException(status_code=502, detail={
                "error": "UPSTREAM_UNREACHABLE",
                "upstream": url,
                "reason": str(e),
            })
        ct = (r.headers.get("content-type") or "").lower()
        body = r.json() if "application/json" in ct else {"raw": r.text}
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
        resp = r.json() if "application/json" in ct else {"raw": r.text}
        if r.status_code >= 400:
            raise HTTPException(status_code=r.status_code, detail={
                "error": "UPSTREAM_ERROR",
                "upstream": url,
                "status": r.status_code,
                "body": resp,
            })
        return resp

    @router.post("/topics/{topic}/publish")
    async def publish(topic: str, req: PublishRequest):
        await refresh_owner_cache_fn()
        owner = owner_for_fn(topic)
        if owner is None:
            raise HTTPException(status_code=404, detail="Topic not found (no owner)")
        if owner != broker_id:
            base = owner_base_fn(owner)
            return await _proxy_post(base, f"/topics/{topic}/publish", req.model_dump())
        off = append_record_fn(topic, req.key, req.value, req.headers)
        await get_notifier(topic).notify()
        return {"topic": topic, "offset": off}

    @router.post("/subscribe")
    async def subscribe(sub: SubscribeRequest):
        await refresh_owner_cache_fn()
        owner = owner_for_fn(sub.topic)
        if owner is None:
            raise HTTPException(status_code=404, detail="Topic not found (no owner)")
        if owner != broker_id:
            base = owner_base_fn(owner)
            return await _proxy_post(base, "/subscribe", sub.model_dump())
        try:
            enforce_group_singleton(sub.topic, sub.group_id)
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        committed_offset = notifier_committed(sub.topic, sub.consumer_id, sub.group_id)
        start = sub.from_offset if sub.from_offset is not None else (committed_offset + 1)
        return {"topic": sub.topic, "start_offset": max(start, 0), "committed": committed_offset}

    @router.get("/poll")
    async def poll(
        topic: str = Query(...),
        consumer_id: Optional[str] = Query(None),
        group_id: Optional[str] = Query(None),
        from_offset: Optional[int] = Query(None),
        max_records: int = Query(100, ge=1, le=1000),
        timeout_ms: int = Query(15000, ge=0, le=60000),
    ):
        await refresh_owner_cache_fn()
        owner = owner_for_fn(topic)
        if owner is None:
            raise HTTPException(status_code=404, detail="Topic not found (no owner)")
        if owner != broker_id:
            base = owner_base_fn(owner)
            params = {"topic": topic, "max_records": max_records, "timeout_ms": timeout_ms}
            if consumer_id is not None:
                params["consumer_id"] = consumer_id
            if group_id is not None:
                params["group_id"] = group_id
            if from_offset is not None:
                params["from_offset"] = from_offset
            return await _proxy_get(base, "/poll", params)

        committed_offset = notifier_committed(topic, consumer_id, group_id)
        start = from_offset if from_offset is not None else (committed_offset + 1)
        start = max(start, 0)
        recs = poll_records_fn(topic, start, max_records)
        if recs:
            return {
                "topic": topic,
                "records": recs,
                "next_offset": start + len(recs),
                "committed": committed_offset,
            }
        if timeout_ms > 0:
            waited = await get_notifier(topic).wait(timeout_ms / 1000.0)
            if waited:
                recs = poll_records_fn(topic, start, max_records)
        return {
            "topic": topic,
            "records": recs or [],
            "next_offset": start + len(recs or []),
            "committed": committed_offset,
        }

    @router.post("/commit")
    async def commit_endpoint(c: CommitRequest):
        if c.offset < -1:
            raise HTTPException(status_code=400, detail="invalid offset")
        await refresh_owner_cache_fn()
        owner = owner_for_fn(c.topic)
        if owner is None:
            raise HTTPException(status_code=404, detail="Topic not found (no owner)")
        if owner != broker_id:
            base = owner_base_fn(owner)
            return await _proxy_post(base, "/commit", c.model_dump())
        notifier_commit(c.topic, c.consumer_id, c.group_id, c.offset)
        return {"topic": c.topic, "committed": c.offset}

    return router
