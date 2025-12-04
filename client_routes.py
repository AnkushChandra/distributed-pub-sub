from typing import Any, Dict, List, Optional
import hashlib

import httpx
from fastapi import APIRouter, HTTPException, Query

from notifier import commit as notifier_commit
from notifier import committed as notifier_committed
from notifier import enforce_group_singleton, get_notifier
from schemas import CommitRequest, PublishRequest, SubscribeRequest

# Track sticky assignments: (topic, consumer_id) -> replica_id
_consumer_sticky_map: Dict[tuple, int] = {}


def build_client_router(
    *,
    broker_id: int,
    owner_for_fn,
    refresh_owner_cache_fn,
    owner_base_fn,
    append_record_fn,
    poll_records_fn,
    isr_members_fn,
    replicate_to_isr_fn,
    followers_for_fn,
    broker_base_fn,
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
        repl_result = await replicate_to_isr_fn(topic, off, req.key, req.value, req.headers)
        if not repl_result.get("quorum_met", True):
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "ISR_REPLICATION_FAILED",
                    "acks": repl_result.get("acks", {}),
                    "required_sync": repl_result.get("required_sync", 0),
                    "success_count": repl_result.get("success_count", 0),
                },
            )
        await get_notifier(topic).notify()
        try:
            record_committed = globals().get("_record_committed_offset")
            if callable(record_committed):
                record_committed(topic, off)
        except Exception:
            pass
        return {
            "topic": topic,
            "offset": off,
            "isr": isr_members_fn(topic),
        }

    def _get_all_replicas(topic: str) -> List[int]:
        """Get all replicas (owner + followers) for a topic."""
        owner = owner_for_fn(topic)
        if owner is None:
            return []
        followers = followers_for_fn(topic)
        replicas = [owner] + [f for f in followers if f != owner]
        return replicas

    def _hash_to_replica(consumer_id: str, replicas: List[int]) -> int:
        """Consistent hash of consumer_id to pick a replica."""
        h = int(hashlib.md5(consumer_id.encode()).hexdigest(), 16)
        return replicas[h % len(replicas)]

    def _select_read_replica(topic: str, consumer_id: Optional[str] = None) -> Optional[int]:
        """Select a replica for read operations. Sticky by consumer_id if provided."""
        replicas = _get_all_replicas(topic)
        if not replicas:
            return None
        if broker_id in replicas:
            return broker_id
        
        if consumer_id:
            key = (topic, consumer_id)
            if key in _consumer_sticky_map:
                sticky = _consumer_sticky_map[key]
                if sticky in replicas:
                    return sticky
            assigned = _hash_to_replica(consumer_id, replicas)
            _consumer_sticky_map[key] = assigned
            return assigned
        
        return _hash_to_replica(f"anon-{topic}", replicas)

    def _mark_replica_failed(topic: str, consumer_id: Optional[str], failed_replica: int) -> None:
        """Remove failed replica from sticky map so next call picks a new one."""
        if consumer_id:
            key = (topic, consumer_id)
            if _consumer_sticky_map.get(key) == failed_replica:
                del _consumer_sticky_map[key]

    @router.post("/subscribe")
    async def subscribe(sub: SubscribeRequest):
        await refresh_owner_cache_fn()
        owner = owner_for_fn(sub.topic)
        if owner is None:
            raise HTTPException(status_code=404, detail="Topic not found (no owner)")
        replicas = _get_all_replicas(sub.topic)
        if broker_id not in replicas:
            target = _select_read_replica(sub.topic)
            if target is None:
                raise HTTPException(status_code=404, detail="Topic not found")
            base = broker_base_fn(target)
            return await _proxy_post(base, "/subscribe", sub.model_dump())
        if sub.group_id and not sub.consumer_id:
            raise HTTPException(status_code=400, detail="group_id requires a consumer_id")
        try:
            enforce_group_singleton(sub.topic, sub.group_id, sub.consumer_id)
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        committed_offset = notifier_committed(sub.topic, sub.consumer_id, sub.group_id)
        start = sub.from_offset if sub.from_offset is not None else (committed_offset + 1)
        return {"topic": sub.topic, "start_offset": max(start, 0), "committed": committed_offset, "replica_id": broker_id}

    @router.get("/poll")
    async def poll(
        topic: str = Query(...),
        consumer_id: Optional[str] = Query(None),
        group_id: Optional[str] = Query(None),
        from_offset: Optional[int] = Query(None),
        max_records: int = Query(100, ge=1, le=1000),
        timeout_ms: int = Query(15000, ge=0, le=60000),
        prefer_replica: Optional[int] = Query(None, description="Preferred replica broker id"),
    ):
        owner = owner_for_fn(topic)
        if owner is None:
            raise HTTPException(status_code=404, detail="Topic not found (no owner)")
        
        replicas = _get_all_replicas(topic)
        is_replica = broker_id in replicas
        
        # Build list of targets to try
        targets_to_try = []
        if prefer_replica is not None and prefer_replica in replicas and prefer_replica != broker_id:
            targets_to_try.append(prefer_replica)
        if not is_replica:
            # Add all other replicas as fallbacks
            for r in replicas:
                if r != broker_id and r not in targets_to_try:
                    targets_to_try.append(r)
        
        # Try each target until one works
        params = {"topic": topic, "max_records": max_records, "timeout_ms": timeout_ms}
        if consumer_id is not None:
            params["consumer_id"] = consumer_id
        if group_id is not None:
            params["group_id"] = group_id
        if from_offset is not None:
            params["from_offset"] = from_offset
        proxy_timeout = max(15.0, (timeout_ms / 1000.0) + 5.0)
        
        for target in targets_to_try:
            base = broker_base_fn(target)
            try:
                async with httpx.AsyncClient(timeout=proxy_timeout) as client:
                    r = await client.get(f"{base}/poll", params=params)
                if r.status_code < 400:
                    return r.json()
                # Got an error response, try next replica
            except httpx.RequestError:
                # Connection failed, try next replica
                _mark_replica_failed(topic, consumer_id, target)
                continue
        
        # All proxies failed - serve locally if we're a replica
        if not is_replica:
            raise HTTPException(status_code=502, detail="All replicas unreachable")

        # Serve locally
        if group_id:
            if consumer_id is None:
                raise HTTPException(status_code=400, detail="group_id requires a consumer_id")
            try:
                enforce_group_singleton(topic, group_id, consumer_id)
            except ValueError as exc:
                raise HTTPException(status_code=409, detail=str(exc))

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
                "replica_id": broker_id,
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
            "replica_id": broker_id,
        }

    @router.post("/commit")
    async def commit_endpoint(c: CommitRequest):
        if c.offset < -1:
            raise HTTPException(status_code=400, detail="invalid offset")
        await refresh_owner_cache_fn()
        owner = owner_for_fn(c.topic)
        if owner is None:
            raise HTTPException(status_code=404, detail="Topic not found (no owner)")
        # Always route commits to owner for consistency (commits sync owner -> followers)
        if owner != broker_id:
            base = owner_base_fn(owner)
            return await _proxy_post(base, "/commit", c.model_dump())
        notifier_commit(c.topic, c.consumer_id, c.group_id, c.offset)
        return {"topic": c.topic, "committed": c.offset, "replica_id": broker_id}

    @router.get("/replicas/{topic}")
    async def get_replicas(topic: str):
        """Return all available replicas for a topic for client-side load balancing."""
        await refresh_owner_cache_fn()
        owner = owner_for_fn(topic)
        if owner is None:
            raise HTTPException(status_code=404, detail="Topic not found")
        followers = followers_for_fn(topic)
        isr = isr_members_fn(topic)
        replicas_info = []
        for bid in [owner] + followers:
            base = broker_base_fn(bid)
            replicas_info.append({
                "broker_id": bid,
                "api_base": base,
                "is_owner": bid == owner,
                "in_isr": bid in isr,
            })
        return {
            "topic": topic,
            "owner": owner,
            "replicas": replicas_info,
            "isr": isr,
        }

    return router
