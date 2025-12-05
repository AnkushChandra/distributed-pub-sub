from typing import Any, Dict, List, Optional
import hashlib
import httpx
from fastapi import APIRouter, HTTPException, Query
from notifier import commit as notifier_commit, committed as notifier_committed
from notifier import enforce_group_singleton, get_notifier
from schemas import CommitRequest, PublishRequest, SubscribeRequest

_consumer_sticky_map: Dict[tuple, int] = {}


def build_client_router(*, broker_id: int, owner_for_fn, refresh_owner_cache_fn, owner_base_fn,
                        append_record_fn, poll_records_fn, isr_members_fn, replicate_to_isr_fn,
                        followers_for_fn, broker_base_fn):
    router = APIRouter()

    async def _proxy(method: str, base: str, path: str, data: Any = None, params: Any = None, timeout: float = 10.0):
        url = f"{base}{path}"
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                if method == "GET":
                    r = await client.get(url, params=params)
                else:
                    r = await client.post(url, json=data)
        except httpx.RequestError as e:
            raise HTTPException(status_code=502, detail=f"Upstream unreachable: {e}")
        if r.status_code >= 400:
            raise HTTPException(status_code=r.status_code, detail=r.json() if "json" in (r.headers.get("content-type") or "") else r.text)
        return r.json()

    def _get_replicas(topic: str) -> List[int]:
        owner = owner_for_fn(topic)
        if not owner:
            return []
        return [owner] + [f for f in followers_for_fn(topic) if f != owner]

    def _select_replica(topic: str, consumer_id: Optional[str] = None) -> Optional[int]:
        replicas = _get_replicas(topic)
        if not replicas:
            return None
        if broker_id in replicas:
            return broker_id
        if consumer_id:
            key = (topic, consumer_id)
            if key in _consumer_sticky_map and _consumer_sticky_map[key] in replicas:
                return _consumer_sticky_map[key]
            h = int(hashlib.md5(consumer_id.encode()).hexdigest(), 16)
            assigned = replicas[h % len(replicas)]
            _consumer_sticky_map[key] = assigned
            return assigned
        return replicas[0]

    @router.post("/topics/{topic}/publish")
    async def publish(topic: str, req: PublishRequest):
        await refresh_owner_cache_fn()
        owner = owner_for_fn(topic)
        if not owner:
            raise HTTPException(status_code=404, detail="Topic not found")
        if owner != broker_id:
            return await _proxy("POST", owner_base_fn(owner), f"/topics/{topic}/publish", req.model_dump())
        off = append_record_fn(topic, req.key, req.value, req.headers)
        result = await replicate_to_isr_fn(topic, off, req.key, req.value, req.headers)
        if not result.get("quorum_met", True):
            raise HTTPException(status_code=503, detail="ISR replication failed")
        await get_notifier(topic).notify()
        return {"topic": topic, "offset": off, "isr": isr_members_fn(topic)}

    @router.post("/subscribe")
    async def subscribe(sub: SubscribeRequest):
        await refresh_owner_cache_fn()
        owner = owner_for_fn(sub.topic)
        if not owner:
            raise HTTPException(status_code=404, detail="Topic not found")
        replicas = _get_replicas(sub.topic)
        if broker_id not in replicas:
            target = _select_replica(sub.topic)
            return await _proxy("POST", broker_base_fn(target), "/subscribe", sub.model_dump())
        if sub.group_id and not sub.consumer_id:
            raise HTTPException(status_code=400, detail="group_id requires consumer_id")
        try:
            enforce_group_singleton(sub.topic, sub.group_id, sub.consumer_id)
        except ValueError as e:
            raise HTTPException(status_code=409, detail=str(e))
        committed = notifier_committed(sub.topic, sub.consumer_id, sub.group_id)
        start = sub.from_offset if sub.from_offset is not None else (committed + 1)
        return {"topic": sub.topic, "start_offset": max(start, 0), "committed": committed, "replica_id": broker_id}

    @router.get("/poll")
    async def poll(topic: str = Query(...), consumer_id: Optional[str] = Query(None),
                   group_id: Optional[str] = Query(None), from_offset: Optional[int] = Query(None),
                   max_records: int = Query(100, ge=1, le=1000), timeout_ms: int = Query(15000, ge=0, le=60000),
                   prefer_replica: Optional[int] = Query(None)):
        owner = owner_for_fn(topic)
        if not owner:
            raise HTTPException(status_code=404, detail="Topic not found")
        
        replicas = _get_replicas(topic)
        is_replica = broker_id in replicas
        
        # Build targets to try
        targets = []
        if prefer_replica and prefer_replica in replicas and prefer_replica != broker_id:
            targets.append(prefer_replica)
        if not is_replica:
            targets.extend(r for r in replicas if r != broker_id and r not in targets)
        
        params = {"topic": topic, "max_records": max_records, "timeout_ms": timeout_ms}
        if consumer_id:
            params["consumer_id"] = consumer_id
        if group_id:
            params["group_id"] = group_id
        if from_offset is not None:
            params["from_offset"] = from_offset
        
        proxy_timeout = max(15.0, timeout_ms / 1000.0 + 5.0)
        for target in targets:
            try:
                async with httpx.AsyncClient(timeout=proxy_timeout) as client:
                    r = await client.get(f"{broker_base_fn(target)}/poll", params=params)
                if r.status_code < 400:
                    return r.json()
            except httpx.RequestError:
                if consumer_id:
                    _consumer_sticky_map.pop((topic, consumer_id), None)
        
        if not is_replica:
            raise HTTPException(status_code=502, detail="All replicas unreachable")

        # Serve locally
        if group_id:
            if not consumer_id:
                raise HTTPException(status_code=400, detail="group_id requires consumer_id")
            try:
                enforce_group_singleton(topic, group_id, consumer_id)
            except ValueError as e:
                raise HTTPException(status_code=409, detail=str(e))

        committed = notifier_committed(topic, consumer_id, group_id)
        start = max(from_offset if from_offset is not None else (committed + 1), 0)
        recs = poll_records_fn(topic, start, max_records)
        if not recs and timeout_ms > 0:
            if await get_notifier(topic).wait(timeout_ms / 1000.0):
                recs = poll_records_fn(topic, start, max_records)
        return {"topic": topic, "records": recs or [], "next_offset": start + len(recs or []),
                "committed": committed, "replica_id": broker_id}

    @router.post("/commit")
    async def commit_endpoint(c: CommitRequest):
        if c.offset < -1:
            raise HTTPException(status_code=400, detail="invalid offset")
        await refresh_owner_cache_fn()
        owner = owner_for_fn(c.topic)
        if not owner:
            raise HTTPException(status_code=404, detail="Topic not found")
        if owner != broker_id:
            return await _proxy("POST", owner_base_fn(owner), "/commit", c.model_dump())
        notifier_commit(c.topic, c.consumer_id, c.group_id, c.offset)
        return {"topic": c.topic, "committed": c.offset, "replica_id": broker_id}

    @router.get("/replicas/{topic}")
    async def get_replicas_endpoint(topic: str):
        await refresh_owner_cache_fn()
        owner = owner_for_fn(topic)
        if not owner:
            raise HTTPException(status_code=404, detail="Topic not found")
        followers = followers_for_fn(topic)
        isr = isr_members_fn(topic)
        return {"topic": topic, "owner": owner, "isr": isr,
                "replicas": [{"broker_id": b, "api_base": broker_base_fn(b), "is_owner": b == owner, "in_isr": b in isr}
                             for b in [owner] + followers]}

    return router
