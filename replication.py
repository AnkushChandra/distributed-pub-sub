from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional

import httpx

import storage

PlacementSnapshot = Dict[str, Dict[str, Any]]

_BROKER_ID: int = 0
_BROKER_API: Dict[int, str] = {}
_get_snapshot: Optional[Callable[[], PlacementSnapshot]] = None
_refresh_fn: Optional[Callable[[], Awaitable[None]]] = None

# Metrics tracking
_metrics = {
    "push_attempts": 0,
    "push_success": 0,
    "push_failed": 0,
    "pull_attempts": 0,
    "pull_success": 0,
    "pull_failed": 0,
    "records_pushed": 0,
    "records_pulled": 0,
    "total_push_latency_ms": 0,
    "total_pull_latency_ms": 0,
}
_metrics_lock = asyncio.Lock()


def get_replication_metrics() -> dict:
    return dict(_metrics)


def _set_broker_api(peers: Dict[int, str]) -> None:
    global _BROKER_API
    _BROKER_API = dict(peers)


def setup(
    *,
    broker_id: int,
    broker_api: Dict[int, str],
    placement_getter: Callable[[], PlacementSnapshot],
    refresh_fn: Callable[[], Awaitable[None]],
) -> None:
    global _BROKER_ID, _get_snapshot, _refresh_fn
    _BROKER_ID = broker_id
    _set_broker_api(broker_api)
    _get_snapshot = placement_getter
    _refresh_fn = refresh_fn


def update_broker_api(peers: Dict[int, str]) -> None:
    _set_broker_api(peers)


async def replicate_to_followers(
    *,
    topic: str,
    offset: int,
    key: Optional[str],
    value: Any,
    headers: Dict[str, Any],
    follower_ids: Iterable[int],
    timeout: float = 3.0,
) -> Dict[int, bool]:
    """Push a record to the given followers IN PARALLEL and return ack status per follower."""
    import time
    results: Dict[int, bool] = {}
    ids = [int(fid) for fid in follower_ids if fid is not None and int(fid) != _BROKER_ID]
    if not ids:
        return results
    payload = {
        "offset": int(offset),
        "key": key,
        "value": value,
        "headers": headers or {},
    }
    
    async def push_to_follower(fid: int, client: httpx.AsyncClient) -> tuple:
        base = _BROKER_API.get(fid)
        if not base:
            _metrics["push_failed"] += 1
            return (fid, False)
        url = f"{base}/internal/topics/{topic}/replicate"
        _metrics["push_attempts"] += 1
        start = time.time()
        try:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            _metrics["push_success"] += 1
            _metrics["records_pushed"] += 1
            _metrics["total_push_latency_ms"] += (time.time() - start) * 1000
            return (fid, True)
        except Exception as exc:
            print(f"[Broker {_BROKER_ID}] replicate push to P{fid} failed: {exc}")
            _metrics["push_failed"] += 1
            return (fid, False)
    
    # Push to all followers in parallel
    async with httpx.AsyncClient(timeout=timeout) as client:
        tasks = [push_to_follower(fid, client) for fid in ids]
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        for resp in responses:
            if isinstance(resp, tuple):
                fid, success = resp
                results[fid] = success
            elif isinstance(resp, Exception):
                print(f"[Broker {_BROKER_ID}] replicate task error: {resp}")
    
    return results


def _placements() -> PlacementSnapshot:
    if _get_snapshot is None:
        return {}
    return _get_snapshot()


async def _refresh_metadata() -> None:
    if _refresh_fn is None:
        return
    await _refresh_fn()


async def follower_replication_loop(
    poll_interval: float = 0.5,
    batch_size: int = 100,
    max_backoff: float = 5.0,
) -> None:
    if _get_snapshot is None or _refresh_fn is None:
        raise RuntimeError("replication.setup() must be called before starting the loop")
    print(f"[Broker {_BROKER_ID}] follower replication loop running")
    backoff = poll_interval
    while True:
        try:
            await _refresh_metadata()
            snapshot = _placements()
            for topic, placement in snapshot.items():
                followers = placement.get("followers") or []
                try:
                    follower_ids = [int(f) for f in followers]
                except Exception:
                    continue
                if _BROKER_ID not in follower_ids:
                    continue
                owner = placement.get("owner")
                try:
                    owner_id = int(owner)
                except (TypeError, ValueError):
                    continue
                if owner_id == _BROKER_ID:
                    continue
                base = _BROKER_API.get(owner_id)
                if not base:
                    continue
                local_last = storage.latest_offset(topic)
                fetch_offset = max(0, local_last + 1)
                import time as _time
                _metrics["pull_attempts"] += 1
                pull_start = _time.time()
                try:
                    async with httpx.AsyncClient(timeout=3.0) as client:
                        resp = await client.get(
                            f"{base}/internal/topics/{topic}/fetch",
                            params={
                                "offset": fetch_offset,
                                "max_batch": batch_size,
                                "include_commits": True,
                            },
                        )
                        if resp.status_code == 404:
                            continue
                        if resp.status_code == 409:
                            await _refresh_metadata()
                            continue
                        resp.raise_for_status()
                        data = resp.json()
                        _metrics["pull_success"] += 1
                        _metrics["total_pull_latency_ms"] += (_time.time() - pull_start) * 1000
                except Exception as e:
                    print(f"[Broker {_BROKER_ID}] replication fetch failed: {e}")
                    _metrics["pull_failed"] += 1
                    continue
                records = data.get("records", [])
                _metrics["records_pulled"] += len(records)
                for rec in records:
                    try:
                        off = int(rec["offset"])
                    except (KeyError, TypeError, ValueError):
                        continue
                    storage.replicate_record(
                        topic,
                        off,
                        rec.get("key"),
                        rec.get("value"),
                        rec.get("headers") or {},
                    )
                storage.apply_commit_snapshot(data.get("commits") or {})
            backoff = poll_interval
        except Exception as e:
            print(f"[Broker {_BROKER_ID}] replication loop error: {e}")
            backoff = min(max_backoff, backoff * 2)
        await asyncio.sleep(backoff)
