from __future__ import annotations

import asyncio
from typing import Dict, List

import httpx
from fastapi import FastAPI

from schemas import BrokerEndpoint, DiscoveryUpdateRequest

app = FastAPI(title="Discovery Service")

_BROKERS: Dict[int, BrokerEndpoint] = {}
_BROADCAST_LOCK = asyncio.Lock()


def _snapshot() -> List[BrokerEndpoint]:
    return list(_BROKERS.values())


def _store_endpoint(endpoint: BrokerEndpoint) -> None:
    _BROKERS[endpoint.broker_id] = endpoint


def _request_payload() -> DiscoveryUpdateRequest:
    return DiscoveryUpdateRequest(brokers=_snapshot())


async def _broadcast_update() -> None:
    request = _request_payload()
    payload = request.model_dump()
    async with _BROADCAST_LOCK:
        async with httpx.AsyncClient(timeout=3.0) as client:
            for endpoint in _snapshot():
                url = endpoint.api_base.rstrip("/") + "/internal/discovery/update"
                try:
                    await client.post(url, json=payload)
                except Exception:
                    continue


@app.get("/brokers")
def brokers():
    return {"brokers": [ep.model_dump() for ep in _snapshot()]}


@app.post("/register")
async def register(endpoint: BrokerEndpoint):
    _store_endpoint(endpoint)
    await _broadcast_update()
    return {"brokers": [ep.model_dump() for ep in _snapshot()]}
