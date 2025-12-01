import asyncio
from typing import Any, Dict, Iterable, List, Sequence, Union

import httpx

from bully import update_peer_config
from config import (
    BROKER_ID,
    DISCOVERY_SERVICE_URL,
    ELECTION_HOST,
    ELECTION_PORT,
    SELF_API_BASE,
    update_broker_api,
)
from lifecycle import update_broker_api as lifecycle_update_broker_api
from replication import update_broker_api as replication_update_broker_api
from schemas import BrokerEndpoint


def _local_endpoint_dict() -> Dict[str, Any]:
    return {
        "broker_id": BROKER_ID,
        "api_base": SELF_API_BASE,
        "election_host": ELECTION_HOST,
        "election_port": ELECTION_PORT,
    }


def _coerce_endpoints(brokers: Iterable[Union[BrokerEndpoint, Dict[str, Any]]]) -> List[BrokerEndpoint]:
    normalized: List[BrokerEndpoint] = []
    for entry in brokers:
        if isinstance(entry, BrokerEndpoint):
            normalized.append(entry)
        else:
            try:
                normalized.append(BrokerEndpoint.model_validate(entry))
            except Exception:
                continue
    return normalized


def apply_discovery_update(brokers: Sequence[Union[BrokerEndpoint, Dict[str, Any]]]) -> None:
    endpoints = _coerce_endpoints(brokers)
    if not endpoints:
        endpoints = _coerce_endpoints([_local_endpoint_dict()])
    api_map: Dict[int, str] = {}
    election_map: Dict[int, Any] = {}
    for endpoint in endpoints:
        api_map[endpoint.broker_id] = endpoint.api_base.rstrip("/")
        election_map[endpoint.broker_id] = (
            endpoint.election_host,
            int(endpoint.election_port),
        )
    if BROKER_ID not in api_map:
        api_map[BROKER_ID] = SELF_API_BASE.rstrip("/")
    if BROKER_ID not in election_map:
        election_map[BROKER_ID] = (ELECTION_HOST, ELECTION_PORT)
    update_broker_api(api_map)
    lifecycle_update_broker_api(api_map)
    replication_update_broker_api(api_map)
    update_peer_config(election_map)


def seed_local_endpoint() -> None:
    apply_discovery_update([_local_endpoint_dict()])


async def register_with_discovery(max_attempts: int = 3, backoff: float = 1.0) -> bool:
    url = DISCOVERY_SERVICE_URL.rstrip("/") + "/register"
    payload = _local_endpoint_dict()
    attempt = 0
    while attempt < max_attempts:
        attempt += 1
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                resp = await client.post(url, json=payload)
                resp.raise_for_status()
                data = resp.json()
        except Exception as exc:
            print(f"[P{BROKER_ID}] discovery registration attempt {attempt} failed: {exc}")
            if attempt >= max_attempts:
                return False
            await asyncio.sleep(backoff * attempt)
            continue
        brokers = data.get("brokers") if isinstance(data, dict) else None
        if isinstance(brokers, list):
            apply_discovery_update(brokers)
        return True
    return False
