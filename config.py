import json
import os
from typing import Dict

# -------------------- Broker identity & API peers --------------------
# This node's ID (must match bully.CONFIG ids)
BROKER_ID = int(os.getenv("BROKER_ID", "1"))
MIN_SYNC_FOLLOWERS = max(0, int(os.getenv("MIN_SYNC_FOLLOWERS", "1")))

DISCOVERY_SERVICE_URL = os.getenv("DISCOVERY_SERVICE_URL", "http://127.0.0.1:9090")


def _default_api_base() -> str:
    env_base = os.getenv("SELF_API_BASE") or os.getenv("BROKER_API_BASE")
    if env_base:
        return env_base.rstrip("/")
    host = os.getenv("BROKER_HTTP_HOST", "127.0.0.1")
    default_port = int(os.getenv("BROKER_HTTP_PORT", str(8080 + BROKER_ID)))
    return f"http://{host}:{default_port}"


SELF_API_BASE = _default_api_base()
ELECTION_HOST = os.getenv("ELECTION_HOST", "127.0.0.1")
ELECTION_PORT = int(os.getenv("ELECTION_PORT", str(5000 + BROKER_ID)))

# Map broker id -> API base URL (used for routing/proxying)
# You can override with JSON in BROKER_API_PEERS='{"1":"http://127.0.0.1:8081",...}'
DEFAULT_API: Dict[int, str] = {}
_api_env = os.getenv("BROKER_API_PEERS")
if _api_env:
    DEFAULT_API = {int(k): v for k, v in json.loads(_api_env).items()}

BROKER_API: Dict[int, str] = dict(DEFAULT_API)


def update_broker_api(peers: Dict[int, str]) -> None:
    BROKER_API.clear()
    for key, base in peers.items():
        try:
            bid = int(key)
        except (TypeError, ValueError):
            continue
        normalized = (base or "").rstrip("/")
        if normalized:
            BROKER_API[bid] = normalized


def self_api_base() -> str:
    base = BROKER_API.get(BROKER_ID, SELF_API_BASE)
    if not base:
        raise RuntimeError(f"No API base configured for broker {BROKER_ID}")
    return base.rstrip("/")


def peer_map(exclude_self: bool = True) -> Dict[str, str]:
    return {
        str(bid): base
        for bid, base in BROKER_API.items()
        if not exclude_self or bid != BROKER_ID
    }
