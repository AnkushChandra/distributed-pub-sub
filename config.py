import json
import os
from typing import Dict

# -------------------- Broker identity & API peers --------------------
# This node's ID (must match bully.CONFIG ids)
BROKER_ID = int(os.getenv("BROKER_ID", "1"))

# Map broker id -> API base URL (used for routing/proxying)
# You can override with JSON in BROKER_API_PEERS='{"1":"http://127.0.0.1:8081",...}'
DEFAULT_API = {
    1: "http://127.0.0.1:8081",
    2: "http://127.0.0.1:8082",
    3: "http://127.0.0.1:8083",
    4: "http://127.0.0.1:8084",
    5: "http://127.0.0.1:8085",
}

_api_env = os.getenv("BROKER_API_PEERS")
BROKER_API: Dict[int, str] = (
    DEFAULT_API
    if not _api_env
    else {int(k): v for k, v in json.loads(_api_env).items()}
)


def self_api_base() -> str:
    base = BROKER_API.get(BROKER_ID)
    if not base:
        raise RuntimeError(f"No API base configured for broker {BROKER_ID}")
    return base


def peer_map(exclude_self: bool = True) -> Dict[str, str]:
    return {
        str(bid): base
        for bid, base in BROKER_API.items()
        if not exclude_self or bid != BROKER_ID
    }
