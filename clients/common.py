"""Shared utilities for CLI clients."""
import os
import sys
from typing import List, Optional
import httpx

DEFAULT_BROKER = os.getenv("BROKER_API", "http://127.0.0.1:8081")


def default_peers() -> List[str]:
    peers_env = os.getenv("BROKER_PEERS")
    if peers_env:
        return [p.strip() for p in peers_env.split(",") if p.strip()]
    return [f"http://127.0.0.1:808{i}" for i in range(1, 6)]


def discover_leader(peers: List[str]) -> str:
    """Find the cluster leader from a list of broker URLs."""
    for base in peers:
        try:
            resp = httpx.get(f"{base.rstrip('/')}/admin/leader", timeout=3.0)
            resp.raise_for_status()
            if resp.json().get("this_broker_is_leader"):
                return base.rstrip("/")
        except httpx.HTTPError:
            continue
    raise SystemExit("Could not discover cluster leader")


def get_leader_url(broker: Optional[str], peers: Optional[List[str]]) -> str:
    """Get leader URL, auto-discovering if needed."""
    if broker:
        try:
            resp = httpx.get(f"{broker.rstrip('/')}/admin/leader", timeout=3.0)
            resp.raise_for_status()
            if resp.json().get("this_broker_is_leader"):
                return broker.rstrip("/")
        except httpx.HTTPError:
            pass
    peer_list = peers or default_peers()
    if broker and broker not in peer_list:
        peer_list.insert(0, broker)
    return discover_leader(peer_list)


def error_exit(msg: str) -> None:
    print(f"Error: {msg}", file=sys.stderr)
    sys.exit(1)
