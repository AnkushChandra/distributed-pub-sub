# election.py
import os
from typing import Optional

from bully import BullyService, CONFIG, config_snapshot, update_peer_config
from config import ELECTION_HOST, ELECTION_PORT

# Read broker/process id from env
BROKER_ID = int(os.getenv("BROKER_ID", "1"))

_node: Optional[BullyService] = None


def _ensure_self_membership() -> None:
    if BROKER_ID in CONFIG:
        return
    peers = config_snapshot()
    peers[BROKER_ID] = (ELECTION_HOST, ELECTION_PORT)
    update_peer_config(peers)


def start_election_service():
    global _node
    if _node is not None:
        return
    _node = BullyService(BROKER_ID)
    _node.start()


def stop_election_service():
    global _node
    if _node is None:
        return
    _node.stop()
    _node = None


def is_cluster_leader() -> bool:
    """True if this broker is currently the cluster-wide coordinator."""
    if _node is None:
        return False
    return _node.is_leader


def get_leader_id() -> Optional[int]:
    if _node is None:
        return None
    return _node.current_leader
