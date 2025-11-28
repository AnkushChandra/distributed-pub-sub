# replication.py
from __future__ import annotations

import asyncio
import json as _json
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import httpx
from pydantic import BaseModel

import storage

# ---------------- Global config ----------------

_BROKER_ID: Optional[int] = None
_BROKER_API: Dict[int, str] = {}
_DATA_DIR: Optional[Path] = None
_get_leader_id: Optional[Callable[[], Optional[int]]] = None

_META_FILE: Optional[Path] = None
_meta_lock = threading.RLock()

# ---------------- Topic placement ----------------

class TopicPlacement(BaseModel):
    owner: int
    followers: List[int] = []

_TOPICS: Dict[str, TopicPlacement] = {}      # leader's authoritative placement
_OWNER_CACHE: Dict[str, TopicPlacement] = {} # follower's cached view

_OWNER_CACHE_TTL_SEC = 10.0
_owner_cache_expiry: float = 0.0


# ---------------- Setup ----------------

def setup_replication(
    broker_id: int,
    broker_api: Dict[int, str],
    data_dir: Path,
    leader_getter: Callable[[], Optional[int]],
) -> None:
    """
    Called once from main.py after election is initialized.
    """
    global _BROKER_ID, _BROKER_API, _DATA_DIR, _get_leader_id, _META_FILE
    _BROKER_ID = broker_id
    _BROKER_API = broker_api
    _DATA_DIR = data_dir
    _get_leader_id = leader_getter
    _META_FILE = data_dir / "topics.json"
    _load_metadata()


def _load_metadata() -> None:
    global _TOPICS, _OWNER_CACHE
    if _META_FILE is None or not _META_FILE.exists():
        _TOPICS = {}
        _OWNER_CACHE = {}
        return

    try:
        raw = _META_FILE.read_text()
        data = _json.loads(raw)
    except Exception as e:
        print(f"[Broker {_BROKER_ID}] failed to read topics metadata: {e}")
        _TOPICS = {}
        _OWNER_CACHE = {}
        return

    topics: Dict[str, TopicPlacement] = {}
    for t, v in data.items():
        if isinstance(v, int):  # backward compat
            topics[t] = TopicPlacement(owner=v, followers=[])
        else:
            topics[t] = TopicPlacement(**v)

    _TOPICS = topics
    _OWNER_CACHE = dict(topics)


def _persist_metadata() -> None:
    if _META_FILE is None:
        return
    with _meta_lock:
        tmp = _META_FILE.with_suffix(".tmp")
        serializable = {t: p.model_dump() for t, p in _TOPICS.items()}
        try:
            tmp.write_text(_json.dumps(serializable, indent=2))
            tmp.replace(_META_FILE)
        except Exception as e:
            print(f"[Broker {_BROKER_ID}] failed to persist topics metadata: {e}")


# ---------------- Leader helpers ----------------

def _leader_id() -> Optional[int]:
    return _get_leader_id() if _get_leader_id is not None else None


def is_leader() -> bool:
    lid = _leader_id()
    return lid is not None and lid == _BROKER_ID


# ---------------- Metadata queries ----------------

def get_owner_for(topic: str) -> Optional[int]:
    """
    Return broker id of the primary for this topic, or None if unknown.
    """
    meta = _TOPICS if is_leader() else _OWNER_CACHE
    placement = meta.get(topic)
    return placement.owner if placement else None


def get_all_metadata_for_api() -> Dict[str, Dict[str, Any]]:
    """
    Used by /metadata endpoint in main.py.
    """
    meta = _TOPICS if is_leader() else _OWNER_CACHE
    return {t: p.model_dump() for t, p in meta.items()}


# ---------------- Owner cache refresh on followers ----------------

async def refresh_owner_cache_if_needed() -> None:
    """
    Followers call this periodically (and on publish) to sync topic placement from leader.
    Leader does nothing.
    """
    global _OWNER_CACHE, _owner_cache_expiry

    if is_leader():
        return

    now = time.time()
    if now < _owner_cache_expiry:
        return

    lid = _leader_id()
    if lid is None or lid not in _BROKER_API:
        return

    base = _BROKER_API[lid]
    url = f"{base}/metadata"

    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            r = await client.get(url)
            r.raise_for_status()
            payload = r.json()
    except Exception as e:
        print(f"[Broker {_BROKER_ID}] failed to refresh metadata from leader: {e}")
        return

    topics_raw = payload.get("topics", {})
    new_cache: Dict[str, TopicPlacement] = {}
    for t, v in topics_raw.items():
        if isinstance(v, int):
            new_cache[t] = TopicPlacement(owner=v, followers=[])
        else:
            new_cache[t] = TopicPlacement(**v)

    _OWNER_CACHE = new_cache
    _owner_cache_expiry = time.time() + _OWNER_CACHE_TTL_SEC


# ---------------- Topic assignment (leader only) ----------------

def _all_broker_ids() -> List[int]:
    return sorted(_BROKER_API.keys())


def assign_topic(topic: str, owner_id: Optional[int] = None, replication_factor: int = 3) -> TopicPlacement:
    """
    Leader only: selects primary + followers for a topic and persists placement.
    """
    if not is_leader():
        raise RuntimeError("assign_topic called on non-leader")

    brokers = _all_broker_ids()

    if owner_id is None:
        lid = _leader_id()
        if lid is None:
            raise RuntimeError("no leader known")
        owner_id = lid

    if owner_id not in brokers:
        raise ValueError(f"Unknown broker id {owner_id}")

    # simple: followers = next replication_factor-1 brokers in the sorted list
    others = [b for b in brokers if b != owner_id]
    followers = others[: max(0, replication_factor - 1)]

    placement = TopicPlacement(owner=owner_id, followers=followers)
    _TOPICS[topic] = placement
    _persist_metadata()
    return placement


# ---------------- Follower replication loop (Kafka-style pull) ----------------

async def follower_replication_loop(poll_interval: float = 0.5, batch_size: int = 100) -> None:
    """
    Runs on ALL brokers.
    - On leaders: mostly idle (they are not followers)
    - On followers: periodically:
        * refresh metadata
        * for each topic we follow:
            - compute local last offset
            - fetch new records from primary via /internal/topics/{topic}/fetch
            - apply them via storage.replicate_record
    """
    print(f"[Broker {_BROKER_ID}] follower replication loop started")

    while True:
        try:
            await refresh_owner_cache_if_needed()

            # Work on a snapshot so it doesn't change during iteration
            cache_snapshot = dict(_OWNER_CACHE)

            for topic, placement in cache_snapshot.items():
                # not a follower for this topic → skip
                if _BROKER_ID not in placement.followers:
                    continue

                owner = placement.owner
                if owner == _BROKER_ID:
                    # we are primary for this topic
                    continue

                base = _BROKER_API.get(owner)
                if not base:
                    continue

                # where are we locally?
                local_last = storage.latest_offset(topic)
                fetch_offset = max(0, local_last + 1)

                fetch_url = f"{base}/internal/topics/{topic}/fetch"
                params = {"offset": fetch_offset, "max_batch": batch_size}

                async with httpx.AsyncClient(timeout=3.0) as client:
                    resp = await client.get(fetch_url, params=params)
                    if resp.status_code == 404:
                        continue
                    resp.raise_for_status()
                    data = resp.json()

                records = data.get("records", [])
                if not records:
                    continue

                # apply records locally with explicit offsets
                for rec in records:
                    off = int(rec["offset"])
                    key = rec.get("key")
                    value = rec.get("value")
                    headers = rec.get("headers") or {}
                    storage.replicate_record(topic, off, key, value, headers)

        except Exception as e:
            print(f"[Broker {_BROKER_ID}] error in follower_replication_loop: {e}")

        await asyncio.sleep(poll_interval)
