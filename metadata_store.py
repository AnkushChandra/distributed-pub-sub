import json as _json
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import storage as st
from config import BROKER_ID
from election import is_cluster_leader

Placement = Dict[str, Any]

TOPIC_METADATA_FILE = st.DATA_DIR / "topics.json"
_topic_meta_lock = threading.RLock()
_metadata_version: int = 0

TOPIC_PLACEMENTS: Dict[str, Placement] = {}
PLACEMENT_CACHE: Dict[str, Placement] = {}
PLACEMENT_CACHE_TTL_SEC = 10.0
_placement_cache_expiry: float = 0.0


def normalize_placement(entry: Any) -> Placement:
    owner = None
    followers: List[int] = []
    if isinstance(entry, dict):
        owner = entry.get("owner")
        followers = entry.get("followers", [])
    elif isinstance(entry, list) and entry:
        owner = entry[0]
        followers = entry[1:]
    else:
        owner = entry
        followers = []
    try:
        owner_int = int(owner)
    except (TypeError, ValueError):
        owner_int = None
    follower_ints: List[int] = []
    for f in followers:
        try:
            follower_ints.append(int(f))
        except (TypeError, ValueError):
            continue
    deduped = sorted(dict.fromkeys(follower_ints))
    return {"owner": owner_int, "followers": deduped}


def clone_placements(src: Dict[str, Placement]) -> Dict[str, Placement]:
    return {
        str(topic): {
            "owner": placement.get("owner"),
            "followers": list(placement.get("followers", [])),
        }
        for topic, placement in src.items()
    }


def _load_persisted_metadata() -> Tuple[int, Dict[str, Placement]]:
    if not TOPIC_METADATA_FILE.exists():
        return 0, {}
    try:
        text = TOPIC_METADATA_FILE.read_text()
        data = _json.loads(text)
        if isinstance(data, dict) and "topics" in data:
            topics_raw = data.get("topics", {})
            version = int(data.get("version", 0))
        else:
            topics_raw = data if isinstance(data, dict) else {}
            version = 0
        placements = {str(k): normalize_placement(v) for k, v in (topics_raw or {}).items()}
        return version, placements
    except Exception as e:
        print(f"[P{BROKER_ID}] failed to load topic metadata: {e}")
    return 0, {}


def _persist_metadata_snapshot(snapshot: Dict[str, Placement], version: int):
    with _topic_meta_lock:
        try:
            payload = {"version": version, "topics": snapshot}
            tmp_path = TOPIC_METADATA_FILE.with_suffix(".tmp")
            tmp_path.write_text(_json.dumps(payload, sort_keys=True))
            tmp_path.replace(TOPIC_METADATA_FILE)
        except Exception as e:
            print(f"[P{BROKER_ID}] failed to persist topic metadata: {e}")


def apply_metadata_snapshot(snapshot: Dict[str, Placement], version: int, *, persist: bool):
    global _metadata_version, _placement_cache_expiry
    with _topic_meta_lock:
        TOPIC_PLACEMENTS.clear()
        TOPIC_PLACEMENTS.update(clone_placements(snapshot))
        PLACEMENT_CACHE.clear()
        PLACEMENT_CACHE.update(clone_placements(snapshot))
        _metadata_version = version
        if persist:
            _persist_metadata_snapshot(clone_placements(TOPIC_PLACEMENTS), _metadata_version)
    _placement_cache_expiry = time.time() + PLACEMENT_CACHE_TTL_SEC


def metadata_snapshot() -> Dict[str, Any]:
    with _topic_meta_lock:
        return {"version": _metadata_version, "topics": clone_placements(TOPIC_PLACEMENTS)}


def metadata_version() -> int:
    with _topic_meta_lock:
        return _metadata_version


def placement_source() -> Dict[str, Placement]:
    return TOPIC_PLACEMENTS if is_cluster_leader() else PLACEMENT_CACHE


def owner_for(topic: str) -> Optional[int]:
    placement = placement_source().get(topic)
    if not placement:
        return None
    owner = placement.get("owner")
    try:
        return int(owner) if owner is not None else None
    except (TypeError, ValueError):
        return None


def followers_for(topic: str, *, leader_view: bool = False) -> List[int]:
    src = TOPIC_PLACEMENTS if leader_view else placement_source()
    placement = src.get(topic)
    if not placement:
        return []
    followers = placement.get("followers", [])
    return list(followers) if isinstance(followers, list) else []


def maybe_update_metadata(snapshot: Dict[str, Placement], version: int, *, persist: bool) -> bool:
    global _metadata_version
    with _topic_meta_lock:
        current = _metadata_version
    if version <= current:
        return False
    apply_metadata_snapshot(snapshot, version, persist=persist)
    return True


def maybe_update_metadata_from_remote(meta: Dict[str, Any]) -> bool:
    if not meta:
        return False
    topics = meta.get("topics")
    version = meta.get("version")
    if topics is None or version is None:
        return False
    try:
        version_int = int(version)
        snapshot = {str(k): normalize_placement(v) for k, v in topics.items()}
    except Exception:
        return False
    return maybe_update_metadata(snapshot, version_int, persist=True)


def set_topic_placement(topic: str, owner_id: int, followers: Optional[List[int]] = None):
    followers_list = sorted(dict.fromkeys(int(f) for f in (followers or []) if f is not None))
    placement = {"owner": int(owner_id), "followers": followers_list}
    with _topic_meta_lock:
        snapshot = clone_placements(TOPIC_PLACEMENTS)
        snapshot[topic] = placement
        version = _metadata_version + 1
    apply_metadata_snapshot(snapshot, version, persist=True)


def set_topic_owner(topic: str, owner_id: int):
    followers = followers_for(topic, leader_view=True)
    set_topic_placement(topic, owner_id, followers)


def cache_is_fresh(now: float) -> bool:
    return now < _placement_cache_expiry


def mark_cache_fresh(now: float):
    global _placement_cache_expiry
    _placement_cache_expiry = now + PLACEMENT_CACHE_TTL_SEC


_boot_version, _boot_meta = _load_persisted_metadata()
apply_metadata_snapshot(_boot_meta, _boot_version, persist=False)
