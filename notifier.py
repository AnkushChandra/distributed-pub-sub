import asyncio
import time
from typing import Dict, Optional, Tuple

import storage as st

LEASE_TTL_SEC = 10.0


class TopicNotifier:
    def __init__(self, topic: str):
        self.topic = topic
        self._cond = asyncio.Condition()

    async def notify(self) -> None:
        async with self._cond:
            self._cond.notify_all()

    async def wait(self, timeout: float) -> bool:
        async with self._cond:
            try:
                await asyncio.wait_for(self._cond.wait(), timeout=timeout)
                return True
            except asyncio.TimeoutError:
                return False


_notifiers: Dict[str, TopicNotifier] = {}
_group_leases: Dict[Tuple[str, str], Tuple[str, float]] = {}


def _prune_expired_leases(now: Optional[float] = None) -> None:
    if not _group_leases:
        return
    if now is None:
        now = time.time()
    expired = [key for key, (_, expiry) in _group_leases.items() if expiry <= now]
    for key in expired:
        _group_leases.pop(key, None)


def get_notifier(topic: str) -> TopicNotifier:
    if topic not in _notifiers:
        st.init_topic(topic)
        _notifiers[topic] = TopicNotifier(topic)
    return _notifiers[topic]


def enforce_group_singleton(topic: str, group_id: Optional[str], consumer_id: Optional[str] = None) -> None:
    if not group_id:
        return
    now = time.time()
    _prune_expired_leases(now)
    key = (topic, group_id)
    identity = consumer_id or f"anon:{topic}:{group_id}"
    holder, expiry = _group_leases.get(key, (None, 0.0))
    if holder is not None and holder != identity and expiry > now:
        raise ValueError("Group already has an active consumer")
    _group_leases[key] = (identity, now + LEASE_TTL_SEC)


def committed(topic: str, consumer_id: Optional[str], group_id: Optional[str]) -> int:
    return st.get_commit(topic, consumer_id, group_id)


def commit(topic: str, consumer_id: Optional[str], group_id: Optional[str], offset: int) -> None:
    st.set_commit(topic, consumer_id, group_id, offset)
    if group_id:
        identity = consumer_id or f"anon:{topic}:{group_id}"
        _group_leases[(topic, group_id)] = (identity, time.time() + LEASE_TTL_SEC)


def leases_snapshot() -> Dict[str, float]:
    _prune_expired_leases()
    return {f"{topic}|{group}": expiry for (topic, group), (_, expiry) in _group_leases.items()}
