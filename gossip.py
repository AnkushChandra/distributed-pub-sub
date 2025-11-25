from __future__ import annotations
import asyncio, random, time
from typing import Dict, Any, List
from dataclasses import dataclass
import aiohttp


# -------------------------------------------------
# Membership metadata
# -------------------------------------------------
@dataclass
class Member:
    node_id: str
    addr: str
    status: str
    incarnation: int
    last_seen: float


def _now() -> float:
    return time.monotonic()


def _status_priority(s: str) -> int:
    # higher value = “stronger” state
    return {"alive": 2, "suspect": 1, "dead": 0}.get(s, 0)


class GossipState:
    """
    GossipState maintains *only* cluster membership metadata.
    No user / topic data is carried here.
    """

    def __init__(self, self_id: str, self_addr: str):
        self.self_id = self_id
        self.self_addr = self_addr

        # membership table: node_id -> Member
        self.members: Dict[str, Member] = {
            self_id: Member(self_id, self_addr, "alive", 1, _now())
        }

        # tuning knobs
        self.interval_sec = 0.5
        self.fanout = 3
        self.suspect_after = 2.0
        self.dead_after = 4.0

    # -------- membership management --------
    def upsert_member(self, rec: Dict[str, Any]) -> None:
        mid = rec["node_id"]
        old = self.members.get(mid)

        newer = (
            old is None
            or rec["incarnation"] > old.incarnation
            or (
                rec["incarnation"] == old.incarnation
                and rec["status_priority"] > _status_priority(old.status)
            )
        )

        if newer:
            self.members[mid] = Member(
                mid,
                rec["addr"],
                rec["status"],
                rec["incarnation"],
                _now(),
            )

    def touch_self(self) -> None:
        me = self.members[self.self_id]
        me.last_seen = _now()
        me.status = "alive"

    def suspect_and_maybe_dead(self) -> None:
        t = _now()
        for m in self.members.values():
            if m.node_id == self.self_id:
                continue
            silent = t - m.last_seen
            if m.status == "alive" and silent >= self.suspect_after:
                m.status = "suspect"
            elif m.status == "suspect" and silent >= self.dead_after:
                m.status = "dead"

    def pick_peers(self, n: int) -> List[Member]:
        peers = [
            m
            for m in self.members.values()
            if m.node_id != self.self_id and m.status != "dead"
        ]
        random.shuffle(peers)
        return peers[:n]

    def serialize_view(self, max_items: int = 64) -> List[Dict[str, Any]]:
        """Return a randomized, truncated view of membership metadata."""
        def s(m: Member) -> Dict[str, Any]:
            return {
                "node_id": m.node_id,
                "addr": m.addr,
                "status": m.status,
                "incarnation": m.incarnation,
                "status_priority": _status_priority(m.status),
                "last_seen": m.last_seen,
            }

        items = list(self.members.values())
        random.shuffle(items)
        return [s(x) for x in items[:max_items]]

    def merge_remote_view(self, remote: List[Dict[str, Any]]) -> None:
        for r in remote:
            self.upsert_member(r)


# -------------------------------------------------
# Gossip service (async loop + HTTP calls)
# -------------------------------------------------
class GossipService:
    def __init__(self, state: GossipState, http: aiohttp.ClientSession):
        self.state = state
        self.http = http
        self._stop = asyncio.Event()

    async def start(self, seed_peers: List[str]) -> None:
        # seed peers are stored as “seed-0”, “seed-1” members initially
        t = _now()
        for i, addr in enumerate(seed_peers):
            nid = f"seed-{i}"
            self.state.members.setdefault(
                nid,
                Member(nid, addr, "alive", 0, t),
            )

        asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._stop.set()

    async def _loop(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(self.state.interval_sec)
            self.state.touch_self()
            self.state.suspect_and_maybe_dead()
            peers = self.state.pick_peers(self.state.fanout)
            if not peers:
                continue
            await asyncio.gather(
                *(self._pushpull(p) for p in peers),
                return_exceptions=True,
            )

    async def _pushpull(self, peer: Member) -> None:
        """Send only membership view; receive only membership view."""
        payload = {
            "from_id": self.state.self_id,
            "from_addr": self.state.self_addr,
            "membership": self.state.serialize_view(),
        }
        url = peer.addr.rstrip("/") + "/_gossip/pushpull"
        try:
            async with self.http.post(url, json=payload, timeout=0.35) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self._apply_pushpull(data)
        except Exception as e:
            pass

    def _apply_pushpull(self, data: Dict[str, Any]) -> None:
        remote = data.get("membership", [])
        self.state.merge_remote_view(remote)
def build_state(broker_id: str, broker_addr: str) -> GossipState:
    return GossipState(broker_id, broker_addr)
