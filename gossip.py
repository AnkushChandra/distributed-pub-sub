from __future__ import annotations
import asyncio, random, time
from typing import Dict, Any, List
from dataclasses import dataclass
import aiohttp

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
    return {"alive": 2, "suspect": 1, "dead": 0}.get(s, 0)


class GossipState:
    def __init__(self, self_id: str, self_addr: str):
        self.self_id = self_id
        self.self_addr = self_addr

        self.members: Dict[str, Member] = {
            self_id: Member(self_id, self_addr, "alive", 1, _now())
        }

        self.interval_sec = 0.5
        self.fanout = 3
        self.suspect_after = 2.0
        self.dead_after = 4.0
        
        # Metrics
        self.metrics = {
            "rounds": 0,
            "messages_sent": 0,
            "messages_received": 0,
            "members_discovered": 0,
            "status_changes": 0,
            "merges_applied": 0,
            "start_time": time.time(),
            # Propagation tracking - tracks rounds to reach all nodes
            "propagation_tests": 0,
            "total_propagation_rounds": 0,
            "last_propagation_rounds": 0,
            "avg_propagation_rounds": 0.0,
        }
        # Track active propagation test
        self._active_test: Dict[str, Any] = {}
        # Track incarnations we've seen from each node
        self._seen_incarnations: Dict[str, int] = {}

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
            if old is None:
                self.metrics["members_discovered"] += 1
            elif old.status != rec["status"]:
                self.metrics["status_changes"] += 1
            self.members[mid] = Member(
                mid,
                rec["addr"],
                rec["status"],
                rec["incarnation"],
                _now(),
            )
            self.metrics["merges_applied"] += 1
            
            # Track when we first see a new incarnation from another node
            # This helps measure propagation - when did info about node X reach us?
            prev_inc = self._seen_incarnations.get(mid, 0)
            if rec["incarnation"] > prev_inc:
                self._seen_incarnations[mid] = rec["incarnation"]
                # If there's an active test from this origin, mark that we received it
                if self._active_test.get("origin") == mid and self._active_test.get("incarnation") == rec["incarnation"]:
                    if "received_round" not in self._active_test:
                        self._active_test["received_round"] = self.metrics["rounds"]
    
    def start_propagation_test(self) -> int:
        """Bump own incarnation to test propagation speed."""
        me = self.members[self.self_id]
        me.incarnation += 1
        self._active_test = {
            "start_round": self.metrics["rounds"],
            "origin": self.self_id,
            "incarnation": me.incarnation,
            "nodes_confirmed": {self.self_id},  # We already have our own info
        }
        return me.incarnation
    
    def record_propagation_ack(self, from_node: str, their_view: List[Dict[str, Any]]) -> None:
        """Check if a remote node has our latest incarnation (confirms propagation)."""
        if not self._active_test or self._active_test.get("completed"):
            return
        
        my_id = self.self_id
        my_inc = self._active_test.get("incarnation", 0)
        
        # Check if the remote node's view includes our latest incarnation
        for entry in their_view:
            if entry.get("node_id") == my_id and entry.get("incarnation", 0) >= my_inc:
                self._active_test.setdefault("nodes_confirmed", set()).add(from_node)
                break
    
    def check_propagation_complete(self) -> None:
        """Check if propagation test has completed (all nodes have our info)."""
        if not self._active_test or self._active_test.get("completed"):
            return
        
        current_round = self.metrics["rounds"]
        start_round = self._active_test.get("start_round", current_round)
        
        # Get all alive nodes
        alive_nodes = {m.node_id for m in self.members.values() if m.status == "alive"}
        confirmed = self._active_test.get("nodes_confirmed", set())
        
        # Check if all alive nodes have confirmed
        all_confirmed = alive_nodes <= confirmed
        timed_out = (current_round - start_round) >= 10  # Timeout after 10 rounds
        
        if all_confirmed or timed_out:
            rounds_taken = current_round - start_round
            if all_confirmed:
                # Only count successful propagations
                self.metrics["propagation_tests"] += 1
                self.metrics["total_propagation_rounds"] += rounds_taken
                self.metrics["last_propagation_rounds"] = rounds_taken
                if self.metrics["propagation_tests"] > 0:
                    self.metrics["avg_propagation_rounds"] = round(
                        self.metrics["total_propagation_rounds"] / self.metrics["propagation_tests"], 2
                    )
            self._active_test["completed"] = True

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

class GossipService:
    def __init__(
        self,
        state: GossipState,
        http: aiohttp.ClientSession,
        *,
        payload_fn=None,
        on_payload_fn=None,
    ):
        self.state = state
        self.http = http
        self._stop = asyncio.Event()
        self._payload_fn = payload_fn
        self._on_payload_fn = on_payload_fn

    async def start(self, seed_peers: Dict[str, str]) -> None:
        t = _now()
        for nid, addr in seed_peers.items():
            self.state.members.setdefault(
                nid,
                Member(nid, addr, "suspect", 0, t),
            )

        asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._stop.set()

    async def _loop(self) -> None:
        test_interval = 20  # Run propagation test every N rounds
        while not self._stop.is_set():
            await asyncio.sleep(self.state.interval_sec)
            self.state.touch_self()
            self.state.suspect_and_maybe_dead()
            peers = self.state.pick_peers(self.state.fanout)
            if not peers:
                continue
            self.state.metrics["rounds"] += 1
            self.state.metrics["messages_sent"] += len(peers)
            
            # Periodically start propagation test
            if self.state.metrics["rounds"] % test_interval == 0:
                self.state.start_propagation_test()
            
            # Check for completed propagation tests
            self.state.check_propagation_complete()
            
            await asyncio.gather(
                *(self._pushpull(p) for p in peers),
                return_exceptions=True,
            )

    async def _pushpull(self, peer: Member) -> None:
        payload = {
            "from_id": self.state.self_id,
            "from_addr": self.state.self_addr,
            "membership": self.state.serialize_view(),
        }
        if self._payload_fn:
            try:
                extra = self._payload_fn()
            except Exception:
                extra = None
            if extra:
                payload["metadata"] = extra
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
        self.state.metrics["messages_received"] += 1
        
        # Check if remote node has our latest info (for propagation tracking)
        from_id = data.get("from_id")
        if from_id and remote:
            self.state.record_propagation_ack(from_id, remote)
        
        self.state.merge_remote_view(remote)
        if self._on_payload_fn:
            try:
                self._on_payload_fn(data.get("metadata"))
            except Exception:
                pass
def build_state(broker_id: str, broker_addr: str) -> GossipState:
    return GossipState(broker_id, broker_addr)