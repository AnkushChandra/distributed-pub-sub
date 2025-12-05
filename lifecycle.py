from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Dict, Iterable, Optional

import aiohttp
import gossip
import replication

GossipPayloadFn = Callable[[], Dict[str, Any]]
GossipHandlerFn = Callable[[Dict[str, Any]], None]
AsyncCallable = Callable[[], Awaitable[None]]
TopicsProviderFn = Callable[[], Iterable[str]]
PromoteTopicFn = Callable[[str], None]
SyncIsrFn = Callable[[str], None]

_BROKER_ID: int = 0
_BROKER_API: Dict[int, str] = {}
_peer_map_fn: Optional[Callable[[], Dict[str, str]]] = None
_self_api_base_fn: Optional[Callable[[], str]] = None
_gossip_payload_fn: Optional[GossipPayloadFn] = None
_handle_gossip_payload_fn: Optional[GossipHandlerFn] = None
_placement_getter: Optional[Callable[[], Dict[str, Any]]] = None
_refresh_owner_cache_fn: Optional[AsyncCallable] = None
_is_leader_fn: Optional[Callable[[], bool]] = None
_topics_provider_fn: Optional[TopicsProviderFn] = None
_promote_topic_fn: Optional[PromoteTopicFn] = None
_sync_isr_fn: Optional[SyncIsrFn] = None

_configured: bool = False
_gossip_state: Optional[gossip.GossipState] = None
_gossip_service: Optional[gossip.GossipService] = None
_gossip_http: Optional[aiohttp.ClientSession] = None
_tasks: Dict[str, asyncio.Task] = {}


def configure_lifecycle(
    *, broker_id: int, broker_api: Dict[int, str],
    peer_map_fn: Callable[[], Dict[str, str]], self_api_base_fn: Callable[[], str],
    gossip_payload_fn: GossipPayloadFn, handle_gossip_payload_fn: GossipHandlerFn,
    placement_getter: Callable[[], Dict[str, Any]], refresh_owner_cache_fn: AsyncCallable,
    is_leader_fn: Callable[[], bool], topics_provider_fn: TopicsProviderFn,
    promote_topic_fn: PromoteTopicFn, sync_isr_fn: SyncIsrFn,
) -> None:
    global _BROKER_ID, _BROKER_API, _peer_map_fn, _self_api_base_fn
    global _gossip_payload_fn, _handle_gossip_payload_fn, _placement_getter
    global _refresh_owner_cache_fn, _is_leader_fn, _topics_provider_fn
    global _promote_topic_fn, _sync_isr_fn, _configured

    _BROKER_ID, _BROKER_API = broker_id, dict(broker_api)
    _peer_map_fn, _self_api_base_fn = peer_map_fn, self_api_base_fn
    _gossip_payload_fn, _handle_gossip_payload_fn = gossip_payload_fn, handle_gossip_payload_fn
    _placement_getter, _refresh_owner_cache_fn = placement_getter, refresh_owner_cache_fn
    _is_leader_fn, _topics_provider_fn = is_leader_fn, topics_provider_fn
    _promote_topic_fn, _sync_isr_fn = promote_topic_fn, sync_isr_fn

    replication.setup(broker_id=_BROKER_ID, broker_api=_BROKER_API,
                      placement_getter=_placement_getter, refresh_fn=_refresh_owner_cache_fn)
    _configured = True


def update_broker_api(peers: Dict[int, str]) -> None:
    global _BROKER_API
    _BROKER_API = dict(peers)
    replication.update_broker_api(_BROKER_API)


def get_gossip_state() -> Optional[gossip.GossipState]:
    return _gossip_state


async def _start_task(name: str, coro_fn: Callable[[], Awaitable[None]]) -> None:
    if name not in _tasks:
        _tasks[name] = asyncio.create_task(coro_fn())


async def _stop_task(name: str) -> None:
    task = _tasks.pop(name, None)
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


async def lifecycle_startup() -> None:
    if not _configured:
        raise RuntimeError("configure_lifecycle() must be called first")
    await _start_gossip()
    await _start_task("replication", replication.follower_replication_loop)
    await _start_task("metadata_sync", _metadata_sync_loop)
    await _start_task("failover", _failover_loop)
    await _start_task("isr", _isr_loop)
    await _start_task("leader_watch", _leader_watch_loop)


async def lifecycle_shutdown() -> None:
    for name in list(_tasks.keys()):
        await _stop_task(name)
    await _stop_gossip()


async def _start_gossip() -> None:
    global _gossip_state, _gossip_service, _gossip_http
    if _gossip_state is not None:
        return
    state = gossip.GossipState(str(_BROKER_ID), _self_api_base_fn())
    http = aiohttp.ClientSession()
    service = gossip.GossipService(state, http, payload_fn=_gossip_payload_fn,
                                    on_payload_fn=_handle_gossip_payload_fn)
    await service.start(_peer_map_fn())
    _gossip_state, _gossip_service, _gossip_http = state, service, http


async def _stop_gossip() -> None:
    global _gossip_state, _gossip_service, _gossip_http
    if _gossip_service:
        await _gossip_service.stop()
    if _gossip_http:
        await _gossip_http.close()
    _gossip_http = _gossip_service = _gossip_state = None


async def _run_loop(fn: Callable[[], None], interval: float, name: str) -> None:
    while True:
        try:
            if _is_leader_fn():
                fn()
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[Broker {_BROKER_ID}] {name} error: {e}")
            await asyncio.sleep(interval)


async def _metadata_sync_loop() -> None:
    while True:
        try:
            if not _is_leader_fn():
                await _refresh_owner_cache_fn()
            await asyncio.sleep(2.0)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[Broker {_BROKER_ID}] metadata sync error: {e}")
            await asyncio.sleep(2.0)


async def _leader_watch_loop() -> None:
    was_leader = False
    while True:
        try:
            is_leader = _is_leader_fn()
            if is_leader and not was_leader:
                await _refresh_owner_cache_fn(force=True)
            was_leader = is_leader
            await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[Broker {_BROKER_ID}] leader watch error: {e}")
            await asyncio.sleep(1.0)


async def _failover_loop() -> None:
    while True:
        try:
            if _is_leader_fn():
                for topic in _topics_provider_fn():
                    _promote_topic_fn(topic)
            await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[Broker {_BROKER_ID}] failover error: {e}")
            await asyncio.sleep(1.0)


async def _isr_loop() -> None:
    while True:
        try:
            if _is_leader_fn():
                for topic in _topics_provider_fn():
                    _sync_isr_fn(topic)
            await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[Broker {_BROKER_ID}] ISR error: {e}")
            await asyncio.sleep(1.0)
