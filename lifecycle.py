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
_replication_task: Optional[asyncio.Task] = None
_metadata_sync_task: Optional[asyncio.Task] = None
_failover_task: Optional[asyncio.Task] = None
_isr_task: Optional[asyncio.Task] = None
_leader_watch_task: Optional[asyncio.Task] = None


def configure_lifecycle(
    *,
    broker_id: int,
    broker_api: Dict[int, str],
    peer_map_fn: Callable[[], Dict[str, str]],
    self_api_base_fn: Callable[[], str],
    gossip_payload_fn: GossipPayloadFn,
    handle_gossip_payload_fn: GossipHandlerFn,
    placement_getter: Callable[[], Dict[str, Any]],
    refresh_owner_cache_fn: AsyncCallable,
    is_leader_fn: Callable[[], bool],
    topics_provider_fn: TopicsProviderFn,
    promote_topic_fn: PromoteTopicFn,
    sync_isr_fn: SyncIsrFn,
) -> None:
    global _BROKER_ID, _BROKER_API, _peer_map_fn, _self_api_base_fn
    global _gossip_payload_fn, _handle_gossip_payload_fn
    global _placement_getter, _refresh_owner_cache_fn, _is_leader_fn
    global _topics_provider_fn, _promote_topic_fn, _sync_isr_fn, _configured

    _BROKER_ID = broker_id
    _BROKER_API = dict(broker_api)
    _peer_map_fn = peer_map_fn
    _self_api_base_fn = self_api_base_fn
    _gossip_payload_fn = gossip_payload_fn
    _handle_gossip_payload_fn = handle_gossip_payload_fn
    _placement_getter = placement_getter
    _refresh_owner_cache_fn = refresh_owner_cache_fn
    _is_leader_fn = is_leader_fn
    _topics_provider_fn = topics_provider_fn
    _promote_topic_fn = promote_topic_fn
    _sync_isr_fn = sync_isr_fn

    replication.setup(
        broker_id=_BROKER_ID,
        broker_api=_BROKER_API,
        placement_getter=_placement_getter,
        refresh_fn=_refresh_owner_cache_fn,
    )

    _configured = True


def get_gossip_state() -> Optional[gossip.GossipState]:
    return _gossip_state


def _require_configured() -> None:
    if not _configured:
        raise RuntimeError("configure_lifecycle() must be called before lifecycle_startup()")


async def lifecycle_startup() -> None:
    _require_configured()
    await _start_gossip()
    await _start_replication()
    await _start_metadata_sync()
    await _start_failover_monitor()
    await _start_isr_monitor()
    await _start_leader_watch()


async def lifecycle_shutdown() -> None:
    await _stop_leader_watch()
    await _stop_isr_monitor()
    await _stop_failover_monitor()
    await _stop_metadata_sync()
    await _stop_replication()
    await _stop_gossip()


async def _start_gossip() -> None:
    global _gossip_state, _gossip_service, _gossip_http
    if _gossip_state is not None:
        return
    assert _self_api_base_fn is not None
    assert _peer_map_fn is not None
    assert _gossip_payload_fn is not None
    assert _handle_gossip_payload_fn is not None

    state = gossip.GossipState(str(_BROKER_ID), _self_api_base_fn())
    http = aiohttp.ClientSession()
    service = gossip.GossipService(
        state,
        http,
        payload_fn=_gossip_payload_fn,
        on_payload_fn=_handle_gossip_payload_fn,
    )
    await service.start(_peer_map_fn())
    _gossip_state = state
    _gossip_service = service
    _gossip_http = http


async def _stop_gossip() -> None:
    global _gossip_state, _gossip_service, _gossip_http
    service = _gossip_service
    if service is not None:
        await service.stop()
    if _gossip_http is not None:
        await _gossip_http.close()
    _gossip_http = None
    _gossip_service = None
    _gossip_state = None


async def _start_replication() -> None:
    global _replication_task
    if _replication_task is not None:
        return
    _replication_task = asyncio.create_task(replication.follower_replication_loop())


async def _stop_replication() -> None:
    global _replication_task
    task = _replication_task
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    _replication_task = None


async def _start_metadata_sync() -> None:
    global _metadata_sync_task
    if _metadata_sync_task is not None:
        return
    _metadata_sync_task = asyncio.create_task(_metadata_sync_loop())


async def _stop_metadata_sync() -> None:
    global _metadata_sync_task
    task = _metadata_sync_task
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    _metadata_sync_task = None


async def _metadata_sync_loop() -> None:
    assert _refresh_owner_cache_fn is not None
    assert _is_leader_fn is not None
    while True:
        try:
            if not _is_leader_fn():
                await _refresh_owner_cache_fn()
            await asyncio.sleep(2.0)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            print(f"[Broker {_BROKER_ID}] metadata sync error: {exc}")
            await asyncio.sleep(2.0)


async def _start_leader_watch() -> None:
    global _leader_watch_task
    if _leader_watch_task is not None:
        return
    _leader_watch_task = asyncio.create_task(_leader_watch_loop())


async def _stop_leader_watch() -> None:
    global _leader_watch_task
    task = _leader_watch_task
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    _leader_watch_task = None


async def _leader_watch_loop() -> None:
    assert _refresh_owner_cache_fn is not None
    assert _is_leader_fn is not None
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
        except Exception as exc:
            print(f"[Broker {_BROKER_ID}] leader watch error: {exc}")
            await asyncio.sleep(1.0)


async def _start_failover_monitor() -> None:
    global _failover_task
    if _failover_task is not None:
        return
    _failover_task = asyncio.create_task(_failover_loop())


async def _stop_failover_monitor() -> None:
    global _failover_task
    task = _failover_task
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    _failover_task = None


async def _failover_loop() -> None:
    assert _is_leader_fn is not None
    assert _topics_provider_fn is not None
    assert _promote_topic_fn is not None
    while True:
        try:
            if _is_leader_fn():
                for topic in _topics_provider_fn():
                    _promote_topic_fn(topic)
            await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            print(f"[Broker {_BROKER_ID}] failover monitor error: {exc}")
            await asyncio.sleep(1.0)


async def _start_isr_monitor() -> None:
    global _isr_task
    if _isr_task is not None:
        return
    _isr_task = asyncio.create_task(_isr_loop())


async def _stop_isr_monitor() -> None:
    global _isr_task
    task = _isr_task
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    _isr_task = None


async def _isr_loop() -> None:
    assert _is_leader_fn is not None
    assert _topics_provider_fn is not None
    assert _sync_isr_fn is not None
    while True:
        try:
            if _is_leader_fn():
                for topic in _topics_provider_fn():
                    _sync_isr_fn(topic)
            await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            print(f"[Broker {_BROKER_ID}] ISR monitor error: {exc}")
            await asyncio.sleep(1.0)
