#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Streaming subscriber CLI for the distributed pub/sub broker."""
from __future__ import annotations

import argparse
import os
import sys
import time
from typing import Any, Dict, Optional

import httpx


DEFAULT_BROKER = os.getenv("BROKER_API", "http://127.0.0.1:8081")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Continuously poll a topic and auto-commit offsets")
    parser.add_argument("topic", help="Topic to consume from")
    parser.add_argument(
        "--broker",
        default=DEFAULT_BROKER,
        help="Primary broker base URL (default: %(default)s)",
    )
    parser.add_argument(
        "--brokers",
        help="Comma-separated list of broker base URLs for failover (overrides --broker)",
    )
    parser.add_argument("--group-id", help="Consumer group id (auto-assigns consumer id if omitted)")
    parser.add_argument("--consumer-id", help="Explicit consumer id (overrides auto-assignment)")
    parser.add_argument(
        "--from-offset",
        type=int,
        help="Override starting offset; otherwise broker uses committed+1",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=10,
        help="Max records per poll (default: %(default)s)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=15000,
        help="Long-poll timeout in ms (default: %(default)s)",
    )
    parser.add_argument(
        "--no-commit",
        action="store_true",
        help="Disable automatic commits after displaying records",
    )
    parser.add_argument(
        "--show-idle",
        action="store_true",
        help="Log a heartbeat when polls return no data",
    )
    parser.add_argument(
        "--prefer-replica",
        type=int,
        default=None,
        help="Broker ID to prefer for reading (forces routing to specific replica)",
    )
    return parser.parse_args()


def ensure_identity(args: argparse.Namespace) -> tuple[Optional[str], Optional[str]]:
    group_id = args.group_id
    consumer_id = args.consumer_id
    if group_id and not consumer_id:
        consumer_id = f"grp-{group_id}-{os.getpid()}-{int(time.time()*1000)}"
    if not group_id and not consumer_id:
        consumer_id = f"cli-{os.getpid()}"
    return consumer_id, group_id


def parse_broker_pool(args: argparse.Namespace) -> list[str]:
    if args.brokers:
        parts = [p.strip().rstrip('/') for p in args.brokers.split(',')]
        pool = [p for p in parts if p]
        if not pool:
            raise SystemExit("--brokers provided but no valid URLs parsed")
        return pool
    base = (args.broker or DEFAULT_BROKER).strip()
    if not base:
        raise SystemExit("No broker URL provided")
    return [base.rstrip('/')]


def maybe_commit(
    client: httpx.Client,
    base: str,
    topic: str,
    offset: int,
    consumer_id: Optional[str],
    group_id: Optional[str],
) -> None:
    payload: Dict[str, Any] = {"topic": topic, "offset": int(offset)}
    if consumer_id:
        payload["consumer_id"] = consumer_id
    if group_id:
        payload["group_id"] = group_id
    try:
        resp = client.post(f"{base}/commit", json=payload, timeout=2.0)
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        print(f"[WARN] commit failed: {exc}", file=sys.stderr)


def format_record(rec: Dict[str, Any], replica_id: Optional[int] = None) -> str:
    ts_ms = rec.get("ts_ms")
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts_ms / 1000.0)) if ts_ms else "-"
    key = rec.get("key")
    value = rec.get("value")
    headers = rec.get("headers") or {}
    replica_str = f" [replica={replica_id}]" if replica_id is not None else ""
    return f"[{ts}] offset={rec.get('offset')} key={key!r} headers={headers} value={value}{replica_str}"


def main() -> None:
    args = parse_args()
    consumer_id, group_id = ensure_identity(args)
    broker_pool = parse_broker_pool(args)
    broker_idx = 0

    client = httpx.Client(timeout=None)
    params: Dict[str, Any] = {
        "topic": args.topic,
        "max_records": max(1, args.max_records),
        "timeout_ms": max(0, args.timeout),
    }
    if consumer_id:
        params["consumer_id"] = consumer_id
    if group_id:
        params["group_id"] = group_id
    if args.from_offset is not None:
        params["from_offset"] = max(0, args.from_offset)
    if args.prefer_replica is not None:
        params["prefer_replica"] = args.prefer_replica

    def current_base() -> str:
        return broker_pool[broker_idx]

    def rotate_base() -> None:
        nonlocal broker_idx
        broker_idx = (broker_idx + 1) % len(broker_pool)

    replica_info = f" prefer_replica={args.prefer_replica}" if args.prefer_replica else ""
    print(
        f"[info] consuming topic='{args.topic}' via {current_base()} (pool={broker_pool}) as "
        f"group={group_id or '-'} consumer={consumer_id or '-'}{replica_info}",
        file=sys.stderr,
    )

    try:
        consecutive_failovers = 0
        while True:
            base = current_base()
            poll_url = f"{base}/poll"
            try:
                resp = client.get(poll_url, params=params)
                resp.raise_for_status()
                consecutive_failovers = 0
            except httpx.HTTPError as exc:
                print(
                    f"[WARN] poll failed against {base}: {exc}; switching brokers",
                    file=sys.stderr,
                )
                rotate_base()
                consecutive_failovers += 1
                if consecutive_failovers >= len(broker_pool):
                    time.sleep(1.0)
                    consecutive_failovers = 0
                continue

            body = resp.json()
            records = body.get("records", [])
            replica_id = body.get("replica_id")
            if records:
                for rec in records:
                    print(format_record(rec, replica_id), flush=True)
                last_offset = records[-1]["offset"]
                if not args.no_commit:
                    maybe_commit(client, base, args.topic, last_offset, consumer_id, group_id)
                params["from_offset"] = last_offset + 1
            else:
                next_offset = body.get("next_offset")
                if next_offset is not None:
                    params["from_offset"] = next_offset
                if args.show_idle:
                    committed = body.get("committed")
                    print(
                        f"[idle] no records; committed={committed} next_offset={body.get('next_offset')} replica={replica_id}",
                        file=sys.stderr,
                    )
    except KeyboardInterrupt:
        print("\n[info] stopping consumer", file=sys.stderr)
    finally:
        client.close()


if __name__ == "__main__":
    main()
