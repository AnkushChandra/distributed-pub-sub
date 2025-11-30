#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Simple CLI to create/assign topics via the broker admin API."""
from __future__ import annotations

import argparse
import os
import sys
from typing import Any, Dict, List

import httpx

DEFAULT_BROKER = os.getenv("BROKER_ADMIN")
DEFAULT_PEERS = os.getenv("BROKER_PEERS")


def _default_peer_list() -> List[str]:
    if DEFAULT_PEERS:
        return [p.strip() for p in DEFAULT_PEERS.split(",") if p.strip()]
    return [f"http://127.0.0.1:808{i}" for i in range(1, 6)]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create or reassign a topic using /admin/topics")
    parser.add_argument("name", help="Topic name")
    parser.add_argument(
        "--broker",
        help="Explicit leader broker base URL (skips auto-discovery)",
    )
    parser.add_argument(
        "--owner-id",
        type=int,
        help="Explicit owner broker id (otherwise server picks automatically)",
    )
    parser.add_argument(
        "--rf",
        type=int,
        default=3,
        help="Replication factor to request (default: %(default)s)",
    )
    parser.add_argument(
        "--peers",
        metavar="URL",
        nargs="+",
        help="Candidate broker URLs to try for leader auto-discovery",
    )
    return parser.parse_args()


def _candidate_peers(args: argparse.Namespace) -> List[str]:
    if args.peers:
        return [p.rstrip("/") for p in args.peers]
    if DEFAULT_BROKER:
        return [DEFAULT_BROKER.rstrip("/")]
    return [p.rstrip("/") for p in _default_peer_list()]


def _discover_leader(peers: List[str]) -> str:
    for base in peers:
        url = f"{base}/admin/leader"
        try:
            resp = httpx.get(url, timeout=3.0)
            resp.raise_for_status()
        except httpx.HTTPError:
            continue
        try:
            data = resp.json()
        except ValueError:
            continue
        if data.get("this_broker_is_leader"):
            return base
    raise SystemExit(
        "could not discover leader from peers: " + ", ".join(peers)
    )


def main() -> None:
    args = parse_args()
    if args.broker:
        base = args.broker.rstrip("/")
    else:
        peers = _candidate_peers(args)
        base = _discover_leader(peers)
    url = f"{base}/admin/topics"
    params: Dict[str, Any] = {"name": args.name, "rf": max(1, args.rf)}
    if args.owner_id is not None:
        params["owner_id"] = args.owner_id

    print(f"[info] posting {params} to {url}")
    try:
        resp = httpx.post(url, params=params, timeout=10.0)
        resp.raise_for_status()
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text
        print(f"[ERROR] server responded {exc.response.status_code}: {detail}", file=sys.stderr)
        sys.exit(1)
    except httpx.HTTPError as exc:
        print(f"[ERROR] request failed: {exc}", file=sys.stderr)
        sys.exit(1)

    print(resp.json())


if __name__ == "__main__":
    main()
