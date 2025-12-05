#!/usr/bin/env python3
"""Simple CLI to create/assign topics via the broker admin API."""
import argparse
import sys
from typing import Any, Dict
import httpx
from common import get_leader_url, default_peers, error_exit


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create or reassign a topic")
    parser.add_argument("name", help="Topic name")
    parser.add_argument("--broker", help="Explicit leader broker URL")
    parser.add_argument("--owner-id", type=int, help="Explicit owner broker id")
    parser.add_argument("--rf", type=int, default=3, help="Replication factor (default: 3)")
    parser.add_argument("--peers", nargs="+", help="Broker URLs for leader discovery")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        base = get_leader_url(args.broker, args.peers)
        print(f"[info] using leader at {base}", file=sys.stderr)
    except SystemExit:
        raise
    except Exception as e:
        error_exit(f"discovering leader: {e}")

    params: Dict[str, Any] = {"name": args.name, "rf": max(1, args.rf)}
    if args.owner_id is not None:
        params["owner_id"] = args.owner_id

    try:
        resp = httpx.post(f"{base}/admin/topics", params=params, timeout=10.0)
        resp.raise_for_status()
        print(resp.json())
    except httpx.HTTPStatusError as e:
        error_exit(f"server responded {e.response.status_code}: {e.response.text}")
    except httpx.HTTPError as e:
        error_exit(f"request failed: {e}")


if __name__ == "__main__":
    main()
