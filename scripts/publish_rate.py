#!/usr/bin/env python3
"""Publish load generator that retries each message until it succeeds."""
from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any, Dict

import httpx


def main() -> None:
    parser = argparse.ArgumentParser(description="Send messages to a broker at a fixed rate")
    parser.add_argument("--topic", required=True, help="Topic to publish to")
    parser.add_argument(
        "--broker",
        default="http://127.0.0.1:8081",
        help="Broker base URL (default: %(default)s)",
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=1000,
        help="Messages per minute (default: %(default)s)",
    )
    parser.add_argument(
        "--minutes",
        type=float,
        default=1.0,
        help="How long to run, in minutes (default: %(default)s)",
    )
    parser.add_argument(
        "--json-payload",
        action="store_true",
        help="Send JSON payloads instead of plain text",
    )
    args = parser.parse_args()

    total_msgs = max(0, int(args.rate * args.minutes))
    if total_msgs == 0:
        print("No messages to send; adjust --rate or --minutes")
        return
    interval = 60.0 / max(1, args.rate)

    publish_url = f"{args.broker.rstrip('/')}/topics/{args.topic}/publish"

    def _value_for(i: int) -> Any:
        if args.json_payload:
            return {"seq": i, "payload": f"message-{i}"}
        return f"message-{i}"

    client = httpx.Client(timeout=5.0)

    try:
        for i in range(total_msgs):
            payload = {
                "key": None,
                "value": _value_for(i),
                "headers": {},
            }
            while True:
                try:
                    resp = client.post(publish_url, json=payload)
                    resp.raise_for_status()
                    print(f"published message-{i}")
                    break
                except httpx.HTTPError as exc:
                    print(
                        f"[WARN] publish failed for message-{i}: {exc}; retrying same message",
                        file=sys.stderr,
                    )
                    time.sleep(min(interval, 0.5))
            time.sleep(interval)
    finally:
        client.close()


if __name__ == "__main__":
    main()
