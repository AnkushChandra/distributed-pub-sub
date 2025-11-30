#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Interactive publisher CLI for the distributed pub/sub broker."""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict, Optional

import httpx


DEFAULT_BROKER = os.getenv("BROKER_API", "http://127.0.0.1:8081")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish messages from stdin to a topic")
    parser.add_argument("topic", help="Topic name to publish to")
    parser.add_argument(
        "--broker",
        default=DEFAULT_BROKER,
        help="Base URL of the broker API (default: %(default)s)",
    )
    parser.add_argument("--key", help="Optional record key", default=None)
    parser.add_argument(
        "--json",
        action="store_true",
        help="Interpret each input line as JSON instead of plain text",
    )
    parser.add_argument(
        "--headers",
        metavar="K=V",
        action="append",
        help="Optional headers to attach to each record (repeatable)",
    )
    return parser.parse_args()


def parse_headers(entries: Optional[list[str]]) -> Dict[str, Any]:
    headers: Dict[str, Any] = {}
    if not entries:
        return headers
    for item in entries:
        if "=" not in item:
            print(f"[WARN] ignoring malformed header '{item}' (expected key=value)", file=sys.stderr)
            continue
        k, v = item.split("=", 1)
        headers[k.strip()] = v.strip()
    return headers


def parse_value(raw: str, as_json: bool) -> Any:
    if not as_json:
        return raw
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON payload: {exc}") from exc


def main() -> None:
    args = parse_args()
    headers = parse_headers(args.headers)

    print(
        "Type messages and press <enter> to publish. "
        "Use /quit or Ctrl-D to exit.",
        file=sys.stderr,
    )

    client = httpx.Client(timeout=10.0)
    url = f"{args.broker.rstrip('/')}/topics/{args.topic}/publish"

    try:
        for line in sys.stdin:
            raw = line.rstrip("\n")
            if raw == "/quit":
                break
            if not raw:
                continue
            try:
                value = parse_value(raw, args.json)
            except ValueError as exc:
                print(f"[ERROR] {exc}", file=sys.stderr)
                continue
            payload = {
                "key": args.key,
                "value": value,
                "headers": headers,
            }
            try:
                resp = client.post(url, json=payload)
                resp.raise_for_status()
            except httpx.HTTPError as exc:
                print(f"[ERROR] publish failed: {exc}", file=sys.stderr)
                continue
            data = resp.json()
            offset = data.get("offset")
            print(f"-> published offset {offset}")
    except KeyboardInterrupt:
        pass
    finally:
        client.close()


if __name__ == "__main__":
    main()
