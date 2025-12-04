#!/usr/bin/env python3
"""Continuously compare replica offsets for a topic to spot inconsistency during failures."""
from __future__ import annotations

import argparse
import sys
import time
from typing import Dict, Optional, Tuple

import httpx
import psutil


def _parse_brokers(raw: list[str]) -> Dict[int, str]:
    mapping: Dict[int, str] = {}
    for entry in raw:
        if "=" not in entry:
            raise argparse.ArgumentTypeError(
                f"--broker entries must look like '1=http://127.0.0.1:8081', got '{entry}'"
            )
        bid_str, url = entry.split("=", 1)
        try:
            bid = int(bid_str.strip())
        except ValueError as exc:  # pragma: no cover - CLI validation
            raise argparse.ArgumentTypeError(f"Invalid broker id '{bid_str}'") from exc
        mapping[bid] = url.rstrip("/")
    if not mapping:
        raise argparse.ArgumentTypeError("At least one --broker entry is required")
    return mapping


def _fetch_metadata_from_broker(client: httpx.Client, base_url: str) -> Optional[dict]:
    try:
        resp = client.get(f"{base_url.rstrip('/')}/metadata", timeout=3.0)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        print(f"[WARN] failed to fetch metadata from {base_url}: {exc}", file=sys.stderr)
        return None


def _fetch_metadata_with_failover(
    client: httpx.Client, broker_map: Dict[int, str], preferred_base: str
) -> Tuple[Optional[dict], str]:
    """Try preferred broker first, then failover to others. Returns (metadata, base_used)."""
    metadata = _fetch_metadata_from_broker(client, preferred_base)
    if metadata is not None:
        return metadata, preferred_base
    for bid, base in broker_map.items():
        if base == preferred_base:
            continue
        metadata = _fetch_metadata_from_broker(client, base)
        if metadata is not None:
            print(f"[INFO] switched metadata source to broker {bid} ({base})", file=sys.stderr)
            return metadata, base
    return None, preferred_base


def _topic_entry(metadata: dict, topic: str) -> Optional[dict]:
    topics = metadata.get("topics") or {}
    entry = topics.get(topic)
    if entry is None:
        return None
    if isinstance(entry, dict):  # future-proofing if JSON changes again
        owner = entry.get("owner")
        followers = entry.get("followers", [])
    else:
        owner = entry[0] if entry else None
        followers = entry[1:] if entry else []
    return {"owner": owner, "followers": followers}


def _fetch_status(client: httpx.Client, base_url: str, topic: str) -> Optional[int]:
    try:
        resp = client.get(
            f"{base_url.rstrip('/')}/internal/topics/{topic}/status",
            timeout=2.0,
        )
        resp.raise_for_status()
        body = resp.json()
        latest = body.get("latest_offset")
        return int(latest) if latest is not None else None
    except Exception as exc:
        print(
            f"[WARN] failed to fetch status from {base_url} for topic {topic}: {exc}",
            file=sys.stderr,
        )
        return None


def _resolve_pid(broker: str) -> Optional[int]:
    try:
        proc = psutil.Process(int(broker))
        return proc.pid
    except Exception:
        return None


def _terminate_broker(proc: psutil.Process) -> None:
    try:
        proc.terminate()
    except Exception as exc:
        print(f"[WARN] failed to terminate process {proc.pid}: {exc}", file=sys.stderr)


def monitor_consistency(args: argparse.Namespace) -> None:
    broker_map = _parse_brokers(args.broker)
    metadata_base = broker_map.get(args.metadata_broker) or next(iter(broker_map.values()))
    stop_at = time.time() + args.duration if args.duration else None
    fail_triggered = False

    with httpx.Client() as client:
        iteration = 0
        while True:
            iteration += 1
            metadata, metadata_base = _fetch_metadata_with_failover(client, broker_map, metadata_base)
            if metadata is None:
                print(f"[WARN] all brokers unreachable for metadata", file=sys.stderr)
                time.sleep(args.interval)
                continue
            topic_info = _topic_entry(metadata, args.topic)
            if topic_info is None:
                print(f"[ERROR] topic '{args.topic}' not present in metadata", file=sys.stderr)
                return
            owner = topic_info.get("owner")
            followers = topic_info.get("followers", [])
            statuses: Dict[int, Optional[int]] = {}
            for bid, base in sorted(broker_map.items()):
                statuses[bid] = _fetch_status(client, base, args.topic)

            offsets = [off for off in statuses.values() if off is not None]
            min_off = min(offsets) if offsets else None
            max_off = max(offsets) if offsets else None
            ts = time.strftime("%H:%M:%S")
            print(f"[{ts}] iteration={iteration} owner={owner} followers={followers}")
            for bid, off in statuses.items():
                label = "OWNER" if owner == bid else ("FOLLOWER" if bid in followers else "OTHER")
                status_str = "NA" if off is None else str(off)
                print(f"  broker {bid:<3} ({label:<8}) offset={status_str}")
            if min_off is not None and max_off is not None:
                drift = max_off - min_off
                if drift > args.max_lag:
                    print(
                        f"  !!! Lag detected: max_offset-min_offset={drift} (> {args.max_lag})",
                        file=sys.stderr,
                    )
                else:
                    print(f"  Lag within threshold (spread={drift})")
            else:
                print("  Offsets unavailable from all brokers", file=sys.stderr)

            if (
                args.inject_failure_at
                and not fail_triggered
                and iteration >= args.inject_failure_at
                and args.fail_pid
            ):
                try:
                    proc = psutil.Process(args.fail_pid)
                    print(f"[INFO] Injecting failure: SIGTERM to PID {proc.pid}")
                    _terminate_broker(proc)
                    fail_triggered = True
                except Exception as exc:
                    print(f"[WARN] could not kill PID {args.fail_pid}: {exc}", file=sys.stderr)

            if args.iterations and iteration >= args.iterations:
                break
            if stop_at and time.time() >= stop_at:
                break
            time.sleep(args.interval)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Continuously compare replica offsets for a topic to spot inconsistency",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--topic", required=True, help="Topic to inspect")
    parser.add_argument(
        "--broker",
        action="append",
        required=True,
        help="Broker mapping in the form id=url (repeat for each broker)",
    )
    parser.add_argument(
        "--metadata-broker",
        type=int,
        default=None,
        help="Broker id to query for /metadata (defaults to first mapping)",
    )
    parser.add_argument("--interval", type=float, default=2.0, help="Seconds between checks")
    parser.add_argument(
        "--duration",
        type=float,
        default=0.0,
        help="Optional duration in seconds (0 = run until interrupted)",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=0,
        help="Optional number of iterations to run (0 = unlimited)",
    )
    parser.add_argument(
        "--max-lag",
        type=int,
        default=0,
        help="Warn when max offset minus min offset exceeds this value",
    )
    parser.add_argument(
        "--fail-pid",
        type=int,
        default=None,
        help="Optional broker PID to send SIGTERM to",
    )
    parser.add_argument(
        "--inject-failure-at",
        type=int,
        default=0,
        help="Iteration number after which to kill --fail-pid (0 disables)",
    )
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    try:
        monitor_consistency(args)
    except KeyboardInterrupt:  # pragma: no cover - interactive script
        print("Interrupted", file=sys.stderr)


if __name__ == "__main__":
    main()
