#!/usr/bin/env python3
"""CLI to manage topic replicas - add, remove, and view replica configuration."""
import argparse
import sys
import httpx
from common import get_leader_url, error_exit


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manage topic replicas")
    parser.add_argument("--broker", help="Broker URL")
    parser.add_argument("--peers", nargs="+", help="Broker URLs for leader discovery")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("show", help="Show topic replica info")
    p.add_argument("topic")

    p = sub.add_parser("add", help="Add a replica")
    p.add_argument("topic")
    p.add_argument("broker_id", type=int)

    p = sub.add_parser("remove", help="Remove a replica")
    p.add_argument("topic")
    p.add_argument("broker_id", type=int)

    sub.add_parser("list", help="List all topics")

    p = sub.add_parser("scale", help="Scale to N replicas")
    p.add_argument("topic")
    p.add_argument("count", type=int)

    return parser.parse_args()


def cmd_show(base: str, topic: str) -> None:
    resp = httpx.get(f"{base}/admin/topics/{topic}", timeout=5.0)
    if resp.status_code == 404:
        error_exit(f"Topic '{topic}' not found")
    resp.raise_for_status()
    d = resp.json()
    print(f"Topic: {d['topic']}\nOwner: {d['owner']}\nFollowers: {d['followers']}")
    print(f"ISR: {d['isr']}\nReplicas: {d['replica_count']}\nAlive: {d['alive_brokers']}")


def cmd_add(base: str, topic: str, broker_id: int) -> None:
    resp = httpx.post(f"{base}/admin/topics/{topic}/replicas",
                      params={"broker_id_to_add": broker_id}, timeout=5.0)
    if resp.status_code >= 400:
        error_exit(resp.json().get("detail", resp.text))
    d = resp.json()
    print(f"Added broker {broker_id}. Owner: {d['owner']}, Followers: {d['followers']}")


def cmd_remove(base: str, topic: str, broker_id: int) -> None:
    resp = httpx.delete(f"{base}/admin/topics/{topic}/replicas/{broker_id}", timeout=5.0)
    if resp.status_code >= 400:
        error_exit(resp.json().get("detail", resp.text))
    d = resp.json()
    print(f"Removed broker {broker_id}. Owner: {d['owner']}, Followers: {d['followers']}")


def cmd_list(base: str) -> None:
    resp = httpx.get(f"{base}/metadata", timeout=5.0)
    resp.raise_for_status()
    topics = resp.json().get("topics", {})
    if not topics:
        print("No topics")
        return
    for topic, replicas in sorted(topics.items()):
        print(f"{topic}: owner={replicas[0] if replicas else '-'}, followers={replicas[1:]}")


def cmd_scale(base: str, topic: str, count: int) -> None:
    if count < 1:
        error_exit("Count must be >= 1")
    resp = httpx.get(f"{base}/admin/topics/{topic}", timeout=5.0)
    if resp.status_code == 404:
        error_exit(f"Topic '{topic}' not found")
    resp.raise_for_status()
    d = resp.json()
    current, owner, followers, alive = d["replica_count"], d["owner"], d["followers"], d["alive_brokers"]

    if count == current:
        print(f"Already at {count} replicas")
        return
    if count > current:
        needed = count - current
        candidates = [b for b in alive if b not in [owner] + followers]
        if len(candidates) < needed:
            error_exit(f"Need {needed} brokers, only {len(candidates)} available")
        for bid in candidates[:needed]:
            cmd_add(base, topic, bid)
    else:
        for bid in followers[-(current - count):]:
            cmd_remove(base, topic, bid)
    print(f"Scaled '{topic}' to {count} replicas")


def main() -> None:
    args = parse_args()
    try:
        base = get_leader_url(args.broker, args.peers)
        print(f"[info] using leader at {base}", file=sys.stderr)
    except Exception as e:
        error_exit(str(e))

    if args.command == "show":
        cmd_show(base, args.topic)
    elif args.command == "add":
        cmd_add(base, args.topic, args.broker_id)
    elif args.command == "remove":
        cmd_remove(base, args.topic, args.broker_id)
    elif args.command == "list":
        cmd_list(base)
    elif args.command == "scale":
        cmd_scale(base, args.topic, args.count)


if __name__ == "__main__":
    main()
