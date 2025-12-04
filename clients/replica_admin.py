#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""CLI to manage topic replicas - add, remove, and view replica configuration."""
from __future__ import annotations

import argparse
import os
import sys
from typing import List, Optional

import httpx

DEFAULT_BROKER = os.getenv("BROKER_API", "http://127.0.0.1:8081")


def _discover_leader(peers: List[str]) -> str:
    """Find the cluster leader from a list of broker URLs."""
    for base in peers:
        try:
            resp = httpx.get(f"{base}/admin/leader", timeout=3.0)
            resp.raise_for_status()
            data = resp.json()
            if data.get("this_broker_is_leader"):
                return base
        except httpx.HTTPError:
            continue
    raise SystemExit("Could not discover cluster leader from provided brokers")


def _default_peers() -> List[str]:
    peers_env = os.getenv("BROKER_PEERS")
    if peers_env:
        return [p.strip() for p in peers_env.split(",") if p.strip()]
    return [f"http://127.0.0.1:808{i}" for i in range(1, 6)]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manage topic replicas")
    parser.add_argument(
        "--broker",
        help="Broker URL (will auto-discover leader if not the leader)",
    )
    parser.add_argument(
        "--peers",
        nargs="+",
        help="List of broker URLs for leader discovery",
    )
    
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # List/show topic info
    show_parser = subparsers.add_parser("show", help="Show topic replica info")
    show_parser.add_argument("topic", help="Topic name")
    
    # Add replica
    add_parser = subparsers.add_parser("add", help="Add a replica to a topic")
    add_parser.add_argument("topic", help="Topic name")
    add_parser.add_argument("broker_id", type=int, help="Broker ID to add as replica")
    
    # Remove replica
    remove_parser = subparsers.add_parser("remove", help="Remove a replica from a topic")
    remove_parser.add_argument("topic", help="Topic name")
    remove_parser.add_argument("broker_id", type=int, help="Broker ID to remove")
    
    # List all topics
    subparsers.add_parser("list", help="List all topics and their replicas")
    
    # Scale - set exact replica count
    scale_parser = subparsers.add_parser("scale", help="Scale topic to N replicas")
    scale_parser.add_argument("topic", help="Topic name")
    scale_parser.add_argument("count", type=int, help="Desired total replica count (including owner)")
    
    return parser.parse_args()


def get_leader_url(args: argparse.Namespace) -> str:
    if args.broker:
        # Check if it's the leader
        try:
            resp = httpx.get(f"{args.broker}/admin/leader", timeout=3.0)
            resp.raise_for_status()
            data = resp.json()
            if data.get("this_broker_is_leader"):
                return args.broker
        except httpx.HTTPError:
            pass
    
    peers = args.peers or _default_peers()
    if args.broker and args.broker not in peers:
        peers.insert(0, args.broker)
    return _discover_leader(peers)


def cmd_show(base: str, topic: str) -> None:
    resp = httpx.get(f"{base}/admin/topics/{topic}", timeout=5.0)
    if resp.status_code == 404:
        print(f"Topic '{topic}' not found", file=sys.stderr)
        sys.exit(1)
    resp.raise_for_status()
    data = resp.json()
    
    print(f"Topic: {data['topic']}")
    print(f"Owner: broker {data['owner']}")
    print(f"Followers: {data['followers']}")
    print(f"ISR: {data['isr']}")
    print(f"Total replicas: {data['replica_count']}")
    print(f"Alive brokers: {data['alive_brokers']}")


def cmd_add(base: str, topic: str, broker_id: int) -> None:
    resp = httpx.post(
        f"{base}/admin/topics/{topic}/replicas",
        params={"broker_id_to_add": broker_id},
        timeout=5.0,
    )
    if resp.status_code >= 400:
        detail = resp.json().get("detail", resp.text)
        print(f"Error: {detail}", file=sys.stderr)
        sys.exit(1)
    
    data = resp.json()
    print(f"Added broker {broker_id} as replica for '{topic}'")
    print(f"Owner: {data['owner']}, Followers: {data['followers']}, Total: {data['replica_count']}")


def cmd_remove(base: str, topic: str, broker_id: int) -> None:
    resp = httpx.delete(
        f"{base}/admin/topics/{topic}/replicas/{broker_id}",
        timeout=5.0,
    )
    if resp.status_code >= 400:
        detail = resp.json().get("detail", resp.text)
        print(f"Error: {detail}", file=sys.stderr)
        sys.exit(1)
    
    data = resp.json()
    print(f"Removed broker {broker_id} from '{topic}'")
    print(f"Owner: {data['owner']}, Followers: {data['followers']}, Total: {data['replica_count']}")


def cmd_list(base: str) -> None:
    resp = httpx.get(f"{base}/metadata", timeout=5.0)
    resp.raise_for_status()
    data = resp.json()
    
    topics = data.get("topics", {})
    if not topics:
        print("No topics found")
        return
    
    print(f"{'Topic':<20} {'Owner':<8} {'Replicas':<30}")
    print("-" * 60)
    for topic, replicas in sorted(topics.items()):
        owner = replicas[0] if replicas else "-"
        followers = replicas[1:] if len(replicas) > 1 else []
        print(f"{topic:<20} {owner:<8} {followers}")


def cmd_scale(base: str, topic: str, count: int) -> None:
    if count < 1:
        print("Replica count must be at least 1 (the owner)", file=sys.stderr)
        sys.exit(1)
    
    # Get current state
    resp = httpx.get(f"{base}/admin/topics/{topic}", timeout=5.0)
    if resp.status_code == 404:
        print(f"Topic '{topic}' not found", file=sys.stderr)
        sys.exit(1)
    resp.raise_for_status()
    data = resp.json()
    
    current_count = data["replica_count"]
    owner = data["owner"]
    followers = data["followers"]
    alive = data["alive_brokers"]
    
    if count == current_count:
        print(f"Topic already has {count} replicas")
        return
    
    if count > current_count:
        # Need to add replicas
        needed = count - current_count
        current_replicas = [owner] + followers
        candidates = [b for b in alive if b not in current_replicas]
        
        if len(candidates) < needed:
            print(f"Not enough available brokers. Need {needed}, have {len(candidates)}", file=sys.stderr)
            sys.exit(1)
        
        for broker_id in candidates[:needed]:
            cmd_add(base, topic, broker_id)
    else:
        # Need to remove replicas
        to_remove = current_count - count
        # Remove from the end of followers list
        for broker_id in followers[-to_remove:]:
            cmd_remove(base, topic, broker_id)
    
    print(f"\nScaled '{topic}' to {count} replicas")


def main() -> None:
    args = parse_args()
    
    try:
        base = get_leader_url(args)
        print(f"[info] using leader at {base}", file=sys.stderr)
    except SystemExit:
        raise
    except Exception as e:
        print(f"Error discovering leader: {e}", file=sys.stderr)
        sys.exit(1)
    
    try:
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
    except httpx.HTTPError as e:
        print(f"HTTP error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
