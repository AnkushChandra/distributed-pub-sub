from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any, Dict, Optional
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

import storage
from bully import BullyService
from replication import (
    setup_replication,
    is_leader,
    get_owner_for,
    get_all_metadata_for_api,
    refresh_owner_cache_if_needed,
    assign_topic,
    follower_replication_loop,
)

BROKER_ID = int(os.getenv("BROKER_ID", "1"))
BROKER_API = {
    1: "http://localhost:8001",
    2: "http://localhost:8002",
    3: "http://localhost:8003",
}

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

# Start bully algorithm
election = BullyService(BROKER_ID)
election.start()

def get_leader_id() -> Optional[int]:
    return election.current_leader


# ---- Lifespan to start follower replication loop ----
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    asyncio.create_task(follower_replication_loop())
    yield
    # Shutdown (nothing needed here)


app = FastAPI(lifespan=lifespan)


# ---- Replication setup ----
setup_replication(
    broker_id=BROKER_ID,
    broker_api=BROKER_API,
    data_dir=DATA_DIR,
    leader_getter=get_leader_id,
)


# ---------------- Models ----------------
class PublishRequest(BaseModel):
    key: str
    value: Any
    headers: Dict[str, Any] = Field(default_factory=dict)


# ---------------- Helpers ----------------
def _owner_base(owner_id: int) -> str:
    if owner_id not in BROKER_API:
        raise HTTPException(500, f"Unknown broker {owner_id}")
    return BROKER_API[owner_id]


async def _proxy_post(base: str, path: str, payload: Dict[str, Any]):
    async with httpx.AsyncClient(timeout=3.0) as client:
        r = await client.post(f"{base}{path}", json=payload)
        r.raise_for_status()
        return r.json()


# ---------------- Publish ----------------
@app.post("/topics/{topic}/publish")
async def publish(topic: str, req: PublishRequest):
    await refresh_owner_cache_if_needed()
    owner = get_owner_for(topic)
    if owner is None:
        raise HTTPException(404, "Topic not found")

    if owner != BROKER_ID:
        return await _proxy_post(_owner_base(owner), f"/topics/{topic}/publish", req.model_dump())

    storage.init_topic(topic)
    offset = storage.append_record(topic, req.key, req.value, req.headers)
    return {"topic": topic, "offset": offset}


# ---------------- Internal Fetch for Followers ----------------
@app.get("/internal/topics/{topic}/fetch")
async def internal_fetch(topic: str, offset: int = 0, max_batch: int = 100):
    owner = get_owner_for(topic)
    if owner != BROKER_ID:
        raise HTTPException(409, "NOT_OWNER")

    storage.init_topic(topic)
    return {"records": storage.poll_records(topic, offset, max_batch)}


# ---------------- Client Poll ----------------
@app.get("/topics/{topic}/poll")
async def poll(topic: str, offset: int = 0, max_batch: int = 100):
    storage.init_topic(topic)
    return {"records": storage.poll_records(topic, offset, max_batch)}


# ---------------- Metadata ----------------
@app.get("/metadata")
def metadata():
    return {
        "leader_id": get_leader_id(),
        "topics": get_all_metadata_for_api(),
    }


@app.post("/admin/topics")
def admin_create_topic(name: str, owner_id: Optional[int] = None, rf: int = 3):
    if not is_leader():
        raise HTTPException(409, {"error": "NOT_LEADER", "leader": get_leader_id()})
    placement = assign_topic(name, owner_id, rf)
    return placement.model_dump()
