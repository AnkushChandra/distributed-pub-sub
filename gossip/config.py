import os
BROKER_ID = os.getenv("BROKER_ID", "broker-1")
BROKER_ADDR = os.getenv("BROKER_ADDR", "http://127.0.0.1:8000")
_raw = os.getenv("SEED_PEERS", "")
SEED_PEERS = [p.strip() for p in _raw.split(",") if p.strip()]
