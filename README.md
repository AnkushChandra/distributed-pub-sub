# Distributed Pub/Sub

Mini distributed publish/subscribe broker that demonstrates:

- Discovery service that pushes broker endpoint updates (no long-lived heartbeats required)
- Brokers that coordinate ownership with a Bully election, gossip, and ISR-based replication
- Routed reads so consumers can talk to any replica, with sticky assignment & automatic failover
- SQLite-backed storage (one DB per broker) with optional payload encryption
- Simple FastAPI client/admin surface plus CLI helpers for topics, publishing, and subscribing

## Architecture

| Component | Responsibility |
|-----------|----------------|
| `discovery_service.py` | Keeps track of broker endpoints. When a broker registers, the service broadcasts the full broker map to every node so each broker can update its peer list immediately. |
| `main.py` (broker) | Runs the FastAPI app, hosts client/admin/internal routes, replicates writes to ISR followers, and manages lifecycle hooks. |
| `bully.py`, `election.py`, `lifecycle.py`, `gossip.py` | Handle Bully elections, gossip heartbeat state, and leader-specific duties such as metadata refresh and ISR synchronization. |
| `metadata_store.py` | Stores topic placement (owner + followers + ISR) and ensures caches stay fresh on every broker. |
| `storage.py` | Per-broker SQLite log (`data{BROKER_ID}/pubsub.sqlite`) that tracks records, topic offsets, and consumer commits. WAL mode keeps reads fast while writers serialize via a lock. |
| `replication.py` | Pushes newly appended records from the topic owner to its in-sync followers and trims ISR membership if replicas fall behind. |
| `client_routes.py` | Implements `/publish`, `/subscribe`, `/poll`, `/commit`, and replica discovery endpoints with proxying when a consumer lands on a non-replica broker. |
| `admin_routes.py`, `internal_routes.py` | Provide topic management, leader information, ISR status, and replication diagnostics. |

## Individual Contributions (Three Equal Parts)

1. **Gautam – Bully Election & Discovery Plane**
   - Designed and implemented all Bully election helpers (`bully.py`, `election.py`) and the discovery layer (`discovery_service.py`, `discovery_client.py`).
   - Ensured brokers register and receive pushed peer maps so leader promotion can happen promptly when peers change.
   - Wired configuration knobs (`config.py`) so every broker exposes the correct API/election endpoints to the rest of the cluster.

2. **Sravani – Gossip, Lifecycle & Metadata Management**
   - Built the gossip heartbeat + lifecycle controllers (`gossip.py`, `lifecycle.py`) that keep track of alive brokers and drive cache refresh decisions.
   - Owns metadata placement logic (`metadata_store.py`) and keeps the placement cache fresh across brokers.
   - Collaborated on admin surfacing but focuses primarily on health tracking and placement updates.

3. **Ankush – Replication, Storage & Client Surface**
   - Implemented the ISR replication algorithm and follower sync pipeline (`replication.py`) together with the SQLite persistence layer (`storage.py`).
   - Owns the FastAPI client/admin/internal routers (`client_routes.py`, `admin_routes.py`, `internal_routes.py`) plus the CLI utilities and verification scripts under `clients/` and `scripts/`.
   - Ensured routed reads, commit tracking, operator visibility (e.g., `/admin/topics`, `/metrics`), failover-friendly subscriber tooling, and the storage encryption/security knobs (`PUBSUB_STORAGE_KEY`, `config.py`) work end to end.

## Requirements

- Python 3.11+
- `pip install -r requirement.txt`
- (Optional) `PUBSUB_STORAGE_KEY` for encrypted record payloads

Create a virtual environment once:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirement.txt
```

## Running Locally

1. **Start the discovery service**

   ```bash
   source .venv/bin/activate
   uvicorn discovery_service:app --host 127.0.0.1 --port 9090
   ```

2. **Launch multiple brokers** (one terminal per broker). Each broker needs a unique ID, HTTP port, API base, and election port:

   ```bash
   # Broker 1
   source .venv/bin/activate
   BROKER_ID=1 BROKER_HTTP_PORT=8081 BROKER_API_BASE=http://127.0.0.1:8081 \
   ELECTION_PORT=5001 uvicorn main:app --host 127.0.0.1 --port 8081

   # Broker 2
   source .venv/bin/activate
   BROKER_ID=2 BROKER_HTTP_PORT=8082 BROKER_API_BASE=http://127.0.0.1:8082 \
   ELECTION_PORT=5002 uvicorn main:app --host 127.0.0.1 --port 8082

   # Broker 3
   source .venv/bin/activate
   BROKER_ID=3 BROKER_HTTP_PORT=8083 BROKER_API_BASE=http://127.0.0.1:8083 \
   ELECTION_PORT=5003 uvicorn main:app --host 127.0.0.1 --port 8083
   ```

   Each broker auto-registers with discovery, seeds its peer map, and opens its own `data{BROKER_ID}` directory.

## Managing Topics & Data Flow

CLI helpers in `clients/` wrap the HTTP APIs.

### Topic creation / reassignment

```bash
python3 clients/topic_admin.py demo-topic --rf 3
```

- `--rf` sets replication factor.

### Publishing

```bash
python3 clients/publisher.py demo-topic --broker http://127.0.0.1:8081
```

### Subscribing

```bash
python3 clients/subscriber.py demo-topic --group-id g1 --consumer-id c1 \
    --brokers http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083
```

- The subscriber polls `/poll` with long-poll timeouts, auto-commits offsets unless `--no-commit`, and rotates across brokers on failures.
- `--prefer-replica <broker_id>` pins reads to a specific replica.
- If only `--broker` is supplied, the client still fails over to peers discovered via `/subscribe` responses.

### Useful Admin Endpoints

- `GET /admin/topics` – list placements and ISR membership.
- `POST /admin/topics?name=foo&rf=3` – same call the CLI makes.
- `GET /replicas/{topic}` – view owner/follower info from any broker.
- `GET /metrics` – shows leases, owned topics, and gossip snapshot.


