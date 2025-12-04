# sqlite_storage.py
from __future__ import annotations
import json, os, sqlite3, threading, time, pathlib
from typing import Any, Dict, List, Optional, Tuple

from cryptography.fernet import Fernet, InvalidToken

from config import BROKER_ID

_data_dir_env = os.getenv("PUBSUB_DATA_DIR")
_data_dir_default = f"data{BROKER_ID}" if BROKER_ID is not None else "data"
DATA_DIR = pathlib.Path(_data_dir_env or _data_dir_default).resolve()
DB_PATH = DATA_DIR / "pubsub.sqlite"
DATA_DIR.mkdir(parents=True, exist_ok=True)

_ENC_PREFIX = "ENC::FERNET::"


def _init_encryptor() -> Optional[Fernet]:
    key = os.getenv("PUBSUB_STORAGE_KEY") or os.getenv("STORAGE_ENCRYPTION_KEY")
    if not key:
        return None
    normalized = key.strip().encode()
    try:
        return Fernet(normalized)
    except Exception as exc:  # pragma: no cover - configuration error
        raise RuntimeError(
            "Invalid PUBSUB_STORAGE_KEY/STORAGE_ENCRYPTION_KEY provided. "
            "Use cryptography.Fernet.generate_key()."
        ) from exc


_ENCRYPTOR: Optional[Fernet] = _init_encryptor()

_lock = threading.RLock()

def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)  # autocommit mode
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("""
    CREATE TABLE IF NOT EXISTS records(
        topic TEXT NOT NULL,
        offset INTEGER NOT NULL,
        ts_ms INTEGER NOT NULL,
        key TEXT,
        value_json TEXT NOT NULL,
        headers_json TEXT NOT NULL,
        PRIMARY KEY(topic, offset)
    );
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS topic_seq(
        topic TEXT PRIMARY KEY,
        next_offset INTEGER NOT NULL
    );
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS commits(
        k TEXT PRIMARY KEY,
        offset INTEGER NOT NULL
    );
    """)
    # helpful index for scans (topic, offset) already PK; ensure coverage anyway
    conn.execute("CREATE INDEX IF NOT EXISTS idx_records_topic_offset ON records(topic, offset);")
    return conn

_conn = _connect()


def _encrypt_blob(raw: str) -> str:
    if not _ENCRYPTOR:
        return raw
    token = _ENCRYPTOR.encrypt(raw.encode("utf-8"))
    return f"{_ENC_PREFIX}{token.decode('ascii')}"


def _decrypt_blob(raw: str) -> str:
    if not raw.startswith(_ENC_PREFIX):
        return raw
    if not _ENCRYPTOR:
        raise RuntimeError(
            "Encrypted payload encountered but no PUBSUB_STORAGE_KEY configured"
        )
    token = raw[len(_ENC_PREFIX):].encode("ascii")
    try:
        decrypted = _ENCRYPTOR.decrypt(token)
    except InvalidToken as exc:
        raise RuntimeError("Failed to decrypt storage payload") from exc
    return decrypted.decode("utf-8")


def _serialize(value: Any) -> str:
    return _encrypt_blob(json.dumps(value, separators=(",", ":")))


def _deserialize(blob: str) -> Any:
    return json.loads(_decrypt_blob(blob))

def _now_ms() -> int:
    return int(time.time() * 1000)

def init_topic(topic: str) -> None:
    with _lock:
        _conn.execute(
            "INSERT INTO topic_seq(topic, next_offset) VALUES(?, 0) "
            "ON CONFLICT(topic) DO NOTHING", (topic,)
        )

def append_record(topic: str, key: Optional[str], value: Any, headers: Dict[str, Any]) -> int:
    """Atomically reserve the next per-topic offset and append a row."""
    payload = _serialize(value)
    hjson   = _serialize(headers or {})
    ts = _now_ms()
    with _lock:
        _conn.execute("BEGIN IMMEDIATE;")  # get a write lock early
        # ensure topic sequence exists
        _conn.execute(
            "INSERT INTO topic_seq(topic, next_offset) VALUES(?, 0) "
            "ON CONFLICT(topic) DO NOTHING", (topic,)
        )
        row = _conn.execute("SELECT next_offset FROM topic_seq WHERE topic=?", (topic,)).fetchone()
        off = int(row[0])
        _conn.execute("UPDATE topic_seq SET next_offset=? WHERE topic=?", (off + 1, topic))
        _conn.execute(
            "INSERT INTO records(topic, offset, ts_ms, key, value_json, headers_json) VALUES(?,?,?,?,?,?)",
            (topic, off, ts, key, payload, hjson)
        )
        _conn.execute("COMMIT;")
    return off

def poll_records(topic: str, start_offset: int, max_records: int) -> List[Dict[str, Any]]:
    with _lock:
        cur = _conn.execute(
            "SELECT offset, ts_ms, key, value_json, headers_json "
            "FROM records WHERE topic=? AND offset>=? ORDER BY offset LIMIT ?",
            (topic, max(0, start_offset), max_records)
        )
        rows = cur.fetchall()
    out: List[Dict[str, Any]] = []
    for off, ts, key, vj, hj in rows:
        out.append({
            "offset": int(off),
            "ts_ms": int(ts),
            "key": key,
            "value": _deserialize(vj),
            "headers": _deserialize(hj),
        })
    return out

def topic_next_offset(topic: str) -> int:
    with _lock:
        row = _conn.execute(
            "SELECT next_offset FROM topic_seq WHERE topic=?", (topic,)
        ).fetchone()
        return int(row[0]) if row else 0

# ---- commit store (persisted) ----
def commit_key(topic: str, consumer_id: Optional[str], group_id: Optional[str]) -> str:
    if group_id:   return f"{topic}|group:{group_id}"
    if consumer_id:return f"{topic}|consumer:{consumer_id}"
    return f"{topic}|consumer:_anon"

def get_commit(topic: str, consumer_id: Optional[str], group_id: Optional[str]) -> int:
    k = commit_key(topic, consumer_id, group_id)
    with _lock:
        row = _conn.execute("SELECT offset FROM commits WHERE k=?", (k,)).fetchone()
    return int(row[0]) if row else -1

def set_commit(topic: str, consumer_id: Optional[str], group_id: Optional[str], offset: int) -> None:
    k = commit_key(topic, consumer_id, group_id)
    with _lock:
        _conn.execute(
            "INSERT INTO commits(k, offset) VALUES(?, ?) "
            "ON CONFLICT(k) DO UPDATE SET offset=excluded.offset", (k, int(offset))
        )

def commit_entries_for_topic(topic: str) -> Dict[str, int]:
    prefix = f"{topic}|%"
    with _lock:
        rows = _conn.execute(
            "SELECT k, offset FROM commits WHERE k LIKE ?",
            (prefix,)
        ).fetchall()
    return {str(k): int(offset) for k, offset in rows}


def apply_commit_snapshot(entries: Dict[str, int]) -> None:
    if not entries:
        return
    with _lock:
        for key, offset in entries.items():
            if not isinstance(key, str):
                continue
            if "|" not in key:
                continue
            _conn.execute(
                "INSERT INTO commits(k, offset) VALUES(?, ?) "
                "ON CONFLICT(k) DO UPDATE SET offset=excluded.offset",
                (key, int(offset)),
            )

# ---- replication helpers ----

def latest_offset(topic: str) -> int:
    """Return the highest committed offset for topic, or -1 if none."""
    with _lock:
        row = _conn.execute(
            "SELECT next_offset FROM topic_seq WHERE topic=?", (topic,)
        ).fetchone()
        if not row:
            return -1
        next_off = int(row[0])
        return next_off - 1 if next_off > 0 else -1


def replicate_record(topic: str, offset: int, key: Optional[str], value: Any, headers: Dict[str, Any]) -> None:
    """Insert a record with an explicit offset (used by followers)."""
    payload = _serialize(value)
    hjson = _serialize(headers or {})
    ts = _now_ms()
    with _lock:
        _conn.execute("BEGIN IMMEDIATE;")
        _conn.execute(
            "INSERT INTO topic_seq(topic, next_offset) VALUES(?, 0) "
            "ON CONFLICT(topic) DO NOTHING", (topic,)
        )
        _conn.execute(
            "INSERT OR IGNORE INTO records(topic, offset, ts_ms, key, value_json, headers_json) "
            "VALUES(?,?,?,?,?,?)",
            (topic, int(offset), ts, key, payload, hjson)
        )
        row = _conn.execute(
            "SELECT next_offset FROM topic_seq WHERE topic=?", (topic,)
        ).fetchone()
        next_off = int(row[0]) if row else 0
        desired_next = int(offset) + 1
        if desired_next > next_off:
            _conn.execute(
                "UPDATE topic_seq SET next_offset=? WHERE topic=?",
                (desired_next, topic)
            )
        _conn.execute("COMMIT;")
