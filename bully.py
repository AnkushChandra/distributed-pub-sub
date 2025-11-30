# bully.py
import json
import socket
import threading
import time
from typing import Dict, Tuple, Optional

HEARTBEAT_INTERVAL = 1.0
HEARTBEAT_TIMEOUT = 3.0
ELECTION_OK_TIMEOUT = 3.0
COORDINATOR_TIMEOUT = 5.0

# You can change these or load from env/config file
CONFIG: Dict[int, Tuple[str, int]] = {
    1: ("127.0.0.1", 5001),
    2: ("127.0.0.1", 5002),
    3: ("127.0.0.1", 5003),
    4: ("127.0.0.1", 5004),
    5: ("127.0.0.1", 5005),
}


class ProcessNode:
    def __init__(self, pid: int):
        if pid not in CONFIG:
            raise ValueError(f"Unknown process id {pid}")
        self.pid = pid
        self.host, self.port = CONFIG[pid]
        self.peers = {i: CONFIG[i] for i in CONFIG if i != pid}

        self.coordinator_id: Optional[int] = None
        self.last_heartbeat: float = 0.0
        self.in_election: bool = False

        self.ok_event = threading.Event()
        self.coord_event = threading.Event()
        self.state_lock = threading.Lock()

        self.running: bool = False
        self.server_socket: Optional[socket.socket] = None

    # ---------- lifecycle ----------
    def start(self):
        """Start TCP server + heartbeat/failure detection."""
        if self.running:
            return
        self.running = True
        print(f"[P{self.pid}] starting on {self.host}:{self.port}")

        t_server = threading.Thread(target=self.server_loop, daemon=True)
        t_server.start()

        t_hb = threading.Thread(target=self.heartbeat_and_failure_detector_loop, daemon=True)
        t_hb.start()

        t_startup = threading.Thread(target=self.startup_election, daemon=True)
        t_startup.start()

    def stop(self):
        print(f"[P{self.pid}] stopping")
        self.running = False
        if self.server_socket:
            try:
                self.server_socket.close()
            except OSError:
                pass

    # ---------- networking ----------
    def server_loop(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket = s
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.host, self.port))
        s.listen()
        print(f"[P{self.pid}] listening on {self.host}:{self.port}")
        while self.running:
            try:
                conn, addr = s.accept()
            except OSError:
                break
            t = threading.Thread(target=self.handle_connection, args=(conn,), daemon=True)
            t.start()

    def handle_connection(self, conn: socket.socket):
        buf = ""
        try:
            while True:
                data = conn.recv(4096)
                if not data:
                    break
                buf += data.decode("utf-8")
                while "\n" in buf:
                    line, buf = buf.split("\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        msg = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    self.handle_message(msg)
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def handle_message(self, msg: dict):
        mtype = msg.get("type")
        src = msg.get("from")
        if mtype == "ELECTION":
            print(f"[P{self.pid}] received ELECTION from P{src}")
            self.on_election(src)
        elif mtype == "OK":
            print(f"[P{self.pid}] received OK from P{src}")
            self.on_ok(src)
        elif mtype == "COORDINATOR":
            leader = msg.get("leader")
            print(f"[P{self.pid}] received COORDINATOR from P{src}, leader=P{leader}")
            self.on_coordinator(leader)
        elif mtype == "HEARTBEAT":
            self.on_heartbeat(src)

    def send_message(self, target_pid: int, payload: dict):
        addr = self.peers.get(target_pid)
        if not addr:
            return
        host, port = addr
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2.0)
            s.connect((host, port))
            payload["from"] = self.pid
            data = json.dumps(payload).encode("utf-8") + b"\n"
            s.sendall(data)
        except OSError:
            pass
        finally:
            try:
                s.close()
            except OSError:
                pass

    # ---------- id helpers ----------
    def higher_ids(self):
        return [i for i in CONFIG if i > self.pid]

    def lower_ids(self):
        return [i for i in CONFIG if i < self.pid]

    # ---------- heartbeat + failure detection ----------
    def heartbeat_and_failure_detector_loop(self):
        while self.running:
            time.sleep(HEARTBEAT_INTERVAL / 2.0)
            with self.state_lock:
                coord = self.coordinator_id
                in_election = self.in_election
                last_hb = self.last_heartbeat
            now = time.time()
            if coord == self.pid:
                self.send_heartbeats(now)
            else:
                if coord is not None and not in_election and last_hb > 0.0:
                    if now - last_hb > HEARTBEAT_TIMEOUT:
                        print(f"[P{self.pid}] heartbeat timeout for coordinator P{coord}")
                        self.on_coordinator_failure()

    def send_heartbeats(self, now: float):
        with self.state_lock:
            self.last_heartbeat = now
        for pid in self.peers:
            self.send_message(pid, {"type": "HEARTBEAT"})

    def on_heartbeat(self, src_pid: int):
        with self.state_lock:
            if self.coordinator_id is None or self.coordinator_id == src_pid:
                self.coordinator_id = src_pid
                self.last_heartbeat = time.time()

    def on_coordinator_failure(self):
        with self.state_lock:
            if self.in_election:
                return
            print(f"[P{self.pid}] detected coordinator failure")
            self.coordinator_id = None
        self.detect_coordinator_failure()

    # ---------- election ----------
    def detect_coordinator_failure(self):
        if not self.higher_ids():
            self.become_coordinator()
        else:
            t = threading.Thread(target=self.run_election, daemon=True)
            t.start()

    def run_election(self):
        with self.state_lock:
            if self.in_election:
                return
            self.in_election = True
            self.ok_event.clear()
            self.coord_event.clear()
        higher = self.higher_ids()
        print(f"[P{self.pid}] starting election, higher={higher}")
        for pid in higher:
            self.send_message(pid, {"type": "ELECTION"})
        if not higher:
            self.become_coordinator()
            return
        got_ok = self.ok_event.wait(ELECTION_OK_TIMEOUT)
        if not got_ok:
            self.become_coordinator()
            return
        print(f"[P{self.pid}] got OK, waiting for COORDINATOR")
        got_coord = self.coord_event.wait(COORDINATOR_TIMEOUT)
        if not got_coord:
            with self.state_lock:
                self.in_election = False
            print(f"[P{self.pid}] no COORDINATOR, restarting election")
            self.run_election()

    def on_election(self, from_pid: int):
        if from_pid is None:
            return
        if self.pid > from_pid:
            self.send_message(from_pid, {"type": "OK"})
            with self.state_lock:
                already = self.in_election
            if not already:
                t = threading.Thread(target=self.run_election, daemon=True)
                t.start()

    def on_ok(self, from_pid: int):
        self.ok_event.set()

    def become_coordinator(self):
        with self.state_lock:
            self.coordinator_id = self.pid
            self.in_election = False
            self.last_heartbeat = time.time()
        print(f"[P{self.pid}] became coordinator")
        for pid in self.lower_ids():
            self.send_message(pid, {"type": "COORDINATOR", "leader": self.pid})

    def on_coordinator(self, leader_id: int):
        with self.state_lock:
            self.coordinator_id = leader_id
            self.in_election = False
            self.last_heartbeat = time.time()
        self.coord_event.set()

    # ---------- startup ----------
    def startup_election(self):
        # Give some time for others to start
        time.sleep(2.0)
        self.detect_coordinator_failure()

    # ---------- helpers ----------
    def print_status(self):
        with self.state_lock:
            coord = self.coordinator_id
            in_election = self.in_election
            last = self.last_heartbeat
        print(f"[P{self.pid}] status: coordinator={coord} in_election={in_election} last_heartbeat={last}")


class BullyService(ProcessNode):
    """Thin wrapper exposing nicer helpers for the broker."""

    @property
    def is_leader(self) -> bool:
        with self.state_lock:
            return self.coordinator_id == self.pid

    @property
    def current_leader(self) -> Optional[int]:
        with self.state_lock:
            return self.coordinator_id
