"""NAT gateway engine.

Implements, with real UDP sockets:

* a configurable public UDP port pool with a per-port session cap;
* per-session reply acceptance ranges:
    - normal session: only the original upstream endpoint (single port);
    - TFTP session before its first packet: any source port of the upstream IP;
    - TFTP session after the first packet: only the actually observed port;
* dispatch keyed on (source address, source port, local receiving port);
* range non-overlap enforcement among sessions sharing one public port;
* allocation that never moves existing sessions;
* lazy binding: a public port is bound only when a session uses it and
  released (unbound) as soon as its last session disappears.
"""
from __future__ import annotations

import socket
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

HISTORY = 500


class NoCompliantPort(Exception):
    """Raised when no compliant public port can accept a new session."""


def _overlap(a: tuple[int, int], b: tuple[int, int]) -> bool:
    return not (a[1] < b[0] or b[1] < a[0])


@dataclass
class Session:
    id: str
    kind: str  # "normal" | "tftp"
    public_port: int
    internal_id: str
    internal_host: str
    internal_port: int
    upstream_host: str
    upstream_port: int
    created_at: float
    pinned_port: Optional[int] = None      # TFTP: observed source port of first packet
    first_recv_at: Optional[float] = None
    last_send_at: Optional[float] = None
    last_send_payload_len: int = 0
    events: list[str] = field(default_factory=list)
    history_ports: list[int] = field(default_factory=list)  # 实际发包端口

    @property
    def accept_range(self) -> tuple[int, int]:
        """Source-port range, on the upstream IP, that may reach this session."""
        if self.kind == "tftp" and self.pinned_port is None:
            return (1, 65535)
        if self.kind == "tftp":
            return (self.pinned_port, self.pinned_port)
        return (self.upstream_port, self.upstream_port)

    def log(self, msg: str) -> None:
        self.events.append(f"{time.strftime('%H:%M:%S')} {msg}")
        if len(self.events) > 200:
            del self.events[: len(self.events) - 200]

    def to_dict(self) -> dict:
        lo, hi = self.accept_range
        return {
            "id": self.id,
            "kind": self.kind,
            "public_port": self.public_port,
            "internal_endpoint_id": self.internal_id,
            "internal_target": f"{self.internal_host}:{self.internal_port}",
            "upstream": f"{self.upstream_host}:{self.upstream_port}",
            "pinned_source_port": self.pinned_port,
            "accepted_source_port_range": f"{lo}-{hi}" if lo != hi else str(lo),
            "first_recv_at": self.first_recv_at,
            "last_send_at": self.last_send_at,
            "actual_send_ports": self.history_ports,
            "events": self.events,
        }


@dataclass
class InternalEndpoint:
    id: str
    host: str
    port: int
    sock: socket.socket
    messages: deque = field(default_factory=lambda: deque(maxlen=HISTORY))
    events: list[str] = field(default_factory=list)


class NatEngine:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.pool: list[int] = []
        self.max_per_port = 1
        self.bind_host = "127.0.0.1"
        self.sessions: dict[str, Session] = {}
        self.public_socks: dict[int, socket.socket] = {}
        self.endpoints: dict[str, InternalEndpoint] = {}
        self.dispatch_log: deque = deque(maxlen=HISTORY)
        self._seq = 0
        self._stopping = False
        # One ephemeral socket is enough for outbound packets to internal endpoints.
        self.internal_send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.internal_send_sock.bind(("127.0.0.1", 0))

    # ------------------------------------------------------------------ setup
    def configure(self, pool_ports: list[int], max_per_port: int,
                  bind_host: str) -> None:
        with self.lock:
            self._close_public_socks()
            self.sessions.clear()
            self.dispatch_log.clear()
            self.pool = list(pool_ports)
            self.max_per_port = max_per_port
            self.bind_host = bind_host
            self._seq = 0

    def reset_round(self) -> None:
        """Reset the drill round: sessions, public sockets, dispatch history."""
        with self.lock:
            self._close_public_socks()
            self.sessions.clear()
            self.dispatch_log.clear()
            self._seq = 0
            for ep in self.endpoints.values():
                ep.messages.clear()
                ep.events.clear()

    def shutdown(self) -> None:
        with self.lock:
            self._stopping = True
            self._close_public_socks()
            for ep in self.endpoints.values():
                try:
                    ep.sock.close()
                except OSError:
                    pass
            self.endpoints.clear()
            try:
                self.internal_send_sock.close()
            except OSError:
                pass

    def _next_id(self, prefix: str) -> str:
        self._seq += 1
        return f"{prefix}-{self._seq}"

    # ------------------------------------------------------------- public UDP
    def _ensure_public_socket(self, port: int) -> socket.socket:
        """Lazy binding: idle public ports stay unbound."""
        sock = self.public_socks.get(port)
        if sock is not None:
            return sock
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.bind_host, port))
        sock.settimeout(0.5)
        self.public_socks[port] = sock
        threading.Thread(target=self._public_reader,
                         args=(sock, port), daemon=True).start()
        return sock

    def _close_public_socks(self) -> None:
        for sock in self.public_socks.values():
            try:
                sock.close()
            except OSError:
                pass
        self.public_socks.clear()

    def _release_public_socket_if_idle(self, port: int) -> None:
        if not any(s.public_port == port for s in self.sessions.values()):
            sock = self.public_socks.pop(port, None)
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass

    def _public_reader(self, sock: socket.socket, local_port: int) -> None:
        while not self._stopping:
            try:
                data, addr = sock.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                return  # socket closed
            self._dispatch(data, addr, local_port)

    def _dispatch(self, data: bytes, addr: tuple[str, int],
                  local_port: int) -> None:
        src_host, src_port = addr
        with self.lock:
            if local_port not in self.public_socks:
                return
            candidate: Optional[Session] = None
            for sess in self.sessions.values():
                if sess.public_port != local_port:
                    continue
                if sess.upstream_host != src_host:
                    continue
                lo, hi = sess.accept_range
                if lo <= src_port <= hi:
                    candidate = sess
                    break
            record = {
                "ts": time.time(),
                "local_port": local_port,
                "src": f"{src_host}:{src_port}",
                "bytes": len(data),
                "matched_session": candidate.id if candidate else None,
                "delivered": False,
            }
            if candidate is None:
                self.dispatch_log.append(record)
                return

            # TFTP first packet: pin to the actually observed source port, then
            # immediately re-allocate within the narrowed range (no move rule).
            if candidate.kind == "tftp" and candidate.pinned_port is None:
                old_port = candidate.public_port
                candidate.pinned_port = src_port
                candidate.first_recv_at = time.time()
                try:
                    # A pinned single-port range always fits on its current
                    # port, so this only fails under pathological state.
                    new_port = self._allocate_locked(candidate)
                except NoCompliantPort:
                    candidate.log("锁定后重新分配失败, 保留原公网端口")
                    new_port = old_port
                candidate.log(f"TFTP 首包实收自 {src_host}:{src_port}, 锁定端口; "
                              f"公网端口 {old_port} -> {new_port}")
                record["tftp_pinned_to"] = src_port
                record["public_port_after_pin"] = new_port
                if new_port != old_port:
                    # Move the session object: its new public port's socket must
                    # exist; the old one may unbind if it is now idle.
                    self._ensure_public_socket(new_port)
                    self._release_public_socket_if_idle(old_port)
            else:
                candidate.log(f"收到回包 {src_host}:{src_port} "
                              f"-> 内网 {candidate.internal_host}:{candidate.internal_port} "
                              f"({len(data)} 字节)")

            ep = self.endpoints.get(candidate.internal_id)
            if ep is None:
                record["error"] = "internal endpoint missing"
                self.dispatch_log.append(record)
                return
            self.internal_send_sock.sendto(
                data, (candidate.internal_host, candidate.internal_port))
            record["delivered"] = True
            record["internal_target"] = f"{candidate.internal_host}:{candidate.internal_port}"
            self.dispatch_log.append(record)

    # ------------------------------------------------------------- allocation
    def _allocate_locked(self, sess: Session) -> int:
        """Pick a compliant public port without moving any existing session.

        Called both at session creation and when a TFTP first packet narrows
        the accepted range.
        """
        rng = sess.accept_range
        for port in self.pool:
            occupants = [s for s in self.sessions.values()
                         if s.public_port == port and s.id != sess.id]
            if len(occupants) >= self.max_per_port:
                continue
            if any(_overlap(rng, s.accept_range) for s in occupants):
                continue
            return port
        raise NoCompliantPort(
            f"no compliant public port for session {sess.id} "
            f"range={rng}; existing allocations stay untouched")

    def create_session(self, kind: str, internal_id: str,
                       upstream_host: str, upstream_port: int) -> Session:
        with self.lock:
            ep = self.endpoints.get(internal_id)
            if ep is None:
                raise ValueError(f"unknown internal endpoint id: {internal_id}")
            sess = Session(
                id=self._next_id("sess"),
                kind=kind,
                public_port=-1,
                internal_id=internal_id,
                internal_host=ep.host,
                internal_port=ep.port,
                upstream_host=upstream_host,
                upstream_port=upstream_port,
                created_at=time.time(),
            )
            # Validate before inserting: on rejection nothing changes at all.
            port = self._allocate_locked(sess)
            sess.public_port = port
            self.sessions[sess.id] = sess
            sess.log(f"会话建立({kind}) 上游 {upstream_host}:{upstream_port} "
                     f"接受范围 {sess.accept_range} 分配公网端口 {port}")
            return sess

    def delete_session(self, session_id: str) -> None:
        with self.lock:
            sess = self.sessions.pop(session_id, None)
            if sess is None:
                raise ValueError(f"unknown session id: {session_id}")
            port = sess.public_port
            self._release_public_socket_if_idle(port)

    # ------------------------------------------------------------- endpoints
    def add_endpoint(self, host: str, port: int) -> InternalEndpoint:
        with self.lock:
            eid = self._next_id("ep")
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
            sock.settimeout(0.5)
            ep = InternalEndpoint(id=eid, host=host, port=port, sock=sock)
            ep.events.append(f"{time.strftime('%H:%M:%S')} 绑定 {host}:{port}")
            self.endpoints[eid] = ep
            threading.Thread(target=self._endpoint_reader,
                             args=(ep,), daemon=True).start()
            return ep

    def _endpoint_reader(self, ep: InternalEndpoint) -> None:
        while not self._stopping:
            try:
                data, addr = ep.sock.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                return
            entry = {
                "ts": time.time(),
                "from": f"{addr[0]}:{addr[1]}",
                "bytes": len(data),
                "hex": data.hex(),
                "text": _safe_decode(data),
            }
            ep.messages.append(entry)
            ep.events.append(
                f"{time.strftime('%H:%M:%S')} 收到 {addr[0]}:{addr[1]} "
                f"({len(data)} 字节)")
            if len(ep.events) > 200:
                del ep.events[: len(ep.events) - 200]

    def remove_endpoint(self, endpoint_id: str) -> None:
        with self.lock:
            ep = self.endpoints.pop(endpoint_id, None)
            if ep is None:
                raise ValueError(f"unknown endpoint id: {endpoint_id}")
            try:
                ep.sock.close()
            except OSError:
                pass

    # -------------------------------------------------------------- transmit
    def transmit(self, session_id: str, payload: bytes) -> dict:
        with self.lock:
            sess = self.sessions.get(session_id)
            if sess is None:
                raise ValueError(f"unknown session id: {session_id}")
            sock = self._ensure_public_socket(sess.public_port)  # lazy bind
            sock.sendto(payload, (sess.upstream_host, sess.upstream_port))
            sess.last_send_at = time.time()
            sess.last_send_payload_len = len(payload)
            sess.history_ports.append(sess.public_port)
            if len(sess.history_ports) > HISTORY:
                del sess.history_ports[: len(sess.history_ports) - HISTORY]
            local = sock.getsockname()
            return {
                "session_id": sess.id,
                "sent_from": f"{local[0]}:{local[1]}",
                "actual_send_port": local[1],
                "public_port": sess.public_port,
                "to": f"{sess.upstream_host}:{sess.upstream_port}",
                "bytes": len(payload),
            }

    # ---------------------------------------------------------------- queries
    def port_status(self) -> list[dict]:
        with self.lock:
            out = []
            for port in self.pool:
                occupants = [s for s in self.sessions.values()
                             if s.public_port == port]
                out.append({
                    "public_port": port,
                    "bound": port in self.public_socks,
                    "sessions": len(occupants),
                    "cap": self.max_per_port,
                    "occupants": [
                        {"session_id": s.id, "kind": s.kind,
                         "range": "%d-%d" % s.accept_range
                         if s.accept_range[0] != s.accept_range[1]
                         else str(s.accept_range[0]),
                         "upstream": f"{s.upstream_host}:{s.upstream_port}",
                         "pinned_port": s.pinned_port}
                        for s in occupants
                    ],
                })
            return out

    def get_session(self, session_id: str) -> Session:
        with self.lock:
            sess = self.sessions.get(session_id)
            if sess is None:
                raise ValueError(f"unknown session id: {session_id}")
            return sess

    def list_sessions(self) -> list[dict]:
        with self.lock:
            return [s.to_dict() for s in self.sessions.values()]

    def dispatch_results(self) -> list[dict]:
        with self.lock:
            return list(self.dispatch_log)


def _safe_decode(data: bytes) -> str:
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return data.hex()
