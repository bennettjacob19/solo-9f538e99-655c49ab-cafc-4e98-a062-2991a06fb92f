"""Simulated upstream host: three plain echo ports and one TFTP sample.

The TFTP sample mimics the classic NAT problem: the first datagram arrives on
the well-known TFTP port, is detained for a while, and the reply is emitted
from a brand-new ephemeral socket (a different source port). Subsequent
datagrams from the same client are answered from that same new socket so the
observed source port stays stable.
"""
from __future__ import annotations

import socket
import threading
import time


class UpstreamSim:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.running = False
        self.host = "127.0.0.1"
        self.echo_ports: list[int] = []
        self.tftp_port = 0
        self.default_detain_ms = 300
        self.socks: list[socket.socket] = []
        self.threads: list[threading.Thread] = []
        self.stop_event = threading.Event()
        # client key (host, gateway public port) -> dedicated TFTP socket
        self.tftp_channels: dict[tuple[str, int], socket.socket] = {}
        self.channel_ports: dict[tuple[str, int], int] = {}

    # ------------------------------------------------------------- lifecycle
    def start(self, host: str, echo_ports: list[int], tftp_port: int,
              default_detain_ms: int) -> None:
        with self.lock:
            if self.running:
                self.stop()
            self.host = host
            self.echo_ports = list(echo_ports)
            self.tftp_port = tftp_port
            self.default_detain_ms = default_detain_ms
            self.stop_event = threading.Event()

            for port in echo_ports:
                sock = self._bind(port)
                self.socks.append(sock)
                self.threads.append(threading.Thread(
                    target=self._echo_loop, args=(sock, port), daemon=True))
            tftp_sock = self._bind(tftp_port)
            self.socks.append(tftp_sock)
            self.threads.append(threading.Thread(
                target=self._tftp_well_known_loop, daemon=True))
            for t in self.threads:
                t.start()
            self.running = True

    def _bind(self, port: int) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.host, port))
        sock.settimeout(0.3)
        return sock

    def stop(self) -> None:
        with self.lock:
            self.stop_event.set()
            for sock in list(self.socks) + list(self.tftp_channels.values()):
                try:
                    sock.close()
                except OSError:
                    pass
            self.socks.clear()
            self.tftp_channels.clear()
            self.channel_ports.clear()
            self.threads.clear()
            self.running = False

    def status(self) -> dict:
        with self.lock:
            return {
                "running": self.running,
                "host": self.host,
                "echo_ports": self.echo_ports,
                "tftp_well_known_port": self.tftp_port,
                "default_detain_ms": self.default_detain_ms,
                "tftp_tid_ports": [
                    {"client": f"{k[0]}:{k[1]}", "reply_source_port": p}
                    for k, p in sorted(self.channel_ports.items())
                ],
            }

    # ------------------------------------------------------------------ echo
    def _echo_loop(self, sock: socket.socket, port: int) -> None:
        while not self.stop_event.is_set():
            try:
                data, addr = sock.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                return
            try:
                # Plain echo: body returned verbatim from the same endpoint.
                sock.sendto(data, addr)
            except OSError:
                return

    # ------------------------------------------------------------------ TFTP
    def _tftp_well_known_loop(self) -> None:
        while not self.stop_event.is_set():
            well_known = next((s for s in self.socks
                               if s.getsockname()[1] == self.tftp_port), None)
            if well_known is None:
                return
            try:
                data, addr = well_known.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                return
            threading.Thread(target=self._handle_first_packet,
                             args=(data, addr), daemon=True).start()

    def _handle_first_packet(self, data: bytes,
                             addr: tuple[str, int]) -> None:
        # Detain the first packet before answering from a different port.
        if self.stop_event.wait(self.default_detain_ms / 1000.0):
            return
        with self.lock:
            key = addr
            chan = self.tftp_channels.get(key)
            if chan is None:
                chan = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                chan.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                chan.bind((self.host, 0))  # ephemeral: the new TFTP TID port
                chan.settimeout(0.3)
                self.tftp_channels[key] = chan
                self.channel_ports[key] = chan.getsockname()[1]
                threading.Thread(target=self._tftp_channel_loop,
                                 args=(chan, key), daemon=True).start()
        try:
            chan.sendto(data, addr)
        except OSError:
            pass

    def _tftp_channel_loop(self, chan: socket.socket,
                           key: tuple[str, int]) -> None:
        while not self.stop_event.is_set():
            try:
                data, addr = chan.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                return
            if addr != key:
                continue
            try:
                # Further packets keep the same TID/source port.
                chan.sendto(data, addr)
            except OSError:
                return
