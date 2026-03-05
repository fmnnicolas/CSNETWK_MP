# reliable_udp.py
from __future__ import annotations
import random
import socket
import time
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

from protocol import WireMessage, MsgType, get_int

Addr = Tuple[str, int]

@dataclass
class ReliableConfig:
    timeout_s: float = 0.5   # RFC recommends 500ms
    max_retries: int = 3     # RFC recommends 3
    loss_rate: float = 0.0   # simulation
    verbose: bool = False

class ReliableUDP:
    """
    RFC 5.1:
    - Every non-ACK MUST include sequence_number
    - Receiver MUST send ACK with ack_number
    - Retransmit if no ACK within timeout
    """
    def __init__(self, bind_ip: str, bind_port: int, cfg: ReliableConfig):
        self.cfg = cfg
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((bind_ip, bind_port))
        self.sock.settimeout(0.1)

        self._seq_out = 1
        self._seen_in: Dict[Addr, set[int]] = {}
        self._pending: Dict[Tuple[Addr, int], Tuple[float, int, bytes]] = {}  # (addr, seq)->(deadline, retries_left, payload)

    def close(self) -> None:
        self.sock.close()

    def _log(self, s: str) -> None:
        if self.cfg.verbose:
            print(f"[RUDP] {s}")

    def _maybe_drop(self) -> bool:
        return random.random() < self.cfg.loss_rate

    def next_seq(self) -> int:
        seq = self._seq_out
        self._seq_out += 1
        return seq

    def send(self, addr: Addr, msg: WireMessage) -> None:
        # Enforce RFC rule: all non-ACK must have sequence_number
        if msg.message_type != MsgType.ACK and "sequence_number" not in msg.fields:
            raise ValueError("All non-ACK messages must include sequence_number")

        payload = msg.to_bytes()
        if msg.message_type == MsgType.ACK:
            if not self._maybe_drop():
                self.sock.sendto(payload, addr)
                self._log(f"TX ACK -> {addr} ack={msg.fields.get('ack_number')}")
            else:
                self._log(f"DROP_SIM TX ACK -> {addr}")
            return

        seq = get_int(msg.fields, "sequence_number", 0)
        if seq <= 0:
            raise ValueError("sequence_number must be positive int")

        if not self._maybe_drop():
            self.sock.sendto(payload, addr)
            self._log(f"TX {msg.message_type.value} -> {addr} seq={seq}")
        else:
            self._log(f"DROP_SIM TX {msg.message_type.value} -> {addr} seq={seq}")

        deadline = time.time() + self.cfg.timeout_s
        self._pending[(addr, seq)] = (deadline, self.cfg.max_retries, payload)

    def _send_ack(self, addr: Addr, seq: int) -> None:
        ack = WireMessage(MsgType.ACK, {"ack_number": str(seq)})
        self.send(addr, ack)

    def poll(self) -> Optional[Tuple[Addr, WireMessage]]:
        # Retransmit pending
        now = time.time()
        for key, (deadline, retries_left, payload) in list(self._pending.items()):
            if now < deadline:
                continue
            addr, seq = key
            if retries_left <= 0:
                self._log(f"GIVEUP {addr} seq={seq} -> assume lost connection")
                del self._pending[key]
                # Caller decides whether to terminate.
                continue

            if not self._maybe_drop():
                self.sock.sendto(payload, addr)
                self._log(f"RETX -> {addr} seq={seq} left={retries_left-1}")
            else:
                self._log(f"DROP_SIM RETX -> {addr} seq={seq} left={retries_left-1}")

            self._pending[key] = (now + self.cfg.timeout_s, retries_left - 1, payload)

        # Receive one packet
        try:
            data, addr = self.sock.recvfrom(65535)
        except socket.timeout:
            return None

        try:
            msg = WireMessage.from_bytes(data)
        except Exception as e:
            self._log(f"RX parse error from {addr}: {e}")
            return None

        # Handle ACK
        if msg.message_type == MsgType.ACK:
            ackn = get_int(msg.fields, "ack_number", 0)
            if (addr, ackn) in self._pending:
                del self._pending[(addr, ackn)]
                self._log(f"RX ACK <- {addr} ack={ackn}")
            return None

        # Non-ACK: must have sequence_number, must be ACKed
        seq = get_int(msg.fields, "sequence_number", 0)
        if seq <= 0:
            self._log(f"RX invalid (no seq) from {addr}")
            return None

        seen = self._seen_in.setdefault(addr, set())
        if seq in seen:
            # Duplicate: re-ACK and ignore
            self._log(f"RX DUP {msg.message_type.value} <- {addr} seq={seq}")
            self._send_ack(addr, seq)
            return None

        seen.add(seq)
        self._send_ack(addr, seq)
        self._log(f"RX {msg.message_type.value} <- {addr} seq={seq}")
        return addr, msg
