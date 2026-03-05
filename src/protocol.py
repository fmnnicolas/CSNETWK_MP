# protocol.py
from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from typing import Dict

class MsgType(str, Enum):
    # Connection / roles
    HANDSHAKE_REQUEST = "HANDSHAKE_REQUEST"
    HANDSHAKE_RESPONSE = "HANDSHAKE_RESPONSE"
    SPECTATOR_REQUEST = "SPECTATOR_REQUEST"
    BATTLE_SETUP = "BATTLE_SETUP"

    # Turn flow
    ATTACK_ANNOUNCE = "ATTACK_ANNOUNCE"
    DEFENSE_ANNOUNCE = "DEFENSE_ANNOUNCE"
    CALCULATION_REPORT = "CALCULATION_REPORT"
    CALCULATION_CONFIRM = "CALCULATION_CONFIRM"
    RESOLUTION_REQUEST = "RESOLUTION_REQUEST"
    GAME_OVER = "GAME_OVER"

    # Chat
    CHAT_MESSAGE = "CHAT_MESSAGE"

    # Reliability
    ACK = "ACK"
    ERROR = "ERROR"

@dataclass(frozen=True)
class WireMessage:
    """
    RFC: plain text, newline-separated key: value pairs.
    Mandatory: message_type (all messages)
    Mandatory: sequence_number for every non-ACK message (RFC).
    """
    message_type: MsgType
    fields: Dict[str, str]

    def to_bytes(self) -> bytes:
        lines = [f"message_type: {self.message_type.value}"]
        for k, v in self.fields.items():
            lines.append(f"{k}: {v}")
        lines.append("")
        return ("\n".join(lines)).encode("utf-8")

    @staticmethod
    def from_bytes(data: bytes) -> "WireMessage":
        text = data.decode("utf-8", errors="replace")
        lines = [ln.strip("\r") for ln in text.split("\n") if ln.strip("\r") != ""]
        kv: Dict[str, str] = {}
        for ln in lines:
            if ":" not in ln:
                continue
            k, v = ln.split(":", 1)
            kv[k.strip()] = v.strip()

        if "message_type" not in kv:
            raise ValueError("Missing message_type")

        mt = MsgType(kv.pop("message_type"))
        return WireMessage(mt, kv)

def get_int(fields: Dict[str, str], key: str, default: int = 0) -> int:
    try:
        return int(fields.get(key, str(default)))
    except ValueError:
        return default

def require(fields: Dict[str, str], key: str) -> str:
    if key not in fields:
        raise ValueError(f"Missing required field: {key}")
    return fields[key]
