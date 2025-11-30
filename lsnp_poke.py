"""
Core features:
- UDP socket networking
- Host / Joiner / Spectator roles
- Handshake + battle setup
- Turn-based battle state machine
- Custom reliability layer with sequence numbers + ACKs
- Damage calculation using pokemon.csv
- Text and sticker chat (Base64)
- Verbose mode to show all protocol messages
"""

import socket
import threading
import time
import base64
import json
import os
from dataclasses import dataclass
from typing import Dict, Tuple, Optional
import random
import csv

# -------------------------
# Config
# -------------------------

ACK_TIMEOUT_SEC = 0.5          # RFC recommended ~500ms
MAX_RETRIES = 3                # Recommended in RFC
BUFFER_SIZE = 65535

DEFAULT_COMM_MODE = "P2P"      # we always run P2P; BROADCAST is left as extension

# -------------------------
# Pokémon data handling
# -------------------------

@dataclass
class PokemonStats:
    name: str
    hp: int
    attack: int
    defense: int
    sp_attack: int
    sp_defense: int
    type1: str
    type2: Optional[str]
    against: Dict[str, float]  # e.g. {'fire': 2.0, 'water': 0.5, ...}


class PokemonDatabase:
    """
    Loads pokemon.csv and gives you stats by name.
    """

    def __init__(self, csv_path: str = "pokemon.csv"):
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"pokemon.csv not found at: {csv_path}")
        self.by_name: Dict[str, PokemonStats] = {}
        self._load(csv_path)

    def _load(self, csv_path: str):
        with open(csv_path, newline='', encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = row["name"].strip()
                against = {}
                for key, val in row.items():
                    if key.startswith("against_") and val != "":
                        atype = key.replace("against_", "").lower()
                        against[atype] = float(val)
                stats = PokemonStats(
                    name=name,
                    hp=int(float(row["hp"])),
                    attack=int(float(row["attack"])),
                    defense=int(float(row["defense"])),
                    sp_attack=int(float(row["sp_attack"])),
                    sp_defense=int(float(row["sp_defense"])),
                    type1=row["type1"].lower() if row["type1"] else "",
                    type2=row["type2"].lower() if row["type2"] else None,
                    against=against
                )
                self.by_name[name.lower()] = stats

    def get(self, name: str) -> PokemonStats:
        key = name.lower().strip()
        if key not in self.by_name:
            raise ValueError(f"Pokémon '{name}' not found in CSV")
        return self.by_name[key]


# -------------------------
# Message codec
# -------------------------

def encode_message(fields: Dict[str, str]) -> bytes:
    """
    Encode dict into RFC-style key: value\n text.
    """
    lines = []
    for k, v in fields.items():
        lines.append(f"{k}: {v}")
    return ("\n".join(lines)).encode("utf-8")


def decode_message(data: bytes) -> Dict[str, str]:
    """
    Decode key: value\n text into dict.
    """
    text = data.decode("utf-8", errors="replace")
    out: Dict[str, str] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        out[k.strip()] = v.strip()
    return out


# -------------------------
# Reliability layer
# -------------------------

@dataclass
class PendingMessage:
    seq: int
    fields: Dict[str, str]
    dest: Tuple[str, int]
    last_sent: float
    retries: int


class ReliableUDP:
    """
    Wraps a UDP socket with:
    - sequence numbers
    - explicit ACK messages
    - retransmission on timeout
    """

    def __init__(self, sock: socket.socket, verbose: bool = False):
        self.sock = sock
        self.verbose = verbose
        self._seq = 1
        self._lock = threading.Lock()
        self._pending: Dict[int, PendingMessage] = {}
        self.running = True

        self.retransmit_thread = threading.Thread(
            target=self._retransmit_loop, daemon=True
        )
        self.retransmit_thread.start()

    def next_seq(self) -> int:
        with self._lock:
            s = self._seq
            self._seq += 1
            return s

    def stop(self):
        self.running = False

    def _retransmit_loop(self):
        while self.running:
            now = time.time()
            to_retry = []
            to_fail = []

            with self._lock:
                for seq, pm in list(self._pending.items()):
                    if now - pm.last_sent >= ACK_TIMEOUT_SEC:
                        if pm.retries >= MAX_RETRIES:
                            to_fail.append(seq)
                        else:
                            to_retry.append(seq)

            for seq in to_retry:
                with self._lock:
                    pm = self._pending.get(seq)
                    if not pm:
                        continue
                    if self.verbose:
                        print(f"[RELIABLE] Retransmitting seq={seq} to {pm.dest}")
                    self.sock.sendto(encode_message(pm.fields), pm.dest)
                    pm.last_sent = time.time()
                    pm.retries += 1

            for seq in to_fail:
                with self._lock:
                    pm = self._pending.pop(seq, None)
                    if pm and self.verbose:
                        print(f"[RELIABLE] Giving up on seq={seq} after {MAX_RETRIES} retries")

            time.sleep(0.05)

    def send(self, fields: Dict[str, str], dest: Tuple[str, int], reliable: bool = True):
        """
        Send a message; if reliable=True, track it and require ACK.
        We assume all messages except ACK themselves are reliable.
        """
        if reliable and "sequence_number" not in fields:
            # The caller must set sequence_number according to protocol.
            raise ValueError("sequence_number is required for reliable messages")

        if self.verbose:
            print(f"[SEND] -> {dest} :: {fields}")

        self.sock.sendto(encode_message(fields), dest)

        if reliable and fields.get("message_type") != "ACK":
            seq = int(fields["sequence_number"])
            with self._lock:
                self._pending[seq] = PendingMessage(
                    seq=seq,
                    fields=dict(fields),
                    dest=dest,
                    last_sent=time.time(),
                    retries=0,
                )

    def handle_incoming_ack(self, fields: Dict[str, str]):
        """
        Remove acked message from pending.
        """
        ack_no = int(fields["ack_number"])
        with self._lock:
            if ack_no in self._pending:
                if self.verbose:
                    print(f"[RELIABLE] ACK received for seq={ack_no}")
                self._pending.pop(ack_no, None)


# -------------------------
# Damage calculation
# -------------------------

def type_effectiveness(move_type: str, defender: PokemonStats) -> Tuple[float, float]:
    """
    Return (Type1Effectiveness, Type2Effectiveness) for RFC formula.

    The bundled pokemon.csv contains a combined multiplier per attacking type
    (e.g., ``against_fire``). We treat that value as the primary type
    multiplier and default the secondary type to 1.0 so we can still express
    the RFC's "Type1Effectiveness * Type2Effectiveness" product without
    double-counting a combined value.
    """

    move_type = move_type.lower()
    combined = defender.against.get(move_type, 1.0)

    # Use the combined multiplier for Type1 and neutral for Type2 if absent.
    type1_eff = combined if defender.type1 else 1.0
    type2_eff = 1.0 if not defender.type2 else combined if combined != 1.0 else 1.0

    return type1_eff, type2_eff


def calculate_damage(
    attacker: PokemonStats,
    defender: PokemonStats,
    move_name: str,
    move_type: str,
    damage_category: str,
    base_power: int,
    use_sp_attack_boost: bool,
    use_sp_defense_boost: bool,
    sp_atk_uses_left: int,
    sp_def_uses_left: int,
) -> Tuple[int, int, int, int, str, int, int]:
    """
    Very simple Pokémon-like damage formula with stat boosts.

    Returns:
    - damage_dealt
    - defender_hp_after
    - attacker_remaining_hp (unchanged here, but used as integrity field)
    - new_sp_atk_uses
    - new_sp_def_uses
    - status_message
    """

    # Select relevant stats based on damage_category
    if damage_category.lower() == "physical":
        atk_stat = attacker.attack
        def_stat = defender.defense
    else:  # "special"
        atk_stat = attacker.sp_attack
        def_stat = defender.sp_defense

    # Apply boosts if requested and allowed
    if damage_category.lower() == "special" and use_sp_attack_boost and sp_atk_uses_left > 0:
        atk_stat = int(atk_stat * 1.5)
        sp_atk_uses_left -= 1

    if damage_category.lower() == "special" and use_sp_defense_boost and sp_def_uses_left > 0:
        def_stat = int(def_stat * 1.5)
        sp_def_uses_left -= 1

    # Type effectiveness (RFC: basePower × attackerStat × Type1Eff × Type2Eff / defenderStat)
    type1_eff, type2_eff = type_effectiveness(move_type, defender)

    damage = int(
        base_power
        * atk_stat
        * type1_eff
        * type2_eff
        / max(def_stat, 1)
    )
    if damage < 1:
        damage = 1

    defender_hp_after = max(defender.hp - damage, 0)
    attacker_remaining_hp = attacker.hp  # we don't change self HP here

    eff = type1_eff * type2_eff
    if eff > 1.0:
        eff_str = "It was super effective!"
    elif eff < 1.0:
        eff_str = "It was not very effective..."
    else:
        eff_str = "It had normal effectiveness."

    status_message = f"{attacker.name} used {move_name}! {eff_str}"

    return (
        damage,
        defender_hp_after,
        attacker_remaining_hp,
        sp_atk_uses_left,
        sp_def_uses_left,
        status_message,
        defender_hp_after,
    )


# -------------------------
# Node / battle state
# -------------------------

class BattleState:
    """
    Holds local view of the battle.
    """

    def __init__(
        self,
        my_pokemon: PokemonStats,
        opp_pokemon: PokemonStats,
        sp_atk_uses: int,
        sp_def_uses: int,
        i_am_host: bool,
    ):
        self.my_pokemon = my_pokemon
        self.opp_pokemon = opp_pokemon
        self.my_hp = my_pokemon.hp
        self.opp_hp = opp_pokemon.hp
        # Track boost uses separately for each side to keep calculations in sync
        self.my_sp_atk_uses = sp_atk_uses
        self.my_sp_def_uses = sp_def_uses
        self.opp_sp_atk_uses = sp_atk_uses
        self.opp_sp_def_uses = sp_def_uses
        self.i_am_host = i_am_host
        self.my_turn = i_am_host  # host starts first


class LSNPNode:
    """
    Implements:
    - Host / Joiner / Spectator basics
    - Handshake
    - Battle setup
    - Turn-based flow
    - Reliability
    - Chat (text + sticker)
    """

    def __init__(
        self,
        role: str,
        local_port: int,
        peer_ip: Optional[str],
        peer_port: Optional[int],
        pokemon_name: Optional[str],
        verbose: bool = False,
        pokemon_csv_path: str = "pokemon.csv",
    ):
        self.role = role.upper()
        self.local_port = local_port
        self.peer_ip = peer_ip
        self.peer_port = peer_port
        self.verbose = verbose

        # Socket & reliability wrapper
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", local_port))
        self.sock.settimeout(1.0)

        self.reliable = ReliableUDP(self.sock, verbose=verbose)

        # Peers
        self.peer_addr: Optional[Tuple[str, int]] = None

        # Pokémon
        self.pdb = PokemonDatabase(pokemon_csv_path)
        self.my_pokemon_name = pokemon_name
        self.my_pokemon: Optional[PokemonStats] = None
        self.opp_pokemon: Optional[PokemonStats] = None

        # Battle state
        self.battle_state: Optional[BattleState] = None

        # Random seed (from host)
        self.seed: Optional[int] = None

        # Verbose flag
        print(f"[INFO] LSNPNode role={self.role}, local_port={local_port}, verbose={verbose}")

    # -----------------------------
    # General helpers
    # -----------------------------

    def _log(self, msg: str):
        print(msg)

    def _send_ack(self, ack_number: int, dest: Tuple[str, int]):
        fields = {
            "message_type": "ACK",
            "ack_number": str(ack_number),
        }
        if self.verbose:
            print(f"[SEND] ACK -> {dest} :: {fields}")
        self.sock.sendto(encode_message(fields), dest)

    # -----------------------------
    # Role entrypoints
    # -----------------------------

    def run(self):
        if self.role == "HOST":
            self.run_host()
        elif self.role == "JOINER":
            self.run_joiner()
        elif self.role == "SPECTATOR":
            self.run_spectator()
        else:
            raise ValueError(f"Invalid role: {self.role}")

    # -----------------------------
    # Common receive loop
    # -----------------------------

    def recv_message(self) -> Optional[Tuple[Dict[str, str], Tuple[str, int]]]:
        try:
            data, addr = self.sock.recvfrom(BUFFER_SIZE)
        except socket.timeout:
            return None
        fields = decode_message(data)
        if self.verbose:
            print(f"[RECV] <- {addr} :: {fields}")
        # ACK handling
        if fields.get("message_type") == "ACK":
            self.reliable.handle_incoming_ack(fields)
            return None
        return fields, addr

    # -----------------------------
    # Host flow
    # -----------------------------

    def run_host(self):
        self._log("[HOST] Waiting for HANDSHAKE_REQUEST or SPECTATOR_REQUEST...")

        while True:
            msg = self.recv_message()
            if msg is None:
                continue
            fields, addr = msg
            mtype = fields.get("message_type")

            if mtype == "HANDSHAKE_REQUEST":
                self._log(f"[HOST] HANDSHAKE_REQUEST from {addr}")
                seq_num = int(fields.get("sequence_number", "0"))
                if seq_num:
                    self._send_ack(seq_num, addr)
                self.peer_addr = addr
                self.handle_handshake_as_host()
                break
            elif mtype == "SPECTATOR_REQUEST":
                self._log(f"[HOST] Spectator joined from {addr} (spectator support limited in this skeleton)")
                # In a full impl you would track spectator addrs and broadcast all battle + chat msgs to them.
            else:
                self._log(f"[HOST] Ignoring unexpected message: {fields}")

        # After handshake and battle setup:
        if self.battle_state:
            self.run_battle_loop(is_host=True)

    def handle_handshake_as_host(self):
        # 1) Send HANDSHAKE_RESPONSE with seed
        seed = random.randint(1, 2**31 - 1)
        self.seed = seed
        seq = self.reliable.next_seq()
        resp = {
            "message_type": "HANDSHAKE_RESPONSE",
            "seed": str(seed),
            "sequence_number": str(seq),
        }
        self.reliable.send(resp, self.peer_addr, reliable=True)

        # 2) Receive BATTLE_SETUP from joiner
        self._log("[HOST] Waiting for Joiner's BATTLE_SETUP...")
        joiner_setup = None
        while joiner_setup is None:
            msg = self.recv_message()
            if msg is None:
                continue
            fields, addr = msg
            mtype = fields.get("message_type")
            if mtype == "BATTLE_SETUP":
                seq_num = int(fields.get("sequence_number", "0"))
                self._send_ack(seq_num, addr)
                joiner_setup = fields
            else:
                self._log(f"[HOST] Unexpected before battle setup: {fields}")

        opp_pokemon_name = joiner_setup.get("pokemon_name", "Unknown")
        self.opp_pokemon = self.pdb.get(opp_pokemon_name)

        # 3) Send our BATTLE_SETUP
        if not self.my_pokemon_name:
            self.my_pokemon_name = input("[HOST] Enter your Pokémon name: ").strip()
        self.my_pokemon = self.pdb.get(self.my_pokemon_name)

        battle_setup = {
            "message_type": "BATTLE_SETUP",
            "communication_mode": DEFAULT_COMM_MODE,
            "pokemon_name": self.my_pokemon.name,
            "stat_boosts": json.dumps(
                {"special_attack_uses": 3, "special_defense_uses": 3}
            ),
            "sequence_number": str(self.reliable.next_seq()),
        }
        self.reliable.send(battle_setup, self.peer_addr, reliable=True)

        self._log(f"[HOST] Battle setup complete: You={self.my_pokemon.name}, Opp={self.opp_pokemon.name}")

        self.battle_state = BattleState(
            my_pokemon=self.my_pokemon,
            opp_pokemon=self.opp_pokemon,
            sp_atk_uses=3,
            sp_def_uses=3,
            i_am_host=True,
        )

    # -----------------------------
    # Joiner flow
    # -----------------------------

    def run_joiner(self):
        if not self.peer_ip or not self.peer_port:
            raise ValueError("Joiner requires peer_ip and peer_port")

        dest = (self.peer_ip, self.peer_port)
        self.peer_addr = dest

        # 1) Send HANDSHAKE_REQUEST
        seq = self.reliable.next_seq()
        req = {
            "message_type": "HANDSHAKE_REQUEST",
            "sequence_number": str(seq),
        }
        self.reliable.send(req, dest, reliable=True)
        self._log("[JOINER] HANDSHAKE_REQUEST sent, waiting for HANDSHAKE_RESPONSE...")

        # 2) Wait for HANDSHAKE_RESPONSE
        resp_fields = None
        while resp_fields is None:
            msg = self.recv_message()
            if msg is None:
                continue
            fields, addr = msg
            if addr != dest:
                continue
            if fields.get("message_type") == "HANDSHAKE_RESPONSE":
                seq_num = int(fields.get("sequence_number", "0"))
                self._send_ack(seq_num, addr)
                resp_fields = fields

        self.seed = int(resp_fields["seed"])
        random.seed(self.seed)
        self._log(f"[JOINER] Got HANDSHAKE_RESPONSE, seed={self.seed}")

        # 3) Send BATTLE_SETUP
        if not self.my_pokemon_name:
            self.my_pokemon_name = input("[JOINER] Enter your Pokémon name: ").strip()
        self.my_pokemon = self.pdb.get(self.my_pokemon_name)

        battle_setup = {
            "message_type": "BATTLE_SETUP",
            "communication_mode": DEFAULT_COMM_MODE,
            "pokemon_name": self.my_pokemon.name,
            "stat_boosts": json.dumps(
                {"special_attack_uses": 3, "special_defense_uses": 3}
            ),
            "sequence_number": str(self.reliable.next_seq()),
        }
        self.reliable.send(battle_setup, dest, reliable=True)

        # 4) Receive host's BATTLE_SETUP
        self._log("[JOINER] Waiting for Host's BATTLE_SETUP...")
        host_setup = None
        while host_setup is None:
            msg = self.recv_message()
            if msg is None:
                continue
            fields, addr = msg
            if addr != dest:
                continue
            if fields.get("message_type") == "BATTLE_SETUP":
                seq_num = int(fields.get("sequence_number", "0"))
                self._send_ack(seq_num, addr)
                host_setup = fields

        opp_name = host_setup.get("pokemon_name", "Unknown")
        self.opp_pokemon = self.pdb.get(opp_name)
        self._log(f"[JOINER] Battle setup complete: You={self.my_pokemon.name}, Opp={self.opp_pokemon.name}")

        self.battle_state = BattleState(
            my_pokemon=self.my_pokemon,
            opp_pokemon=self.opp_pokemon,
            sp_atk_uses=3,
            sp_def_uses=3,
            i_am_host=False,
        )

        # 5) Run battle loop
        self.run_battle_loop(is_host=False)

    # -----------------------------
    # Spectator flow (minimal)
    # -----------------------------

    def run_spectator(self):
        if not self.peer_ip or not self.peer_port:
            raise ValueError("Spectator requires peer_ip and peer_port")
        dest = (self.peer_ip, self.peer_port)
        self.peer_addr = dest

        seq = self.reliable.next_seq()
        req = {
            "message_type": "SPECTATOR_REQUEST",
            "sequence_number": str(seq),
        }
        self.reliable.send(req, dest, reliable=True)
        self._log("[SPECTATOR] SPECTATOR_REQUEST sent. Now passively printing battle/chat messages...")

        while True:
            msg = self.recv_message()
            if msg is None:
                continue
            fields, addr = msg
            mtype = fields.get("message_type")
            if mtype in {
                "HANDSHAKE_REQUEST",
                "HANDSHAKE_RESPONSE",
                "BATTLE_SETUP",
                "ATTACK_ANNOUNCE",
                "DEFENSE_ANNOUNCE",
                "CALCULATION_REPORT",
                "CALCULATION_CONFIRM",
                "RESOLUTION_REQUEST",
                "GAME_OVER",
            }:
                print(f"[SPECTATE] {mtype} :: {fields}")
            elif mtype == "CHAT_MESSAGE":
                self._display_chat(fields)
            # ACK handling is already done in recv_message / ReliableUDP

    # -----------------------------
    # Battle loop core
    # -----------------------------

    def run_battle_loop(self, is_host: bool):
        assert self.battle_state is not None
        bs = self.battle_state
        self._log("[BATTLE] Starting battle loop. Host moves first.")

        while True:
            print(f"\n[BATTLE] Your HP={bs.my_hp}, Opp HP={bs.opp_hp}")
            if bs.my_hp <= 0 or bs.opp_hp <= 0:
                break

            if bs.my_turn:
                # My turn: ATTACK_ANNOUNCE => DEFENSE_ANNOUNCE => CALCULATION_REPORT => CONFIRM/RESOLVE
                self._perform_attacking_turn()
            else:
                # Opponent's turn: wait for ATTACK_ANNOUNCE, send DEFENSE_ANNOUNCE, then CALCULATION_REPORT, then CONFIRM/RESOLVE
                self._perform_defending_turn()

        self._log("[BATTLE] Battle finished.")

    def _prompt_move(self) -> Tuple[str, str, str, int, bool, bool]:
        """
        Very minimal UI: ask user for move.
        - move_name
        - move_type
        - damage_category
        - base_power
        - use_sp_attack_boost?
        - use_sp_defense_boost?
        """
        move_name = input("Enter move name: ").strip() or "Tackle"
        move_type = input("Enter move type (e.g. fire, water, normal): ").strip().lower() or "normal"
        cat = input("Damage category (physical/special) [special]: ").strip().lower() or "special"
        base_power_str = input("Base power [60]: ").strip() or "60"
        try:
            base_power = int(base_power_str)
        except ValueError:
            base_power = 60

        use_sp_atk = input("Use special attack boost? (y/N): ").strip().lower() == "y"
        use_sp_def = input("Use special defense boost? (y/N): ").strip().lower() == "y"

        return move_name, move_type, cat, base_power, use_sp_atk, use_sp_def

    def _perform_attacking_turn(self):
        assert self.battle_state is not None
        bs = self.battle_state
        dest = self.peer_addr

        print("[TURN] It is your turn to attack.")
        cmd = input("Type 'a' to attack, 'c' to send chat, 's' to send sticker: ").strip().lower()
        if cmd == "c":
            self._send_chat_interactive()
            # Still attack afterwards
        elif cmd == "s":
            self._send_sticker_interactive()
            # Still attack afterwards

        move_name, move_type, cat, base_power, use_sp_atk, use_sp_def = self._prompt_move()

        # 1) ATTACK_ANNOUNCE
        seq = self.reliable.next_seq()
        attack_msg = {
            "message_type": "ATTACK_ANNOUNCE",
            "move_name": move_name,
            "move_type": move_type,
            "damage_category": cat,
            "base_power": str(base_power),
            "use_sp_attack_boost": str(use_sp_atk).lower(),
            "use_sp_defense_boost": str(use_sp_def).lower(),
            "sequence_number": str(seq),
        }
        self.reliable.send(attack_msg, dest, reliable=True)

        # 2) Wait for DEFENSE_ANNOUNCE
        print("[TURN] Waiting for DEFENSE_ANNOUNCE...")
        defense_received = None
        while defense_received is None:
            msg = self.recv_message()
            if msg is None:
                continue
            fields, addr = msg
            mtype = fields.get("message_type")
            if mtype == "DEFENSE_ANNOUNCE":
                seq_num = int(fields.get("sequence_number", "0"))
                self._send_ack(seq_num, addr)
                defense_received = fields
            elif mtype == "CHAT_MESSAGE":
                self._display_chat(fields)
            else:
                print(f"[TURN] Ignoring message while waiting DEFENSE: {fields}")

        # 3) Both players compute damage + send CALCULATION_REPORT
        damage, opp_hp_after, my_hp_same, sp_atk_left, sp_def_left, status_msg, defender_hp = calculate_damage(
            attacker=bs.my_pokemon,
            defender=bs.opp_pokemon,
            move_name=move_name,
            move_type=move_type,
            damage_category=cat,
            base_power=base_power,
            use_sp_attack_boost=use_sp_atk,
            use_sp_defense_boost=use_sp_def,
            sp_atk_uses_left=bs.my_sp_atk_uses,
            sp_def_uses_left=bs.my_sp_def_uses,
        )

        bs.my_sp_atk_uses = sp_atk_left
        bs.my_sp_def_uses = sp_def_left
        bs.opp_hp = opp_hp_after

        seq = self.reliable.next_seq()
        calc_report = {
            "message_type": "CALCULATION_REPORT",
            "attacker": bs.my_pokemon.name,
            "move_used": move_name,
            "remaining_health": str(my_hp_same),
            "damage_dealt": str(damage),
            "defender_hp_remaining": str(opp_hp_after),
            "status_message": status_msg,
            "sequence_number": str(seq),
        }
        self.reliable.send(calc_report, dest, reliable=True)

        # 4) Receive opponent's CALCULATION_REPORT and confirm or request resolution
        print("[TURN] Waiting for opponent CALCULATION_REPORT...")
        opp_report = None
        while opp_report is None:
            msg = self.recv_message()
            if msg is None:
                continue
            fields, addr = msg
            mtype = fields.get("message_type")
            if mtype == "CALCULATION_REPORT":
                seq_num = int(fields.get("sequence_number", "0"))
                self._send_ack(seq_num, addr)
                opp_report = fields
            elif mtype == "CHAT_MESSAGE":
                self._display_chat(fields)
            else:
                print(f"[TURN] Ignoring message while waiting CALC_REPORT: {fields}")

        # Check discrepancy
        discrepancy = (
            opp_report.get("damage_dealt") != str(damage)
            or opp_report.get("defender_hp_remaining") != str(opp_hp_after)
        )

        if not discrepancy:
            print(f"[TURN] CALCULATION_REPORT matches. {status_msg}")
            # Send CALCULATION_CONFIRM
            seq = self.reliable.next_seq()
            confirm = {
                "message_type": "CALCULATION_CONFIRM",
                "sequence_number": str(seq),
            }
            self.reliable.send(confirm, dest, reliable=True)
        else:
            print("[TURN] Discrepancy detected; sending RESOLUTION_REQUEST.")
            seq = self.reliable.next_seq()
            resolution_req = {
                "message_type": "RESOLUTION_REQUEST",
                "attacker": bs.my_pokemon.name,
                "move_used": move_name,
                "move_type": move_type,
                "damage_category": cat,
                "base_power": str(base_power),
                "use_sp_attack_boost": str(use_sp_atk).lower(),
                "use_sp_defense_boost": str(use_sp_def).lower(),
                "damage_dealt": str(damage),
                "defender_hp_remaining": str(opp_hp_after),
                "sequence_number": str(seq),
            }
            self.reliable.send(resolution_req, dest, reliable=True)

            # Wait for CALCULATION_CONFIRM before proceeding
            print("[TURN] Waiting for CALCULATION_CONFIRM after RESOLUTION_REQUEST...")
            while True:
                msg = self.recv_message()
                if msg is None:
                    continue
                fields, addr = msg
                mtype = fields.get("message_type")
                if mtype == "CALCULATION_CONFIRM":
                    seq_num = int(fields.get("sequence_number", "0"))
                    self._send_ack(seq_num, addr)
                    break
                elif mtype == "CHAT_MESSAGE":
                    self._display_chat(fields)
                else:
                    print(f"[TURN] Ignoring message while awaiting CALCULATION_CONFIRM: {fields}")

        # Check for GAME_OVER condition
        if bs.opp_hp <= 0:
            print("[TURN] Opponent fainted! Sending GAME_OVER.")
            seq = self.reliable.next_seq()
            game_over = {
                "message_type": "GAME_OVER",
                "winner": bs.my_pokemon.name,
                "loser": bs.opp_pokemon.name,
                "sequence_number": str(seq),
            }
            self.reliable.send(game_over, dest, reliable=True)
            return

        # Reverse turn
        bs.my_turn = False

    def _perform_defending_turn(self):
        assert self.battle_state is not None
        bs = self.battle_state
        dest = self.peer_addr

        print("[TURN] Waiting for opponent ATTACK_ANNOUNCE...")
        attack_msg = None
        while attack_msg is None:
            msg = self.recv_message()
            if msg is None:
                continue
            fields, addr = msg
            mtype = fields.get("message_type")
            if mtype == "ATTACK_ANNOUNCE":
                seq_num = int(fields.get("sequence_number", "0"))
                self._send_ack(seq_num, addr)
                attack_msg = fields
            elif mtype == "CHAT_MESSAGE":
                self._display_chat(fields)
            else:
                print(f"[TURN] Ignoring message while waiting ATTACK_ANNOUNCE: {fields}")

        move_name = attack_msg.get("move_name", "Unknown")

        # 1) Send DEFENSE_ANNOUNCE
        seq = self.reliable.next_seq()
        defense_msg = {
            "message_type": "DEFENSE_ANNOUNCE",
            "sequence_number": str(seq),
        }
        self.reliable.send(defense_msg, dest, reliable=True)

        # 2) We compute damage as defender using the announced parameters (no prompts)
        move_type = attack_msg.get("move_type", "normal")
        cat = attack_msg.get("damage_category", "special").lower()
        try:
            base_power = int(attack_msg.get("base_power", "60"))
        except ValueError:
            base_power = 60
        use_sp_atk = attack_msg.get("use_sp_attack_boost", "false").lower() == "true"
        use_sp_def = attack_msg.get("use_sp_defense_boost", "false").lower() == "true"

        # We are the defender; attacker is opponent Pokémon
        damage, my_hp_after, opp_hp_same, sp_atk_left, sp_def_left, status_msg, defender_hp = calculate_damage(
            attacker=bs.opp_pokemon,
            defender=bs.my_pokemon,
            move_name=move_name,
            move_type=move_type,
            damage_category=cat,
            base_power=base_power,
            use_sp_attack_boost=use_sp_atk,
            use_sp_defense_boost=use_sp_def,
            sp_atk_uses_left=bs.opp_sp_atk_uses,
            sp_def_uses_left=bs.opp_sp_def_uses,
        )
        bs.my_hp = my_hp_after
        bs.opp_sp_atk_uses = sp_atk_left
        bs.opp_sp_def_uses = sp_def_left

        # 3) Send our CALCULATION_REPORT
        seq = self.reliable.next_seq()
        calc_report = {
            "message_type": "CALCULATION_REPORT",
            "attacker": bs.opp_pokemon.name,
            "move_used": move_name,
            "remaining_health": str(opp_hp_same),
            "damage_dealt": str(damage),
            "defender_hp_remaining": str(my_hp_after),
            "status_message": status_msg,
            "sequence_number": str(seq),
        }
        self.reliable.send(calc_report, dest, reliable=True)

        # 4) Receive opponent CALCULATION_REPORT and decide confirm or resolution
        print("[TURN] Waiting for opponent CALCULATION_REPORT...")
        opp_report = None
        while opp_report is None:
            msg = self.recv_message()
            if msg is None:
                continue
            fields, addr = msg
            mtype = fields.get("message_type")
            if mtype == "CALCULATION_REPORT":
                seq_num = int(fields.get("sequence_number", "0"))
                self._send_ack(seq_num, addr)
                opp_report = fields
            elif mtype == "CHAT_MESSAGE":
                self._display_chat(fields)
            else:
                print(f"[TURN] Ignoring message while waiting CALC_REPORT: {fields}")

        discrepancy = (
            opp_report.get("damage_dealt") != str(damage)
            or opp_report.get("defender_hp_remaining") != str(my_hp_after)
        )
        if not discrepancy:
            print(f"[TURN] CALCULATION_REPORT matches. {status_msg}")
            # Wait for CALCULATION_CONFIRM or RESOLUTION_REQUEST from attacker
            print("[TURN] Waiting for CALCULATION_CONFIRM or RESOLUTION_REQUEST...")
            while True:
                msg = self.recv_message()
                if msg is None:
                    continue
                fields, addr = msg
                mtype = fields.get("message_type")
                if mtype == "CALCULATION_CONFIRM":
                    seq_num = int(fields.get("sequence_number", "0"))
                    self._send_ack(seq_num, addr)
                    break
                elif mtype == "RESOLUTION_REQUEST":
                    seq_num = int(fields.get("sequence_number", "0"))
                    self._send_ack(seq_num, addr)
                    print("[TURN] Attacker sent RESOLUTION_REQUEST despite match; terminate for safety.")
                    return
                elif mtype == "CHAT_MESSAGE":
                    self._display_chat(fields)
        else:
            print("[TURN] Discrepancy detected; waiting for RESOLUTION_REQUEST...")
            while True:
                msg = self.recv_message()
                if msg is None:
                    continue
                fields, addr = msg
                mtype = fields.get("message_type")
                if mtype == "RESOLUTION_REQUEST":
                    seq_num = int(fields.get("sequence_number", "0"))
                    self._send_ack(seq_num, addr)
                    print("[TURN] RESOLUTION_REQUEST received; reconciling and confirming.")

                    # Adopt attacker-provided authoritative results
                    bs.my_hp = int(fields.get("defender_hp_remaining", bs.my_hp))
                    bs.opp_hp = int(fields.get("remaining_health", bs.opp_hp))

                    # Send CALCULATION_CONFIRM to finalize resolution
                    confirm_seq = self.reliable.next_seq()
                    confirm = {
                        "message_type": "CALCULATION_CONFIRM",
                        "sequence_number": str(confirm_seq),
                        "acknowledged_damage": fields.get("damage_dealt", ""),
                        "acknowledged_defender_hp": fields.get("defender_hp_remaining", ""),
                    }
                    self.reliable.send(confirm, dest, reliable=True)
                    break
                elif mtype == "CHAT_MESSAGE":
                    self._display_chat(fields)

        # Check for GAME_OVER
        if bs.my_hp <= 0:
            print("[TURN] Your Pokémon fainted. Waiting for GAME_OVER...")
            while True:
                msg = self.recv_message()
                if msg is None:
                    continue
                fields, addr = msg
                if fields.get("message_type") == "GAME_OVER":
                    seq_num = int(fields.get("sequence_number", "0"))
                    self._send_ack(seq_num, addr)
                    print(f"[BATTLE] GAME_OVER received. Winner={fields.get('winner')}")
                    return
                elif fields.get("message_type") == "CHAT_MESSAGE":
                    self._display_chat(fields)
        else:
            bs.my_turn = True  # Reverse turn

    # -----------------------------
    # Chat
    # -----------------------------

    def _send_chat_interactive(self):
        if not self.peer_addr:
            print("[CHAT] No peer connected.")
            return
        text = input("Chat message: ").strip()
        if not text:
            return
        seq = self.reliable.next_seq()
        msg = {
            "message_type": "CHAT_MESSAGE",
            "sender_name": self.my_pokemon_name or "Player",
            "content_type": "TEXT",
            "message_text": text,
            "sequence_number": str(seq),
        }
        self.reliable.send(msg, self.peer_addr, reliable=True)

    def _send_sticker_interactive(self):
        if not self.peer_addr:
            print("[CHAT] No peer connected.")
            return
        path = input("Path to sticker image (will be Base64 encoded): ").strip()
        if not os.path.exists(path):
            print("[CHAT] File not found.")
            return
        with open(path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode("ascii")
        seq = self.reliable.next_seq()
        msg = {
            "message_type": "CHAT_MESSAGE",
            "sender_name": self.my_pokemon_name or "Player",
            "content_type": "STICKER",
            "sticker_data": b64,
            "sequence_number": str(seq),
        }
        self.reliable.send(msg, self.peer_addr, reliable=True)

    def _display_chat(self, fields: Dict[str, str]):
        sender = fields.get("sender_name", "Unknown")
        ctype = fields.get("content_type", "TEXT")
        if ctype == "TEXT":
            print(f"[CHAT] {sender}: {fields.get('message_text','')}")
        elif ctype == "STICKER":
            print(f"[CHAT] {sender} sent a STICKER (Base64 length={len(fields.get('sticker_data',''))})")
        else:
            print(f"[CHAT] Unknown chat content: {fields}")


# -----------------------------
# Main CLI entry
# -----------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(description="LSNP / PokeProtocol over UDP (reference implementation).")
    parser.add_argument("--role", choices=["HOST", "JOINER", "SPECTATOR"], required=True)
    parser.add_argument("--local-port", type=int, default=9000, help="Local UDP port to bind to.")
    parser.add_argument("--peer-ip", type=str, help="Peer IP (required for JOINER/SPECTATOR).")
    parser.add_argument("--peer-port", type=int, help="Peer port (required for JOINER/SPECTATOR).")
    parser.add_argument("--pokemon", type=str, help="Your Pokémon name.")
    parser.add_argument("--verbose", action="store_true", help="Verbose mode (print all protocol messages).")
    parser.add_argument("--pokemon-csv", type=str, default="pokemon.csv", help="Path to pokemon.csv.")

    args = parser.parse_args()

    node = LSNPNode(
        role=args.role,
        local_port=args.local_port,
        peer_ip=args.peer_ip,
        peer_port=args.peer_port,
        pokemon_name=args.pokemon,
        verbose=args.verbose,
        pokemon_csv_path=args.pokemon_csv,
    )
    try:
        node.run()
    finally:
        node.reliable.stop()
        node.sock.close()


if __name__ == "__main__":
    main()
