# main.py
from __future__ import annotations
import argparse
import secrets
import time
import threading
from typing import Optional, Tuple

from protocol import WireMessage, MsgType, get_int, require
from reliable_udp import ReliableUDP, ReliableConfig
from battle import Battle, Pokemon, StatBoosts, Move, compute_damage, CalcResult, State
from util import b64_encode_file, b64_save

Addr = Tuple[str, int]

def args():
    p = argparse.ArgumentParser()
    p.add_argument("--bind-ip", default="0.0.0.0")
    p.add_argument("--bind-port", type=int, required=True)
    p.add_argument("--role", choices=["host", "join", "spectator"], required=True)
    p.add_argument("--peer-ip", default=None)
    p.add_argument("--peer-port", type=int, default=None)
    p.add_argument("--mode", choices=["P2P", "BROADCAST"], default="P2P")
    p.add_argument("--loss", type=float, default=0.0)
    p.add_argument("--verbose", action="store_true")
    return p.parse_args()

def default_pokemon(name: str) -> Pokemon:
    # Replace with CSV stats in your project if required.
    return Pokemon(
        pokemon_name=name,
        hp=100,
        attack=20,
        physical_defense=14,
        special_attack=22,
        special_defense=12
    )

class App:
    def __init__(self, rudp: ReliableUDP, role: str, peer: Optional[Addr], mode: str):
        self.rudp = rudp
        self.role = role
        self.peer: Optional[Addr] = peer
        self.mode = mode

        self.seed: Optional[int] = None
        self.running = True

        self.me = default_pokemon("PlayerPokemon")
        self.them = default_pokemon("OpponentPokemon")

        self.boosts = StatBoosts(special_attack_uses=5, special_defense_uses=5)
        self.battle: Optional[Battle] = None

        self.spectators: set[Addr] = set()

        self._local_name = self.me.pokemon_name

    def seq(self) -> int:
        return self.rudp.next_seq()

    def send(self, addr: Addr, mt: MsgType, fields: dict):
        fields = dict(fields)
        if mt != MsgType.ACK:
            fields["sequence_number"] = str(self.seq())
        self.rudp.send(addr, WireMessage(mt, fields))

    def start(self):
        # Input thread (chat + moves)
        threading.Thread(target=self.input_loop, daemon=True).start()

        # Start handshake depending on role
        if self.role == "host":
            print("[HOST] Waiting for HANDSHAKE_REQUEST / SPECTATOR_REQUEST...")
        elif self.role == "join":
            if not self.peer:
                raise SystemExit("join requires --peer-ip/--peer-port")
            self.send(self.peer, MsgType.HANDSHAKE_REQUEST, {})
            print("[JOIN] Sent HANDSHAKE_REQUEST, waiting for HANDSHAKE_RESPONSE...")
        else:
            if not self.peer:
                raise SystemExit("spectator requires --peer-ip/--peer-port")
            self.send(self.peer, MsgType.SPECTATOR_REQUEST, {})
            print("[SPEC] Sent SPECTATOR_REQUEST...")

        while self.running:
            pkt = self.rudp.poll()
            if self.rudp.connection_lost_event.is_set():
                print("[ERROR] Connection lost - max retries exceeded; battle terminated.")
                self.running = False
                if self.battle:
                    self.battle.state = State.GAME_OVER
                break
            if pkt:
                addr, msg = pkt
                self.on_msg(addr, msg)
            time.sleep(0.01)

    def input_loop(self):
        print("Commands:")
        print("  /move <name> [physical|special]")
        print("  /chat <text>")
        print("  /sticker <file>")
        print("  /quit")
        while self.running:
            try:
                s = input().strip()
            except EOFError:
                return
            if not self.running or self.rudp.connection_lost_event.is_set():
                return
            if not s:
                continue
            if s == "/quit":
                self.running = False
                return

            if s.startswith("/chat "):
                if not self.peer:
                    print("[ERR] Not connected yet.")
                    continue
                text = s[len("/chat "):]
                self.send(self.peer, MsgType.CHAT_MESSAGE, {
                    "sender_name": self._local_name,
                    "content_type": "TEXT",
                    "message_text": text
                })
                continue

            if s.startswith("/sticker "):
                if not self.peer:
                    print("[ERR] Not connected yet.")
                    continue
                path = s[len("/sticker "):].strip()
                try:
                    b64 = b64_encode_file(path)
                except Exception as e:
                    print(f"[ERR] sticker read failed: {e}")
                    continue
                self.send(self.peer, MsgType.CHAT_MESSAGE, {
                    "sender_name": self._local_name,
                    "content_type": "STICKER",
                    "sticker_data": b64
                })
                continue

            if s.startswith("/move "):
                if not self.peer or not self.battle:
                    print("[ERR] Battle not ready yet.")
                    continue
                if self.role == "spectator":
                    print("[ERR] Spectators cannot send battle messages.")
                    continue
                if self.battle.state != State.WAITING_FOR_MOVE or not self.battle.my_turn:
                    print("[ERR] Not your turn (or not in WAITING_FOR_MOVE).")
                    continue

                parts = s.split()
                if len(parts) < 2:
                    print("Usage: /move <name> [physical|special]")
                    continue
                move_name = parts[1]
                cat = (parts[2].upper() if len(parts) >= 3 else "PHYSICAL")
                if cat not in ("PHYSICAL", "SPECIAL"):
                    cat = "PHYSICAL"

                mv = Move(move_name=move_name, damage_category=cat)
                self.battle.pending_attack = mv
                self.battle.state = State.WAITING_FOR_DEFENSE

                # Step 1: ATTACK_ANNOUNCE
                self.send(self.peer, MsgType.ATTACK_ANNOUNCE, {
                    "move_name": mv.move_name
                })
                print(f"[YOU] ATTACK_ANNOUNCE: {mv.move_name} ({mv.damage_category})")
                continue

            print("[ERR] Unknown command.")

    def start_battle_if_ready(self, host_goes_first: bool):
        if self.battle is None and self.seed is not None:
            # Host designated to go first (RFC)
            self.battle = Battle(self.me, self.them, self.boosts, host_goes_first=host_goes_first)
            self.battle.state = State.WAITING_FOR_MOVE
            print("[OK] Battle ready. Host goes first. State=WAITING_FOR_MOVE")

    def on_msg(self, addr: Addr, msg: WireMessage):
        # Host learns peer automatically
        if self.role == "host" and self.peer is None:
            self.peer = addr

        # Spectator joins host
        if msg.message_type == MsgType.SPECTATOR_REQUEST and self.role == "host":
            self.spectators.add(addr)
            print(f"[HOST] spectator connected: {addr}")
            # no extra response defined in RFC; reuse handshake response for seed sync
            if self.seed is None:
                self.seed = secrets.randbelow(1_000_000_000)
            self.send(addr, MsgType.HANDSHAKE_RESPONSE, {"seed": str(self.seed)})
            return

        # Join handshake
        if msg.message_type == MsgType.HANDSHAKE_REQUEST and self.role == "host":
            if self.seed is None:
                self.seed = secrets.randbelow(1_000_000_000)
            self.send(addr, MsgType.HANDSHAKE_RESPONSE, {"seed": str(self.seed)})
            print(f"[HOST] Sent HANDSHAKE_RESPONSE seed={self.seed}")

            # After handshake, exchange BATTLE_SETUP (RFC 5.2)
            self.send(addr, MsgType.BATTLE_SETUP, {
                "communication_mode": self.mode,
                "pokemon_name": self.me.pokemon_name,
                "stat_boosts": f'{{ "special_attack_uses": {self.boosts.special_attack_uses}, "special_defense_uses": {self.boosts.special_defense_uses} }}',
            })
            print("[HOST] Sent BATTLE_SETUP")
            self.start_battle_if_ready(host_goes_first=True)
            return

        if msg.message_type == MsgType.HANDSHAKE_RESPONSE:
            self.seed = get_int(msg.fields, "seed", 0)
            print(f"[OK] HANDSHAKE_RESPONSE seed={self.seed}")

            # Send BATTLE_SETUP (RFC)
            assert self.peer
            self.send(self.peer, MsgType.BATTLE_SETUP, {
                "communication_mode": self.mode,
                "pokemon_name": self.me.pokemon_name,
                "stat_boosts": f'{{ "special_attack_uses": {self.boosts.special_attack_uses}, "special_defense_uses": {self.boosts.special_defense_uses} }}',
            })
            print("[JOIN/SPEC] Sent BATTLE_SETUP")
            # Host goes first, so joiner starts with my_turn=False
            if self.role == "join":
                self.start_battle_if_ready(host_goes_first=False)
            return

        if msg.message_type == MsgType.BATTLE_SETUP:
            # Record opponent pokemon name
            opp_name = msg.fields.get("pokemon_name", "OpponentPokemon")
            self.them.pokemon_name = opp_name
            print(f"[OK] Received BATTLE_SETUP from {addr} pokemon_name={opp_name} mode={msg.fields.get('communication_mode')}")

            # If host already has seed, battle can start; if joiner, seed already set from handshake response
            if self.role == "host":
                self.start_battle_if_ready(host_goes_first=True)
            elif self.role == "join":
                self.start_battle_if_ready(host_goes_first=False)
            return

        # Chat overlay (any time, any role) RFC 4.11 / 5.2
        if msg.message_type == MsgType.CHAT_MESSAGE:
            sender = msg.fields.get("sender_name", "Unknown")
            ctype = msg.fields.get("content_type", "TEXT")
            if ctype == "TEXT":
                print(f"[CHAT] {sender}: {msg.fields.get('message_text','')}")
            elif ctype == "STICKER":
                b64 = msg.fields.get("sticker_data", "")
                out = f"recv_sticker_{int(time.time())}.bin"
                try:
                    b64_save(b64, out)
                    print(f"[CHAT] {sender} sent a sticker -> saved {out}")
                except Exception as e:
                    print(f"[ERR] sticker decode failed: {e}")
            return

        # Spectators cannot influence battle state; just print battle messages if you want
        if self.role == "spectator":
            print(f"[SPEC] {msg.message_type.value} received.")
            return

        if not self.battle or not self.peer:
            return

        # Step 1: attacker sends ATTACK_ANNOUNCE; defender replies DEFENSE_ANNOUNCE (RFC 5.2)
        if msg.message_type == MsgType.ATTACK_ANNOUNCE:
            move_name = require(msg.fields, "move_name")
            print(f"[OPP] ATTACK_ANNOUNCE: {move_name}")

            # Defender responds Step 2
            self.send(self.peer, MsgType.DEFENSE_ANNOUNCE, {})
            print("[YOU] DEFENSE_ANNOUNCE sent")

            # Both transition to PROCESSING_TURN after DEFENSE_ANNOUNCE is received by attacker,
            # but defender can prepare now.
            self.battle.pending_attack = Move(move_name=move_name, damage_category="PHYSICAL")  # if you have move DB, set category correctly
            # Defender stays waiting; attacker will move to processing when they receive DEFENSE_ANNOUNCE.
            return

        if msg.message_type == MsgType.DEFENSE_ANNOUNCE:
            print("[OPP] DEFENSE_ANNOUNCE received -> PROCESSING_TURN")
            self.battle.state = State.PROCESSING_TURN

            # Compute and send CALCULATION_REPORT (RFC 5.2 step 3)
            self.send_calc_report(attacker_is_me=True)
            return

        # Step 3: exchange CALCULATION_REPORT
        if msg.message_type == MsgType.CALCULATION_REPORT:
            # Compare with local calc; if match -> CALCULATION_CONFIRM and reverse turn
            attacker = require(msg.fields, "attacker")
            move_used = require(msg.fields, "move_used")
            damage_dealt = get_int(msg.fields, "damage_dealt", -1)
            defender_hp_remaining = get_int(msg.fields, "defender_hp_remaining", -1)
            status_message = msg.fields.get("status_message", "")

            print(f"[OPP] CALCULATION_REPORT: attacker={attacker} move={move_used} dmg={damage_dealt} def_hp={defender_hp_remaining}")
            if self.battle.last_calc is None:
                # If we haven't computed yet (defender side), compute now and send our report.
                self.battle.state = State.PROCESSING_TURN
                self.send_calc_report(attacker_is_me=False)

            local = self.battle.last_calc
            if local and local.damage_dealt == damage_dealt and local.defender_hp_remaining == defender_hp_remaining and local.move_used == move_used:
                # Step 4 success: confirm
                self.send(self.peer, MsgType.CALCULATION_CONFIRM, {})
                print("[YOU] CALCULATION_CONFIRM sent -> reversing turn")
                self.apply_local(local)
                self.battle.flip_turn()
                self.battle.state = State.WAITING_FOR_MOVE
            else:
                # discrepancy: send RESOLUTION_REQUEST with our calculated values (RFC 4.9, 5.2)
                if local:
                    self.send(self.peer, MsgType.RESOLUTION_REQUEST, {
                        "attacker": local.attacker,
                        "move_used": local.move_used,
                        "damage_dealt": str(local.damage_dealt),
                        "defender_hp_remaining": str(local.defender_hp_remaining),
                    })
                    print("[WARN] Discrepancy -> RESOLUTION_REQUEST sent")
                else:
                    print("[WARN] Discrepancy but no local calc; terminating recommended")
                    self.running = False
            return

        if msg.message_type == MsgType.CALCULATION_CONFIRM:
            # Peer confirms your report matched theirs; reverse turn and continue
            print("[OK] CALCULATION_CONFIRM received -> reversing turn")
            if self.battle.last_calc:
                self.apply_local(self.battle.last_calc)
            self.battle.flip_turn()
            self.battle.state = State.WAITING_FOR_MOVE
            return

        if msg.message_type == MsgType.RESOLUTION_REQUEST:
            # RFC 5.2: re-evaluate based on received data; if agree -> ACK + update state to match; else terminate
            attacker = require(msg.fields, "attacker")
            move_used = require(msg.fields, "move_used")
            dmg = get_int(msg.fields, "damage_dealt", -1)
            def_hp = get_int(msg.fields, "defender_hp_remaining", -1)

            print(f"[OPP] RESOLUTION_REQUEST: attacker={attacker} move={move_used} dmg={dmg} def_hp={def_hp}")

            local = self.battle.last_calc
            if local and local.move_used == move_used and local.damage_dealt == dmg and local.defender_hp_remaining == def_hp:
                print("[OK] Resolution agrees -> updating state and continuing")
                self.apply_local(local)
                self.battle.flip_turn()
                self.battle.state = State.WAITING_FOR_MOVE
            else:
                print("[FATAL] Fundamental mismatch remains -> terminate battle (RFC says SHOULD terminate)")
                self.running = False
            return

        if msg.message_type == MsgType.GAME_OVER:
            winner = msg.fields.get("winner", "?")
            loser = msg.fields.get("loser", "?")
            print(f"[GAME_OVER] winner={winner} loser={loser}")
            self.running = False
            return

    def send_calc_report(self, attacker_is_me: bool):
        """
        RFC requires fields:
        attacker, move_used, remaining_health, damage_dealt, defender_hp_remaining, status_message
        """
        assert self.battle and self.peer and self.battle.pending_attack

        move = self.battle.pending_attack
        if attacker_is_me:
            attacker = self.me
            defender = self.them
        else:
            attacker = self.them
            defender = self.me

        dmg = compute_damage(attacker, defender, move)
        defender_hp_after = max(0, defender.hp - dmg)
        status = f"{attacker.pokemon_name} used {move.move_name}!"

        calc = CalcResult(
            attacker=attacker.pokemon_name,
            move_used=move.move_name,
            remaining_health=attacker.hp,
            damage_dealt=dmg,
            defender_hp_remaining=defender_hp_after,
            status_message=status
        )
        self.battle.last_calc = calc

        self.send(self.peer, MsgType.CALCULATION_REPORT, {
            "attacker": calc.attacker,
            "move_used": calc.move_used,
            "remaining_health": str(calc.remaining_health),
            "damage_dealt": str(calc.damage_dealt),
            "defender_hp_remaining": str(calc.defender_hp_remaining),
            "status_message": calc.status_message
        })
        print("[YOU] CALCULATION_REPORT sent")

    def apply_local(self, calc: CalcResult):
        """
        Apply the result locally and send GAME_OVER if opponent fainted (RFC 5.2 final state).
        """
        assert self.battle and self.peer

        # Determine who is defender from calc.attacker name
        if calc.attacker == self.me.pokemon_name:
            # you attacked -> opponent loses hp
            self.them.hp = max(0, self.them.hp - calc.damage_dealt)
        else:
            # opponent attacked -> you lose hp
            self.me.hp = max(0, self.me.hp - calc.damage_dealt)

        print(f"[HP] You={self.me.hp} Opp={self.them.hp}")

        if self.me.hp <= 0 or self.them.hp <= 0:
            winner = self.them.pokemon_name if self.me.hp <= 0 else self.me.pokemon_name
            loser = self.me.pokemon_name if self.me.hp <= 0 else self.them.pokemon_name
            self.send(self.peer, MsgType.GAME_OVER, {"winner": winner, "loser": loser})
            print(f"[YOU] GAME_OVER sent winner={winner} loser={loser}")
            self.running = False

def main():
    a = args()
    peer = (a.peer_ip, a.peer_port) if a.peer_ip and a.peer_port else None

    cfg = ReliableConfig(timeout_s=0.5, max_retries=3, loss_rate=a.loss, verbose=a.verbose)
    rudp = ReliableUDP(a.bind_ip, a.bind_port, cfg)

    try:
        app = App(rudp, a.role, peer, a.mode)
        app.start()
    finally:
        rudp.close()

if __name__ == "__main__":
    main()
