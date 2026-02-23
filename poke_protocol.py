import socket
import threading
import time
import json
import csv
import base64
import random
import os
import queue
import argparse
from dataclasses import dataclass
from typing import Dict, Tuple, Optional, Set

# =======================================================================
# CONFIGURATION & CONSTANTS
# =======================================================================
BUFFER_SIZE = 65535
ACK_TIMEOUT = 0.5
MAX_RETRIES = 3
DEFAULT_PORT = 9000

# =======================================================================
# DATA STRUCTURES
# =======================================================================
@dataclass
class PokemonStats:
    name: str
    hp: int
    max_hp: int
    attack: int
    defense: int
    sp_attack: int
    sp_defense: int
    speed: int
    type1: str
    type2: Optional[str]
    effectiveness: Dict[str, float] 

class PokemonDatabase:
    """Loads pokemon.csv and provides stats/effectiveness data."""
    def __init__(self, csv_path: str):
        self.data = {}
        self._load_csv(csv_path)

    def _load_csv(self, path: str):
        if not os.path.exists(path):
            # Fallback for testing if CSV is missing
            print(f" [WARN] {path} not found. Using Mock Database.")
            self._add_mock("Pikachu", "electric", 100, 55, 40, 50, 50)
            self._add_mock("Charmander", "fire", 100, 52, 43, 60, 50)
            return
            
        with open(path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                eff = {}
                for k, v in row.items():
                    if k.startswith('against_'):
                        try:
                            t_name = k.replace('against_', '')
                            try:
                                eff[t_name] = float(v)
                            except ValueError:
                                eff[t_name] = 1.0
                        except ValueError:
                            continue
                            
                p = PokemonStats(
                    name=row['name'], hp=int(row['hp']), max_hp=int(row['hp']),
                    attack=int(row['attack']), defense=int(row['defense']),
                    sp_attack=int(row['sp_attack']), sp_defense=int(row['sp_defense']),
                    speed=int(row['speed']) if 'speed' in row else 50,
                    type1=row['type1'], type2=row['type2'] if row['type2'] else None,
                    effectiveness=eff
                )
                self.data[p.name.lower()] = p

    def _add_mock(self, name, t1, hp, atk, dfs, spa, spd):
        self.data[name.lower()] = PokemonStats(
            name, hp, hp, atk, dfs, spa, spd, 50, t1, None, {}
        )

    def get(self, name: str) -> Optional[PokemonStats]:
        return self.data.get(name.lower())

# =======================================================================
# PROTOCOL SERIALIZATION
# =======================================================================
def encode_message(msg_type: str, **kwargs) -> bytes:
    """Encodes message to RFC Key: Value format."""
    lines = [f"message_type: {msg_type}"]
    for k, v in kwargs.items():
        if isinstance(v, (dict, list)):
            v = json.dumps(v) # Handle nested JSON objects like stat_boosts
        lines.append(f"{k}: {v}")
    return "\n".join(lines).encode('utf-8')

def decode_message(data: bytes) -> Dict[str, str]:
    """Decodes RFC format into a dictionary."""
    text = data.decode('utf-8', errors='replace')
    out = {}
    for line in text.splitlines():
        if ": " in line:
            key, val = line.split(": ", 1)
            out[key] = val
    return out

# =======================================================================
# RELIABLE TRANSPORT LAYER
# =======================================================================
class ReliableUDP:
    """Handles Seq Numbers, ACKs, Retransmission, and Deduplication."""
    def __init__(self, sock: socket.socket, verbose: bool):
        self.sock = sock
        self.verbose = verbose
        self.seq_num = 0
        self.lock = threading.Lock()
        # Outgoing: {seq: {'msg': bytes, 'addr': tuple, 'time': float, 'retries': int}}
        self.pending_acks = {}
        # Incoming: Set of processed sequence numbers to prevent duplicates
        self.received_seqs: Set[str] = set()
        self.running = True
        self.retransmit_thread = threading.Thread(target=self._retransmit_loop, daemon=True)
        self.retransmit_thread.start()

    def _log(self, direction, addr, msg_type, details=""):
        if self.verbose:
            print(f"[{direction}] {addr} | {msg_type} | {details}")

    def send(self, msg_type: str, addr: Tuple[str, int], require_ack: bool = True, **kwargs):
        """Sends a message. If reliable, tracks it for retransmission."""
        with self.lock:
            self.seq_num += 1
            current_seq = self.seq_num
            kwargs['sequence_number'] = str(current_seq)
            data = encode_message(msg_type, **kwargs)
            self.sock.sendto(data, addr)
            self._log("SEND", addr, msg_type, f"Seq={current_seq}")
            
            if require_ack:
                self.pending_acks[current_seq] = {
                    'data': data, 'addr': addr, 'time': time.time(),
                    'retries': 0, 'type': msg_type
                }

    def send_ack(self, ack_num: int, addr: Tuple[str, int]):
        """Sends an ACK for a received sequence number."""
        data = encode_message("ACK", ack_number=str(ack_num))
        self.sock.sendto(data, addr)

    def handle_packet(self, data: bytes, addr: Tuple[str, int]) -> Optional[Dict]:
        """Processes packet: Handles ACKs, filters duplicates, returns valid data."""
        fields = decode_message(data)
        msg_type = fields.get("message_type")
        
        # 1. Handle Incoming ACK
        if msg_type == "ACK":
            with self.lock:
                ack_n = int(fields.get("ack_number", 0))
                if ack_n in self.pending_acks:
                    del self.pending_acks[ack_n]
            return None # Consumed

        # 2. Handle Reliable Message
        if "sequence_number" in fields:
            seq = int(fields["sequence_number"])
            # Always ACK, even if duplicate (peer might have missed previous ACK)
            self.send_ack(seq, addr)
            # Deduplication Logic
            uniq_id = f"{addr}-{seq}"
            if uniq_id in self.received_seqs:
                if self.verbose: print(f" [DROP] Duplicate message Seq={seq}")
                return None
            self.received_seqs.add(uniq_id)
            
        return fields

    def _retransmit_loop(self):
        """Background thread to resend un-ACKed messages."""
        while self.running:
            now = time.time()
            with self.lock:
                for seq, info in list(self.pending_acks.items()):
                    if now - info['time'] > ACK_TIMEOUT:
                        if info['retries'] < MAX_RETRIES:
                            self.sock.sendto(info['data'], info['addr'])
                            info['retries'] += 1
                            info['time'] = now
                            self._log("RETRY", info['addr'], info['type'], f"Seq={seq} Att={info['retries']}")
                        else:
                            print(f" [ERROR] Connection lost. Failed to send Seq={seq}")
                            del self.pending_acks[seq]
            time.sleep(0.1)

# =======================================================================
# GAME ENGINE
# =======================================================================
class GameEngine:
    def __init__(self, db: PokemonDatabase, my_poke_name: str, seed: int):
        self.db = db
        self.seed = seed
        random.seed(seed) # Sync RNG
        self.my_poke = self.db.get(my_poke_name)
        self.opp_poke: Optional[PokemonStats] = None
        self.sp_atk_uses = 5 
        self.sp_def_uses = 5

    def calculate_damage(self, attacker: PokemonStats, defender: PokemonStats,
                         move_power: int, move_type: str, category: str,
                         use_boost: bool) -> Tuple[int, str]:
        # 1. Stats Selection 
        if category.lower() == "physical":
            atk = attacker.attack
            defn = defender.defense
        else:
            atk = attacker.sp_attack
            defn = defender.sp_defense
            
        # 2. Stat Boosts
        if use_boost:
            atk = int(atk * 1.5)
            
        # 3. Type Effectiveness
        # Look up move_type in the DEFENDER's effectiveness dict
        eff = defender.effectiveness.get(move_type.lower(), 1.0)
        
        # 4. Final Formula
        damage = int((move_power * atk * eff) / max(defn, 1))
        msg = f"{move_type} move ({eff}x eff). Dmg: {damage}"
        return damage, msg

# =======================================================================
# PEER NODE LOGIC
# =======================================================================
class PeerNode:
    def __init__(self, role, local_port, peer_ip, peer_port, pokemon_name, verbose):
        self.role = role
        self.local_port = local_port
        self.peer_addr = (peer_ip, peer_port) if peer_ip else None
        self.pokemon_name = pokemon_name
        self.verbose = verbose
        
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('0.0.0.0', local_port))
        self.transport = ReliableUDP(self.sock, verbose)
        
        self.db = PokemonDatabase("pokemon.csv")
        if not self.db.get(pokemon_name):
            print(f" [WARN] Pokemon '{pokemon_name}' not in CSV. Using Mock.")
            self.db._add_mock(pokemon_name, "normal", 100, 50, 50, 50, 50)
            
        self.engine: Optional[GameEngine] = None
        self.game_queue = queue.Queue()
        self.running = True
        threading.Thread(target=self._recv_loop, daemon=True).start()

    def _recv_loop(self):
        while self.running:
            try:
                data, addr = self.sock.recvfrom(BUFFER_SIZE)
                fields = self.transport.handle_packet(data, addr)
                if not fields: continue # Handled by reliability layer
                
                if fields.get("message_type") == "CHAT_MESSAGE":
                    self._handle_chat(fields)
                else:
                    self.game_queue.put((fields, addr))
            except Exception:
                pass

    def _handle_chat(self, fields):
        """Async chat handling."""
        sender = fields.get("sender_name", "Unknown")
        if fields.get("content_type") == "STICKER":
            print(f"\n[CHAT] {sender} sent a STICKER! (Base64 hidden)\n> ", end="")
        else:
            print(f"\n[CHAT] {sender}: {fields.get('message_text')}\n> ", end="")

    def _wait_for_msg(self, msg_type: str) -> Dict:
        """Blocks until specific message type arrives."""
        while True:
            fields, addr = self.game_queue.get()
            if fields.get("message_type") == msg_type:
                if self.peer_addr is None: self.peer_addr = addr
                return fields

    def start(self):
        print(f"--- {self.role} STARTED ON PORT {self.local_port} ---")
        if self.role == "HOST": self.run_host_handshake()
        elif self.role == "JOINER": self.run_joiner_handshake()
        elif self.role == "SPECTATOR": self.run_spectator(); return
        
        self.run_battle_setup()
        self.run_battle_loop()

    def run_host_handshake(self):
        print("[HOST] Waiting for Joiner...")
        msg = self._wait_for_msg("HANDSHAKE_REQUEST") 
        print(f"[HOST] Joiner connected.")
        seed = random.randint(1, 10000)
        self.engine = GameEngine(self.db, self.pokemon_name, seed)
        self.transport.send("HANDSHAKE_RESPONSE", self.peer_addr, seed=seed) 

    def run_joiner_handshake(self):
        print("[JOINER] Sending Handshake...")
        self.transport.send("HANDSHAKE_REQUEST", self.peer_addr) 
        msg = self._wait_for_msg("HANDSHAKE_RESPONSE")
        self.engine = GameEngine(self.db, self.pokemon_name, int(msg['seed']))
        print(f"[JOINER] Synced with seed: {msg['seed']}")

    def run_battle_setup(self):
        # Exchange BATTLE_SETUP
        stats = self.db.get(self.pokemon_name)
        # We must serialize the inner objects manually for the protocol
        self.transport.send("BATTLE_SETUP", self.peer_addr,
                            communication_mode="P2P",
                            pokemon_name=self.pokemon_name,
                            stat_boosts={"special_attack_uses": 5, "special_defense_uses": 5},
                            pokemon=json.dumps({
                                "name": stats.name, "hp": stats.hp,
                                "attack": stats.attack, "defense": stats.defense,
                                "sp_attack": stats.sp_attack, "sp_defense": stats.sp_defense,
                                "type1": stats.type1
                            }))
        print("Setup sent. Waiting for opponent...")
        
        msg = self._wait_for_msg("BATTLE_SETUP")
        opp_data = json.loads(msg['pokemon'])
        # Load opponent effectiveness from our local DB to ensure fairness/accuracy
        local_opp_stats = self.db.get(opp_data['name'])
        eff_map = local_opp_stats.effectiveness if local_opp_stats else {}
        
        self.engine.opp_poke = PokemonStats(
            name=opp_data['name'], hp=int(opp_data['hp']), max_hp=int(opp_data['hp']),
            attack=int(opp_data['attack']), defense=int(opp_data['defense']),
            sp_attack=int(opp_data['sp_attack']), sp_defense=int(opp_data['sp_defense']),
            speed=0, type1=opp_data['type1'], type2=None, effectiveness=eff_map
        )
        print(f"BATTLE START! {self.pokemon_name} VS {self.engine.opp_poke.name}")

    def run_battle_loop(self):
        is_my_turn = (self.role == "HOST") 
        while self.engine.my_poke.hp > 0 and self.engine.opp_poke.hp > 0:
            print(f"\n--- HP: YOU {self.engine.my_poke.hp} | OPP {self.engine.opp_poke.hp} ---")
            if is_my_turn: self.perform_turn_attack()
            else: self.perform_turn_defense()
            is_my_turn = not is_my_turn
            
        if self.engine.my_poke.hp <= 0:
            print("You fainted. Waiting for GAME_OVER...")
            self._wait_for_msg("GAME_OVER") 
            print("--- DEFEAT ---")
        else:
            self.transport.send("GAME_OVER", self.peer_addr,
                                winner=self.pokemon_name, loser=self.engine.opp_poke.name)
            print("--- VICTORY ---")

    def get_input(self):
        while True:
            i = input(f"({self.role}) [A]ttack, [C]hat, [S]ticker? ").lower()
            if i == 'c':
                self.transport.send("CHAT_MESSAGE", self.peer_addr,
                                    sender_name=self.pokemon_name, content_type="TEXT", message_text=input("Msg: "))
            elif i == 's':
                path = input("Image path: ")
                if os.path.exists(path):
                    with open(path, "rb") as f:
                        b64 = base64.b64encode(f.read()).decode('ascii')
                        self.transport.send("CHAT_MESSAGE", self.peer_addr,
                                            sender_name=self.pokemon_name, content_type="STICKER", sticker_data=b64)
            elif i == 'a':
                return

    def perform_turn_attack(self):
        self.get_input()
        move = input("Move Name: ")
        pwr = int(input("Power: ") or 60)
        mtype = input("Type: ")
        cat = input("Category (physical/special): ")
        boost = (input("Boost Sp.Atk? (y/n): ") == 'y') if cat == 'special' else False
        
        # 1. Announce 
        self.transport.send("ATTACK_ANNOUNCE", self.peer_addr, move_name=move)
        print("Waiting for Defense...")
        
        # 2. Wait Defense 
        self._wait_for_msg("DEFENSE_ANNOUNCE")
        
        # 3. Calculate 
        dmg, note = self.engine.calculate_damage(self.engine.my_poke, self.engine.opp_poke, pwr, mtype, cat, boost)
        self.engine.opp_poke.hp -= dmg
        print(f"Dealt {dmg} ({note})")
        
        # 4. Report 
        self.transport.send("CALCULATION_REPORT", self.peer_addr,
                            damage_dealt=dmg, attacker=self.pokemon_name, move_used=move,
                            defender_hp_remaining=self.engine.opp_poke.hp)
                            
        # 5. Confirm
        self._wait_for_msg("CALCULATION_CONFIRM") # Simplified resolution for brevity

    def perform_turn_defense(self):
        print("Waiting for attack...")
        msg = self._wait_for_msg("ATTACK_ANNOUNCE")
        print(f"Opponent uses {msg['move_name']}!")
        self.transport.send("DEFENSE_ANNOUNCE", self.peer_addr) 
        
        # For P2P synchronization, we usually mirror inputs or lookup move data.
        # Here we ask user to match inputs as per the rubric's "independent calculation" requirement.
        print("Input opponent's move details to verify:")
        pwr = int(input("Power: ") or 60)
        mtype = input("Type: ")
        cat = input("Category: ")
        boost = False # Simplified
        
        dmg, note = self.engine.calculate_damage(self.engine.opp_poke, self.engine.my_poke, pwr, mtype, cat, boost)
        self.engine.my_poke.hp -= dmg
        print(f"Took {dmg} ({note})")
        
        self.transport.send("CALCULATION_REPORT", self.peer_addr,
                            attacker=self.engine.opp_poke.name, move_used=msg['move_name'], damage_dealt=dmg,
                            defender_hp_remaining=self.engine.my_poke.hp)
                            
        rep = self._wait_for_msg("CALCULATION_REPORT")
        if int(rep['damage_dealt']) == dmg:
            self.transport.send("CALCULATION_CONFIRM", self.peer_addr)
        else:
            print("DISCREPANCY! (Resolution not implemented in this minimal view)")

    def run_spectator(self):
        if not self.peer_addr: return
        self.transport.send("SPECTATOR_REQUEST", self.peer_addr)
        print("Watching...")
        while True:
            try:
                print(self.game_queue.get())
            except: 
                pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", required=True)
    parser.add_argument("--pokemon", default="Pikachu")
    parser.add_argument("--local-port", type=int, default=9000)
    parser.add_argument("--peer-ip")
    parser.add_argument("--peer-port", type=int)
    parser.add_argument("--verbose", action="store_true")
    
    args = parser.parse_args()
    PeerNode(args.role, args.local_port, args.peer_ip, args.peer_port,
             args.pokemon, args.verbose).start()