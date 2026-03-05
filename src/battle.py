# battle.py
from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from typing import Optional

class State(str, Enum):
    SETUP = "SETUP"
    WAITING_FOR_MOVE = "WAITING_FOR_MOVE"
    WAITING_FOR_DEFENSE = "WAITING_FOR_DEFENSE"      # attacker waiting for DEFENSE_ANNOUNCE
    PROCESSING_TURN = "PROCESSING_TURN"              # both compute + exchange CALCULATION_REPORT
    WAITING_FOR_CONFIRM = "WAITING_FOR_CONFIRM"      # waiting for CALCULATION_CONFIRM or RESOLUTION flow
    GAME_OVER = "GAME_OVER"

@dataclass
class Pokemon:
    pokemon_name: str
    hp: int
    attack: int
    physical_defense: int
    special_attack: int
    special_defense: int

@dataclass
class StatBoosts:
    special_attack_uses: int
    special_defense_uses: int

@dataclass
class Move:
    move_name: str
    base_power: float = 1.0
    damage_category: str = "PHYSICAL"  # PHYSICAL or SPECIAL

@dataclass
class CalcResult:
    attacker: str
    move_used: str
    remaining_health: int
    damage_dealt: int
    defender_hp_remaining: int
    status_message: str

def compute_damage(attacker: Pokemon, defender: Pokemon, move: Move) -> int:
    # RFC 6: choose correct stats based on damage_category.
    atk_stat = attacker.attack if move.damage_category == "PHYSICAL" else attacker.special_attack
    def_stat = defender.physical_defense if move.damage_category == "PHYSICAL" else defender.special_defense
    # Minimal deterministic formula (replace with your final one if you implement type effectiveness)
    dmg = max(1, int((atk_stat * move.base_power) - (def_stat * 0.5)))
    return dmg

class Battle:
    def __init__(self, me: Pokemon, them: Pokemon, boosts: StatBoosts, host_goes_first: bool):
        self.me = me
        self.them = them
        self.boosts = boosts
        self.host_goes_first = host_goes_first

        self.state = State.SETUP
        self.my_turn = host_goes_first  # host starts per RFC

        self.pending_attack: Optional[Move] = None
        self.last_calc: Optional[CalcResult] = None

    def flip_turn(self):
        self.my_turn = not self.my_turn
