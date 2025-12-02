# 🕹️ P2P Pokémon Battle Protocol — Python Implementation

Reference Python implementation of the **PokeProtocol P2P Pokémon battle** using **UDP + custom reliability**.
This version supports **Host / Joiner gameplay**, a basic **Spectator handshake**, and **asynchronous text/sticker chat**.

---

## 📦 What This Implementation Actually Does

✔️ **Peer-to-peer over UDP**
✔️ **Roles: HOST, JOINER, SPECTATOR (handshake only)**
✔️ **Handshake with shared RNG seed**
✔️ **Turn-based battles, attacker authoritative**
✔️ **Custom reliability layer (ACK, retry, seq numbers)**
✔️ **Independent damage computation + discrepancy resolution**
✔️ **Chat at any time (text + Base64 stickers)**
✔️ **Pokémon stats loaded from `pokemon.csv`**

⚠️ **Spectator receives only the handshake**

> Spectators do **not** receive forwarded battle/game messages or chat broadcasts.
> No spectator forwarding system exists yet.

⚠️ **P2P only**

> `DEFAULT_COMM_MODE = "P2P"`.
> There is **no broadcast discovery** and no LAN scanning.

---

# 🗂️ Files

* `poke_ultimate.py` — main program
* `pokemon.csv` — source of stats & type effectiveness
* `stickers/` — automatically saved incoming stickers

---

# 🚀 How to Run

## Host

```bash
python poke_ultimate.py --role HOST --local-port 9000 --pokemon Pikachu --verbose
```

Host waits for a Joiner to handshake.

## Joiner

```bash
python poke_ultimate.py --role JOINER \
    --peer-ip 127.0.0.1 --peer-port 9000 \
    --pokemon Charmander --verbose
```

**Joiner must specify IP + port.**

## Spectator

```bash
python poke_ultimate.py --role SPECTATOR \
    --peer-ip 127.0.0.1 --peer-port 9000
```

Spectator completes handshake → sits idle.
**Does not mirror the battle.**

---

# 🧠 Protocol Flow Implemented

### 1. Handshake

* JOINER → `HANDSHAKE_REQUEST`
* HOST → `HANDSHAKE_RESPONSE(seed)`
* Both seed RNG identically

Spectator:

* Sends `SPECTATOR_REQUEST` to HOST
* HOST replies with `HANDSHAKE_RESPONSE`
* No further interaction

### 2. Battle Setup (host + joiner)

Both send:

* Pokémon name
* Pokémon object (stats)
* Stat boost counts

Opponents are created via local copies from CSV.

### 3. Turn Order

* HOST always goes first
* Turns alternate per round

---

# 🎮 Battle Logic (as implemented)

Attacker flow:

1. `ATTACK_ANNOUNCE` (require ACK)
2. Wait `DEFENSE_ANNOUNCE`
3. Compute damage locally
4. `CALCULATION_REPORT`
5. Receive opp report
6. Match = send `CALCULATION_CONFIRM`
7. Mismatch = send `RESOLUTION_REQUEST`
   → attacker’s report is authoritative

Defender flow:

1. Wait `ATTACK_ANNOUNCE`
2. `DEFENSE_ANNOUNCE`
3. Compute their own version of damage
4. Send `CALCULATION_REPORT`
5. Compare attacker’s
6. Mismatch → wait `RESOLUTION_REQUEST`, apply attacker values, send confirm

Game ends when:

* My Pokémon faints → expect remote `GAME_OVER`
* Enemy faints → I send `GAME_OVER` then exit

---

# 🧮 Damage Model (no hidden mechanics)

* physical → Attack vs Defense
* special → SpAttack vs SpDefense
* optional attacker SpAtk boost = x1.5
* type multiplier from defender’s `effectiveness` dict

Minimum damage = **1**.

Moves are **not fixed**:

> User inputs move name, type, category, power each turn.
> This avoids hardcoding a moveset.

---

# 🔐 Reliability Layer (important)

Every non-ACK message:

* Gets a **sequence_number**
* Receiver **must reply with ACK**
* Unacked → retransmit ≤ 3 attempts
* After max retries → **connection_lost = True**

Duplicates:

* Identified via `(ip:port:sequence)`
* Message ignored if already handled

---

# 💬 Chat System

Chat runs in its own background thread.
Does not block the battle state machine.

### Text:

Just type during runtime.

### Stickers:

```
/s path/to/file.png
```

* ≤10MB
* Base64 encoded
* Saved to: `stickers/sticker_<sender>_<timestamp>.bin`

Spectators **can send chat**, but they do not receive game logs.

---

# ⚠️ Limitations (intentionally stated clearly)

* **No broadcast discovery** (only direct IP)
* **No spectator streaming**
* **No reconnect**
* **No move database**
* Input is **trusted**, not sanitized
* GUI: **None** (CLI only)

---

# 🧪 Local / LAN Testing

Two consoles or two machines:

**Host**

```
python poke_ultimate.py --role HOST --local-port 9000
```

**Joiner**

```
python poke_ultimate.py --role JOINER --peer-ip <HOST-IP> --peer-port 9000
```

---

# 🤖 AI Usage

* It helped by suggesting key fixes to the code.
* All code was tested & reviewed by the group.
* The group completely understand & can explain every part.

---

