"""Deterministic datagram relay tests using the real wire/reliability code."""
import socket
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from main import App
from protocol import MsgType, WireMessage
from reliable_udp import ReliableUDP, ReliableConfig

SPECTATOR_MESSAGES = (
    MsgType.ATTACK_ANNOUNCE, MsgType.DEFENSE_ANNOUNCE,
    MsgType.CALCULATION_REPORT, MsgType.GAME_OVER,
)


class SpectatorRelayTests(unittest.TestCase):
    def setUp(self):
        self.socket_patch = patch("reliable_udp.socket.socket")
        self.sock = self.socket_patch.start().return_value
        self.addCleanup(self.socket_patch.stop)
        self.sock.recvfrom.side_effect = socket.timeout
        self.rudp = ReliableUDP("127.0.0.1", 0, ReliableConfig())
        self.peer = ("127.0.0.1", 9001)
        self.spec = ("127.0.0.1", 9002)
        self.spec2 = ("127.0.0.1", 9003)
        self.app = App(self.rudp, "host", self.peer, "P2P")
        self.app.spectators.update((self.spec, self.spec2))

    def copies(self, dest):
        return [WireMessage.from_bytes(call.args[0])
                for call in self.sock.sendto.call_args_list
                if call.args[1] == dest]

    def test_both_directions_all_four_types_and_sequence_collisions(self):
        # No Battle object: inbound relay must precede battle handling, including
        # GAME_OVER. Remote seq=1 deliberately collides with host's first send.
        original = {"move_name": "Tackle", "damage_dealt": "12"}
        for mt in sorted(SPECTATOR_MESSAGES, key=lambda mt: mt.value):
            self.app.send(self.peer, mt, original)
            self.app.on_msg(self.peer, WireMessage(mt, dict(original, sequence_number="1")))
        self.assertNotIn("sequence_number", original)
        copies = self.copies(self.spec)
        self.assertEqual(len(copies), 8)
        self.assertEqual(copies, self.copies(self.spec2))
        self.assertEqual(len({m.fields["sequence_number"] for m in copies}), 8)
        for msg in copies:
            self.assertEqual({k: v for k, v in msg.fields.items() if k != "sequence_number"}, original)
        # Only player sends enter retransmission tracking.
        self.assertEqual(len(self.rudp._pending), 4)
        self.assertTrue(all(addr == self.peer for addr, seq in self.rudp._pending))
        # Feed actual relay bytes through a spectator's duplicate filter.
        spectator_udp = ReliableUDP("127.0.0.1", 0, ReliableConfig())
        host_addr = ("127.0.0.1", 9000)
        self.sock.recvfrom.side_effect = [(m.to_bytes(), host_addr) for m in copies]
        delivered = [spectator_udp.poll()[1] for _ in copies]
        self.assertEqual(delivered, copies)

    def test_inbound_retries_relay_once_and_reack(self):
        msg = WireMessage(MsgType.ATTACK_ANNOUNCE,
                          {"move_name": "Tackle", "sequence_number": "1"})
        self.sock.recvfrom.side_effect = [(msg.to_bytes(), self.peer)] * 2
        for _ in range(2):
            pkt = self.rudp.poll()
            if pkt:
                self.app.on_msg(*pkt)
        self.assertEqual(len(self.copies(self.spec)), 1)
        self.assertEqual(len(self.copies(self.peer)), 2)  # ACK + re-ACK

    def test_spectator_first_does_not_claim_peer_or_change_setup(self):
        self.app.peer = None
        self.app.on_msg(self.spec, WireMessage(MsgType.SPECTATOR_REQUEST, {}))
        self.assertIsNone(self.app.peer)
        self.app.on_msg(self.spec, WireMessage(MsgType.BATTLE_SETUP, {"pokemon_name": "Spectator"}))
        self.assertEqual(self.app.them.pokemon_name, "OpponentPokemon")
        self.app.on_msg(self.peer, WireMessage(MsgType.HANDSHAKE_REQUEST, {}))
        self.assertEqual(self.app.peer, self.peer)

    def test_spectator_handshake_sends_no_battle_setup(self):
        app = App(self.rudp, "spectator", self.peer, "P2P")
        app.on_msg(self.peer, WireMessage(MsgType.HANDSHAKE_RESPONSE, {"seed": "42"}))
        self.sock.sendto.assert_not_called()
        self.assertEqual(app.seed, 42)
        self.assertIsNone(app.battle)

    def test_unrelated_and_spectator_messages_are_not_relayed(self):
        self.app.on_msg(self.spec, WireMessage(MsgType.ATTACK_ANNOUNCE, {"move_name": "bad"}))
        self.app.on_msg(("127.0.0.1", 9999), WireMessage(MsgType.GAME_OVER, {}))
        self.app.send(self.peer, MsgType.CHAT_MESSAGE, {"message_text": "hello"})
        self.assertFalse(self.copies(self.spec))

    def test_failed_spectator_copy_does_not_block_others_or_player(self):
        def sendto(payload, addr):
            if addr == self.spec:
                raise OSError("unreachable")
        self.sock.sendto.side_effect = sendto
        self.app.send(self.peer, MsgType.GAME_OVER, {"winner": "A", "loser": "B"})
        self.assertEqual(len(self.copies(self.spec2)), 1)
        self.assertFalse(self.rudp.connection_lost_event.is_set())


if __name__ == "__main__":
    unittest.main()
