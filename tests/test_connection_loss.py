"""Deterministic retry/termination regressions; no network or real sleeps."""
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from battle import State
from main import App
from protocol import MsgType, WireMessage
from reliable_udp import ReliableConfig, ReliableUDP


class ConnectionLossTests(unittest.TestCase):
    def setUp(self):
        self.socket_patch = patch("reliable_udp.socket.socket")
        self.sock = self.socket_patch.start().return_value
        self.addCleanup(self.socket_patch.stop)
        import socket
        self.sock.recvfrom.side_effect = socket.timeout
        self.rudp = ReliableUDP("127.0.0.1", 0, ReliableConfig())
        self.peer = ("127.0.0.1", 9999)
        self.msg = WireMessage(MsgType.ATTACK_ANNOUNCE,
                               {"sequence_number": "1", "move_name": "Tackle"})

    def exhaust(self):
        with patch("reliable_udp.time.time", return_value=0):
            self.rudp.send(self.peer, self.msg)
        for now in (0.5, 1.0, 1.5):
            with patch("reliable_udp.time.time", return_value=now):
                self.assertIsNone(self.rudp.poll())
            self.assertFalse(self.rudp.connection_lost_event.is_set())
        self.assertEqual(self.sock.sendto.call_count, 4)  # original + 3 retries

    def test_exhaustion_is_sticky_and_does_not_receive_more_packets(self):
        self.exhaust()
        self.sock.recvfrom.reset_mock()
        with patch("reliable_udp.time.time", return_value=2.0):
            self.assertIsNone(self.rudp.poll())
        self.assertTrue(self.rudp.connection_lost_event.is_set())
        self.assertNotIn((self.peer, 1), self.rudp._pending)
        self.assertIsNone(self.rudp.poll())
        self.sock.recvfrom.assert_not_called()
        self.assertEqual(self.sock.sendto.call_count, 4)

    def test_ack_prevents_connection_loss(self):
        with patch("reliable_udp.time.time", return_value=0):
            self.rudp.send(self.peer, self.msg)
        ack = WireMessage(MsgType.ACK, {"ack_number": "1"})
        self.sock.recvfrom.side_effect = [(ack.to_bytes(), self.peer)]
        with patch("reliable_udp.time.time", return_value=0.1):
            self.rudp.poll()
        self.assertFalse(self.rudp._pending)
        import socket
        self.sock.recvfrom.side_effect = socket.timeout
        with patch("reliable_udp.time.time", return_value=10):
            self.rudp.poll()
        self.assertFalse(self.rudp.connection_lost_event.is_set())
        self.assertEqual(self.sock.sendto.call_count, 1)

    def test_app_exits_waiting_battle_on_real_retry_exhaustion(self):
        for state in (State.WAITING_FOR_DEFENSE, State.WAITING_FOR_CONFIRM):
            with self.subTest(state=state):
                self.rudp.connection_lost_event.clear()
                self.rudp._pending.clear()
                self.sock.reset_mock()
                self.exhaust()
                app = App(self.rudp, "host", self.peer, "P2P")
                app.seed = 1
                app.start_battle_if_ready(True)
                app.battle.state = state
                app.on_msg = Mock()
                with patch("main.threading.Thread"), patch("main.time.sleep") as sleep:
                    with patch("reliable_udp.time.time", return_value=2.0):
                        app.start()
                self.assertFalse(app.running)
                self.assertEqual(app.battle.state, State.GAME_OVER)
                app.on_msg.assert_not_called()
                sleep.assert_not_called()

    def test_packet_returned_with_loss_is_not_dispatched(self):
        app = App(self.rudp, "host", self.peer, "P2P")
        def lost_poll():
            self.rudp.connection_lost_event.set()
            return self.peer, self.msg
        app.on_msg = Mock()
        with patch.object(self.rudp, "poll", side_effect=lost_poll), patch("main.threading.Thread"):
            app.start()
        self.assertFalse(app.running)
        app.on_msg.assert_not_called()

    def test_input_unblocked_after_loss_does_not_send(self):
        app = App(self.rudp, "host", self.peer, "P2P")
        def delayed_input():
            self.rudp.connection_lost_event.set()
            return "/chat too late"
        with patch("builtins.input", side_effect=delayed_input):
            app.input_loop()
        self.sock.sendto.assert_not_called()


if __name__ == "__main__":
    unittest.main()
