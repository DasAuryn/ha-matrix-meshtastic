import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

from matrix_meshtastic.bridge.state import State


class StateTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / "state.sqlite3"
        self.state = State(self.path, "selected-room-and-channel")
        self.addCleanup(lambda: self.state.close())
        self.now = int(time.time() * 1000)

    def restart(self, binding="selected-room-and-channel"):
        self.state.close()
        self.state = State(self.path, binding)

    def test_cursor_and_batch_rollback_together_if_an_enqueue_fails(self):
        self.state.matrix_batch([("$before", ["before"], self.now)], "old-cursor")
        self.state.db.execute(
            "CREATE TRIGGER reject_message BEFORE INSERT ON outbox "
            "WHEN NEW.event_id = '$fail' BEGIN SELECT RAISE(ABORT, 'disk failure'); END"
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.state.matrix_batch(
                [("$first", ["first"], self.now), ("$fail", ["fail"], self.now)],
                "new-cursor",
            )
        self.assertEqual(self.state.get("sync_token"), "old-cursor")
        message = self.state.next("mesh", 300)
        self.assertEqual(message.event_id, "$before")
        self.state.claim(message)
        self.state.complete(message)
        self.assertIsNone(self.state.next("mesh", 300))

    def test_restart_resumes_only_unsent_message_parts(self):
        self.state.matrix_batch([("$long", ["first", "second", "third"], self.now)], "cursor")
        first = self.state.next("mesh", 300)
        self.state.claim(first)
        self.state.complete(first)
        self.restart()
        second = self.state.next("mesh", 300)
        self.assertEqual(second.part, "second")
        self.assertEqual(second.part_index, 1)
        self.state.matrix_batch([("$long", ["replayed"], self.now)], "cursor-2")
        self.assertEqual(self.state.next("mesh", 300), second)
        self.assertEqual(self.state.get("sync_token"), "cursor-2")

    def test_crashed_mesh_send_is_not_blindly_retried(self):
        self.state.matrix_batch([("$uncertain", ["first", "second"], self.now)], "cursor")
        self.state.claim(self.state.next("mesh", 300))
        self.restart()
        self.assertIsNone(self.state.next("mesh", 300))
        self.state.matrix_batch([("$uncertain", ["first", "second"], self.now)], "next-cursor")
        self.assertIsNone(self.state.next("mesh", 300))

    def test_crashed_matrix_send_is_available_for_idempotent_retry(self):
        self.state.radio_message("radio:123", "Incoming radio", self.now)
        previous = self.state.next("matrix", 300)
        self.state.claim(previous)
        self.restart()
        self.assertEqual(self.state.next("matrix", 300), previous)

    def test_duplicate_mesh_packet_is_not_requeued_after_delivery(self):
        self.assertTrue(self.state.radio_message("radio:123", "Incoming radio", self.now))
        previous = self.state.next("matrix", 300)
        self.state.claim(previous)
        self.state.complete(previous)
        self.assertFalse(self.state.radio_message("radio:123", "Incoming radio", self.now))
        self.assertIsNone(self.state.next("matrix", 300))

    def test_replayed_event_does_not_fail_when_queue_is_full(self):
        self.state.matrix_batch(
            [(f"$event-{index}", [str(index)], self.now) for index in range(1000)],
            "full-cursor",
        )
        self.state.matrix_batch([("$event-0", ["duplicate"], self.now)], "replay-cursor")
        self.assertEqual(self.state.get("sync_token"), "replay-cursor")
        with self.assertRaises(RuntimeError):
            self.state.matrix_batch([("$extra", ["extra"], self.now)], "discarded-cursor")
        self.assertEqual(self.state.get("sync_token"), "replay-cursor")

    def test_changed_routing_binding_never_replays_into_new_destination(self):
        self.state.matrix_batch([("$old-room", ["private body"], self.now)], "old-cursor")
        self.state.radio_message("old-radio:123", "private reply", self.now)
        self.restart("another-room-or-key")
        self.assertIsNone(self.state.get("sync_token"))
        self.assertIsNone(self.state.next("mesh", 300))
        self.assertIsNone(self.state.next("matrix", 300))

    def test_expired_packets_are_not_sent_in_either_direction(self):
        old = self.now - 301000
        self.state.matrix_batch([("$expired", ["old Matrix"], old)], "cursor")
        self.state.radio_message("old-radio:123", "old radio", old)
        self.assertIsNone(self.state.next("mesh", 300))
        self.assertIsNone(self.state.next("matrix", 300))


if __name__ == "__main__":
    unittest.main()
