import asyncio
import base64
import hashlib
import tempfile
import time
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock, patch

from matrix_meshtastic.bridge.config import (
    ForwardingSettings,
    MatrixSettings,
    RadioSettings,
    Settings,
)
from matrix_meshtastic.bridge.matrix import MatrixMessage
from matrix_meshtastic.bridge.security import ChannelSecurityError
from matrix_meshtastic.bridge.service import Bridge
from matrix_meshtastic.bridge.transports import RadioMessage, TransportUnavailable


def configured_settings():
    return Settings(
        matrix=MatrixSettings(
            homeserver="https://matrix.example.org",
            user_id="@bridge:example.org",
            password="secret",
            room_id="!selected:example.org",
        ),
        meshtastic=RadioSettings(
            channel_psk=base64.b64encode(bytes(range(32))).decode(),
            channel_index=2,
            tcp_host="mesh.local",
        ),
        forwarding=ForwardingSettings(),
    )


class FakeRadio:
    def __init__(self):
        self.connect = AsyncMock()
        self.check_channel = AsyncMock()
        self.send_text = AsyncMock()
        self.close = AsyncMock()
        self.receive = AsyncMock(side_effect=self.wait_for_message)

    async def wait_for_message(self):
        await asyncio.Event().wait()


class FakeMatrix:
    def __init__(self, *args):
        self.send_text = AsyncMock()
        self.close = AsyncMock()
        self.run = AsyncMock(side_effect=self.wait_for_messages)

    async def wait_for_messages(self, callback, since, stop):
        await stop.wait()


class ServiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.data_dir = Path(self.temporary.name)
        self.settings = configured_settings()
        self.radio = FakeRadio()
        self.transport_patch = patch(
            "matrix_meshtastic.bridge.service.create_transport", return_value=self.radio
        )
        self.transport_patch.start()
        self.addCleanup(self.transport_patch.stop)
        self.matrix_patch = patch("matrix_meshtastic.bridge.service.MatrixReceiver", FakeMatrix)
        self.matrix_patch.start()
        self.addCleanup(self.matrix_patch.stop)
        self.bridge = Bridge(self.settings, self.data_dir)
        self.now = int(time.time() * 1000)

    async def asyncTearDown(self):
        try:
            self.bridge.state.close()
        except Exception:
            pass

    async def no_pause(self, stop, seconds):
        await asyncio.sleep(0)

    async def test_matrix_message_reaches_only_configured_channel(self):
        await self.bridge._matrix_batch(
            [MatrixMessage("$matrix", "@alice:example.org", "Hello mesh", self.now)], "cursor"
        )
        self.bridge.radio_ready.set()
        self.radio.send_text.side_effect = lambda *args: self.bridge.stop.set()
        with patch("matrix_meshtastic.bridge.service.pause", self.no_pause):
            await self.bridge._deliver_mesh()
        self.radio.send_text.assert_awaited_once()
        self.assertEqual(self.radio.send_text.await_args.args[1], 2)
        self.assertIn("Hello mesh", self.radio.send_text.await_args.args[0])
        self.assertIsNone(self.bridge.state.next("mesh", 300))

    async def test_uncertain_radio_write_is_not_automatically_retried(self):
        await self.bridge._matrix_batch(
            [MatrixMessage("$matrix", "@alice:example.org", "Hello mesh", self.now)], "cursor"
        )
        self.bridge.radio_ready.set()

        async def uncertain(*args):
            self.bridge.stop.set()
            raise OSError("connection lost during write")

        self.radio.send_text.side_effect = uncertain
        with patch("matrix_meshtastic.bridge.service.pause", self.no_pause):
            await self.bridge._deliver_mesh()
        self.radio.send_text.assert_awaited_once()
        self.assertIsNone(self.bridge.state.next("mesh", 300))

    async def test_preflight_channel_failure_sends_nothing_and_keeps_queue(self):
        await self.bridge._matrix_batch(
            [MatrixMessage("$matrix", "@alice:example.org", "private", self.now)], "cursor"
        )
        self.bridge.radio_ready.set()
        self.radio.check_channel.side_effect = ChannelSecurityError("key changed")
        with patch("matrix_meshtastic.bridge.service.pause", self.no_pause):
            with self.assertRaises(ChannelSecurityError):
                await self.bridge._deliver_mesh()
        self.radio.send_text.assert_not_awaited()
        self.assertEqual(self.bridge.state.next("mesh", 300).event_id, "$matrix")

    async def test_transient_preflight_failure_pauses_instead_of_spinning(self):
        await self.bridge._matrix_batch(
            [MatrixMessage("$matrix", "@alice:example.org", "private", self.now)], "cursor"
        )
        self.bridge.radio_ready.set()
        attempts = 0

        async def unavailable():
            nonlocal attempts
            attempts += 1
            if attempts > 1:
                self.bridge.stop.set()
            raise OSError("not ready")

        async def stop_after_backoff(stop, seconds):
            if seconds > 0:
                stop.set()

        self.radio.check_channel.side_effect = unavailable
        with patch("matrix_meshtastic.bridge.service.pause", stop_after_backoff):
            await self.bridge._deliver_mesh()
        self.assertEqual(attempts, 1)
        self.radio.send_text.assert_not_awaited()
        self.assertFalse(self.bridge.radio_ready.is_set())

    async def test_transient_receive_disconnect_does_not_terminate_bridge(self):
        message = RadioMessage("node:123", "!1234abcd", "Hello Matrix", self.now)
        attempts = 0

        async def receive():
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise TransportUnavailable("radio reconnecting")
            self.bridge.stop.set()
            return message

        self.radio.receive.side_effect = receive
        with patch("matrix_meshtastic.bridge.service.pause", self.no_pause):
            await self.bridge._radio_receive()
        self.assertEqual(self.bridge.state.next("matrix", 300).event_id, "node:123")

    async def test_mesh_packet_reaches_matrix_with_idempotent_transaction(self):
        self.bridge.state.radio_message("node:123", "Hello Matrix", self.now)
        self.bridge.matrix.send_text.side_effect = lambda *args: self.bridge.stop.set()
        await self.bridge._deliver_matrix()
        transaction = hashlib.sha256(b"!selected:example.org:node:123").hexdigest()
        self.bridge.matrix.send_text.assert_awaited_once_with("Hello Matrix", transaction)
        self.assertIsNone(self.bridge.state.next("matrix", 300))

    async def test_matrix_retry_reuses_transaction_after_unknown_response(self):
        self.bridge.state.radio_message("node:123", "Hello Matrix", self.now)
        attempts = 0

        async def send(*args):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise OSError("response was lost")
            self.bridge.stop.set()

        self.bridge.matrix.send_text.side_effect = send
        with patch("matrix_meshtastic.bridge.service.pause", self.no_pause):
            await self.bridge._deliver_matrix()
        calls = self.bridge.matrix.send_text.await_args_list
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0].args, calls[1].args)
        self.assertIsNone(self.bridge.state.next("matrix", 300))

    async def test_protected_channel_failure_aborts_and_closes_all_workers(self):
        self.radio.check_channel.side_effect = ChannelSecurityError("public channel")
        with self.assertRaises(ChannelSecurityError):
            await self.bridge.run()
        self.assertTrue(self.bridge.stop.is_set())
        self.radio.close.assert_awaited_once()
        self.bridge.matrix.close.assert_awaited_once()
        self.radio.send_text.assert_not_awaited()

    async def test_rotated_broker_credentials_preserve_pending_messages(self):
        self.bridge.state.matrix_batch([("$pending", ["private"], self.now)], "cursor")
        self.bridge.state.close()
        rotated = replace(
            self.settings,
            meshtastic=replace(self.settings.meshtastic, mqtt_password="rotated"),
        )
        self.bridge = Bridge(rotated, self.data_dir)
        self.assertEqual(self.bridge.state.get("sync_token"), "cursor")
        self.assertEqual(self.bridge.state.next("mesh", 300).event_id, "$pending")

    async def test_changed_destination_never_receives_old_queue(self):
        self.bridge.state.matrix_batch([("$pending", ["private"], self.now)], "cursor")
        self.bridge.state.close()
        changed = replace(self.settings, matrix=replace(self.settings.matrix, room_id="!other:example.org"))
        self.bridge = Bridge(changed, self.data_dir)
        self.assertIsNone(self.bridge.state.get("sync_token"))
        self.assertIsNone(self.bridge.state.next("mesh", 300))


if __name__ == "__main__":
    unittest.main()
