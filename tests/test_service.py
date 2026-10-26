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
            enabled=True,
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
        self.ready = asyncio.Event()
        self.started = asyncio.Event()
        self.error = None
        self.bridge_error = None
        self.send_text = AsyncMock()
        self.close = AsyncMock()
        self.run = AsyncMock(side_effect=self.wait_for_messages)

    async def wait_for_messages(self, callback, since, stop):
        self.started.set()
        await stop.wait()


class FakeIngress:
    def __init__(self, *args):
        self.start = AsyncMock()
        self.close = AsyncMock()


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
        self.create_transport = self.transport_patch.start()
        self.addCleanup(self.transport_patch.stop)
        self.matrix_patch = patch("matrix_meshtastic.bridge.service.MatrixReceiver", FakeMatrix)
        self.matrix_patch.start()
        self.addCleanup(self.matrix_patch.stop)
        self.ingress_patch = patch("matrix_meshtastic.bridge.service.IngressServer", FakeIngress)
        self.ingress_patch.start()
        self.addCleanup(self.ingress_patch.stop)
        self.bridge = Bridge(self.settings, self.data_dir)
        self.bridge.matrix.ready.set()
        self.now = int(time.time() * 1000)

    async def asyncTearDown(self):
        try:
            self.bridge.state.close()
        except Exception:
            pass

    async def no_pause(self, stop, seconds):
        await asyncio.sleep(0)

    def replace_bridge(self, settings):
        self.bridge.state.close()
        self.create_transport.reset_mock()
        self.bridge = Bridge(settings, self.data_dir)

    async def test_matrix_and_ingress_start_without_radio_configuration(self):
        standalone = replace(
            self.settings,
            matrix=replace(self.settings.matrix, room_id=""),
            meshtastic=RadioSettings(enabled=False),
        )
        self.replace_bridge(standalone)

        async def receive(callback, since, stop):
            self.bridge.ingress.start.assert_awaited_once()
            await callback([], "standalone-cursor")
            stop.set()

        self.bridge.matrix.run.side_effect = receive
        await asyncio.wait_for(self.bridge.run(), 1)
        self.create_transport.assert_not_called()
        self.assertIsNone(self.bridge.radio)
        self.assertEqual(self.bridge.radio_status()["state"], "disabled")
        self.bridge.ingress.close.assert_awaited_once()

    async def test_missing_aes_key_blocks_only_radio_startup(self):
        missing_key = replace(
            self.settings,
            meshtastic=replace(self.settings.meshtastic, channel_psk=""),
        )
        self.replace_bridge(missing_key)
        self.create_transport.assert_not_called()
        self.assertIsNone(self.bridge.radio)
        self.assertEqual(self.bridge.radio_status()["state"], "blocked")
        task = asyncio.create_task(self.bridge.run())
        await asyncio.wait_for(self.bridge.matrix.started.wait(), 1)
        self.assertFalse(task.done())
        self.bridge.ingress.start.assert_awaited_once()
        self.bridge.ingress.close.assert_not_awaited()
        self.bridge.stop.set()
        await asyncio.wait_for(task, 1)

    async def test_missing_bridge_room_keeps_matrix_available_and_never_opens_radio(self):
        self.replace_bridge(replace(self.settings, matrix=replace(self.settings.matrix, room_id="")))
        self.create_transport.assert_not_called()
        self.assertEqual(self.bridge.radio_status()["state"], "blocked")
        task = asyncio.create_task(self.bridge.run())
        await asyncio.wait_for(self.bridge.matrix.started.wait(), 1)
        self.assertFalse(task.done())
        self.bridge.stop.set()
        await asyncio.wait_for(task, 1)

    async def test_disconnected_hardware_does_not_stop_matrix_or_ingress(self):
        attempted = asyncio.Event()

        async def unavailable():
            attempted.set()
            raise TransportUnavailable("radio absent")

        self.radio.connect.side_effect = unavailable
        task = asyncio.create_task(self.bridge.run())
        await asyncio.wait_for(attempted.wait(), 1)
        await asyncio.wait_for(self.bridge.matrix.started.wait(), 1)
        self.assertFalse(task.done())
        self.assertFalse(self.bridge.stop.is_set())
        self.assertEqual(self.bridge.radio_status()["state"], "disconnected")
        self.bridge.ingress.close.assert_not_awaited()
        self.bridge.stop.set()
        await asyncio.wait_for(task, 1)

    async def test_unexpected_matrix_failure_retries_while_ingress_stays_open(self):
        attempts = 0

        async def matrix_run(callback, since, stop):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("server interrupted")
            self.bridge.matrix.started.set()
            await stop.wait()

        self.bridge.matrix.run.side_effect = matrix_run
        with patch("matrix_meshtastic.bridge.service.pause", self.no_pause):
            task = asyncio.create_task(self.bridge.run())
            await asyncio.wait_for(self.bridge.matrix.started.wait(), 1)
            self.assertEqual(attempts, 2)
            self.assertFalse(task.done())
            self.bridge.ingress.close.assert_not_awaited()
            self.assertNotIn("server interrupted", self.bridge.matrix.error)
            self.bridge.stop.set()
            await asyncio.wait_for(task, 1)

    async def test_disabled_radio_keeps_sync_cursor_without_building_transmit_queue(self):
        self.replace_bridge(replace(self.settings, meshtastic=RadioSettings(enabled=False)))
        await self.bridge._matrix_batch(
            [MatrixMessage("$matrix", "@alice:example.org", "Messenger only", self.now)], "next-cursor"
        )
        self.assertEqual(self.bridge.state.get("sync_token"), "next-cursor")
        self.assertIsNone(self.bridge.state.next("mesh", 300))

    async def test_blocked_radio_ignores_new_matrix_and_messenger_transmissions(self):
        self.bridge._block_radio("private key changed")
        event = MatrixMessage("$matrix", "@alice:example.org", "private", self.now)
        await self.bridge._matrix_batch([event], "next-cursor")
        await self.bridge._messenger_send(event)
        self.assertEqual(self.bridge.state.get("sync_token"), "next-cursor")
        self.assertIsNone(self.bridge.state.next("mesh", 300))

    async def test_messenger_send_queues_mesh_without_overwriting_sync_cursor(self):
        await self.bridge._matrix_batch([], "existing-cursor")
        await self.bridge.matrix.on_messenger_send(
            MatrixMessage("$own", "@bridge:example.org", "Hello mesh", self.now)
        )
        self.assertEqual(self.bridge.state.get("sync_token"), "existing-cursor")
        self.assertEqual(self.bridge.state.next("mesh", 300).event_id, "$own")

    async def test_inaccessible_bridge_room_blocks_both_directions_while_matrix_is_connected(self):
        self.bridge.matrix.ready.set()
        self.bridge.matrix.bridge_error = "Der konfigurierte Bridge-Raum ist nicht verfügbar."
        self.bridge.state.mesh_message("$pending", ["old private text"], self.now)
        self.bridge.state.radio_message("node:old", "old radio message", self.now)
        event = MatrixMessage("$new", "@alice:example.org", "new private text", self.now)
        await self.bridge._matrix_batch([event], "current-cursor")
        await self.bridge._messenger_send(event)
        self.bridge.radio_ready.set()
        await self.bridge._deliver_mesh()
        await self.bridge._deliver_matrix()
        self.radio.send_text.assert_not_awaited()
        self.bridge.matrix.send_text.assert_not_awaited()
        self.assertEqual(self.bridge.state.next("mesh", 300).event_id, "$pending")
        self.assertEqual(self.bridge.state.next("matrix", 300).event_id, "node:old")
        self.assertEqual(self.bridge.state.get("sync_token"), "current-cursor")
        self.assertEqual(self.bridge.radio_status()["state"], "blocked")
        self.assertTrue(self.bridge.matrix.ready.is_set())
        self.assertFalse(self.bridge.stop.is_set())
        task = asyncio.create_task(self.bridge.run())
        await asyncio.wait_for(self.bridge.matrix.started.wait(), 1)
        self.assertFalse(task.done())
        self.bridge.ingress.close.assert_not_awaited()
        self.bridge.stop.set()
        await asyncio.wait_for(task, 1)

    async def test_pending_mesh_send_waits_for_initial_matrix_destination_check(self):
        self.bridge.state.mesh_message("$pending", ["old private text"], self.now)
        self.bridge.matrix.ready.clear()
        self.bridge.radio_ready.set()
        task = asyncio.create_task(self.bridge._deliver_mesh())
        await asyncio.sleep(0)
        self.radio.send_text.assert_not_awaited()
        self.assertFalse(task.done())
        self.bridge.matrix.bridge_error = "Der Raumalias zeigt auf einen anderen Raum."
        self.bridge.matrix.ready.set()
        await asyncio.wait_for(task, 1)
        self.radio.send_text.assert_not_awaited()
        self.assertEqual(self.bridge.state.next("mesh", 300).event_id, "$pending")
        self.assertEqual(self.bridge.radio_status()["state"], "blocked")

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
            await self.bridge._deliver_mesh()
        self.radio.send_text.assert_not_awaited()
        self.assertEqual(self.bridge.state.next("mesh", 300).event_id, "$matrix")
        self.assertEqual(self.bridge.radio_status()["state"], "blocked")
        self.assertFalse(self.bridge.stop.is_set())

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
            if attempts > 2:
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

    async def test_protected_channel_failure_stops_only_radio_workers(self):
        self.radio.check_channel.side_effect = ChannelSecurityError("public channel")
        radio_closed = asyncio.Event()
        self.radio.close.side_effect = radio_closed.set
        task = asyncio.create_task(self.bridge.run())
        await asyncio.wait_for(self.bridge.matrix.started.wait(), 1)
        await asyncio.wait_for(radio_closed.wait(), 1)
        self.assertFalse(self.bridge.stop.is_set())
        self.assertFalse(task.done())
        self.assertEqual(self.bridge.radio_status()["state"], "blocked")
        self.bridge.matrix.close.assert_not_awaited()
        self.bridge.ingress.close.assert_not_awaited()
        self.radio.close.assert_awaited_once()
        self.radio.send_text.assert_not_awaited()
        self.bridge.stop.set()
        await asyncio.wait_for(task, 1)
        self.bridge.matrix.close.assert_awaited_once()
        self.bridge.ingress.close.assert_awaited_once()

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
