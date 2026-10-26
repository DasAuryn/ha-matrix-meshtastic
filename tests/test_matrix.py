from __future__ import annotations

import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from nio import MegolmEvent, RoomMessageText, ToDeviceEvent, JoinError
from nio.crypto import OlmDevice
from nio.exceptions import EncryptionError

from matrix_meshtastic.bridge.config import MatrixSettings
from matrix_meshtastic.bridge.matrix import MatrixMessage, MatrixReceiver, MatrixSetupError, MatrixConnectionError, _read_private, _write_private


def settings(**overrides):
    values = {
        "homeserver": "https://matrix.example.org",
        "user_id": "@bridge:example.org",
        "password": "secret",
        "access_token": "",
        "device_id": "",
        "room_id": "!room:example.org",
    }
    values.update(overrides)
    return MatrixSettings(**values)


def event(event_id="$event", sender="@alice:example.org", msgtype="m.text", content=None):
    return RoomMessageText.from_dict(
        {
            "type": "m.room.message",
            "event_id": event_id,
            "sender": sender,
            "origin_server_ts": 123456,
            "content": content or {"msgtype": msgtype, "body": "Hello mesh"},
        }
    )


def encrypted_event():
    return MegolmEvent.from_dict(
        {
            "type": "m.room.encrypted",
            "event_id": "$encrypted",
            "sender": "@alice:example.org",
            "origin_server_ts": 123456,
            "content": {
                "algorithm": "m.megolm.v1.aes-sha2",
                "ciphertext": "ciphertext",
                "sender_key": "sender-key",
                "session_id": "session",
                "device_id": "ALICE",
            },
        }
    )


class MatrixTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.receiver = MatrixReceiver(settings(), Path(self.temporary.name))
        self.room = SimpleNamespace(room_id="!room:example.org")

    async def test_only_selected_room_new_messages_and_other_senders(self):
        await self.receiver._on_event(SimpleNamespace(room_id="!other:example.org"), event())
        await self.receiver._on_event(self.room, event(sender="@bridge:example.org"))
        await self.receiver._on_event(self.room, event(content={"msgtype": "m.text", "body": "   "}))
        await self.receiver._on_event(
            self.room,
            event(content={"msgtype": "m.text", "body": "edit", "m.relates_to": {"rel_type": "m.replace"}}),
        )
        self.assertEqual(self.receiver._messages, [])
        await self.receiver._on_event(self.room, event())
        self.assertEqual(self.receiver._messages, [MatrixMessage("$event", "@alice:example.org", "Hello mesh", 123456)])

    async def test_initial_snapshot_is_not_forwarded(self):
        self.receiver._baseline = True
        await self.receiver._on_event(self.room, event())
        self.assertEqual(self.receiver._messages, [])
        self.assertEqual(self.receiver.chat_store.get_messages(self.room.room_id)["messages"][0]["body"], "Hello mesh")

    async def test_messenger_keeps_other_rooms_and_own_messages_without_bridging(self):
        other = SimpleNamespace(room_id="!other")
        await self.receiver._on_event(other, event(event_id="$other"))
        await self.receiver._on_event(self.room, event(event_id="$own", sender=self.receiver.settings.user_id))
        self.assertEqual(self.receiver._messages, [])
        self.assertEqual(len(self.receiver.chat_store.get_messages("!other")["messages"]), 1)
        self.assertEqual(self.receiver.chat_store.get_messages(self.room.room_id)["messages"][0]["sender"], self.receiver.settings.user_id)

    async def test_matrix_login_without_a_bridge_room_does_not_join_any_room(self):
        receiver = MatrixReceiver(settings(room_id=""), Path(self.temporary.name))
        client = SimpleNamespace(
            user_id=receiver.settings.user_id, device_id="MATRIX", access_token="secret",
            login=AsyncMock(return_value=SimpleNamespace()), join=AsyncMock(),
        )
        with patch.object(receiver, "_new_client", return_value=client):
            await receiver._login()
        client.join.assert_not_awaited()
        self.assertTrue(receiver._initialized)

    async def test_unavailable_bridge_room_does_not_block_matrix_login(self):
        client = SimpleNamespace(
            user_id=self.receiver.settings.user_id, device_id="MATRIX", access_token="secret",
            login=AsyncMock(return_value=SimpleNamespace()),
            join=AsyncMock(return_value=JoinError.from_dict({"errcode": "M_FORBIDDEN", "error": "Access denied"})),
        )
        with patch.object(self.receiver, "_new_client", return_value=client):
            await self.receiver._login()
        self.assertTrue(self.receiver._initialized)
        self.assertEqual(self.receiver.room_id, "")
        self.assertIn("M_FORBIDDEN", self.receiver.bridge_error)

    async def test_ui_send_notifies_bridge_only_for_selected_room(self):
        room = SimpleNamespace(room_id=self.room.room_id, encrypted=False)
        other = SimpleNamespace(room_id="!other", encrypted=False)
        self.receiver.client = SimpleNamespace(
            rooms={room.room_id: room, other.room_id: other},
            room_send=AsyncMock(return_value=SimpleNamespace(event_id="$sent")),
        )
        self.receiver.ready.set()
        self.receiver.chat_store.upsert_room(room.room_id, membership="join")
        self.receiver.chat_store.upsert_room(other.room_id, membership="join")
        self.receiver.on_messenger_send = AsyncMock()
        await self.receiver.send_room_text(other.room_id, "Direktnachricht", "tx1")
        self.receiver.on_messenger_send.assert_not_awaited()
        await self.receiver.send_room_text(room.room_id, "Ins Mesh", "tx2")
        self.receiver.on_messenger_send.assert_awaited_once()
        self.assertEqual(self.receiver.on_messenger_send.await_args.args[0].body, "Ins Mesh")
        await self.receiver.send_text("Vom Mesh", "tx3")
        self.receiver.on_messenger_send.assert_awaited_once()

    async def test_ui_send_rejects_unknown_rooms_before_network_write(self):
        self.receiver.client = SimpleNamespace(rooms={}, room_send=AsyncMock())
        self.receiver.ready.set()
        with self.assertRaises(MatrixSetupError):
            await self.receiver.send_room_text("!foreign", "Private Nachricht", "tx")
        self.receiver.client.room_send.assert_not_awaited()

    async def test_left_room_stays_left_despite_stale_nio_cache(self):
        room = SimpleNamespace(room_id="!left", display_name="Left", encrypted=False)
        self.receiver.client = SimpleNamespace(rooms={room.room_id: room}, room_send=AsyncMock())
        self.receiver.ready.set()
        self.receiver.chat_store.upsert_room(room.room_id, membership="join")
        self.receiver._update_rooms(SimpleNamespace(rooms=SimpleNamespace(join={}, leave={room.room_id: object()})))
        self.receiver._update_rooms(SimpleNamespace(rooms=SimpleNamespace(join={}, leave={})))
        self.assertEqual(self.receiver.chat_store.room(room.room_id)["membership"], "leave")
        self.assertEqual(self.receiver.messenger_state()["rooms"], [])
        with self.assertRaises(MatrixSetupError):
            await self.receiver.send_room_text(room.room_id, "Geheim", "tx")
        self.receiver.client.room_send.assert_not_awaited()

    async def test_limited_timeline_of_another_room_is_filled_without_mesh_forwarding(self):
        room = SimpleNamespace(room_id="!other")
        self.receiver.client = SimpleNamespace(rooms={room.room_id: room}, room_messages=AsyncMock(return_value=SimpleNamespace(chunk=[event(event_id="$missing")], end="end")))
        response = SimpleNamespace(rooms=SimpleNamespace(join={room.room_id: SimpleNamespace(timeline=SimpleNamespace(limited=True, prev_batch="end"))}))
        await self.receiver._fill_gap(response, "cursor")
        self.assertEqual(self.receiver.chat_store.get_messages(room.room_id)["messages"][0]["event_id"], "$missing")
        self.assertEqual(self.receiver._messages, [])

    def test_account_switch_clears_pending_encrypted_file_before_any_login(self):
        _write_private(self.receiver.data_dir / "pending_encrypted.json", {"$old": {"room_id": "!old"}})
        changed = MatrixReceiver(settings(user_id="@new:example.org"), Path(self.temporary.name))
        self.assertEqual(changed._encrypted, {})
        self.assertEqual(_read_private(changed.data_dir / "pending_encrypted.json", None), {})
        restarted = MatrixReceiver(settings(user_id="@new:example.org"), Path(self.temporary.name))
        self.assertEqual(restarted._encrypted, {})
        changed.chat_store.close()
        restarted.chat_store.close()

    async def test_offline_history_is_available_but_sending_requires_connection(self):
        self.receiver.chat_store.save_message("!chat", "$cached", "@alice:x", "Alice", "Offline lesbar", 100)
        result = await self.receiver.room_history("!chat")
        self.assertEqual(result["messages"][0]["body"], "Offline lesbar")
        with self.assertRaises(MatrixConnectionError):
            await self.receiver.send_room_text("!chat", "Neu", "tx")

    async def test_initial_encrypted_history_never_gets_forwarded_after_keys_arrive(self):
        self.receiver._baseline = True
        await self.receiver._on_event(self.room, encrypted_event())
        self.receiver._baseline = False
        self.receiver.client = SimpleNamespace(decrypt_event=Mock(return_value=event(event_id="$encrypted")))
        await self.receiver._retry_encrypted()
        self.assertEqual(self.receiver._messages, [])
        self.assertEqual(self.receiver.chat_store.get_messages(self.room.room_id)["messages"][0]["body"], "Hello mesh")

    async def test_existing_direct_chat_is_reused(self):
        self.receiver._direct_rooms = {"@alice:x": ["!dm"]}
        self.receiver.client = SimpleNamespace(rooms={"!dm": object()}, room_create=AsyncMock())
        self.receiver.ready.set()
        self.receiver.chat_store.upsert_room("!dm", membership="join")
        self.assertEqual(await self.receiver.create_direct("@alice:x"), {"room_id": "!dm"})
        self.receiver.client.room_create.assert_not_awaited()

    async def test_new_direct_chat_is_encrypted_and_records_account_data(self):
        self.receiver.client = SimpleNamespace(
            rooms={}, room_create=AsyncMock(return_value=SimpleNamespace(room_id="!dm")),
            _send=AsyncMock(return_value=SimpleNamespace()),
        )
        self.receiver.ready.set()
        self.assertEqual(await self.receiver.create_direct("@alice:x"), {"room_id": "!dm"})
        arguments = self.receiver.client.room_create.await_args.kwargs
        self.assertEqual(arguments["invite"], ["@alice:x"])
        self.assertTrue(arguments["is_direct"])
        self.assertEqual(arguments["initial_state"][0]["content"]["algorithm"], "m.megolm.v1.aes-sha2")
        self.assertEqual(self.receiver.chat_store.get_metadata("direct_rooms"), {"@alice:x": ["!dm"]})

    async def test_sas_confirmation_is_blocked_until_comparison_is_available(self):
        sas = SimpleNamespace(canceled=False, timed_out=False, verified=False, established_sas=None)
        self.receiver.client = SimpleNamespace(key_verifications={"transaction": sas}, confirm_short_auth_string=AsyncMock())
        self.receiver.ready.set()
        with self.assertRaises(MatrixSetupError):
            await self.receiver.verification_action("transaction", "confirm")
        self.receiver.client.confirm_short_auth_string.assert_not_awaited()

    async def test_sas_confirmation_uses_explicit_user_action(self):
        sas = SimpleNamespace(canceled=False, timed_out=False, verified=False, established_sas=object())
        self.receiver.client = SimpleNamespace(key_verifications={"transaction": sas}, confirm_short_auth_string=AsyncMock(return_value=SimpleNamespace()), olm=None)
        self.receiver.ready.set()
        await self.receiver.verification_action("transaction", "confirm")
        self.receiver.client.confirm_short_auth_string.assert_awaited_once_with("transaction")

    async def test_real_sas_handshake_requires_matching_emoji_and_both_confirmations(self):
        alice = MatrixReceiver(settings(user_id="@alice:example.org", room_id=""), Path(self.temporary.name) / "alice")
        bob = MatrixReceiver(settings(user_id="@bob:example.org", room_id=""), Path(self.temporary.name) / "bob")
        try:
            for receiver, device_id in ((alice, "ALICE"), (bob, "BOB")):
                receiver.client = receiver._new_client(device_id)
                receiver.client.restore_login(receiver.settings.user_id, device_id, "test-token")
                receiver.client.to_device = AsyncMock(return_value=SimpleNamespace())
                receiver.ready.set()
            for local, remote in ((alice, bob), (bob, alice)):
                device = OlmDevice(remote.settings.user_id, remote.client.device_id, dict(remote.client.olm.account.identity_keys))
                local.client.device_store.add(device)
                local.client.store.save_device_keys({device.user_id: {device.id: device}})

            def deliver(sender, recipient, message):
                incoming = ToDeviceEvent.parse_event({"type": message.type, "sender": sender.settings.user_id, "content": message.content})
                recipient.client.olm.handle_key_verification(incoming)

            start = alice.client.create_key_verification(alice.client.device_store[bob.settings.user_id]["BOB"])
            transaction = start.content["transaction_id"]
            deliver(alice, bob, start)
            self.assertEqual(bob.messenger_state()["verification"][0]["state"], "requested")
            await bob.verification_action(transaction, "accept")
            deliver(bob, alice, bob.client.to_device.await_args.args[0])
            deliver(alice, bob, alice.client.outgoing_to_device_messages.pop(0))
            deliver(bob, alice, bob.client.outgoing_to_device_messages.pop(0))
            alice_emoji = alice.messenger_state()["verification"][0]["emoji"]
            bob_emoji = bob.messenger_state()["verification"][0]["emoji"]
            self.assertEqual(len(alice_emoji), 7)
            self.assertEqual(alice_emoji, bob_emoji)
            self.assertFalse(alice.client.device_store[bob.settings.user_id]["BOB"].verified)
            await alice.verification_action(transaction, "confirm")
            deliver(alice, bob, alice.client.to_device.await_args.args[0])
            self.assertFalse(bob.client.device_store[alice.settings.user_id]["ALICE"].verified)
            await bob.verification_action(transaction, "confirm")
            deliver(bob, alice, bob.client.to_device.await_args.args[0])
            self.assertTrue(alice.client.device_store[bob.settings.user_id]["BOB"].verified)
            self.assertTrue(bob.client.device_store[alice.settings.user_id]["ALICE"].verified)
        finally:
            await alice.close()
            await bob.close()

    async def test_matrix_send_uses_stable_transaction_id(self):
        self.receiver.client = SimpleNamespace(room_send=AsyncMock(return_value=SimpleNamespace()))
        self.receiver.ready.set()
        await self.receiver.send_text("Incoming mesh", "mesh-123")
        self.receiver.client.room_send.assert_awaited_once_with(
            "!room:example.org",
            "m.room.message",
            {"msgtype": "m.text", "body": "Incoming mesh"},
            tx_id="mesh-123",
        )

    async def test_sync_cursor_is_committed_after_batch(self):
        stop = asyncio.Event()
        response = SimpleNamespace(next_batch="next", rooms=SimpleNamespace(join={}))
        client = SimpleNamespace(sync=AsyncMock(return_value=response), next_batch="")
        self.receiver.client = client
        self.receiver._initialized = True
        batches = []

        async def commit(messages, cursor):
            batches.append((messages, cursor))
            stop.set()

        with patch.object(self.receiver, "_crypto_maintenance", AsyncMock()):
            await self.receiver.run(commit, None, stop)
        self.assertEqual(batches, [([], "next")])
        self.assertIsNone(client.sync.await_args.kwargs["since"])
        self.assertTrue(client.sync.await_args.kwargs["full_state"])

    async def test_resume_keeps_events_and_uses_committed_cursor(self):
        stop = asyncio.Event()

        async def sync(**kwargs):
            await self.receiver._on_event(self.room, event())
            return SimpleNamespace(next_batch="next", rooms=SimpleNamespace(join={}))

        client = SimpleNamespace(sync=AsyncMock(side_effect=sync), next_batch="uncommitted")
        self.receiver.client = client
        self.receiver._initialized = True
        commit = AsyncMock(side_effect=lambda *args: stop.set())
        with patch.object(self.receiver, "_crypto_maintenance", AsyncMock()):
            await self.receiver.run(commit, "committed", stop)
        self.assertEqual(client.sync.await_args.kwargs["since"], "committed")
        self.assertEqual(len(commit.await_args.args[0]), 1)

    async def test_failed_batch_does_not_advance_receiver(self):
        response = SimpleNamespace(next_batch="next", rooms=SimpleNamespace(join={}))
        self.receiver.client = SimpleNamespace(sync=AsyncMock(return_value=response), next_batch="")
        self.receiver._initialized = True
        commit = AsyncMock(side_effect=RuntimeError("database unavailable"))
        with patch.object(self.receiver, "_crypto_maintenance", AsyncMock()):
            with self.assertRaisesRegex(RuntimeError, "database unavailable"):
                await self.receiver.run(commit, "old", asyncio.Event())
        self.assertFalse(self.receiver.ready.is_set())

    async def test_encrypted_messages_survive_cursor_commit_until_keys_arrive(self):
        stop = asyncio.Event()

        async def sync(**kwargs):
            await self.receiver._on_event(self.room, encrypted_event())
            return SimpleNamespace(next_batch="next", rooms=SimpleNamespace(join={}))

        self.receiver.client = SimpleNamespace(
            sync=AsyncMock(side_effect=sync),
            next_batch="old",
            decrypt_event=Mock(side_effect=EncryptionError("missing key")),
            outgoing_key_requests={},
            request_room_key=AsyncMock(return_value=SimpleNamespace()),
        )
        self.receiver._initialized = True
        commit = AsyncMock(side_effect=lambda *args: stop.set())
        with patch.object(self.receiver, "_crypto_maintenance", AsyncMock()):
            await self.receiver.run(commit, "old", stop)
        self.assertEqual(commit.await_args.args, ([], "next"))
        restarted = MatrixReceiver(settings(), Path(self.temporary.name))
        restarted.client = SimpleNamespace(decrypt_event=Mock(return_value=event(event_id="$encrypted")))
        self.assertEqual(await restarted._retry_encrypted(), ["$encrypted"])
        self.assertEqual(restarted._messages[0].event_id, "$encrypted")

    async def test_pending_encrypted_messages_never_cross_rooms(self):
        self.receiver._encrypted = {"$other": {"room_id": "!other:example.org"}}
        self.receiver.client = SimpleNamespace(decrypt_event=Mock())
        self.assertEqual(await self.receiver._retry_encrypted(), ["$other"])
        self.receiver.client.decrypt_event.assert_not_called()
        self.assertEqual(self.receiver._messages, [])

    async def test_limited_sync_gap_is_filled_before_current_timeline(self):
        self.receiver._messages = [MatrixMessage("$new", "@alice:example.org", "new", 123457)]
        self.receiver.client = SimpleNamespace(
            rooms={self.room.room_id: self.room},
            room_messages=AsyncMock(
                return_value=SimpleNamespace(chunk=[event(event_id="$old")], end="end")
            ),
        )
        response = SimpleNamespace(
            rooms=SimpleNamespace(
                join={self.room.room_id: SimpleNamespace(timeline=SimpleNamespace(limited=True, prev_batch="end"))}
            )
        )
        await self.receiver._fill_gap(response, "old-cursor")
        self.assertEqual([message.event_id for message in self.receiver._messages], ["$old", "$new"])
        self.assertEqual(self.receiver.client.room_messages.await_args.kwargs["start"], "old-cursor")

    async def test_retargeted_alias_is_blocked_before_join_or_delivery(self):
        alias_settings = settings(room_id="#bridge:example.org")
        receiver = MatrixReceiver(alias_settings, Path(self.temporary.name))
        binding = {
            "homeserver": alias_settings.homeserver,
            "user_id": alias_settings.user_id,
            "configured_room": alias_settings.room_id,
            "resolved_room": "!original:example.org",
        }
        _write_private(receiver.data_dir / "room_binding.json", binding)
        client = SimpleNamespace(
            user_id=alias_settings.user_id,
            device_id="BRIDGE",
            access_token="secret",
            login=AsyncMock(return_value=SimpleNamespace()),
            room_resolve_alias=AsyncMock(return_value=SimpleNamespace(room_id="!other:example.org")),
            join=AsyncMock(return_value=SimpleNamespace(room_id="!other:example.org")),
        )
        with patch.object(receiver, "_new_client", return_value=client):
            await receiver._login()
        client.join.assert_not_awaited()
        self.assertFalse(receiver.ready.is_set())
        self.assertTrue(receiver._initialized)
        self.assertEqual(receiver.room_id, "")
        self.assertIn("anderen Raum", receiver.bridge_error)
        self.assertEqual(_read_private(receiver.data_dir / "room_binding.json", {}), binding)

    async def test_explicit_room_change_pins_new_destination_after_successful_join(self):
        alias_settings = settings(room_id="#new:example.org")
        receiver = MatrixReceiver(alias_settings, Path(self.temporary.name))
        _write_private(
            receiver.data_dir / "room_binding.json",
            {
                "homeserver": alias_settings.homeserver,
                "user_id": alias_settings.user_id,
                "configured_room": "#old:example.org",
                "resolved_room": "!old:example.org",
            },
        )
        client = SimpleNamespace(
            user_id=alias_settings.user_id,
            device_id="BRIDGE",
            access_token="secret",
            login=AsyncMock(return_value=SimpleNamespace()),
            room_resolve_alias=AsyncMock(return_value=SimpleNamespace(room_id="!new:example.org")),
            join=AsyncMock(return_value=SimpleNamespace(room_id="!new:example.org")),
        )
        with patch.object(receiver, "_new_client", return_value=client):
            await receiver._login()
        client.join.assert_awaited_once_with("!new:example.org")
        binding = _read_private(receiver.data_dir / "room_binding.json", {})
        self.assertEqual(binding["configured_room"], "#new:example.org")
        self.assertEqual(binding["resolved_room"], "!new:example.org")
        self.assertEqual(os.stat(receiver.data_dir / "room_binding.json").st_mode & 0o777, 0o600)

    def test_session_files_are_private_and_atomic(self):
        path = Path(self.temporary.name) / "session.json"
        _write_private(path, {"access_token": "secret"})
        self.assertEqual(_read_private(path, {}), {"access_token": "secret"})
        self.assertEqual(os.stat(path).st_mode & 0o777, 0o600)
        self.assertFalse(path.with_suffix(".json.tmp").exists())


if __name__ == "__main__":
    unittest.main()
