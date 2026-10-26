from __future__ import annotations

import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from nio import MegolmEvent, RoomMessageText
from nio.exceptions import EncryptionError

from matrix_meshtastic.bridge.config import MatrixSettings
from matrix_meshtastic.bridge.matrix import MatrixMessage, MatrixReceiver, MatrixSetupError, _read_private, _write_private


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
            with self.assertRaisesRegex(MatrixSetupError, "anderen Raum"):
                await receiver._login()
        client.join.assert_not_awaited()
        self.assertFalse(receiver.ready.is_set())
        self.assertFalse(receiver._initialized)
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
