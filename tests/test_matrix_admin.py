import json
import stat
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from nio import ErrorResponse, MatrixRoom, MegolmEvent, RoomMessageText
from nio.crypto import InboundGroupSession
from nio.crypto.device import OlmDevice
from nio.crypto.sessions import OutboundGroupSession
from nio.exceptions import EncryptionError, OlmUnverifiedDeviceError

from bridge.config import MatrixSettings
from bridge.matrix import MatrixActionError, MatrixReceiver, MatrixSetupError


class MatrixAdminTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.receiver = MatrixReceiver(MatrixSettings(
            homeserver="https://matrix.example.invalid", user_id="@bot:example.invalid",
            password="test-password", room_id="!bridge:example.invalid",
        ), Path(self.directory.name) / "target")
        self.room = MatrixRoom("!bridge:example.invalid", self.receiver.settings.user_id, encrypted=True)
        self.receiver.chat_store.upsert_room(self.room.room_id, name="Bridge", encrypted=True, membership="join")
        self.receiver.client = SimpleNamespace(
            rooms={self.room.room_id: self.room}, invited_rooms={},
            room_create=AsyncMock(return_value=SimpleNamespace(room_id="!new:example.invalid")),
            room_invite=AsyncMock(return_value=SimpleNamespace()),
            room_leave=AsyncMock(return_value=SimpleNamespace()),
            room_put_state=AsyncMock(return_value=SimpleNamespace()),
            join=AsyncMock(return_value=SimpleNamespace(room_id="!joined:example.invalid")),
            close=AsyncMock(),
        )
        self.receiver.ready.set()

    async def asyncTearDown(self):
        await self.receiver.close()

    async def test_new_room_is_private_and_encrypted_by_default(self):
        result = await self.receiver.create_room("Team", ["@alice:example.invalid"])
        self.assertEqual(result, {"room_id": "!new:example.invalid"})
        args = self.receiver.client.room_create.await_args.kwargs
        self.assertEqual(args["name"], "Team")
        self.assertEqual(args["invite"], ["@alice:example.invalid"])
        self.assertEqual(args["initial_state"][0]["content"]["algorithm"], "m.megolm.v1.aes-sha2")
        self.assertTrue(self.receiver.chat_store.room(result["room_id"])["encrypted"])

    async def test_room_admin_requires_membership_before_network_action(self):
        with self.assertRaises(MatrixSetupError):
            await self.receiver.invite_user("!unknown", "@alice:example.invalid")
        with self.assertRaises(MatrixSetupError):
            await self.receiver.rename_room("!unknown", "New name")
        with self.assertRaises(MatrixSetupError):
            await self.receiver.leave_room("!unknown")
        self.receiver.client.room_invite.assert_not_awaited()
        self.receiver.client.room_put_state.assert_not_awaited()
        self.receiver.client.room_leave.assert_not_awaited()

    async def test_rename_and_invite_use_selected_room(self):
        await self.receiver.rename_room(self.room.room_id, "Einsatz")
        await self.receiver.invite_user(self.room.room_id, "@alice:example.invalid")
        self.receiver.client.room_put_state.assert_awaited_once_with(self.room.room_id, "m.room.name", {"name": "Einsatz"})
        self.receiver.client.room_invite.assert_awaited_once_with(self.room.room_id, "@alice:example.invalid")
        self.assertEqual(self.receiver.chat_store.room(self.room.room_id)["name"], "Einsatz")

    async def test_permission_error_is_actionable_without_server_details(self):
        self.receiver.client.room_invite.return_value = ErrorResponse("private server detail", "M_FORBIDDEN")
        with self.assertRaises(MatrixActionError) as raised:
            await self.receiver.invite_user(self.room.room_id, "@alice:example.invalid")
        self.assertEqual(raised.exception.code, "permission_denied")
        self.assertEqual(raised.exception.status, 403)
        self.assertNotIn("private server detail", str(raised.exception))

    async def test_leaving_bridge_room_blocks_radio_and_removes_stale_membership(self):
        self.receiver._encrypted["$old"] = {"room_id": self.room.room_id}
        await self.receiver.leave_room(self.room.room_id)
        self.assertEqual(self.receiver.room_id, "")
        self.assertTrue(self.receiver.bridge_error)
        self.assertNotIn(self.room.room_id, self.receiver.client.rooms)
        self.assertNotIn("$old", self.receiver._encrypted)
        self.assertEqual(self.receiver.chat_store.room(self.room.room_id)["membership"], "leave")

    async def test_explicit_join_accepts_room_alias(self):
        result = await self.receiver.join_room("#team:example.invalid")
        self.receiver.client.join.assert_awaited_once_with("#team:example.invalid")
        self.assertEqual(result["room_id"], "!joined:example.invalid")

    async def _real_crypto_client(self, receiver, device_id):
        receiver.client = receiver._new_client(device_id)
        receiver.client.restore_login(receiver.settings.user_id, device_id, "test-token")
        receiver.ready.set()
        receiver.client.rooms[self.room.room_id] = MatrixRoom(self.room.room_id, receiver.settings.user_id, encrypted=True)

    async def test_real_encrypted_export_import_decrypts_history_without_radio_replay(self):
        source = MatrixReceiver(self.receiver.settings, Path(self.directory.name) / "source")
        try:
            await self._real_crypto_client(source, "SOURCE")
            await self._real_crypto_client(self.receiver, "TARGET")
            keys = source.client.olm.account.identity_keys
            self.receiver.client.device_store.add(OlmDevice(source.settings.user_id, "SOURCE", dict(keys)))
            outbound = OutboundGroupSession()
            inbound = InboundGroupSession(outbound.session_key, keys["ed25519"], keys["curve25519"], self.room.room_id)
            source.client.olm.inbound_group_store.add(inbound)
            source.client.store.save_inbound_group_session(inbound)
            outbound.mark_as_shared()
            encrypted = MegolmEvent.from_dict({
                "type": "m.room.encrypted", "event_id": "$exported", "sender": source.settings.user_id,
                "origin_server_ts": 123456, "room_id": self.room.room_id,
                "content": {"algorithm": "m.megolm.v1.aes-sha2", "session_id": outbound.id,
                            "sender_key": keys["curve25519"], "device_id": "SOURCE",
                            "ciphertext": outbound.encrypt(json.dumps({"room_id": self.room.room_id,
                                "type": "m.room.message", "content": {"msgtype": "m.text", "body": "Alte Nachricht"}}))},
            })
            self.receiver._baseline = True
            await self.receiver._on_event(self.room, encrypted)
            self.receiver._baseline = False
            self.receiver.on_messenger_send = AsyncMock()
            export_path = Path(self.directory.name) / "elements-keys.txt"
            await source.client.export_keys(str(export_path), "export-passphrase", count=1000)
            data = export_path.read_bytes()
            self.assertIn(b"BEGIN MEGOLM SESSION DATA", data)
            original_import = self.receiver.client.import_keys
            temporary_paths = []

            async def inspect_import(path, passphrase):
                temporary_paths.append(Path(path))
                self.assertEqual(stat.S_IMODE(Path(path).stat().st_mode), 0o600)
                await original_import(path, passphrase)

            with patch.object(self.receiver.client, "import_keys", side_effect=inspect_import):
                result = await self.receiver.import_room_keys(data, "export-passphrase")
            self.assertEqual(result["imported_keys"], 1)
            self.assertEqual(result["decrypted_messages"], 1)
            self.assertEqual(result["pending_encrypted_count"], 0)
            self.assertEqual(self.receiver.chat_store.message(self.room.room_id, "$exported")["body"], "Alte Nachricht")
            self.assertEqual(self.receiver._messages, [])
            self.receiver.on_messenger_send.assert_not_awaited()
            self.assertTrue(temporary_paths)
            self.assertTrue(all(not path.exists() for path in temporary_paths))
            self.assertEqual(list(self.receiver.data_dir.glob(".room-keys-*")), [])
            restarted = MatrixReceiver(self.receiver.settings, Path(self.directory.name) / "target")
            try:
                await self._real_crypto_client(restarted, "TARGET")
                self.assertIsNotNone(restarted.client.olm.inbound_group_store.get(self.room.room_id, keys["curve25519"], outbound.id))
            finally:
                await restarted.close()
        finally:
            await source.close()

    async def test_wrong_export_passphrase_is_sanitized_and_temporary_file_removed(self):
        source = MatrixReceiver(self.receiver.settings, Path(self.directory.name) / "source")
        try:
            await self._real_crypto_client(source, "SOURCE")
            await self._real_crypto_client(self.receiver, "TARGET")
            export_path = Path(self.directory.name) / "elements-keys.txt"
            await source.client.export_keys(str(export_path), "correct-export-passphrase", count=1000)
            with self.assertRaises(MatrixActionError) as raised:
                await self.receiver.import_room_keys(export_path.read_bytes(), "wrong-secret-value")
            self.assertEqual(raised.exception.code, "key_import_failed")
            self.assertNotIn("wrong-secret-value", str(raised.exception))
            self.assertEqual(list(self.receiver.data_dir.glob(".room-keys-*")), [])
        finally:
            await source.close()

    async def test_manual_fingerprint_verification_requires_explicit_confirmation_and_current_key(self):
        await self._real_crypto_client(self.receiver, "TARGET")
        device = OlmDevice("@alice:example.invalid", "ALICE", {"ed25519": "expected-public-key", "curve25519": "curve-public-key"})
        self.receiver.client.device_store.add(device)
        self.receiver.client.store.save_device_keys({device.user_id: {device.id: device}})
        for confirmed, fingerprint, code in ((False, device.ed25519, "confirmation_required"), (True, "stale-public-key", "fingerprint_mismatch")):
            with self.assertRaises(MatrixActionError) as raised:
                await self.receiver.verify_device_fingerprint(device.user_id, device.id, fingerprint, confirmed)
            self.assertEqual(raised.exception.code, code)
            self.assertFalse(device.verified)
        result = await self.receiver.verify_device_fingerprint(device.user_id, device.id, device.ed25519, True)
        self.assertTrue(result["verified"])
        self.assertTrue(device.verified)

    async def test_unverified_devices_have_actionable_send_error_without_bypassing_encryption(self):
        await self._real_crypto_client(self.receiver, "TARGET")
        device = OlmDevice("@alice:example.invalid", "ALICE", {"ed25519": "expected-public-key", "curve25519": "curve-public-key"})
        self.receiver.client.device_store.add(device)
        with patch.object(self.receiver.client, "room_send", AsyncMock(side_effect=OlmUnverifiedDeviceError(device))):
            with self.assertRaises(MatrixActionError) as raised:
                await self.receiver.send_room_text(self.room.room_id, "Private Nachricht", "same-tx")
        self.assertEqual(raised.exception.code, "unverified_devices")
        self.assertEqual(raised.exception.details["devices"][0]["device_id"], "ALICE")
        self.assertFalse(device.verified)
        self.assertEqual(self.receiver.chat_store.get_messages(self.room.room_id)["messages"], [])

    def pending_event(self, event_id="$pending"):
        return {
            "type": "m.room.encrypted", "event_id": event_id, "sender": "@alice:example.invalid",
            "origin_server_ts": 123456, "room_id": self.room.room_id, "bridge_eligible": False,
            "content": {"algorithm": "m.megolm.v1.aes-sha2", "session_id": "missing-session",
                        "sender_key": "sender-key", "device_id": "ALICE", "ciphertext": "encrypted-data"},
        }

    async def test_manual_key_retry_deduplicates_session_requests(self):
        self.receiver._encrypted = {"$one": self.pending_event("$one"), "$two": self.pending_event("$two")}
        self.receiver.client = SimpleNamespace(
            rooms={self.room.room_id: self.room}, device_id="TARGET", outgoing_key_requests={"missing-session": object()},
            to_device=AsyncMock(return_value=SimpleNamespace()), request_room_key=AsyncMock(),
            decrypt_event=Mock(side_effect=EncryptionError("missing")), close=AsyncMock(),
        )
        with patch.object(self.receiver, "_crypto_maintenance", AsyncMock()):
            result = await self.receiver.retry_keys(self.room.room_id)
        self.assertEqual(result["requested_sessions"], 1)
        self.assertEqual(result["pending_encrypted_count"], 2)
        self.receiver.client.to_device.assert_awaited_once()
        self.receiver.client.request_room_key.assert_not_awaited()

    async def test_retry_iteration_survives_new_events_while_key_request_awaits(self):
        self.receiver._encrypted = {"$one": self.pending_event("$one")}

        async def request_key(event):
            self.receiver._encrypted["$two"] = self.pending_event("$two")
            return SimpleNamespace()

        self.receiver.client = SimpleNamespace(
            decrypt_event=Mock(side_effect=EncryptionError("missing")), outgoing_key_requests={},
            request_room_key=AsyncMock(side_effect=request_key), close=AsyncMock(),
        )
        self.assertEqual(await self.receiver._retry_encrypted(), [])
        self.assertIn("$two", self.receiver._encrypted)

    async def test_manual_recovery_of_live_message_waits_for_normal_bridge_commit(self):
        source = self.pending_event()
        source["bridge_eligible"] = True
        self.receiver._encrypted = {source["event_id"]: source}
        cleartext = RoomMessageText.from_dict({
            "type": "m.room.message", "event_id": source["event_id"], "sender": source["sender"],
            "origin_server_ts": source["origin_server_ts"], "content": {"msgtype": "m.text", "body": "Nachricht"},
        })
        self.receiver.client = SimpleNamespace(
            rooms={self.room.room_id: self.room}, decrypt_event=Mock(return_value=cleartext), close=AsyncMock(),
        )
        self.assertEqual(self.receiver._decrypt_for_display(), 1)
        self.assertEqual(self.receiver._messages, [])
        self.assertIn(source["event_id"], self.receiver._encrypted)
        self.assertEqual(self.receiver.encryption_diagnostics()["pending_encrypted_count"], 0)
        self.assertEqual(await self.receiver._retry_encrypted(), [source["event_id"]])
        self.assertEqual(self.receiver._messages[0].event_id, source["event_id"])

    async def test_login_rejects_a_different_account_before_persisting_session(self):
        client = SimpleNamespace(user_id="@other:example.invalid", device_id="OTHER", access_token="not-to-persist", login=AsyncMock(return_value=SimpleNamespace()), close=AsyncMock())
        self.receiver.client = None
        with patch.object(self.receiver, "_new_client", return_value=client):
            with self.assertRaises(MatrixSetupError):
                await self.receiver._login()
        self.assertFalse((self.receiver.data_dir / "session.json").exists())
