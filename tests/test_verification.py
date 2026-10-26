import asyncio
import base64
import hashlib
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from nio import SyncResponse
from nio.api import Api
from nio.crypto import OlmDevice

from bridge.config import MatrixSettings
from bridge.matrix import MatrixReceiver, MatrixSetupError
from bridge.verification import MAC_V2, PREFIX


class ModernVerificationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.queue = []
        self.sequence = 0
        self.receivers = []
        for device_id in ("BRIDGE", "ELEMENT"):
            receiver = MatrixReceiver(MatrixSettings(homeserver="https://example.invalid", user_id="@owner:example.invalid"), Path(self.directory.name) / device_id)
            receiver.client = receiver._new_client(device_id)
            receiver.client.restore_login(receiver.settings.user_id, device_id, "synthetic-token")
            receiver.ready.set()
            receiver.client.to_device = AsyncMock(side_effect=lambda message, owner=receiver: self.queue.append((owner, message)) or SimpleNamespace())
            self.receivers.append(receiver)
        self.bridge, self.element = self.receivers
        for own, other in ((self.bridge, self.element), (self.element, self.bridge)):
            device = OlmDevice(other.settings.user_id, other.client.device_id, dict(other.client.olm.account.identity_keys))
            own.client.device_store.add(device)
            own.client.store.save_device_keys({device.user_id: {device.id: device}})
        self.manager = self.bridge.client.verification_manager

    async def asyncTearDown(self):
        for receiver in self.receivers:
            await receiver.close()

    def device(self, own=None, other=None):
        own = own or self.bridge
        other = other or self.element
        return own.client.device_store[other.settings.user_id][other.client.device_id]

    async def incoming(self, kind, content, sender=None, recipient=None):
        self.sequence += 1
        response = SyncResponse.from_dict({"next_batch": str(self.sequence), "to_device": {"events": [{"type": PREFIX + kind, "sender": sender or self.element.settings.user_id, "content": content}]}})
        await (recipient or self.bridge).client.receive_response(response)

    async def drain(self):
        while self.queue:
            sender, message = self.queue.pop(0)
            recipient = self.element if sender is self.bridge else self.bridge
            await self.incoming(message.type[len(PREFIX):], message.content, sender.settings.user_id, recipient)

    async def request(self, transaction="incoming-request", **overrides):
        content = {"transaction_id": transaction, "from_device": "ELEMENT", "timestamp": int(time.time() * 1000), "methods": ["m.sas.v1"]}
        content.update(overrides)
        await self.incoming("request", content)
        return transaction

    async def handshake(self):
        await self.bridge.start_verification(self.element.settings.user_id, "ELEMENT")
        transaction = self.manager.snapshot()[0]["transaction_id"]
        await self.drain()
        await self.element.verification_action(transaction, "accept")
        await self.drain()
        return transaction

    async def test_modern_request_is_visible_but_never_trusted_before_explicit_accept(self):
        transaction = await self.request()
        self.assertEqual(self.bridge.messenger_state()["verification"][0]["state"], "requested")
        self.assertEqual(self.bridge.client.key_verifications, {})
        self.assertFalse(self.device().verified)
        self.assertEqual(self.queue, [])
        await self.bridge.verification_action(transaction, "accept")
        self.assertEqual(self.queue[0][1].type, PREFIX + "ready")
        self.assertEqual(self.queue[0][1].content["transaction_id"], transaction)
        self.assertEqual(self.manager.snapshot()[0]["state"], "accepted")
        self.assertFalse(self.device().verified)

    async def test_actual_nio_sync_request_ready_sas_and_done_requires_both_confirmations(self):
        transaction = await self.handshake()
        left = self.manager.snapshot()[0]
        right = self.element.client.verification_manager.snapshot()[0]
        self.assertEqual(left["state"], "emoji")
        self.assertEqual(left["emoji"], right["emoji"])
        self.assertEqual(len(left["emoji"]), 7)
        self.assertFalse(self.device().verified)
        await self.bridge.verification_action(transaction, "confirm")
        await self.drain()
        self.assertFalse(self.device().verified)
        self.assertFalse(self.device(self.element, self.bridge).verified)
        await self.element.verification_action(transaction, "confirm")
        await self.drain()
        self.assertTrue(self.device().verified)
        self.assertTrue(self.device(self.element, self.bridge).verified)
        self.assertEqual(self.manager.snapshot()[0]["state"], "done")
        self.assertTrue(self.manager.sessions[transaction].sent_done)
        self.assertTrue(self.bridge.client.store.load_device_keys()[self.element.settings.user_id]["ELEMENT"].verified)

    async def test_commitment_is_spec_base64_and_symbols_use_vodozemac_indices_directly(self):
        transaction = await self.handshake()
        ours = self.manager.sessions[transaction].sas
        theirs = self.element.client.verification_manager.sessions[transaction].sas
        expected = base64.b64encode(hashlib.sha256((theirs.pubkey + Api.to_canonical_json(ours.start_verification().content)).encode()).digest()).decode().rstrip("=")
        self.assertEqual(ours.commitment, expected)
        self.assertEqual(ours.chosen_mac_method, MAC_V2)
        indices = ours.established_sas.bytes(ours._extra_info).emoji_indices
        self.assertEqual(ours.get_emoji(), [ours.emoji[index] for index in indices])

    async def test_ready_and_done_alone_cannot_verify_a_device(self):
        transaction = await self.request()
        await self.incoming("done", {"transaction_id": transaction})
        await self.incoming("ready", {"transaction_id": transaction, "from_device": "ELEMENT", "methods": ["m.sas.v1"]})
        self.assertFalse(self.device().verified)
        self.assertIsNone(self.manager.sessions[transaction].sas)
        self.assertEqual(self.manager.snapshot()[0]["state"], "requested")

    async def test_request_rejects_unknown_deleted_blacklisted_and_current_devices(self):
        await self.request("unknown", from_device="UNKNOWN")
        await self.request("current", from_device="BRIDGE")
        self.device().deleted = True
        await self.request("deleted")
        self.device().deleted = False
        self.bridge.client.blacklist_device(self.device())
        await self.request("blacklisted")
        self.assertEqual(self.manager.snapshot(), [])

    async def test_requests_expire_and_replays_do_not_reopen_cancelled_flow(self):
        transaction = await self.request()
        session = self.manager.sessions[transaction]
        session.expires = time.monotonic() - 1
        self.assertEqual(self.manager.snapshot()[0]["state"], "cancelled")
        await self.request(transaction)
        self.assertIs(self.manager.sessions[transaction], session)
        self.assertEqual(session.state, "cancelled")
        self.assertFalse(self.device().verified)

    async def test_stale_future_invalid_and_unsupported_requests_do_not_appear(self):
        for index, changes in enumerate(({"timestamp": 1}, {"timestamp": int(time.time() * 1000) + 120000}, {"timestamp": True}, {"methods": ["m.qr_code.scan.v1"]}, {"from_device": ""})):
            await self.request(str(index), **changes)
        self.assertEqual(self.manager.snapshot(), [])

    async def test_transaction_is_bound_to_sender_and_device(self):
        transaction = await self.request()
        await self.incoming("cancel", {"transaction_id": transaction}, "@attacker:example.invalid")
        await self.incoming("ready", {"transaction_id": transaction, "from_device": "OTHER", "methods": ["m.sas.v1"]})
        self.assertEqual(self.manager.sessions[transaction].state, "requested")
        await self.request(transaction, from_device="OTHER")
        self.assertEqual(self.manager.sessions[transaction].device_id, "ELEMENT")
        self.assertFalse(self.device().verified)

    async def test_changed_identity_cannot_continue_an_accepted_request(self):
        transaction = await self.request()
        self.device().keys["ed25519"] = "changed-public-key"
        with self.assertRaises(MatrixSetupError):
            await self.bridge.verification_action(transaction, "accept")
        self.assertEqual(self.manager.snapshot()[0]["state"], "cancelled")
        self.assertFalse(self.device().verified)

    async def test_cancel_is_visible_and_sends_cancel_without_trusting(self):
        transaction = await self.request()
        await self.bridge.verification_action(transaction, "cancel")
        self.assertEqual(self.queue[-1][1].type, PREFIX + "cancel")
        self.assertEqual(self.manager.snapshot()[0]["state"], "cancelled")
        self.assertFalse(self.device().verified)

    async def test_explicit_confirmation_is_unavailable_before_sas_keys(self):
        transaction = await self.request()
        with self.assertRaises(MatrixSetupError):
            await self.bridge.verification_action(transaction, "confirm")
        self.assertFalse(self.device().verified)
        self.assertEqual(self.queue, [])

    async def test_tampered_commitment_cancels_before_symbols_or_trust(self):
        await self.bridge.start_verification(self.element.settings.user_id, "ELEMENT")
        transaction = self.manager.snapshot()[0]["transaction_id"]
        await self.drain()
        await self.element.verification_action(transaction, "accept")
        while self.queue:
            sender, message = self.queue.pop(0)
            if message.type == PREFIX + "accept":
                message.content["commitment"] = "invalid-commitment"
            recipient = self.element if sender is self.bridge else self.bridge
            await self.incoming(message.type[len(PREFIX):], message.content, sender.settings.user_id, recipient)
        self.assertEqual(self.manager.snapshot()[0]["state"], "cancelled")
        self.assertFalse(self.device().verified)

    async def test_missing_target_device_mac_never_verifies_even_with_valid_other_mac(self):
        transaction = await self.handshake()
        ours = self.manager.sessions[transaction].sas
        peer = self.element.client.verification_manager.sessions[transaction].sas
        await self.bridge.verification_action(transaction, "confirm")
        self.queue.clear()
        key_id = "ed25519:some-master-public-key"
        info = f"MATRIX_KEY_VERIFICATION_MAC{peer.own_user}{peer.own_device}{ours.own_user}{ours.own_device}{transaction}"
        content = {"transaction_id": transaction, "mac": {key_id: peer.established_sas.calculate_mac("some-master-public-key", info + key_id)}, "keys": peer.established_sas.calculate_mac(key_id, info + "KEY_IDS")}
        await self.incoming("mac", content)
        self.assertFalse(self.device().verified)
        self.assertEqual(self.manager.snapshot()[0]["state"], "cancelled")

    async def test_tampered_device_mac_cannot_verify(self):
        transaction = await self.handshake()
        await self.bridge.verification_action(transaction, "confirm")
        self.queue.clear()
        peer = self.element.client.verification_manager.sessions[transaction].sas
        peer.accept_sas()
        content = peer.get_mac().content
        content["mac"]["ed25519:ELEMENT"] = "invalid-mac"
        await self.incoming("mac", content)
        self.assertFalse(self.device().verified)
        self.assertEqual(self.manager.snapshot()[0]["state"], "cancelled")

    async def test_active_requests_are_bounded_and_unknown_senders_do_not_grow_device_store(self):
        for index in range(20):
            await self.request(str(index))
        self.assertEqual(len(self.manager.sessions), 8)
        known_users = list(self.bridge.client.device_store.users)
        for index in range(20):
            await self.incoming("request", {"transaction_id": "spoof-" + str(index), "from_device": "UNKNOWN", "methods": ["m.sas.v1"], "timestamp": int(time.time() * 1000)}, "@unknown" + str(index) + ":example.invalid")
        self.assertEqual(list(self.bridge.client.device_store.users), known_users)

    async def test_failed_ready_send_remains_requested_for_visible_retry(self):
        transaction = await self.request()
        self.bridge.client.to_device.side_effect = OSError("synthetic offline")
        with self.assertRaises(OSError):
            await self.bridge.verification_action(transaction, "accept")
        self.assertEqual(self.manager.snapshot()[0]["state"], "requested")
        self.assertFalse(self.device().verified)

    async def test_malformed_ready_methods_cancel_without_crashing_nio_sync(self):
        for methods in (None, 42, {}, "m.sas.v1", ["m.sas.v1", 42]):
            await self.bridge.start_verification(self.element.settings.user_id, "ELEMENT")
            transaction = self.manager.snapshot()[-1]["transaction_id"]
            await self.incoming("ready", {"transaction_id": transaction, "from_device": "ELEMENT", "methods": methods})
            self.assertEqual(self.manager.sessions[transaction].state, "cancelled")
            self.assertFalse(self.device().verified)

    async def test_key_change_during_done_transmission_cannot_grant_trust(self):
        transaction = await self.handshake()
        peer = self.element.client.verification_manager.sessions[transaction].sas
        peer.accept_sas()
        await self.incoming("mac", peer.get_mac().content)
        original_send = self.bridge.client.to_device.side_effect

        def send(message):
            if message.type == PREFIX + "done":
                self.device().keys["ed25519"] = "changed-while-awaiting-done"
            return original_send(message)

        self.bridge.client.to_device.side_effect = send
        with self.assertRaises(MatrixSetupError):
            await self.bridge.verification_action(transaction, "confirm")
        self.assertFalse(self.device().verified)
        self.assertEqual(self.manager.sessions[transaction].state, "cancelled")

    async def test_expiry_during_done_transmission_cannot_grant_trust(self):
        transaction = await self.handshake()
        peer = self.element.client.verification_manager.sessions[transaction].sas
        peer.accept_sas()
        await self.incoming("mac", peer.get_mac().content)
        original_send = self.bridge.client.to_device.side_effect

        def send(message):
            if message.type == PREFIX + "done":
                self.manager.sessions[transaction].expires = time.monotonic() - 1
                self.manager.snapshot()
            return original_send(message)

        self.bridge.client.to_device.side_effect = send
        await self.bridge.verification_action(transaction, "confirm")
        self.assertFalse(self.device().verified)
        self.assertEqual(self.manager.sessions[transaction].state, "cancelled")

    async def test_expiry_during_ready_send_does_not_reopen_request(self):
        transaction = await self.request()

        async def send(message):
            await asyncio.sleep(0)
            self.manager.sessions[transaction].expires = time.monotonic() - 1
            self.manager.snapshot()
            return SimpleNamespace()

        self.bridge.client.to_device.side_effect = send
        with self.assertRaises(MatrixSetupError):
            await self.bridge.verification_action(transaction, "accept")
        self.assertEqual(self.manager.sessions[transaction].state, "cancelled")
        self.assertFalse(self.device().verified)
