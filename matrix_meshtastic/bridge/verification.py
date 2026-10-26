from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import secrets
import time
from collections import OrderedDict
from dataclasses import dataclass

from nio import AsyncClient, ErrorResponse, ToDeviceEvent
from nio.api import Api
from nio.crypto import Sas
from nio.crypto.sas import SasState
from nio.event_builders import ToDeviceMessage
from nio.exceptions import LocalProtocolError


MAC_V2 = "hkdf-hmac-sha256.v2"
PREFIX = "m.key.verification."


def _commitment(key: str, content: dict) -> str:
    digest = hashlib.sha256((key + Api.to_canonical_json(content)).encode()).digest()
    return base64.b64encode(digest).decode().rstrip("=")


class MatrixSas(Sas):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mac_methods = [MAC_V2]

    def start_verification(self):
        message = super().start_verification()
        message.content["message_authentication_codes"] = [MAC_V2]
        message.content["key_agreement_protocols"] = ["curve25519-hkdf-sha256"]
        return message

    @classmethod
    def from_start(cls, client, device, event):
        content = event.source["content"]
        if (content.get("method") != "m.sas.v1"
                or MAC_V2 not in content.get("message_authentication_codes", [])
                or "curve25519-hkdf-sha256" not in content.get("key_agreement_protocols", [])
                or "sha256" not in content.get("hashes", [])
                or "emoji" not in content.get("short_authentication_string", [])):
            raise ValueError("Die angebotene Gerätebestätigung wird nicht unterstützt.")
        sas = cls(client.user_id, client.device_id, client.olm.account.identity_keys["ed25519"], device, event.transaction_id)
        sas.we_started_it = False
        sas.state = SasState.started
        sas.commitment = _commitment(sas.pubkey, content)
        sas.key_agreement_protocols = ["curve25519-hkdf-sha256"]
        sas.short_auth_string = ["emoji"]
        return sas

    def accept_verification(self):
        message = super().accept_verification()
        self.chosen_mac_method = MAC_V2
        message.content["message_authentication_code"] = MAC_V2
        return message

    def receive_accept_event(self, event):
        if not self._event_ok(event):
            return
        if (self.state != SasState.created
                or event.message_authentication_code != MAC_V2
                or event.key_agreement_protocol != "curve25519-hkdf-sha256"
                or event.hash != "sha256" or "emoji" not in event.short_authentication_string):
            self.cancel()
            return
        self.commitment = event.commitment
        self.chosen_mac_method = MAC_V2
        self.chosen_key_agreement = event.key_agreement_protocol
        self.short_auth_string = event.short_authentication_string
        self.state = SasState.accepted

    def _check_commitment(self, key):
        return hmac.compare_digest(self.commitment or "", _commitment(key, self.start_verification().content))

    def _generate_emoji(self, extra_info):
        return [self.emoji[index] for index in self.established_sas.bytes(extra_info).emoji_indices]

    def receive_mac_event(self, event):
        if f"ed25519:{self.other_olm_device.id}" not in event.mac:
            self.cancel()
            return
        super().receive_mac_event(event)
        if self.other_olm_device.id not in self.verified_devices:
            self.cancel()


@dataclass
class Verification:
    transaction_id: str
    user_id: str
    device_id: str
    keys: dict
    expires: float
    outgoing: bool = False
    state: str = "requested"
    sas: MatrixSas | None = None
    sent_done: bool = False
    peer_done: bool = False


class VerificationManager:
    def __init__(self, client):
        self.client = client
        self.sessions: dict[str, Verification] = {}
        self.finished: OrderedDict[str, float] = OrderedDict()
        self.lock = asyncio.Lock()

    def _expire(self):
        now = time.monotonic()
        for session in list(self.sessions.values()):
            if session.state not in ("done", "cancelled") and (session.expires <= now or session.sas is not None and session.sas.timed_out):
                try:
                    device = self._device(session.user_id, session.device_id)
                    if dict(device.keys) == session.keys:
                        self.client.outgoing_to_device_messages.append(ToDeviceMessage(PREFIX + "cancel", session.user_id, session.device_id, {"transaction_id": session.transaction_id, "code": "m.timeout", "reason": "Verification timed out"}))
                except ValueError:
                    pass
                self._finish(session, "cancelled")
        for transaction, expires in list(self.finished.items()):
            if expires <= now:
                self.finished.pop(transaction, None)
                self.sessions.pop(transaction, None)

    def _finish(self, session, state):
        session.state = state
        if state == "cancelled" and session.sas is not None:
            session.sas.cancel()
        self.finished[session.transaction_id] = time.monotonic() + 600
        while len(self.finished) > 128:
            transaction, _ = self.finished.popitem(last=False)
            self.sessions.pop(transaction, None)

    def _device(self, user_id, device_id):
        if not isinstance(user_id, str) or user_id not in self.client.device_store.users:
            raise ValueError("Dieses Matrix-Gerät ist nicht verfügbar.")
        device = self.client.device_store[user_id].get(device_id)
        if (device is None or device.deleted or device.blacklisted
                or not device.ed25519 or not device.curve25519
                or (user_id == self.client.user_id and device_id == self.client.device_id)):
            raise ValueError("Dieses Matrix-Gerät ist nicht verfügbar.")
        return device

    def _current_device(self, session):
        self._expire()
        if session.state in ("done", "cancelled"):
            raise ValueError("Diese Gerätebestätigung ist nicht mehr aktiv.")
        try:
            device = self._device(session.user_id, session.device_id)
        except ValueError:
            self._finish(session, "cancelled")
            raise
        if dict(device.keys) != session.keys:
            self._finish(session, "cancelled")
            raise ValueError("Der Geräteschlüssel hat sich geändert. Starte eine neue Bestätigung.")
        return device

    def _add(self, transaction, user_id, device_id, outgoing=False):
        self._expire()
        if (transaction in self.sessions or transaction in self.finished or transaction in self.client.key_verifications
                or sum(item.state not in ("done", "cancelled") for item in self.sessions.values()) >= 8):
            raise ValueError("Eine Gerätebestätigung ist bereits aktiv oder nicht mehr gültig.")
        device = self._device(user_id, device_id)
        session = Verification(transaction, user_id, device_id, dict(device.keys), time.monotonic() + 600, outgoing)
        self.sessions[transaction] = session
        return session

    def intercepts(self, event):
        source = getattr(event, "source", {})
        event_type = source.get("type", "")
        content = source.get("content", {})
        transaction = content.get("transaction_id") if isinstance(content, dict) else None
        return event_type in (PREFIX + "request", PREFIX + "ready", PREFIX + "done") or isinstance(transaction, str) and (transaction in self.sessions or transaction in self.finished)

    async def _send(self, session, kind, content):
        self._current_device(session)
        response = await self.client.to_device(ToDeviceMessage(PREFIX + kind, session.user_id, session.device_id, {"transaction_id": session.transaction_id, **content}))
        if isinstance(response, ErrorResponse):
            raise ValueError("Die Gerätebestätigung konnte nicht übertragen werden.")

    async def _send_sas(self, session, message):
        self._current_device(session)
        response = await self.client.to_device(message)
        if isinstance(response, ErrorResponse):
            raise ValueError("Die Gerätebestätigung konnte nicht übertragen werden.")

    @staticmethod
    def _supports_sas(methods):
        return isinstance(methods, list) and len(methods) <= 32 and all(isinstance(method, str) and len(method) <= 255 for method in methods) and "m.sas.v1" in methods

    async def _cancel(self, session, code="m.invalid_message"):
        try:
            await self._send(session, "cancel", {"code": code, "reason": "Verification failed"})
        except Exception:
            pass
        finally:
            self._finish(session, "cancelled")

    async def start(self, user_id, device_id):
        async with self.lock:
            session = self._add(secrets.token_urlsafe(24), user_id, device_id, True)
            session.state = "accepted"
            try:
                await self._send(session, "request", {"from_device": self.client.device_id, "methods": ["m.sas.v1"], "timestamp": int(time.time() * 1000)})
            except Exception:
                self._finish(session, "cancelled")
                raise

    async def action(self, transaction, action):
        async with self.lock:
            self._expire()
            session = self.sessions.get(transaction)
            if session is None or session.state in ("done", "cancelled"):
                raise ValueError("Diese Gerätebestätigung ist nicht mehr aktiv.")
            self._current_device(session)
            if action == "cancel":
                try:
                    await self._send(session, "cancel", {"code": "m.user", "reason": "Verification cancelled"})
                finally:
                    self._finish(session, "cancelled")
            elif action == "accept" and session.state == "requested":
                await self._send(session, "ready", {"from_device": self.client.device_id, "methods": ["m.sas.v1"]})
                self._current_device(session)
                session.state = "accepted"
            elif action == "confirm" and session.sas is not None and session.sas.established_sas is not None and not session.sas.sas_accepted:
                session.sas.accept_sas()
                try:
                    await self._send_sas(session, session.sas.get_mac())
                    self._current_device(session)
                    await self._complete(session)
                except Exception:
                    self._finish(session, "cancelled")
                    raise
            else:
                raise ValueError("Die Vergleichssymbole sind noch nicht verfügbar.")

    async def _complete(self, session):
        if session.sas is not None and session.sas.verified:
            self._current_device(session)
            if not session.sent_done:
                await self._send(session, "done", {})
                session.sent_done = True
            self._expire()
            if session.state == "cancelled":
                return
            device = self._current_device(session)
            self.client.verify_device(device)
            self._finish(session, "done")

    async def handle(self, event):
        if not self.intercepts(event):
            return
        async with self.lock:
            self._expire()
            source = event.source
            kind = source.get("type", "")[len(PREFIX):]
            content = source.get("content", {})
            transaction = content.get("transaction_id")
            if not isinstance(transaction, str) or not 1 <= len(transaction) <= 256:
                return
            if kind == "request":
                timestamp = content.get("timestamp")
                device_id = content.get("from_device")
                methods = content.get("methods")
                now = time.time() * 1000
                if (not isinstance(device_id, str) or not 1 <= len(device_id) <= 255
                        or not isinstance(timestamp, int) or isinstance(timestamp, bool) or not now - 600000 <= timestamp <= now + 60000
                        or not self._supports_sas(methods)):
                    return
                try:
                    self._add(transaction, event.sender, device_id)
                except ValueError:
                    pass
                return
            session = self.sessions.get(transaction)
            if session is None or session.state in ("done", "cancelled") or event.sender != session.user_id:
                return
            if kind in ("ready", "start") and content.get("from_device") != session.device_id:
                return
            if "from_device" in content and content["from_device"] != session.device_id:
                return
            try:
                device = self._current_device(session)
                if kind == "cancel":
                    self._finish(session, "cancelled")
                elif kind == "done":
                    session.peer_done = True
                elif kind == "ready" and session.outgoing and session.sas is None:
                    if not self._supports_sas(content.get("methods")):
                        raise ValueError("Unsupported verification method")
                    session.sas = MatrixSas(self.client.user_id, self.client.device_id, self.client.olm.account.identity_keys["ed25519"], device, transaction)
                    await self._send_sas(session, session.sas.start_verification())
                elif kind == "start" and session.state == "accepted":
                    if session.sas is not None:
                        own = (self.client.user_id, self.client.device_id)
                        other = (session.user_id, session.device_id)
                        if not session.sas.we_started_it or own < other:
                            return
                    session.sas = MatrixSas.from_start(self.client, device, event)
                    await self._send_sas(session, session.sas.accept_verification())
                elif session.sas is not None:
                    sas = session.sas
                    if kind == "accept" and sas.we_started_it:
                        sas.receive_accept_event(event)
                        if not sas.canceled:
                            await self._send_sas(session, sas.share_key())
                    elif kind == "key":
                        sas.receive_key_event(event)
                        if not sas.canceled and not sas.we_started_it:
                            await self._send_sas(session, sas.share_key())
                    elif kind == "mac":
                        sas.receive_mac_event(event)
                        await self._complete(session)
                    if sas.canceled:
                        raise ValueError("Verification failed")
            except (ValueError, LocalProtocolError):
                await self._cancel(session)

    def snapshot(self):
        self._expire()
        result = []
        for session in self.sessions.values():
            state = session.state
            emoji = []
            sas = session.sas
            if state not in ("done", "cancelled") and sas is not None:
                if sas.sas_accepted:
                    state = "confirmed"
                elif sas.established_sas is not None:
                    state = "emoji"
                    emoji = [{"emoji": symbol, "description": description} for symbol, description in sas.get_emoji()]
            result.append({"transaction_id": session.transaction_id, "user_id": session.user_id, "device_id": session.device_id, "state": state, "emoji": emoji})
        return result


class VerificationClient(AsyncClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.verification_manager = VerificationManager(self)
        self.add_to_device_callback(self.verification_manager.handle, ToDeviceEvent)

    def _handle_decrypt_to_device(self, event):
        if self.verification_manager.intercepts(event):
            return None
        return super()._handle_decrypt_to_device(event)
