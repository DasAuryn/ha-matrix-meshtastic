from __future__ import annotations

import asyncio
import json
import logging
import os
import secrets
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.parse import quote

from aiohttp import ClientError
from nio import (
    AsyncClient,
    AsyncClientConfig,
    ErrorResponse,
    MegolmEvent,
    MessageDirection,
    RoomMessageEmote,
    RoomMessageNotice,
    RoomMessageText,
    RoomMessage,
    RedactionEvent,
    RoomPreset,
)
from nio.exceptions import EncryptionError, LocalProtocolError
from nio.responses import EmptyResponse

from .config import MatrixSettings
from .chat_store import ChatStore

LOGGER = logging.getLogger(__name__)
TEXT_EVENTS = (RoomMessageText, RoomMessageNotice, RoomMessageEmote)


@dataclass(frozen=True)
class MatrixMessage:
    event_id: str
    sender: str
    body: str
    timestamp_ms: int


class MatrixSetupError(RuntimeError):
    pass


class MatrixConnectionError(RuntimeError):
    pass


def _write_private(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False)
        handle.flush()
        os.fsync(handle.fileno())
    temporary.chmod(0o600)
    temporary.replace(path)


def _read_private(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    path.chmod(0o600)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (ValueError, OSError) as error:
        raise MatrixSetupError("Matrix-Sitzungsspeicher kann nicht gelesen werden") from error


class MatrixReceiver:
    def __init__(self, settings: MatrixSettings, data_dir: Path):
        self.settings = settings
        self.data_dir = data_dir / "matrix"
        self.data_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.data_dir.chmod(0o700)
        self.crypto_dir = self.data_dir / "crypto"
        self.crypto_dir.mkdir(exist_ok=True, mode=0o700)
        self.crypto_dir.chmod(0o700)
        self.chat_store = ChatStore(self.data_dir / "chats.sqlite3", settings.homeserver, settings.user_id)
        if self.chat_store.account_changed:
            _write_private(self.data_dir / "pending_encrypted.json", {})
        self.client: AsyncClient | None = None
        self.room_id = settings.room_id
        self.ready = asyncio.Event()
        self._messages: list[MatrixMessage] = []
        self._encrypted: dict[str, dict[str, Any]] = _read_private(
            self.data_dir / "pending_encrypted.json", {}
        )
        self._baseline = False
        self._initialized = False
        self._send_lock = asyncio.Lock()
        self.error: str | None = None
        self.bridge_error: str | None = None
        self._direct_rooms: dict[str, list[str]] = self.chat_store.get_metadata("direct_rooms", {})
        self.on_messenger_send: Callable[[MatrixMessage], Awaitable[None]] | None = None

    def _new_client(self, device_id: str) -> AsyncClient:
        key_path = self.data_dir / "crypto_key.json"
        pickle_key = _read_private(key_path, None)
        if pickle_key is None:
            pickle_key = secrets.token_urlsafe(48)
            _write_private(key_path, pickle_key)
        client = AsyncClient(
            self.settings.homeserver,
            self.settings.user_id,
            device_id=device_id,
            store_path=str(self.crypto_dir),
            config=AsyncClientConfig(
                encryption_enabled=True,
                store_sync_tokens=False,
                pickle_key=pickle_key,
                max_limit_exceeded=0,
                max_timeouts=0,
                request_timeout=45,
            ),
        )
        client.add_event_callback(self._on_event, (RoomMessage, MegolmEvent, RedactionEvent))
        return client

    def _require_ok(self, response: Any, action: str) -> Any:
        if isinstance(response, ErrorResponse):
            code = response.status_code or "unbekannt"
            if code in ("M_UNKNOWN_TOKEN", "M_FORBIDDEN", "M_UNAUTHORIZED"):
                raise MatrixSetupError(f"Matrix {action} verweigert ({code})")
            raise MatrixConnectionError(f"Matrix {action} fehlgeschlagen ({code})")
        return response

    async def _login(self) -> None:
        if self.client is not None:
            await self.client.close()
        session_path = self.data_dir / "session.json"
        stored = _read_private(session_path, {})
        if (
            stored.get("homeserver") != self.settings.homeserver
            or stored.get("user_id") != self.settings.user_id
        ):
            stored = {}
        device_id = self.settings.device_id or stored.get("device_id", "")
        if self.settings.access_token and not device_id:
            raise MatrixSetupError("Matrix-Access-Token benötigt eine Geräte-ID")
        token = self.settings.access_token or stored.get("access_token", "")
        self.client = self._new_client(device_id)
        client = self.client
        if token:
            client.restore_login(self.settings.user_id, device_id, token)
            response = await client.whoami()
            if isinstance(response, ErrorResponse):
                if self.settings.access_token or not self.settings.password:
                    self._require_ok(response, "Anmeldung")
                if response.status_code not in ("M_UNKNOWN_TOKEN", "M_FORBIDDEN"):
                    self._require_ok(response, "Anmeldung")
                await client.close()
                client = self._new_client(device_id)
                self.client = client
                response = await client.login(
                    self.settings.password,
                    device_name="Home Assistant Matrix Meshtastic",
                )
                self._require_ok(response, "Anmeldung")
            elif (
                response.user_id != self.settings.user_id
                or getattr(response, "device_id", device_id) != device_id
            ):
                raise MatrixSetupError("Matrix-Token gehört nicht zum konfigurierten Konto/Gerät")
        else:
            if not self.settings.password:
                raise MatrixSetupError("Matrix-Passwort oder Access-Token fehlt")
            self._require_ok(
                await client.login(
                    self.settings.password,
                    device_name="Home Assistant Matrix Meshtastic",
                ),
                "Anmeldung",
            )
        _write_private(
            session_path,
            {
                "homeserver": self.settings.homeserver,
                "user_id": client.user_id,
                "device_id": client.device_id,
                "access_token": client.access_token,
            },
        )
        for path in self.crypto_dir.iterdir():
            if path.is_file():
                path.chmod(0o600)
        try:
            await self._bind_bridge_room(client)
            self.bridge_error = None
        except (MatrixSetupError, MatrixConnectionError, ClientError, asyncio.TimeoutError, OSError) as error:
            self.room_id = ""
            self.bridge_error = str(error) if isinstance(error, (MatrixSetupError, MatrixConnectionError)) else "Der konfigurierte Bridge-Raum ist vorübergehend nicht erreichbar."
            LOGGER.warning("Matrix-Bridge-Raum ist nicht verfügbar; Messenger bleibt aktiv.")
        self._initialized = True
        LOGGER.info("Matrix-Messenger verbunden; Geräte-ID: %s", client.device_id)

    async def _bind_bridge_room(self, client: AsyncClient) -> None:
        if not self.settings.room_id:
            self.room_id = ""
            return
        if self.settings.room_id.startswith("#"):
            resolved = self._require_ok(
                await client.room_resolve_alias(self.settings.room_id), "Raumauflösung"
            )
            self.room_id = resolved.room_id
        else:
            self.room_id = self.settings.room_id
        binding_path = self.data_dir / "room_binding.json"
        previous_binding = _read_private(binding_path, {})
        binding = {
            "homeserver": self.settings.homeserver,
            "user_id": self.settings.user_id,
            "configured_room": self.settings.room_id,
            "resolved_room": self.room_id,
        }
        same_destination = all(
            previous_binding.get(key) == binding[key]
            for key in ("homeserver", "user_id", "configured_room")
        )
        if same_destination and previous_binding.get("resolved_room") != self.room_id:
            raise MatrixSetupError(
                "Der Matrix-Raumalias zeigt auf einen anderen Raum; Weiterleitung gesperrt. "
                "Den gewünschten Raum ausdrücklich mit seiner Raum-ID konfigurieren."
            )
        joined = self._require_ok(await client.join(self.room_id), "Raumbeitritt")
        if joined.room_id != self.room_id:
            raise MatrixSetupError("Matrix-Raumbeitritt lieferte einen anderen Raum")
        _write_private(binding_path, binding)

    def _message(self, event: Any) -> MatrixMessage | None:
        if not isinstance(event, TEXT_EVENTS):
            return None
        if event.sender == self.settings.user_id:
            return None
        relation = event.source.get("content", {}).get("m.relates_to", {})
        if relation.get("rel_type") == "m.replace":
            return None
        if not event.body.strip():
            return None
        return MatrixMessage(event.event_id, event.sender, event.body, event.server_timestamp)

    async def _on_event(self, room: Any, event: Any) -> None:
        room_id = room.room_id
        if isinstance(event, RedactionEvent):
            self.chat_store.redact_message(room_id, event.redacts)
            return
        if isinstance(event, MegolmEvent):
            source = dict(event.source)
            source["room_id"] = room_id
            source["bridge_eligible"] = not self._baseline and room_id == self.room_id and event.sender != self.settings.user_id
            self._encrypted[event.event_id] = source
            self.chat_store.save_message(
                room_id, event.event_id, event.sender, self._displayname(room, event.sender),
                "Verschlüsselte Nachricht – Raumschlüssel fehlen", event.server_timestamp, True, True,
            )
            return
        self._store_event(room, event)
        if room_id != self.room_id or self._baseline:
            return
        message = self._message(event)
        if message is not None:
            self._messages.append(message)

    @staticmethod
    def _displayname(room: Any, sender: str) -> str:
        method = getattr(room, "user_name", None)
        return (method(sender) if callable(method) else None) or sender

    def _store_event(self, room: Any, event: Any) -> None:
        if not isinstance(event, RoomMessage):
            return
        content = event.source.get("content", {})
        relation = content.get("m.relates_to", {})
        if relation.get("rel_type") == "m.replace":
            new_content = content.get("m.new_content", {})
            body = new_content.get("body")
            if isinstance(body, str):
                self.chat_store.edit_message(room.room_id, relation.get("event_id", ""), event.sender, body)
            return
        body = getattr(event, "body", "")
        if not isinstance(body, str) or not body.strip():
            return
        if not isinstance(event, TEXT_EVENTS):
            body = f"[Anhang] {body}"
        self.chat_store.save_message(
            room.room_id, event.event_id, event.sender, self._displayname(room, event.sender),
            body, event.server_timestamp, bool(getattr(room, "encrypted", False)),
        )

    def _update_rooms(self, response: Any) -> None:
        for account_event in getattr(response, "account_data_events", []):
            if getattr(account_event, "type", None) == "m.direct":
                self._direct_rooms = {
                    user: [room for room in rooms if isinstance(room, str)]
                    for user, rooms in account_event.content.items()
                    if isinstance(user, str) and isinstance(rooms, list)
                }
                self.chat_store.set_metadata("direct_rooms", self._direct_rooms)
        direct_ids = {room for rooms in self._direct_rooms.values() for room in rooms}
        client = self.client
        for room_id, room in getattr(client, "rooms", {}).items():
            room_info = getattr(response.rooms, "join", {}).get(room_id)
            stored_room = self.chat_store.room(room_id)
            if stored_room and stored_room["membership"] == "leave" and room_info is None:
                continue
            unread = getattr(getattr(room_info, "unread_notifications", None), "notification_count", None)
            values = {
                "name": getattr(room, "display_name", None) or room_id,
                "encrypted": bool(getattr(room, "encrypted", False)),
                "membership": "join", "is_direct": room_id in direct_ids,
            }
            if unread is not None:
                values["unread_count"] = unread
            self.chat_store.upsert_room(room_id, **values)
            cursor = getattr(getattr(room_info, "timeline", None), "prev_batch", None)
            if cursor:
                self.chat_store.set_history_cursor(room_id, cursor, only_if_empty=True)
        for room_id, room in getattr(client, "invited_rooms", {}).items():
            info = getattr(response.rooms, "invite", {}).get(room_id)
            direct = room_id in direct_ids or any(
                getattr(event, "content", {}).get("is_direct", False)
                for event in getattr(info, "invite_state", [])
            )
            self.chat_store.upsert_room(room_id, name=getattr(room, "display_name", None) or room_id,
                                       membership="invite", is_direct=bool(direct), encrypted=bool(getattr(room, "encrypted", False)))
        for room_id in getattr(response.rooms, "leave", {}):
            self.chat_store.upsert_room(room_id, membership="leave")

    async def _crypto_maintenance(self) -> None:
        assert self.client is not None
        client = self.client
        if client.should_upload_keys:
            self._require_ok(await client.keys_upload(), "Schlüssel-Upload")
        if client.should_query_keys:
            self._require_ok(await client.keys_query(), "Schlüsselabfrage")
        if client.should_claim_keys:
            self._require_ok(
                await client.keys_claim(client.get_users_for_key_claiming()),
                "Schlüsselanforderung",
            )
        for response in await client.send_to_device_messages():
            self._require_ok(response, "Gerätenachricht")

    async def _retry_encrypted(self) -> list[str]:
        assert self.client is not None
        decrypted: list[str] = []
        for event_id, source in self._encrypted.items():
            room_id = source.get("room_id")
            if not isinstance(room_id, str) or not room_id:
                decrypted.append(event_id)
                continue
            event = MegolmEvent.from_dict(source)
            if not isinstance(event, MegolmEvent):
                decrypted.append(event_id)
                continue
            try:
                cleartext = self.client.decrypt_event(event)
            except EncryptionError:
                if event.session_id not in self.client.outgoing_key_requests:
                    try:
                        self._require_ok(
                            await self.client.request_room_key(event), "Raumschlüsselanforderung"
                        )
                        LOGGER.warning(
                            "Matrix-Raumschlüssel fehlen; Schlüssel beim Bot-Gerät freigeben"
                        )
                    except LocalProtocolError:
                        pass
                continue
            room = getattr(self.client, "rooms", {}).get(room_id) or SimpleNamespace(room_id=room_id, encrypted=True)
            self._store_event(room, cleartext)
            message = self._message(cleartext)
            eligible = source.get("bridge_eligible", room_id == self.room_id)
            if message is not None and eligible and room_id == self.room_id:
                self._messages.append(message)
            decrypted.append(event_id)
        return decrypted

    async def _fill_gap(self, response: Any, since: str | None) -> None:
        assert self.client is not None
        if not since:
            return
        previous_messages = self._messages
        self._messages = []
        for room_id, room_info in response.rooms.join.items():
            if not room_info.timeline.limited:
                continue
            start = since
            end = room_info.timeline.prev_batch
            if not end:
                raise MatrixConnectionError("Matrix-Zeitlinie hat eine Lücke ohne Fortsetzungsmarke")
            room = self.client.rooms[room_id]
            for _ in range(100):
                page = self._require_ok(await self.client.room_messages(
                    room_id, start=start, end=end, direction=MessageDirection.front, limit=100,
                ), "Nachrichten-Nachladen")
                for event in page.chunk:
                    await self._on_event(room, event)
                next_start = page.end
                if not next_start or next_start == end or not page.chunk:
                    break
                if next_start == start:
                    raise MatrixConnectionError("Matrix-Nachladen liefert keinen Fortschritt")
                start = next_start
            else:
                raise MatrixConnectionError("Matrix-Nachrichtenlücke ist zu groß; keine Fortschrittsmarke gespeichert")
        self._messages.extend(previous_messages)

    async def run(
        self,
        on_batch: Callable[[list[MatrixMessage], str], Awaitable[None]],
        since: str | None,
        stop: asyncio.Event,
    ) -> None:
        cursor = since or self.chat_store.get_metadata("sync_token")
        retry_delay = 2
        first_sync = True
        while not stop.is_set():
            try:
                if self.client is None or not self._initialized:
                    await self._login()
                assert self.client is not None
                self._baseline = cursor is None or (first_sync and since is None)
                self._messages = []
                self.client.next_batch = cursor or ""
                response = self._require_ok(
                    await self.client.sync(
                        timeout=0 if first_sync else 30000,
                        since=cursor,
                        full_state=first_sync,
                        set_presence="offline",
                        sync_filter={
                            "presence": {"types": []},
                            "room": {
                                "include_leave": True,
                                "ephemeral": {"types": []},
                                "state": {"lazy_load_members": True},
                                "timeline": {"limit": 100},
                            },
                        },
                    ),
                    "Synchronisierung",
                )
                self._update_rooms(response)
                await self._crypto_maintenance()
                await self._fill_gap(response, cursor)
                decrypted = await self._retry_encrypted()
                _write_private(self.data_dir / "pending_encrypted.json", self._encrypted)
                messages = list({message.event_id: message for message in self._messages}.values())
                messages.sort(key=lambda message: message.timestamp_ms)
            except (MatrixSetupError, MatrixConnectionError, ClientError, asyncio.TimeoutError, OSError) as error:
                self.ready.clear()
                self.error = str(error) if isinstance(error, (MatrixSetupError, MatrixConnectionError)) else "Matrix-Server ist vorübergehend nicht erreichbar."
                if isinstance(error, MatrixSetupError):
                    self._initialized = False
                LOGGER.warning("Matrix-Verbindung unterbrochen (%s); erneuter Versuch", type(error).__name__)
                try:
                    await asyncio.wait_for(stop.wait(), retry_delay)
                except asyncio.TimeoutError:
                    pass
                retry_delay = min(retry_delay * 2, 60)
                continue
            await on_batch(messages, response.next_batch)
            cursor = response.next_batch
            self.chat_store.set_metadata("sync_token", cursor)
            for event_id in decrypted:
                self._encrypted.pop(event_id, None)
            _write_private(self.data_dir / "pending_encrypted.json", self._encrypted)
            self._baseline = False
            first_sync = False
            retry_delay = 2
            self.error = None
            self.ready.set()

    def messenger_state(self) -> dict:
        devices = []
        verifications = []
        client = self.client
        if client is not None and getattr(client, "olm", None) is not None:
            for device in client.device_store:
                if device.deleted:
                    continue
                devices.append({
                    "user_id": device.user_id, "device_id": device.id,
                    "display_name": device.display_name or device.id,
                    "verified": bool(device.verified),
                    "own_device": device.user_id == self.settings.user_id and device.id == client.device_id,
                    "fingerprint": device.ed25519,
                })
            for transaction_id, sas in client.key_verifications.items():
                emoji = []
                if sas.verified:
                    state = "done"
                elif sas.canceled or sas.timed_out:
                    state = "cancelled"
                elif sas.sas_accepted:
                    state = "confirmed"
                elif sas.established_sas is not None:
                    state = "emoji"
                    emoji = [{"emoji": symbol, "description": description} for symbol, description in sas.get_emoji()]
                elif not sas.we_started_it and getattr(sas.state, "name", "") == "started":
                    state = "requested"
                else:
                    state = "accepted"
                verifications.append({
                    "transaction_id": transaction_id, "user_id": sas.other_olm_device.user_id,
                    "device_id": sas.other_olm_device.id, "state": state, "emoji": emoji,
                })
        return {
            "connected": self.ready.is_set(), "user_id": self.settings.user_id,
            "device_id": getattr(client, "device_id", self.settings.device_id) or "",
            "rooms": [room for room in self.chat_store.snapshot_rooms() if room["membership"] != "leave"],
            "verification": verifications, "devices": devices, "error": self.error,
            "bridge_error": self.bridge_error,
        }

    def _require_ready(self) -> AsyncClient:
        if not self.ready.is_set() or self.client is None:
            raise MatrixConnectionError("Matrix ist noch nicht verbunden.")
        return self.client

    def _require_joined(self, room_id: str) -> Any:
        client = self._require_ready()
        room = getattr(client, "rooms", {}).get(room_id)
        stored_room = self.chat_store.room(room_id)
        if room is None or stored_room is None or stored_room["membership"] != "join":
            raise MatrixSetupError("Dieser Matrix-Raum ist nicht beigetreten.")
        return room

    async def room_history(self, room_id: str, before: str | None = None, limit: int = 50) -> dict:
        known = self.chat_store.room(room_id)
        if known is None or known["membership"] == "leave":
            raise MatrixSetupError("Dieser Matrix-Raum ist nicht verfügbar.")
        limit = min(max(int(limit), 1), 200)
        if before is None or before.startswith("local:"):
            return self.chat_store.get_messages(room_id, limit, before)
        room = self._require_joined(room_id)
        response = self._require_ok(await self.client.room_messages(
            room_id, start=before, direction=MessageDirection.back, limit=limit,
        ), "Nachrichten-Nachladen")
        messages = []
        for event in response.chunk:
            if isinstance(event, MegolmEvent):
                source = dict(event.source)
                source["room_id"] = room_id
                source["bridge_eligible"] = False
                self._encrypted[event.event_id] = source
                await self._on_event(room, event)
                self._encrypted[event.event_id]["bridge_eligible"] = False
            else:
                self._store_event(room, event)
            message = self.chat_store.message(room_id, getattr(event, "event_id", ""))
            if message:
                messages.append(message)
        _write_private(self.data_dir / "pending_encrypted.json", self._encrypted)
        next_batch = response.end if response.end != before and response.chunk else None
        self.chat_store.set_history_cursor(room_id, next_batch)
        return {"messages": sorted(messages, key=lambda message: message["timestamp_ms"]), "next_batch": next_batch}

    async def send_room_text(self, room_id: str, text: str, transaction_id: str) -> dict:
        room = self._require_joined(room_id)
        if not isinstance(text, str) or not text.strip() or len(text.encode()) > 32768:
            raise ValueError("Eine Nachricht mit höchstens 32768 Bytes wird benötigt.")
        if not isinstance(transaction_id, str) or not 1 <= len(transaction_id) <= 128:
            raise ValueError("Ungültige Nachrichten-Transaktions-ID.")
        async with self._send_lock:
            try:
                response = self._require_ok(await self.client.room_send(
                    room_id, "m.room.message", {"msgtype": "m.text", "body": text}, tx_id=transaction_id,
                ), "Nachrichtenversand")
            except (EncryptionError, LocalProtocolError) as exc:
                raise MatrixSetupError("Die Geräte dieses verschlüsselten Chats müssen zuerst verifiziert werden.") from exc
        timestamp = int(time.time() * 1000)
        self.chat_store.save_message(room_id, response.event_id, self.settings.user_id,
                                     self._displayname(room, self.settings.user_id), text, timestamp, bool(room.encrypted))
        if room_id == self.room_id and self.on_messenger_send is not None:
            await self.on_messenger_send(MatrixMessage(response.event_id, self.settings.user_id, text, timestamp))
        return {"event_id": response.event_id}

    async def join_room(self, room_id: str) -> dict:
        client = self._require_ready()
        known = self.chat_store.room(room_id)
        if known is None or known["membership"] != "invite":
            raise MatrixSetupError("Für diesen Raum liegt keine Einladung vor.")
        response = self._require_ok(await client.join(room_id), "Raumbeitritt")
        self.chat_store.upsert_room(response.room_id, membership="join")
        return {"room_id": response.room_id}

    async def create_direct(self, user_id: str) -> dict:
        client = self._require_ready()
        if not isinstance(user_id, str) or not user_id.startswith("@") or ":" not in user_id or any(char.isspace() for char in user_id):
            raise ValueError("Die vollständige Matrix-Benutzer-ID wird benötigt.")
        for room_id in self._direct_rooms.get(user_id, []):
            stored_room = self.chat_store.room(room_id)
            if room_id in client.rooms and stored_room is not None and stored_room["membership"] == "join":
                return {"room_id": room_id}
        response = self._require_ok(await client.room_create(
            is_direct=True, preset=RoomPreset.trusted_private_chat, invite=[user_id],
            initial_state=[{"type": "m.room.encryption", "state_key": "", "content": {"algorithm": "m.megolm.v1.aes-sha2"}}],
        ), "Direktnachricht erstellen")
        self._direct_rooms.setdefault(user_id, []).append(response.room_id)
        path = f"/_matrix/client/v3/user/{quote(self.settings.user_id, safe='')}/account_data/m.direct"
        self._require_ok(await client._send(EmptyResponse, "PUT", path, json.dumps(self._direct_rooms)), "Direktchat-Zuordnung")
        self.chat_store.set_metadata("direct_rooms", self._direct_rooms)
        self.chat_store.upsert_room(response.room_id, name=user_id, is_direct=True, encrypted=True, membership="join")
        return {"room_id": response.room_id}

    async def mark_read(self, room_id: str) -> None:
        self._require_joined(room_id)
        messages = self.chat_store.get_messages(room_id, 1)["messages"]
        if messages:
            marker = messages[-1]["event_id"]
            self._require_ok(await self.client.room_read_markers(room_id, fully_read_event=marker, read_event=marker), "Lesebestätigung")
        self.chat_store.upsert_room(room_id, unread_count=0)

    async def start_verification(self, user_id: str, device_id: str) -> dict:
        client = self._require_ready()
        if not getattr(client, "olm", None):
            raise MatrixSetupError("Die Matrix-Verschlüsselung ist noch nicht bereit.")
        device = client.device_store[user_id].get(device_id)
        if device is None or device.deleted or (user_id == client.user_id and device_id == client.device_id):
            raise MatrixSetupError("Dieses Matrix-Gerät ist nicht verfügbar.")
        self._require_ok(await client.start_key_verification(device), "Geräteverifikation")
        return {"verification": self.messenger_state()["verification"]}

    async def verification_action(self, transaction_id: str, action: str) -> dict:
        client = self._require_ready()
        sas = client.key_verifications.get(transaction_id)
        if sas is None or sas.canceled or sas.timed_out or sas.verified:
            raise MatrixSetupError("Diese Geräteverifikation ist nicht mehr aktiv.")
        if action == "accept":
            response = await client.accept_key_verification(transaction_id)
        elif action == "confirm":
            if sas.established_sas is None:
                raise MatrixSetupError("Die Vergleichssymbole sind noch nicht verfügbar.")
            response = await client.confirm_short_auth_string(transaction_id)
        elif action == "cancel":
            response = await client.cancel_key_verification(transaction_id)
        else:
            raise ValueError("Unbekannte Verifikationsaktion.")
        self._require_ok(response, "Geräteverifikation")
        return {"verification": self.messenger_state()["verification"]}

    async def send_text(self, text: str, transaction_id: str) -> None:
        await self.ready.wait()
        if not self.room_id:
            raise MatrixSetupError("Der konfigurierte Matrix-Bridge-Raum ist nicht verfügbar.")
        async with self._send_lock:
            if self.client is None or not self.ready.is_set():
                raise MatrixConnectionError("Matrix ist noch nicht verbunden")
            self._require_ok(
                await self.client.room_send(
                    self.room_id,
                    "m.room.message",
                    {"msgtype": "m.text", "body": text},
                    tx_id=transaction_id,
                ),
                "Nachrichtenversand",
            )

    async def close(self) -> None:
        self.ready.clear()
        if self.client is not None:
            await self.client.close()
            self.client = None
        self._initialized = False
        self.chat_store.close()
