from __future__ import annotations

import asyncio
import json
import logging
import os
import secrets
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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
)
from nio.exceptions import EncryptionError, LocalProtocolError

from .config import MatrixSettings

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
        client.add_event_callback(self._on_event, (*TEXT_EVENTS, MegolmEvent))
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
        self._initialized = True
        LOGGER.info("Matrix-Bot verbunden; Geräte-ID: %s", client.device_id)

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
        if room.room_id != self.room_id or self._baseline:
            return
        if isinstance(event, MegolmEvent):
            if event.sender != self.settings.user_id:
                source = dict(event.source)
                source["room_id"] = room.room_id
                self._encrypted[event.event_id] = source
            return
        message = self._message(event)
        if message is not None:
            self._messages.append(message)

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
            if source.get("room_id") != self.room_id:
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
            message = self._message(cleartext)
            if message is not None:
                self._messages.append(message)
            decrypted.append(event_id)
        return decrypted

    async def _fill_gap(self, response: Any, since: str | None) -> None:
        assert self.client is not None
        room_info = response.rooms.join.get(self.room_id)
        if not since or not room_info or not room_info.timeline.limited:
            return
        start = since
        end = room_info.timeline.prev_batch
        if not end:
            raise MatrixConnectionError("Matrix-Zeitlinie hat eine Lücke ohne Fortsetzungsmarke")
        room = self.client.rooms[self.room_id]
        previous_messages = self._messages
        self._messages = []
        for _ in range(100):
            page = self._require_ok(
                await self.client.room_messages(
                    self.room_id,
                    start=start,
                    end=end,
                    direction=MessageDirection.front,
                    limit=100,
                ),
                "Nachrichten-Nachladen",
            )
            for event in page.chunk:
                await self._on_event(room, event)
            next_start = page.end
            if not next_start or next_start == end or not page.chunk:
                self._messages.extend(previous_messages)
                return
            if next_start == start:
                raise MatrixConnectionError("Matrix-Nachladen liefert keinen Fortschritt")
            start = next_start
        raise MatrixConnectionError("Matrix-Nachrichtenlücke ist zu groß; keine Fortschrittsmarke gespeichert")

    async def run(
        self,
        on_batch: Callable[[list[MatrixMessage], str], Awaitable[None]],
        since: str | None,
        stop: asyncio.Event,
    ) -> None:
        cursor = since
        retry_delay = 2
        first_sync = True
        while not stop.is_set():
            try:
                if self.client is None or not self._initialized:
                    await self._login()
                assert self.client is not None
                self._baseline = cursor is None
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
                                "rooms": [self.room_id],
                                "ephemeral": {"types": []},
                                "state": {"lazy_load_members": True},
                                "timeline": {"limit": 100},
                            },
                        },
                    ),
                    "Synchronisierung",
                )
                await self._crypto_maintenance()
                await self._fill_gap(response, cursor)
                decrypted = await self._retry_encrypted() if cursor is not None else []
                if not self._baseline:
                    _write_private(self.data_dir / "pending_encrypted.json", self._encrypted)
                messages = list({message.event_id: message for message in self._messages}.values())
                messages.sort(key=lambda message: message.timestamp_ms)
            except (MatrixConnectionError, ClientError, asyncio.TimeoutError, OSError) as error:
                self.ready.clear()
                LOGGER.warning("Matrix-Verbindung unterbrochen (%s); erneuter Versuch", type(error).__name__)
                try:
                    await asyncio.wait_for(stop.wait(), retry_delay)
                except asyncio.TimeoutError:
                    pass
                retry_delay = min(retry_delay * 2, 60)
                continue
            await on_batch(messages, response.next_batch)
            cursor = response.next_batch
            for event_id in decrypted:
                self._encrypted.pop(event_id, None)
            _write_private(self.data_dir / "pending_encrypted.json", self._encrypted)
            self._baseline = False
            first_sync = False
            retry_delay = 2
            self.ready.set()

    async def send_text(self, text: str, transaction_id: str) -> None:
        await self.ready.wait()
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
