import asyncio
import hashlib
import json
import logging
import time
from pathlib import Path

from .config import Settings
from .formatting import matrix_message, mesh_messages
from .matrix import MatrixReceiver
from .security import ChannelSecurityError
from .state import State
from .transports import TransportUnavailable, create_transport
from .web import IngressServer


LOGGER = logging.getLogger(__name__)


async def pause(stop: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except TimeoutError:
        pass


class Bridge:
    def __init__(self, settings: Settings, data_dir: Path, manage_ingress: bool = True):
        self.settings = settings
        binding = json.dumps({
            "matrix": [settings.matrix.homeserver, settings.matrix.user_id, settings.matrix.room_id],
            "radio": [settings.meshtastic.channel_index, settings.meshtastic.channel_psk,
                      settings.meshtastic.mqtt_channel_name],
        }, sort_keys=True)
        self.state = State(data_dir / "bridge.sqlite3", hashlib.sha256(binding.encode()).hexdigest())
        self.matrix = MatrixReceiver(settings.matrix, data_dir)
        self.matrix.on_messenger_send = self._messenger_send
        self.radio = None
        self.radio_ready = asyncio.Event()
        self.stop = asyncio.Event()
        self._radio_blocked = asyncio.Event()
        self._radio_closed = False
        self._radio_state = "disabled"
        self._radio_reason = "Meshtastic ist deaktiviert. Matrix läuft eigenständig."
        if settings.meshtastic.enabled:
            try:
                settings.validate_radio()
                if not settings.matrix.room_id:
                    raise ValueError("Für die Funkweiterleitung muss ein Matrix-Raum ausgewählt sein.")
                self.radio = create_transport(settings.meshtastic, data_dir=data_dir)
            except (ChannelSecurityError, ValueError) as exc:
                self._block_radio(str(exc))
            except Exception as exc:
                self._block_radio(f"Meshtastic konnte nicht vorbereitet werden ({type(exc).__name__}).")
            else:
                self._set_radio_status("connecting", "Die Meshtastic-Verbindung wird hergestellt.")
        self.ingress = IngressServer(self.matrix, self.radio_status) if manage_ingress else None

    def radio_status(self) -> dict:
        self._check_bridge_destination()
        return {
            "enabled": self.settings.meshtastic.enabled,
            "state": self._radio_state,
            "reason": self._radio_reason,
        }

    def _set_radio_status(self, state: str, reason: str) -> None:
        if self._radio_blocked.is_set() and state != "blocked":
            return
        self._radio_state = state
        self._radio_reason = reason

    def _block_radio(self, reason: str) -> None:
        self._radio_blocked.set()
        self.radio_ready.clear()
        self._set_radio_status("blocked", reason)
        LOGGER.error("Meshtastic-Weiterleitung gesperrt: %s Matrix bleibt verfügbar.", reason)

    def _radio_running(self) -> bool:
        self._check_bridge_destination()
        return self.radio is not None and not self.stop.is_set() and not self._radio_blocked.is_set()

    def _check_bridge_destination(self) -> bool:
        problem = getattr(self.matrix, "bridge_error", None)
        if not problem:
            return True
        if self.settings.meshtastic.enabled and not self._radio_blocked.is_set():
            self._block_radio(problem)
        return False

    def _fresh(self, timestamp_ms: int) -> bool:
        age_ms = int(time.time() * 1000) - timestamp_ms
        return -60000 <= age_ms <= self.settings.forwarding.max_message_age_seconds * 1000

    async def _matrix_batch(self, events, next_batch: str) -> None:
        messages = []
        if self._check_bridge_destination() and self.radio is not None and not self._radio_blocked.is_set():
            messages = [
                (event.event_id, mesh_messages(event.body, event.sender, self.settings.forwarding), event.timestamp_ms)
                for event in events if self._fresh(event.timestamp_ms)
            ]
        self.state.matrix_batch(messages, next_batch)

    async def _messenger_send(self, event) -> None:
        if not self._check_bridge_destination() or self.radio is None or self._radio_blocked.is_set() or not self._fresh(event.timestamp_ms):
            return
        self.state.mesh_message(
            event.event_id,
            mesh_messages(event.body, event.sender, self.settings.forwarding),
            event.timestamp_ms,
        )

    async def _matrix_connection(self) -> None:
        while not self.stop.is_set():
            try:
                await self.matrix.run(self._matrix_batch, self.state.get("sync_token"), self.stop)
            except Exception as exc:
                self.matrix.ready.clear()
                self.matrix.error = f"Matrix-Verbindung unterbrochen ({type(exc).__name__})."
                LOGGER.warning("Matrix-Verbindung unterbrochen (%s); neuer Versuch folgt.", type(exc).__name__)
            if not self.stop.is_set():
                await pause(self.stop, 5)

    async def _radio_connection(self) -> None:
        while self._radio_running():
            try:
                self._set_radio_status("connecting", "Die Meshtastic-Verbindung wird hergestellt.")
                await self.radio.connect()
                await self.radio.check_channel()
                if not self._radio_running():
                    return
                self.radio_ready.set()
                self._set_radio_status("connected", "Privater Meshtastic-Channel verbunden.")
                while self._radio_running():
                    await pause(self.stop, 5)
                    if not self._radio_running():
                        return
                    await self.radio.check_channel()
                    if self._radio_running():
                        self.radio_ready.set()
                        self._set_radio_status("connected", "Privater Meshtastic-Channel verbunden.")
            except ChannelSecurityError as exc:
                self._block_radio(str(exc))
                return
            except Exception as exc:
                LOGGER.warning("Meshtastic-Verbindung unterbrochen (%s); neuer Versuch folgt.", type(exc).__name__)
                self.radio_ready.clear()
                self._set_radio_status("disconnected", "Meshtastic ist nicht verbunden; erneuter Versuch folgt.")
                await pause(self.stop, 5)

    async def _radio_receive(self) -> None:
        while self._radio_running():
            try:
                event = await self.radio.receive()
            except ChannelSecurityError as exc:
                self._block_radio(str(exc))
                return
            except TransportUnavailable:
                self.radio_ready.clear()
                self._set_radio_status("disconnected", "Meshtastic ist nicht verbunden; erneuter Versuch folgt.")
                await pause(self.stop, 2)
                continue
            if not self._radio_running() or not self._fresh(event.timestamp_ms):
                continue
            self.state.radio_message(
                event.event_id,
                matrix_message(event.body, event.sender, self.settings.forwarding),
                event.timestamp_ms,
            )

    async def _deliver_mesh(self) -> None:
        last_send = 0.0
        while self._radio_running():
            await self.radio_ready.wait()
            await self.matrix.ready.wait()
            if not self._radio_running():
                return
            message = self.state.next("mesh", self.settings.forwarding.max_message_age_seconds)
            if message is None:
                await pause(self.stop, 0.5)
                continue
            interval = self.settings.forwarding.message_interval_seconds
            await pause(self.stop, max(0, last_send + interval - time.monotonic()))
            if not self._radio_running():
                return
            if not self._fresh(message.timestamp_ms):
                continue
            claimed = False
            try:
                await self.radio.check_channel()
                if not self._radio_running():
                    return
                self.state.claim(message)
                claimed = True
                last_send = time.monotonic()
                await self.radio.send_text(message.part, self.settings.meshtastic.channel_index)
            except ChannelSecurityError as exc:
                if claimed:
                    self.state.retry(message)
                self._block_radio(str(exc))
                return
            except TransportUnavailable:
                if claimed:
                    self.state.retry(message)
                self.radio_ready.clear()
                self._set_radio_status("disconnected", "Meshtastic ist nicht verbunden; erneuter Versuch folgt.")
                await pause(self.stop, 2)
            except Exception as exc:
                if claimed:
                    self.state.uncertain(message)
                    LOGGER.error("Meshtastic-Übergabe unbestätigt (%s); keine automatische Wiederholung.", type(exc).__name__)
                else:
                    self.radio_ready.clear()
                    self._set_radio_status("disconnected", "Meshtastic ist nicht sendebereit; erneuter Versuch folgt.")
                    LOGGER.warning("Meshtastic ist nicht sendebereit (%s).", type(exc).__name__)
                    await pause(self.stop, 5)
            else:
                self.state.complete(message)
                LOGGER.info("Nachricht an Meshtastic übergeben.")

    async def _deliver_matrix(self) -> None:
        while not self.stop.is_set():
            if not self._check_bridge_destination():
                return
            message = self.state.next("matrix", self.settings.forwarding.max_message_age_seconds)
            if message is None:
                await pause(self.stop, 0.5)
                continue
            transaction = hashlib.sha256(f"{self.settings.matrix.room_id}:{message.event_id}".encode()).hexdigest()
            self.state.claim(message)
            try:
                lifetime = self.settings.forwarding.max_message_age_seconds
                remaining = lifetime - (time.time() - message.timestamp_ms / 1000)
                await asyncio.wait_for(self.matrix.send_text(message.part, transaction), timeout=max(0.1, min(45, remaining)))
            except Exception as exc:
                self.state.retry(message)
                LOGGER.warning("Matrix-Übergabe fehlgeschlagen (%s); erneuter Versuch mit gleicher Transaktions-ID.", type(exc).__name__)
                await pause(self.stop, 5)
            else:
                self.state.complete(message)
                LOGGER.info("Nachricht an Matrix übergeben.")

    async def _close_radio(self) -> None:
        if self.radio is not None and not self._radio_closed:
            self._radio_closed = True
            await self.radio.close()

    async def _radio_workers(self) -> None:
        tasks = [
            asyncio.create_task(self._radio_connection()),
            asyncio.create_task(self._radio_receive()),
            asyncio.create_task(self._deliver_mesh()),
            asyncio.create_task(self._radio_blocked.wait()),
            asyncio.create_task(self.stop.wait()),
        ]
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        except Exception as exc:
            self._block_radio(f"Meshtastic wurde angehalten ({type(exc).__name__}).")
        finally:
            self.radio_ready.clear()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self._close_radio()

    async def run(self) -> None:
        tasks = []
        try:
            if self.ingress is not None:
                await self.ingress.start()
            tasks.append(asyncio.create_task(self._matrix_connection()))
            if self.radio is not None:
                tasks.append(asyncio.create_task(self._radio_workers()))
            if self.settings.matrix.room_id:
                tasks.append(asyncio.create_task(self._deliver_matrix()))
            await self.stop.wait()
        finally:
            self.stop.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            cleanup = [self.matrix.close(), self._close_radio()]
            if self.ingress is not None:
                cleanup.append(self.ingress.close())
            await asyncio.gather(*cleanup, return_exceptions=True)
            self.state.close()
