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


LOGGER = logging.getLogger(__name__)


async def pause(stop: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except TimeoutError:
        pass


class Bridge:
    def __init__(self, settings: Settings, data_dir: Path):
        self.settings = settings
        binding = json.dumps({
            "matrix": [settings.matrix.homeserver, settings.matrix.user_id, settings.matrix.room_id],
            "radio": [settings.meshtastic.channel_index, settings.meshtastic.channel_psk,
                      settings.meshtastic.mqtt_channel_name],
        }, sort_keys=True)
        self.state = State(data_dir / "bridge.sqlite3", hashlib.sha256(binding.encode()).hexdigest())
        self.radio = create_transport(settings.meshtastic, data_dir=data_dir)
        self.matrix = MatrixReceiver(settings.matrix, data_dir)
        self.radio_ready = asyncio.Event()
        self.stop = asyncio.Event()

    def _fresh(self, timestamp_ms: int) -> bool:
        age_ms = int(time.time() * 1000) - timestamp_ms
        return -60000 <= age_ms <= self.settings.forwarding.max_message_age_seconds * 1000

    async def _matrix_batch(self, events, next_batch: str) -> None:
        messages = [
            (event.event_id, mesh_messages(event.body, event.sender, self.settings.forwarding), event.timestamp_ms)
            for event in events if self._fresh(event.timestamp_ms)
        ]
        self.state.matrix_batch(messages, next_batch)

    async def _radio_connection(self) -> None:
        while not self.stop.is_set():
            try:
                await self.radio.connect()
                await self.radio.check_channel()
                self.radio_ready.set()
                while not self.stop.is_set():
                    await pause(self.stop, 5)
                    await self.radio.check_channel()
                    self.radio_ready.set()
            except ChannelSecurityError:
                raise
            except Exception as exc:
                LOGGER.warning("Meshtastic-Verbindung unterbrochen (%s); neuer Versuch folgt.", type(exc).__name__)
                self.radio_ready.clear()
                await pause(self.stop, 5)

    async def _radio_receive(self) -> None:
        while not self.stop.is_set():
            try:
                event = await self.radio.receive()
            except TransportUnavailable:
                self.radio_ready.clear()
                await pause(self.stop, 2)
                continue
            if not self._fresh(event.timestamp_ms):
                continue
            self.state.radio_message(
                event.event_id,
                matrix_message(event.body, event.sender, self.settings.forwarding),
                event.timestamp_ms,
            )

    async def _deliver_mesh(self) -> None:
        last_send = 0.0
        while not self.stop.is_set():
            await self.radio_ready.wait()
            message = self.state.next("mesh", self.settings.forwarding.max_message_age_seconds)
            if message is None:
                await pause(self.stop, 0.5)
                continue
            interval = self.settings.forwarding.message_interval_seconds
            await pause(self.stop, max(0, last_send + interval - time.monotonic()))
            if self.stop.is_set():
                return
            if not self._fresh(message.timestamp_ms):
                continue
            claimed = False
            try:
                await self.radio.check_channel()
                self.state.claim(message)
                claimed = True
                last_send = time.monotonic()
                await self.radio.send_text(message.part, self.settings.meshtastic.channel_index)
            except ChannelSecurityError:
                if claimed:
                    self.state.retry(message)
                raise
            except TransportUnavailable:
                if claimed:
                    self.state.retry(message)
                self.radio_ready.clear()
                await pause(self.stop, 2)
            except Exception as exc:
                if claimed:
                    self.state.uncertain(message)
                    LOGGER.error("Meshtastic-Übergabe unbestätigt (%s); keine automatische Wiederholung.", type(exc).__name__)
                else:
                    self.radio_ready.clear()
                    LOGGER.warning("Meshtastic ist nicht sendebereit (%s).", type(exc).__name__)
                    await pause(self.stop, 5)
            else:
                self.state.complete(message)
                LOGGER.info("Nachricht an Meshtastic übergeben.")

    async def _deliver_matrix(self) -> None:
        while not self.stop.is_set():
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

    async def run(self) -> None:
        tasks = [
            asyncio.create_task(self.matrix.run(self._matrix_batch, self.state.get("sync_token"), self.stop)),
            asyncio.create_task(self._radio_connection()),
            asyncio.create_task(self._radio_receive()),
            asyncio.create_task(self._deliver_mesh()),
            asyncio.create_task(self._deliver_matrix()),
            asyncio.create_task(self.stop.wait()),
        ]
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        finally:
            self.stop.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self.matrix.close()
            await self.radio.close()
            self.state.close()
