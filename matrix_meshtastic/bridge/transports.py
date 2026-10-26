from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from .config import RadioSettings

LOGGER = logging.getLogger(__name__)


class TransportUnavailable(Exception):
    pass


@dataclass(frozen=True)
class RadioMessage:
    event_id: str
    sender: str
    body: str
    timestamp_ms: int


class RadioTransport(Protocol):
    async def connect(self) -> None: ...

    async def check_channel(self) -> None: ...

    async def send_text(self, text: str, channel_index: int) -> None: ...

    async def receive(self) -> RadioMessage: ...

    async def close(self) -> None: ...


class MeshTransport:
    def __init__(self, settings: RadioSettings) -> None:
        from .security import decode_private_psk

        self.settings = settings
        self._expected_psk = decode_private_psk(settings.channel_psk)
        self._interface = None
        self._loop = None
        self._lock = asyncio.Lock()
        self._messages: asyncio.Queue[RadioMessage | Exception] = asyncio.Queue(256)
        self._subscribed = False

    async def connect(self) -> None:
        self._loop = asyncio.get_running_loop()
        async with self._lock:
            if self._is_connected():
                await asyncio.to_thread(self._check_channel)
                return
            await asyncio.to_thread(self._close_interface)
            try:
                self._interface = await asyncio.to_thread(self._create_interface)
            except (Exception, SystemExit) as exc:
                raise TransportUnavailable("Meshtastic connection failed") from exc
            try:
                await asyncio.to_thread(self._check_channel)
            except Exception:
                await asyncio.to_thread(self._close_interface)
                raise
            if not self._subscribed:
                from pubsub import pub

                pub.subscribe(self._on_receive, "meshtastic.receive.text")
                self._subscribed = True

    def _create_interface(self):
        if self.settings.connection in {"usb", "serial"}:
            from meshtastic.serial_interface import SerialInterface

            return SerialInterface(devPath=self.settings.serial_device, timeout=30)
        if self.settings.connection == "tcp":
            from meshtastic.tcp_interface import TCPInterface

            return TCPInterface(
                hostname=self.settings.tcp_host,
                portNumber=self.settings.tcp_port,
                timeout=30,
            )
        if self.settings.connection in {"bluetooth", "ble"}:
            from meshtastic.ble_interface import BLEInterface

            return BLEInterface(address=self.settings.ble_address, timeout=30)
        raise ValueError("Unknown Meshtastic connection type")

    def _is_connected(self) -> bool:
        interface = self._interface
        return bool(
            interface is not None
            and interface.isConnected.is_set()
            and not getattr(interface, "_wantExit", False)
            and not getattr(interface, "failure", None)
        )

    def _check_channel(self) -> None:
        from .security import require_private_channel

        if not self._is_connected():
            raise TransportUnavailable("Meshtastic is not connected")
        channels = self._interface.localNode.channels or []
        require_private_channel(
            channels, self.settings.channel_index, self._expected_psk
        )

    async def check_channel(self) -> None:
        async with self._lock:
            await asyncio.to_thread(self._check_channel)

    def _refresh_channel(self) -> None:
        if not self._is_connected():
            raise TransportUnavailable("Meshtastic is not connected")
        node = self._interface.localNode
        try:
            node.requestChannels()
            ready = node.waitForConfig()
        except (Exception, SystemExit) as exc:
            raise TransportUnavailable("Meshtastic channel refresh failed") from exc
        if not ready:
            raise TransportUnavailable("Meshtastic channel refresh timed out")
        self._check_channel()

    async def send_text(self, text: str, channel_index: int) -> None:
        if channel_index != self.settings.channel_index:
            raise ValueError("Sending on another channel is forbidden")
        if not text or len(text.encode("utf-8")) > 233:
            raise ValueError("Meshtastic text must contain between 1 and 233 bytes")
        async with self._lock:
            await asyncio.to_thread(self._send_text, text, channel_index)

    def _send_text(self, text: str, channel_index: int) -> None:
        self._refresh_channel()
        self._interface.sendText(
            text,
            destinationId="^all",
            wantAck=False,
            channelIndex=channel_index,
        )

    def _on_receive(self, packet, interface) -> None:
        if interface is not self._interface or not self._loop or self._loop.is_closed():
            return
        if packet.get("channel", 0) != self.settings.channel_index:
            return
        if packet.get("to") != 0xFFFFFFFF:
            return
        decoded = packet.get("decoded", {})
        body = decoded.get("text")
        if not isinstance(body, str) or not body:
            return
        sender_num = packet.get("from")
        packet_id = packet.get("id")
        if not isinstance(sender_num, int) or not isinstance(packet_id, int):
            return
        my_info = getattr(interface, "myInfo", None)
        if my_info is not None and sender_num == my_info.my_node_num:
            return
        timestamp = packet.get("rxTime")
        if not isinstance(timestamp, (int, float)) or timestamp <= 0:
            timestamp = time.time()
        message = RadioMessage(
            event_id=f"{sender_num:08x}:{packet_id:08x}",
            sender=f"!{sender_num:08x}",
            body=body,
            timestamp_ms=int(timestamp * 1000),
        )
        self._loop.call_soon_threadsafe(self._enqueue, message)

    def _enqueue(self, value: RadioMessage | Exception) -> None:
        try:
            self._messages.put_nowait(value)
        except asyncio.QueueFull:
            LOGGER.error("Meshtastic receive queue is full; packet dropped")

    async def receive(self) -> RadioMessage:
        value = await self._messages.get()
        if isinstance(value, Exception):
            raise value
        async with self._lock:
            await asyncio.to_thread(self._refresh_channel)
        return value

    def _close_interface(self) -> None:
        interface, self._interface = self._interface, None
        if interface is not None:
            with contextlib.suppress(Exception):
                interface.close()

    async def close(self) -> None:
        async with self._lock:
            if self._subscribed:
                from pubsub import pub

                pub.unsubscribe(self._on_receive, "meshtastic.receive.text")
                self._subscribed = False
            await asyncio.to_thread(self._close_interface)


def create_transport(
    settings: RadioSettings, data_dir: Path | None = None
) -> RadioTransport:
    if settings.connection == "mqtt":
        from .mqtt import MqttTransport

        return MqttTransport(settings, data_dir=data_dir)
    return MeshTransport(settings)
