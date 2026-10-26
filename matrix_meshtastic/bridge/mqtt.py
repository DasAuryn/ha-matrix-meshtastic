import asyncio
import logging
import secrets
import sqlite3
import struct
import time
from pathlib import Path

import paho.mqtt.client as mqtt
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from google.protobuf.message import DecodeError
from meshtastic.protobuf import mesh_pb2, mqtt_pb2, portnums_pb2

from .config import RadioSettings
from .security import ChannelSecurityError, decode_private_psk
from .transports import RadioMessage, TransportUnavailable


LOGGER = logging.getLogger(__name__)
BROADCAST = 0xFFFFFFFF


def channel_hash(name: str, key: bytes) -> int:
    result = 0
    for value in name.encode("utf-8") + key:
        result ^= value
    return result


def crypt_payload(payload: bytes, key: bytes, sender: int, packet_id: int) -> bytes:
    nonce = struct.pack("<QI", packet_id, sender) + bytes(4)
    encryptor = Cipher(algorithms.AES(key), modes.CTR(nonce)).encryptor()
    return encryptor.update(payload) + encryptor.finalize()


class MqttTransport:
    def __init__(self, settings: RadioSettings, data_dir: Path | None = None):
        self.settings = settings
        self.key = decode_private_psk(settings.channel_psk)
        self.hash = channel_hash(settings.mqtt_channel_name, self.key)
        self.gateway_id = settings.mqtt_gateway_id.lower()
        directory = data_dir or Path("/data")
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.database = sqlite3.connect(directory / "mqtt_identity.sqlite3")
        self.database.execute("PRAGMA synchronous=FULL")
        self.database.execute("CREATE TABLE IF NOT EXISTS identity (sender INTEGER NOT NULL, next_id INTEGER NOT NULL)")
        row = self.database.execute("SELECT sender FROM identity").fetchone()
        if row is None:
            sender = self._new_sender()
            with self.database:
                self.database.execute("INSERT INTO identity VALUES (?, 1)", (sender,))
        else:
            sender = row[0]
        self.sender = sender
        self.bridge_id = f"!{sender:08x}"
        if self.bridge_id == self.gateway_id:
            raise ChannelSecurityError("MQTT-Bridge-ID und Gateway-ID dürfen nicht gleich sein.")
        base = f"{settings.mqtt_topic.rstrip('/')}/{settings.mqtt_channel_name}"
        self.inbound_topic = f"{base}/{self.gateway_id}"
        self.outbound_topic = f"{base}/{self.bridge_id}"
        self.queue: asyncio.Queue[RadioMessage] = asyncio.Queue(maxsize=256)
        self.connected = asyncio.Event()
        self.loop: asyncio.AbstractEventLoop | None = None
        self.client: mqtt.Client | None = None

    def _new_sender(self) -> int:
        while True:
            sender = secrets.randbelow(0xFFFFFFFB) + 4
            if f"!{sender:08x}" != self.gateway_id:
                return sender

    async def connect(self) -> None:
        if self.client is not None and self.connected.is_set() and self.client.is_connected():
            return
        if self.client is not None:
            await self._disconnect()
        self.loop = asyncio.get_running_loop()
        self.connected.clear()
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"matrix-mesh-{self.bridge_id[1:]}")
        client.enable_logger(logging.getLogger("bridge.mqtt.protocol"))
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_subscribe = self._on_subscribe
        client.on_message = self._on_message
        if self.settings.mqtt_username:
            client.username_pw_set(self.settings.mqtt_username, self.settings.mqtt_password)
        if self.settings.mqtt_tls:
            client.tls_set()
        client.reconnect_delay_set(min_delay=2, max_delay=60)
        self.client = client
        try:
            await asyncio.to_thread(client.connect, self.settings.mqtt_host, self.settings.mqtt_port, 60)
            client.loop_start()
            await asyncio.wait_for(self.connected.wait(), timeout=30)
        except Exception:
            await self._disconnect()
            raise TransportUnavailable("MQTT-Verbindung konnte nicht hergestellt werden.") from None

    def _dispatch(self, callback, *args) -> None:
        if self.loop and not self.loop.is_closed():
            self.loop.call_soon_threadsafe(callback, *args)

    def _on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        if reason_code.is_failure:
            LOGGER.error("MQTT-Verbindung wurde vom Broker abgelehnt.")
            return
        result, _ = client.subscribe(self.inbound_topic, qos=1)
        if result != mqtt.MQTT_ERR_SUCCESS:
            LOGGER.error("MQTT-Empfang konnte nicht abonniert werden.")

    def _on_subscribe(self, client, userdata, mid, reason_codes, properties) -> None:
        if any(code.is_failure for code in reason_codes):
            LOGGER.error("MQTT-Broker hat das Abonnement abgelehnt.")
            return
        self._dispatch(self.connected.set)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties) -> None:
        self._dispatch(self.connected.clear)

    def _on_message(self, client, userdata, message) -> None:
        if message.retain or message.topic != self.inbound_topic or len(message.payload) > 4096:
            return
        try:
            envelope = mqtt_pb2.ServiceEnvelope.FromString(message.payload)
            packet = envelope.packet
            if (
                envelope.gateway_id.lower() != self.gateway_id
                or envelope.channel_id != self.settings.mqtt_channel_name
                or packet.WhichOneof("payload_variant") != "encrypted"
                or packet.channel != self.hash
                or packet.pki_encrypted
                or packet.to != BROADCAST
                or getattr(packet, "from") in (0, self.sender)
                or not packet.id
            ):
                return
            data = mesh_pb2.Data.FromString(crypt_payload(packet.encrypted, self.key, getattr(packet, "from"), packet.id))
            if data.portnum != portnums_pb2.TEXT_MESSAGE_APP or not 0 < len(data.payload) <= 233:
                return
            body = data.payload.decode("utf-8")
            sender = f"!{getattr(packet, 'from'):08x}"
            event = RadioMessage(
                event_id=f"{getattr(packet, 'from'):08x}:{packet.id:08x}",
                sender=sender,
                body=body,
                timestamp_ms=(packet.rx_time * 1000 if packet.rx_time else int(time.time() * 1000)),
            )
        except (DecodeError, UnicodeError, ValueError):
            LOGGER.debug("Ungültiges verschlüsseltes MQTT-Paket verworfen.")
            return
        self._dispatch(self._enqueue, event)

    def _enqueue(self, event: RadioMessage) -> None:
        try:
            self.queue.put_nowait(event)
        except asyncio.QueueFull:
            LOGGER.warning("Meshtastic-Empfangspuffer ist voll; MQTT-Nachricht verworfen.")

    async def check_channel(self) -> None:
        decode_private_psk(self.settings.channel_psk)
        if not self.client or not self.connected.is_set() or not self.client.is_connected():
            raise TransportUnavailable("Der MQTT-Broker ist nicht verbunden.")

    async def send_text(self, text: str, channel_index: int) -> None:
        if channel_index != self.settings.channel_index:
            raise ChannelSecurityError("Senden auf einen anderen Channel wurde blockiert.")
        payload = text.encode("utf-8")
        if not 0 < len(payload) <= 233:
            raise ValueError("Die Meshtastic-Nachricht überschreitet die Paketgröße.")
        await self.check_channel()
        with self.database:
            packet_id = self.database.execute("SELECT next_id FROM identity").fetchone()[0]
            if packet_id > 0xFFFFFFFF:
                raise ChannelSecurityError("Der MQTT-Paketnummernbereich ist ausgeschöpft; Senden gesperrt.")
            self.database.execute("UPDATE identity SET next_id = ?", (packet_id + 1,))
        data = mesh_pb2.Data(portnum=portnums_pb2.TEXT_MESSAGE_APP, payload=payload)
        packet = mesh_pb2.MeshPacket(
            to=BROADCAST,
            id=packet_id,
            channel=self.hash,
            encrypted=crypt_payload(data.SerializeToString(), self.key, self.sender, packet_id),
            hop_limit=3,
            hop_start=3,
            want_ack=False,
        )
        setattr(packet, "from", self.sender)
        envelope = mqtt_pb2.ServiceEnvelope(packet=packet, channel_id=self.settings.mqtt_channel_name, gateway_id=self.bridge_id)
        info = self.client.publish(self.outbound_topic, envelope.SerializeToString(), qos=0, retain=False)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError("Die MQTT-Übergabe konnte nicht bestätigt werden.")
        await asyncio.to_thread(info.wait_for_publish, 15)
        if not info.is_published():
            raise RuntimeError("Die MQTT-Übergabe ist unbestätigt.")

    async def receive(self) -> RadioMessage:
        return await self.queue.get()

    async def _disconnect(self) -> None:
        client, self.client = self.client, None
        self.connected.clear()
        if client:
            await asyncio.to_thread(client.disconnect)
            await asyncio.to_thread(client.loop_stop)

    async def close(self) -> None:
        await self._disconnect()
        self.database.close()
