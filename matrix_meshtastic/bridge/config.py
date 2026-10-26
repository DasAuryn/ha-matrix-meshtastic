import json
import math
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse

from .security import decode_private_psk


@dataclass(frozen=True)
class MatrixSettings:
    homeserver: str = "https://matrix.org"
    user_id: str = ""
    password: str = field(default="", repr=False)
    access_token: str = field(default="", repr=False)
    device_id: str = ""
    room_id: str = ""


@dataclass(frozen=True)
class RadioSettings:
    connection: str = "tcp"
    channel_index: int = 0
    channel_psk: str = field(default="", repr=False)
    tcp_host: str = ""
    tcp_port: int = 4403
    serial_device: str = ""
    ble_address: str = ""
    mqtt_host: str = ""
    mqtt_port: int = 1883
    mqtt_username: str = ""
    mqtt_password: str = field(default="", repr=False)
    mqtt_tls: bool = False
    mqtt_topic: str = "msh/EU_868/2/e"
    mqtt_gateway_id: str = ""
    mqtt_channel_name: str = ""


@dataclass(frozen=True)
class ForwardingSettings:
    include_sender: bool = True
    max_message_age_seconds: int = 300
    message_interval_seconds: float = 5
    long_message: str = "split"
    max_parts: int = 4


@dataclass(frozen=True)
class Settings:
    matrix: MatrixSettings
    meshtastic: RadioSettings
    forwarding: ForwardingSettings

    @classmethod
    def load(cls, path: Path) -> "Settings":
        try:
            raw = json.loads(path.read_text())
            settings = cls(
                MatrixSettings(**raw.get("matrix", {})),
                RadioSettings(**raw.get("meshtastic", {})),
                ForwardingSettings(**raw.get("forwarding", {})),
            )
        except (OSError, ValueError, TypeError, AttributeError) as exc:
            raise ValueError("Die Add-on-Konfiguration konnte nicht gelesen werden.") from exc
        settings.validate()
        return settings

    def validate(self) -> None:
        matrix = self.matrix
        if not all(isinstance(value, str) for value in vars(matrix).values()):
            raise ValueError("Die Matrix-Einstellungen müssen Zeichenketten sein.")
        server = urlparse(matrix.homeserver)
        if server.scheme not in ("http", "https") or not server.hostname or server.username or server.password:
            raise ValueError("Matrix benötigt eine Server-URL ohne Zugangsdaten.")
        if not matrix.user_id.startswith("@") or ":" not in matrix.user_id:
            raise ValueError("Die vollständige Matrix-Benutzer-ID wird benötigt.")
        room_id = matrix.room_id
        is_room = room_id.startswith("!") and len(room_id) > 1
        alias_local, separator, alias_server = room_id.partition(":")
        is_alias = alias_local.startswith("#") and len(alias_local) > 1 and separator and alias_server
        if not (is_room or is_alias) or "\x00" in room_id:
            raise ValueError("Eine Matrix-Raum-ID oder ein Raumalias wird benötigt.")
        if not matrix.password and not matrix.access_token:
            raise ValueError("Ein Matrix-Passwort oder Access-Token wird benötigt.")
        if matrix.access_token and not matrix.device_id:
            raise ValueError("Zum Matrix-Access-Token gehört die zugehörige Geräte-ID.")
        radio = self.meshtastic
        text_fields = (
            "connection", "channel_psk", "tcp_host", "serial_device", "ble_address",
            "mqtt_host", "mqtt_username", "mqtt_password", "mqtt_topic",
            "mqtt_gateway_id", "mqtt_channel_name",
        )
        if not all(isinstance(getattr(radio, key), str) for key in text_fields):
            raise ValueError("Die Verbindungseinstellungen müssen Zeichenketten sein.")
        if not isinstance(radio.mqtt_tls, bool):
            raise ValueError("mqtt_tls muss ein Wahrheitswert sein.")
        if radio.connection not in ("usb", "tcp", "bluetooth", "mqtt"):
            raise ValueError("Unbekannte Meshtastic-Verbindungsart.")
        if type(radio.channel_index) is not int or not 0 <= radio.channel_index <= 7:
            raise ValueError("Der Meshtastic-Channel muss zwischen 0 und 7 liegen.")
        decode_private_psk(radio.channel_psk)
        for port in (radio.tcp_port, radio.mqtt_port):
            if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
                raise ValueError("Ungültiger Netzwerkport.")
        required = {
            "tcp": (radio.tcp_host,),
            "usb": (radio.serial_device,),
            "bluetooth": (radio.ble_address,),
            "mqtt": (radio.mqtt_host, radio.mqtt_channel_name, radio.mqtt_gateway_id, radio.mqtt_topic),
        }
        if not all(value.strip() for value in required[radio.connection]):
            raise ValueError("Für die ausgewählte Verbindung fehlen Einstellungen.")
        if radio.connection == "mqtt":
            if any(char in radio.mqtt_topic for char in ("+", "#", "\x00")) or not radio.mqtt_topic.rstrip("/").endswith("/2/e"):
                raise ValueError("Das MQTT-Topic muss auf /2/e enden und darf keine Wildcards enthalten.")
            if any(char in radio.mqtt_channel_name for char in ("/", "+", "#", "\x00")):
                raise ValueError("Der MQTT-Channelname enthält ungültige Zeichen.")
            try:
                node_id = int(radio.mqtt_gateway_id.removeprefix("!"), 16)
            except ValueError as exc:
                raise ValueError("Die MQTT-Gateway-ID muss im Format !1234abcd vorliegen.") from exc
            if not radio.mqtt_gateway_id.startswith("!") or len(radio.mqtt_gateway_id) != 9 or not 3 < node_id < 0xFFFFFFFF:
                raise ValueError("Die MQTT-Gateway-ID muss im Format !1234abcd vorliegen.")
        forwarding = self.forwarding
        if (
            not isinstance(forwarding.include_sender, bool)
            or type(forwarding.max_parts) is not int
            or type(forwarding.max_message_age_seconds) is not int
            or type(forwarding.message_interval_seconds) not in (int, float)
            or not math.isfinite(forwarding.message_interval_seconds)
            or not isinstance(forwarding.long_message, str)
        ):
            raise ValueError("Ungültige Datentypen für die Weiterleitung.")
        if forwarding.long_message not in ("split", "truncate") or not 1 <= forwarding.max_parts <= 16:
            raise ValueError("Ungültige Behandlung langer Nachrichten.")
        if not 2 <= forwarding.message_interval_seconds <= 3600 or not 1 <= forwarding.max_message_age_seconds <= 86400:
            raise ValueError("Ungültiges Sendeintervall oder Nachrichtenalter.")
