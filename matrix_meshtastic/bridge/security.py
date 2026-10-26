import base64
import binascii
import hmac
from collections.abc import Sequence


class ChannelSecurityError(ValueError):
    pass


def decode_private_psk(value: str) -> bytes:
    try:
        key = base64.b64decode(value.strip(), validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ChannelSecurityError("Der Channel-Schlüssel muss gültiges Base64 sein.") from exc
    _require_private_key(key)
    return key


def _require_private_key(key: bytes) -> None:
    if len(key) not in (16, 32) or not any(key):
        raise ChannelSecurityError("Ein eigener AES-Schlüssel mit 16 oder 32 Bytes ist erforderlich.")
    public_prefix = bytes.fromhex("d4f1bb3a20290759f0bcffabcf4e69")
    if len(key) == 16 and key.startswith(public_prefix):
        raise ChannelSecurityError("Öffentliche Meshtastic-Standardschlüssel sind nicht erlaubt.")


def require_private_channel(channels: Sequence, channel_index: int, expected_psk: bytes) -> None:
    _require_private_key(expected_psk)
    channel = next((item for item in channels if item.index == channel_index), None)
    if channel is None or channel.role not in (1, 2):
        raise ChannelSecurityError("Der ausgewählte Meshtastic-Channel ist nicht aktiv.")
    actual = bytes(channel.settings.psk)
    _require_private_key(actual)
    if not hmac.compare_digest(actual, expected_psk):
        raise ChannelSecurityError("Der Schlüssel des Meshtastic-Channels stimmt nicht mit dem Add-on überein.")
