import base64
from types import SimpleNamespace

import pytest

from bridge.security import ChannelSecurityError, decode_private_psk, require_private_channel


PRIVATE_16 = bytes(range(16))
PRIVATE_32 = bytes(range(32))


def encoded(key):
    return base64.b64encode(key).decode("ascii")


def channel(index=0, role=1, key=PRIVATE_16):
    return SimpleNamespace(index=index, role=role, settings=SimpleNamespace(psk=key))


@pytest.mark.parametrize("key", [b"", bytes(16), bytes(32), b"short", bytes(15), bytes(17), bytes(31), bytes(33)])
def test_rejects_missing_zero_and_wrong_length_keys(key):
    with pytest.raises(ChannelSecurityError):
        decode_private_psk(encoded(key))


def test_rejects_every_one_byte_alias():
    for index in range(256):
        with pytest.raises(ChannelSecurityError):
            decode_private_psk(encoded(bytes([index])))


def test_rejects_all_public_default_key_variants():
    prefix = bytes.fromhex("d4f1bb3a20290759f0bcffabcf4e69")
    for index in range(256):
        with pytest.raises(ChannelSecurityError):
            decode_private_psk(encoded(prefix + bytes([index])))


@pytest.mark.parametrize("value", ["%%%", "a", "YWJj!", "YWJj===", "\N{SNOWMAN}", "AA AA"])
def test_rejects_malformed_base64(value):
    with pytest.raises(ChannelSecurityError):
        decode_private_psk(value)


@pytest.mark.parametrize("key", [PRIVATE_16, PRIVATE_32])
def test_accepts_private_aes_keys(key):
    assert decode_private_psk(encoded(key)) == key
    assert decode_private_psk(" \n" + encoded(key) + "\n ") == key


@pytest.mark.parametrize("key", [PRIVATE_16, PRIVATE_32])
def test_accepts_matching_enabled_private_channel(key):
    require_private_channel([channel(index=2, role=2, key=key)], 2, key)


def test_selects_channel_by_index_instead_of_list_position():
    require_private_channel([channel(index=4, role=2), channel(index=0)], 4, PRIVATE_16)


def test_rejects_mismatched_private_channel_key():
    with pytest.raises(ChannelSecurityError):
        require_private_channel([channel(key=bytes(reversed(PRIVATE_16)))], 0, PRIVATE_16)


@pytest.mark.parametrize("channels", [[], [channel(index=1)], [channel(role=0)], [channel(role=3)]])
def test_rejects_missing_or_disabled_channel(channels):
    with pytest.raises(ChannelSecurityError):
        require_private_channel(channels, 0, PRIVATE_16)


def test_rejects_secondary_channel_inheriting_primary_key():
    channels = [channel(index=0), channel(index=1, role=2, key=b"")]
    with pytest.raises(ChannelSecurityError):
        require_private_channel(channels, 1, PRIVATE_16)


@pytest.mark.parametrize("key", [b"", b"\x01", bytes(16), bytes.fromhex("d4f1bb3a20290759f0bcffabcf4e6901")])
def test_rejects_public_or_unencrypted_device_configuration(key):
    with pytest.raises(ChannelSecurityError):
        require_private_channel([channel(key=key)], 0, PRIVATE_16)


def test_rejects_invalid_expected_key_even_if_device_matches():
    with pytest.raises(ChannelSecurityError):
        require_private_channel([channel(key=b"\x01")], 0, b"\x01")


def test_key_validation_error_does_not_expose_key():
    value = encoded(bytes(16))
    with pytest.raises(ChannelSecurityError) as result:
        decode_private_psk(value)
    assert value not in str(result.value)
