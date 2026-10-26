import base64
import copy
import json
from pathlib import Path

import pytest
import yaml

from bridge.config import Settings


@pytest.fixture
def addon_defaults():
    path = Path(__file__).parents[1] / "matrix_meshtastic" / "config.yaml"
    return yaml.safe_load(path.read_text())["options"]


@pytest.fixture
def valid_options(addon_defaults):
    options = copy.deepcopy(addon_defaults)
    options["matrix"].update(
        homeserver="https://matrix.example.invalid",
        user_id="@meshbot:example.invalid",
        password="test-matrix-password",
        room_id="!testroom:example.invalid",
    )
    options["meshtastic"].update(
        channel_psk=base64.b64encode(bytes(range(16))).decode("ascii"),
        tcp_host="radio.example.invalid",
    )
    return options


def load_options(tmp_path, options):
    path = tmp_path / "options.json"
    path.write_text(json.dumps(options))
    return Settings.load(path)


def test_addon_defaults_require_user_configuration(tmp_path, addon_defaults):
    with pytest.raises(ValueError):
        load_options(tmp_path, addon_defaults)


@pytest.mark.parametrize(
    ("connection", "fields"),
    [
        ("usb", {"serial_device": "/dev/serial/by-id/test-node"}),
        ("tcp", {"tcp_host": "radio.example.invalid", "tcp_port": 4403}),
        ("bluetooth", {"ble_address": "AA:BB:CC:DD:EE:FF"}),
        (
            "mqtt",
            {
                "mqtt_host": "broker.example.invalid",
                "mqtt_port": 8883,
                "mqtt_tls": True,
                "mqtt_username": "test-broker-user",
                "mqtt_password": "test-broker-password",
                "mqtt_topic": "msh/EU_868/2/e",
                "mqtt_channel_name": "PrivateMesh",
                "mqtt_gateway_id": "!abcd1234",
            },
        ),
    ],
)
def test_loads_each_supported_connection(tmp_path, valid_options, connection, fields):
    valid_options["meshtastic"].update(connection=connection, **fields)
    settings = load_options(tmp_path, valid_options)
    assert settings.meshtastic.connection == connection
    for name, value in fields.items():
        assert getattr(settings.meshtastic, name) == value


def test_accepts_token_login_with_device_id(tmp_path, valid_options):
    valid_options["matrix"].update(password="", access_token="test-access-token", device_id="TESTDEVICE")
    settings = load_options(tmp_path, valid_options)
    assert settings.matrix.device_id == "TESTDEVICE"


def test_accepts_room_alias_and_private_256_bit_key(tmp_path, valid_options):
    valid_options["matrix"]["room_id"] = "#testroom:example.invalid"
    valid_options["meshtastic"]["channel_psk"] = base64.b64encode(bytes(range(32))).decode("ascii")
    assert load_options(tmp_path, valid_options).matrix.room_id.startswith("#")


@pytest.mark.parametrize("room_id", [
    "!testroom:example.invalid",
    "!Nhcu5BS-UMnFX7hBVfVSoXiD7OgH6iRT-xyIuqDnpYQ",
    "!Opaque_Future-Format",
    "#testroom:example.invalid",
])
def test_accepts_opaque_room_ids_and_preserves_them(tmp_path, valid_options, room_id):
    valid_options["matrix"]["room_id"] = room_id
    assert load_options(tmp_path, valid_options).matrix.room_id == room_id


@pytest.mark.parametrize("room_id", ["!", "#", "#room", "#:example.invalid", "#room:", "!room\x00"])
def test_rejects_empty_room_ids_and_incomplete_aliases(tmp_path, valid_options, room_id):
    valid_options["matrix"]["room_id"] = room_id
    with pytest.raises(ValueError):
        load_options(tmp_path, valid_options)


def test_authentication_and_keys_are_hidden_from_settings_repr(tmp_path, valid_options):
    settings = load_options(tmp_path, valid_options)
    result = repr(settings)
    assert valid_options["matrix"]["password"] not in result
    assert valid_options["meshtastic"]["channel_psk"] not in result


@pytest.mark.parametrize(
    ("group", "name", "value"),
    [
        ("matrix", "homeserver", "ftp://matrix.example.invalid"),
        ("matrix", "homeserver", "https://user:password@matrix.example.invalid"),
        ("matrix", "user_id", "meshbot"),
        ("matrix", "room_id", "testroom"),
        ("matrix", "password", ""),
        ("meshtastic", "connection", "udp"),
        ("meshtastic", "channel_index", -1),
        ("meshtastic", "channel_index", 8),
        ("meshtastic", "channel_index", True),
        ("meshtastic", "channel_psk", ""),
        ("meshtastic", "channel_psk", "AQ=="),
        ("meshtastic", "tcp_host", " "),
        ("forwarding", "long_message", "unlimited"),
        ("forwarding", "max_parts", 0),
        ("forwarding", "max_parts", 21),
        ("forwarding", "message_interval_seconds", 1),
        ("forwarding", "message_interval_seconds", 3601),
        ("forwarding", "max_message_age_seconds", 0),
        ("forwarding", "max_message_age_seconds", 86401),
    ],
)
def test_rejects_invalid_settings(tmp_path, valid_options, group, name, value):
    valid_options[group][name] = value
    with pytest.raises(ValueError):
        load_options(tmp_path, valid_options)


@pytest.mark.parametrize("port_name", ["tcp_port", "mqtt_port"])
@pytest.mark.parametrize("port", [0, -1, 65536, True, "1883", 1883.5, None])
def test_rejects_invalid_ports(tmp_path, valid_options, port_name, port):
    valid_options["meshtastic"][port_name] = port
    with pytest.raises(ValueError):
        load_options(tmp_path, valid_options)


def test_token_requires_matching_device_id(tmp_path, valid_options):
    valid_options["matrix"].update(password="", access_token="test-access-token", device_id="")
    with pytest.raises(ValueError):
        load_options(tmp_path, valid_options)


@pytest.fixture
def mqtt_options(valid_options):
    valid_options["meshtastic"].update(
        connection="mqtt",
        mqtt_host="broker.example.invalid",
        mqtt_channel_name="PrivateMesh",
        mqtt_gateway_id="!abcd1234",
    )
    return valid_options


@pytest.mark.parametrize("name", ["mqtt_host", "mqtt_channel_name", "mqtt_gateway_id", "mqtt_topic"])
def test_mqtt_requires_each_connection_field(tmp_path, mqtt_options, name):
    mqtt_options["meshtastic"][name] = ""
    with pytest.raises(ValueError):
        load_options(tmp_path, mqtt_options)


@pytest.mark.parametrize("gateway", ["abcd1234", "!abcd123", "!zzzzzzzz", "!00000000", "!00000003", "!ffffffff"])
def test_rejects_invalid_mqtt_gateway(tmp_path, mqtt_options, gateway):
    mqtt_options["meshtastic"]["mqtt_gateway_id"] = gateway
    with pytest.raises(ValueError):
        load_options(tmp_path, mqtt_options)


@pytest.mark.parametrize("topic", ["msh/EU_868/2/json", "msh/+/2/e", "msh/#/2/e", "msh/\x00/2/e"])
def test_rejects_invalid_mqtt_topic(tmp_path, mqtt_options, topic):
    mqtt_options["meshtastic"]["mqtt_topic"] = topic
    with pytest.raises(ValueError):
        load_options(tmp_path, mqtt_options)


@pytest.mark.parametrize("name", ["Private/Mesh", "Private+Mesh", "Private#Mesh", "Private\x00Mesh"])
def test_rejects_invalid_mqtt_channel_name(tmp_path, mqtt_options, name):
    mqtt_options["meshtastic"]["mqtt_channel_name"] = name
    with pytest.raises(ValueError):
        load_options(tmp_path, mqtt_options)


@pytest.mark.parametrize("connection,field", [("usb", "serial_device"), ("bluetooth", "ble_address")])
def test_requires_device_for_direct_connection(tmp_path, valid_options, connection, field):
    valid_options["meshtastic"].update(connection=connection)
    valid_options["meshtastic"][field] = ""
    with pytest.raises(ValueError):
        load_options(tmp_path, valid_options)


@pytest.mark.parametrize(
    ("group", "name", "value"),
    [
        ("matrix", "homeserver", None),
        ("matrix", "user_id", None),
        ("matrix", "password", True),
        ("matrix", "room_id", []),
        ("meshtastic", "channel_index", "1"),
        ("meshtastic", "channel_index", 1.5),
        ("meshtastic", "tcp_host", None),
        ("meshtastic", "mqtt_tls", "false"),
        ("forwarding", "include_sender", "true"),
        ("forwarding", "max_parts", "4"),
        ("forwarding", "max_parts", True),
        ("forwarding", "message_interval_seconds", None),
        ("forwarding", "message_interval_seconds", float("nan")),
        ("forwarding", "max_message_age_seconds", True),
    ],
)
def test_malformed_value_types_report_configuration_error(tmp_path, valid_options, group, name, value):
    valid_options[group][name] = value
    with pytest.raises(ValueError):
        load_options(tmp_path, valid_options)


@pytest.mark.parametrize("options", [None, [], 7, {"matrix": []}, {"matrix": None}, {"matrix": {"unknown": "value"}}])
def test_malformed_configuration_structure_reports_configuration_error(tmp_path, options):
    with pytest.raises(ValueError):
        load_options(tmp_path, options)


def test_missing_configuration_file_reports_configuration_error(tmp_path):
    with pytest.raises(ValueError):
        Settings.load(tmp_path / "missing.json")


def test_invalid_json_reports_configuration_error(tmp_path):
    path = tmp_path / "options.json"
    path.write_text("{")
    with pytest.raises(ValueError):
        Settings.load(path)
