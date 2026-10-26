import json
import stat

import pytest

from bridge.settings_store import SettingsStore


def configured_store(tmp_path):
    options = tmp_path / "options.json"
    options.write_text(json.dumps({
        "matrix": {"homeserver": "https://matrix.org", "user_id": "@alice:matrix.org", "password": "existing-private-password"},
        "meshtastic": {"mqtt_password": "existing-mqtt-password", "channel_psk": "private-channel-placeholder"},
    }))
    return SettingsStore(options, tmp_path)


def test_first_start_without_options_remains_available_for_setup(tmp_path):
    store = SettingsStore(tmp_path / "missing.json", tmp_path)
    snapshot = store.public()
    assert snapshot["configured"] is False
    assert snapshot["matrix"]["homeserver"] == "https://matrix.org"
    assert snapshot["meshtastic"]["enabled"] is False


def test_public_settings_never_contain_existing_secrets(tmp_path):
    store = configured_store(tmp_path)
    snapshot = store.public()
    payload = json.dumps(snapshot)
    for value in ("existing-private-password", "existing-mqtt-password", "private-channel-placeholder"):
        assert value not in payload
    assert snapshot["matrix"]["has_password"] is True
    assert "password" not in snapshot["matrix"]
    assert "channel_psk" not in snapshot["meshtastic"]


def test_invalid_legacy_server_url_cannot_expose_embedded_credentials(tmp_path):
    store = configured_store(tmp_path)
    store.raw["matrix"]["homeserver"] = "https://user:private-password@matrix.org?access_token=private-token#private-fragment"
    payload = json.dumps(store.public())
    for value in ("private-password", "private-token", "private-fragment"):
        assert value not in payload


def test_empty_secret_fields_preserve_credentials_during_other_changes(tmp_path):
    store = configured_store(tmp_path)
    raw, settings = store.candidate({"matrix": {"password": ""}, "meshtastic": {"mqtt_password": "", "channel_index": 2}})
    assert settings.matrix.password == "existing-private-password"
    assert settings.meshtastic.mqtt_password == "existing-mqtt-password"
    assert raw["meshtastic"]["channel_index"] == 2


def test_clear_flag_removes_only_explicit_secret(tmp_path):
    store = configured_store(tmp_path)
    raw, _ = store.candidate({"meshtastic": {"mqtt_password_clear": True}})
    assert raw["meshtastic"]["mqtt_password"] == ""
    assert raw["matrix"]["password"] == "existing-private-password"


def test_account_or_server_change_never_reuses_old_credentials(tmp_path):
    store = configured_store(tmp_path)
    with pytest.raises(ValueError):
        store.candidate({"matrix": {"homeserver": "https://other.invalid", "user_id": "@alice:other.invalid"}})
    raw, _ = store.candidate({"matrix": {"homeserver": "https://other.invalid", "user_id": "@alice:other.invalid", "password": "new-password"}})
    assert raw["matrix"]["password"] == "new-password"
    assert raw["matrix"]["access_token"] == ""
    assert raw["matrix"]["device_id"] == ""


def test_saved_ingress_settings_survive_restart_and_have_private_permissions(tmp_path):
    store = configured_store(tmp_path)
    raw, _ = store.candidate({"forwarding": {"message_interval_seconds": 8}})
    store.save(raw)
    reopened = SettingsStore(store.options_path, tmp_path)
    assert reopened.settings().forwarding.message_interval_seconds == 8
    assert stat.S_IMODE(store.path.stat().st_mode) == 0o600
    assert not store.path.with_suffix(".tmp").exists()


def test_corrupt_settings_show_setup_error_instead_of_failing_startup(tmp_path):
    (tmp_path / "settings.json").write_text("not-json")
    store = SettingsStore(tmp_path / "options.json", tmp_path)
    assert store.public()["configured"] is False
    assert store.public()["validation_error"]


@pytest.mark.parametrize("patch", [[], {"unknown": {}}, {"matrix": {"unknown": "x"}}, {"matrix": {"password_clear": "yes"}}])
def test_unknown_or_malformed_setting_updates_are_rejected(tmp_path, patch):
    store = configured_store(tmp_path)
    with pytest.raises(ValueError):
        store.candidate(patch)


def test_simple_server_name_gets_https_without_changing_account_identity(tmp_path):
    store = configured_store(tmp_path)
    raw, _ = store.candidate({"matrix": {"homeserver": "matrix.org/"}})
    assert raw["matrix"]["homeserver"] == "https://matrix.org"
    assert raw["matrix"]["password"] == "existing-private-password"


def test_radio_only_change_keeps_credentials_when_existing_server_has_trailing_slash(tmp_path):
    store = configured_store(tmp_path)
    store.raw["matrix"]["homeserver"] = "https://matrix.org/"
    raw, settings = store.candidate({"meshtastic": {"channel_index": 2}})
    assert settings.matrix.password == "existing-private-password"
    assert raw["matrix"]["homeserver"] == "https://matrix.org"


def test_legacy_ingress_settings_use_normal_encrypted_sending(tmp_path):
    store = configured_store(tmp_path)
    store.raw["matrix"].pop("require_verified_devices", None)
    store.save(store.raw)
    reopened = SettingsStore(store.options_path, tmp_path)
    assert reopened.settings().matrix.require_verified_devices is False
    raw, settings = reopened.candidate({"matrix": {"require_verified_devices": True}})
    assert settings.matrix.require_verified_devices is True
    reopened.save(raw)
    assert SettingsStore(store.options_path, tmp_path).settings().matrix.require_verified_devices is True
