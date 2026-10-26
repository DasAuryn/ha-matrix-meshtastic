import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from bridge.runtime import RuntimeController
from bridge.matrix import MatrixActionError
from bridge.request_context import requested_account


class FakeIngress:
    def __init__(self, matrix, radio_status, management=None):
        self.matrix = matrix
        self.management = management
        self.started = False
        self.closed = False

    async def start(self):
        self.started = True

    async def close(self):
        self.closed = True


class FakeBridge:
    def __init__(self, settings, data_dir, manage_ingress=True):
        assert manage_ingress is False
        self.settings = settings
        self.stop = asyncio.Event()
        self.closed = False
        self.matrix = SimpleNamespace(
            messenger_state=lambda: {"connected": True, "user_id": settings.matrix.user_id, "rooms": []},
            send_room_text=AsyncMock(return_value={"event_id": "$sent"}),
        )

    async def run(self):
        await self.stop.wait()
        self.closed = True

    def radio_status(self):
        return {"enabled": False, "state": "disabled", "reason": "Matrix-only"}


def runtime(tmp_path, monkeypatch):
    monkeypatch.setattr("bridge.runtime.IngressServer", FakeIngress)
    monkeypatch.setattr("bridge.runtime.Bridge", FakeBridge)
    return RuntimeController(tmp_path, tmp_path / "options.json")


def test_ingress_opens_with_no_credentials_and_applies_setup_without_restarting_it(tmp_path, monkeypatch):
    async def scenario():
        controller = runtime(tmp_path, monkeypatch)
        task = asyncio.create_task(controller.run())
        await asyncio.sleep(0)
        assert controller.ingress.started
        assert controller.bridge is None
        assert controller.matrix.messenger_state()["setup_required"]
        await controller.save_settings({"matrix": {"user_id": "@alice:matrix.org", "password": "password"}})
        await asyncio.sleep(0)
        assert controller.ingress.started and not controller.ingress.closed
        assert controller.matrix.messenger_state()["connected"]
        assert "password" not in controller.get_settings()["matrix"]
        controller.stop.set()
        await task
        assert controller.ingress.closed
    asyncio.run(scenario())


def test_reconfiguration_closes_previous_connection_and_proxy_uses_new_receiver(tmp_path, monkeypatch):
    async def scenario():
        controller = runtime(tmp_path, monkeypatch)
        await controller.save_settings({"matrix": {"user_id": "@alice:matrix.org", "password": "first"}})
        await asyncio.sleep(0)
        previous = controller.bridge
        await controller.save_settings({"matrix": {"user_id": "@bob:matrix.org", "password": "second"}})
        await asyncio.sleep(0)
        assert previous.closed
        assert controller.matrix.messenger_state()["user_id"] == "@bob:matrix.org"
        assert await controller.matrix.send_room_text("!room", "text", "tx") == {"event_id": "$sent"}
        previous.matrix.send_room_text.assert_not_awaited()
        await controller._close_bridge()
    asyncio.run(scenario())


def test_invalid_setup_update_keeps_current_working_connection(tmp_path, monkeypatch):
    async def scenario():
        controller = runtime(tmp_path, monkeypatch)
        await controller.save_settings({"matrix": {"user_id": "@alice:matrix.org", "password": "secret"}})
        await asyncio.sleep(0)
        previous = controller.bridge
        with pytest.raises(MatrixActionError) as error:
            await controller.save_settings({"matrix": {"user_id": "bad"}})
        assert error.value.code == "invalid_settings"
        assert controller.bridge is previous
        assert not previous.stop.is_set()
        await controller._close_bridge()
    asyncio.run(scenario())


def test_logout_requires_confirmation_and_removes_credentials_without_deleting_keys(tmp_path, monkeypatch):
    async def scenario():
        controller = runtime(tmp_path, monkeypatch)
        await controller.save_settings({"matrix": {"user_id": "@alice:matrix.org", "password": "secret"}})
        await asyncio.sleep(0)
        account_id = controller.matrix.messenger_state()["account_id"]
        key_dir = tmp_path / "matrix"
        key_dir.mkdir()
        protected_keys = key_dir / "preserved-keys.db"
        protected_keys.write_bytes(b"private-test-key-store")
        session = key_dir / "session.json"
        session.write_text(json.dumps({"homeserver": "https://matrix.org", "user_id": "@alice:matrix.org", "device_id": "DEVICE", "access_token": "old-session-token"}))
        with pytest.raises(MatrixActionError):
            await controller.logout(False)
        assert controller.bridge is not None
        result = await controller.logout(True)
        assert not result["configured"]
        assert result["logged_out"]
        assert result["meshtastic"]["enabled"] is False
        assert controller.store.raw["matrix"]["password"] == ""
        assert "old-session-token" not in session.read_text()
        assert protected_keys.read_bytes() == b"private-test-key-store"
        assert account_id != controller.matrix.messenger_state()["account_id"]
    asyncio.run(scenario())


def test_delayed_action_from_previous_account_cannot_use_new_account(tmp_path, monkeypatch):
    async def scenario():
        controller = runtime(tmp_path, monkeypatch)
        await controller.save_settings({"matrix": {"user_id": "@alice:matrix.org", "password": "first"}})
        await asyncio.sleep(0)
        previous_account = controller.matrix.account_id()
        await controller.save_settings({"matrix": {"user_id": "@bob:matrix.org", "password": "second"}})
        await asyncio.sleep(0)
        token = requested_account.set(previous_account)
        try:
            with pytest.raises(MatrixActionError) as error:
                await controller.matrix.send_room_text("!room", "old-account-text", "tx")
            assert error.value.code == "account_changed"
            controller.bridge.matrix.send_room_text.assert_not_awaited()
        finally:
            requested_account.reset(token)
            await controller._close_bridge()
    asyncio.run(scenario())


def test_logout_stops_reconnects_before_revoking_the_server_session(tmp_path, monkeypatch):
    async def scenario():
        controller = runtime(tmp_path, monkeypatch)
        await controller.save_settings({"matrix": {"user_id": "@alice:matrix.org", "password": "secret"}})
        await asyncio.sleep(0)
        old_bridge = controller.bridge

        async def revoke():
            assert old_bridge.closed
            return SimpleNamespace()

        client = SimpleNamespace(access_token="test-session", logout=AsyncMock(side_effect=revoke), close=AsyncMock())
        old_bridge.matrix.client = client
        result = await controller.logout(True)
        assert result["server_session_revoked"]
        client.logout.assert_awaited_once()
        client.close.assert_awaited_once()
    asyncio.run(scenario())
