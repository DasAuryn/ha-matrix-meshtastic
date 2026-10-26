import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from aiohttp import FormData
from aiohttp.test_utils import TestClient, TestServer

from bridge.web import IngressServer, create_app


class WebTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.matrix = SimpleNamespace(
            messenger_state=Mock(return_value={
                "connected": True,
                "user_id": "@test:example.invalid",
                "device_id": "DEVICE",
                "rooms": [{"room_id": "!room", "name": "<script>test</script>", "membership": "join"}],
                "verification": [],
                "devices": [],
                "error": None,
                "access_token": "must-not-reach-browser",
            }),
            room_history=AsyncMock(return_value={"messages": [], "next_batch": "older"}),
            send_room_text=AsyncMock(return_value={"event_id": "$sent"}),
            join_room=AsyncMock(return_value={"room_id": "!room"}),
            create_direct=AsyncMock(return_value={"room_id": "!direct"}),
            mark_read=AsyncMock(),
            verification_action=AsyncMock(return_value={"ok": True}),
            start_verification=AsyncMock(return_value={"transaction_id": "verify-1"}),
            create_room=AsyncMock(return_value={"room_id": "!created"}),
            invite_user=AsyncMock(return_value={"ok": True}),
            rename_room=AsyncMock(return_value={"ok": True}),
            leave_room=AsyncMock(return_value={"ok": True}),
            retry_keys=AsyncMock(return_value={"requested": 2}),
            import_room_keys=AsyncMock(return_value={"imported": 2}),
            verify_device_fingerprint=AsyncMock(return_value={"verified": True}),
        )
        self.radio = lambda: {"enabled": False, "state": "disabled", "reason": "Matrix funktioniert eigenständig", "channel_psk": "must-not-reach-browser"}
        self.client = TestClient(TestServer(create_app(self.matrix, self.radio, allowed_peers={"127.0.0.1"})))
        await self.client.start_server()
        self.headers = {"X-Matrix-Messenger": "1"}

    async def management_client(self):
        self.management = SimpleNamespace(
            get_settings=Mock(return_value={
                "configured": False, "config_valid": False, "setup_required": True,
                "validation_error": "Matrix-Benutzer fehlt.",
                "matrix": {"homeserver": "https://matrix.example.invalid", "user_id": "", "password": "private-password", "access_token": "private-token", "has_password": True, "has_access_token": True},
                "meshtastic": {"enabled": False, "channel_psk": "private-channel-key", "mqtt_password": "private-mqtt-password", "has_channel_psk": True},
                "forwarding": {"include_sender": True},
            }),
            save_settings=AsyncMock(return_value={"saved": True, "configured": True, "matrix": {"user_id": "@test:example.invalid", "password": "private-password"}}),
            logout=AsyncMock(return_value={"configured": False, "logged_out": True, "server_session_revoked": False, "logout_notice": "Lokal abgemeldet. Die Server-Sitzung bei Bedarf im bisherigen Matrix-Client beenden.", "matrix": {"user_id": "", "password": "private-password"}}),
            discover_devices=Mock(return_value={"serial": [{"path": "/dev/serial/by-id/radio", "label": "Radio", "credential": "private-usb-field"}], "token": "private-token"}),
        )
        client = TestClient(TestServer(create_app(self.matrix, self.radio, allowed_peers={"127.0.0.1"}, management=self.management)))
        await client.start_server()
        self.addAsyncCleanup(client.close)
        return client

    async def asyncTearDown(self):
        await self.client.close()

    async def test_default_access_denies_direct_and_forged_proxy_requests(self):
        client = TestClient(TestServer(create_app(self.matrix, self.radio)))
        await client.start_server()
        try:
            for path in ("/", "/api/state", "/static/app.js"):
                response = await client.get(path, headers={"X-Forwarded-For": "172.30.32.2", "X-Real-IP": "172.30.32.2"})
                self.assertEqual(response.status, 403)
        finally:
            await client.close()

    async def test_matrix_state_is_available_without_radio_and_excludes_credentials(self):
        response = await self.client.get("/api/state")
        self.assertEqual(response.status, 200)
        value = await response.json()
        self.assertTrue(value["connected"])
        self.assertEqual(value["radio"]["state"], "disabled")
        self.assertNotIn("access_token", value)
        self.assertNotIn("channel_psk", value["radio"])
        self.assertEqual(value["rooms"][0]["name"], "<script>test</script>")

    async def test_setup_settings_never_return_stored_secrets(self):
        client = await self.management_client()
        response = await client.get("/api/settings")
        self.assertEqual(response.status, 200)
        value = await response.json()
        self.assertTrue(value["setup_required"])
        self.assertTrue(value["matrix"]["has_password"])
        self.assertFalse(value["config_valid"])
        self.assertNotIn("password", value["matrix"])
        self.assertNotIn("access_token", value["matrix"])
        self.assertNotIn("channel_psk", value["meshtastic"])
        self.assertNotIn("mqtt_password", value["meshtastic"])
        self.assertNotIn("private-", await response.text())
        payload = {"matrix": {"password": "new-password"}, "meshtastic": {"enabled": False}}
        response = await client.post("/api/settings", json=payload, headers=self.headers)
        self.assertEqual(response.status, 200)
        self.management.save_settings.assert_awaited_once_with(payload)
        self.assertNotIn("private-password", await response.text())

    async def test_discovered_devices_only_return_paths_and_labels(self):
        client = await self.management_client()
        response = await client.get("/api/devices/discover")
        self.assertEqual(await response.json(), {"serial": [{"path": "/dev/serial/by-id/radio", "label": "Radio"}]})

    async def test_account_binding_is_public_for_browser_cache_isolation(self):
        self.matrix.messenger_state.return_value["account_id"] = "public-account-binding"
        response = await self.client.get("/api/state")
        self.assertEqual((await response.json())["account_id"], "public-account-binding")

    async def test_logout_requires_explicit_confirmation_and_returns_no_credentials(self):
        client = await self.management_client()
        response = await client.post("/api/account/logout", json={}, headers=self.headers)
        self.assertEqual(response.status, 400)
        self.management.logout.assert_not_awaited()
        response = await client.post("/api/account/logout", json={"confirmed": True}, headers=self.headers)
        self.assertEqual(response.status, 200)
        self.management.logout.assert_awaited_once_with(True)
        value = await response.json()
        self.assertTrue(value["logged_out"])
        self.assertFalse(value["configured"])
        self.assertIn("Lokal abgemeldet", value["logout_notice"])
        self.assertNotIn("private-", await response.text())

    async def test_missing_management_does_not_break_messenger(self):
        response = await self.client.get("/api/settings")
        self.assertEqual(response.status, 503)
        response = await self.client.get("/api/state")
        self.assertEqual(response.status, 200)

    async def test_diagnostics_and_devices_exclude_private_nested_fields(self):
        value = self.matrix.messenger_state.return_value
        value["setup_required"] = True
        value["devices"] = [{"user_id": "@alice:example.invalid", "device_id": "ALICE", "fingerprint": "public-fingerprint", "access_token": "private-token"}]
        value["diagnostics"] = {"pending_encrypted_count": 2, "secret": "private-key", "rooms_missing_keys": [{"room_id": "!room", "name": "Room", "count": 2, "session_key": "private-session-key"}]}
        response = await self.client.get("/api/state")
        result = await response.json()
        self.assertTrue(result["setup_required"])
        self.assertEqual(result["devices"][0]["fingerprint"], "public-fingerprint")
        self.assertEqual(result["diagnostics"]["pending_encrypted_count"], 2)
        self.assertNotIn("private-", await response.text())

    async def test_mutations_require_custom_header_and_json(self):
        for options in (
            {"json": {"text": "Hello", "transaction_id": "same-id"}},
            {"data": '{"text":"Hello","transaction_id":"same-id"}', "headers": self.headers},
        ):
            response = await self.client.post("/api/rooms/!room/send", **options)
            self.assertEqual(response.status, 403)
        self.matrix.send_room_text.assert_not_awaited()

    async def test_send_passes_stable_transaction_id_to_selected_room(self):
        for _ in range(2):
            response = await self.client.post("/api/rooms/!room/send", json={"text": "Hello Matrix", "transaction_id": "stable-id"}, headers=self.headers)
            self.assertEqual(response.status, 200)
            self.assertEqual((await response.json())["event_id"], "$sent")
        self.assertEqual(self.matrix.send_room_text.await_count, 2)
        self.matrix.send_room_text.assert_awaited_with("!room", "Hello Matrix", "stable-id")

    async def test_send_rejects_empty_text_or_missing_transaction(self):
        for value in ({"text": " ", "transaction_id": "id"}, {"text": "Hello"}, {"text": "a" * 16385, "transaction_id": "id"}, ["text"]):
            response = await self.client.post("/api/rooms/!room/send", json=value, headers=self.headers)
            self.assertEqual(response.status, 400)
        self.matrix.send_room_text.assert_not_awaited()

    async def test_history_passes_pagination_and_validates_limit(self):
        response = await self.client.get("/api/rooms/!room/messages", params={"before": "token/+", "limit": "25"})
        self.assertEqual(response.status, 200)
        self.matrix.room_history.assert_awaited_once_with("!room", before="token/+", limit=25)
        for limit in ("0", "101", "oops"):
            response = await self.client.get("/api/rooms/!room/messages", params={"limit": limit})
            self.assertEqual(response.status, 400)

    async def test_invitation_direct_and_read_routes(self):
        response = await self.client.post("/api/rooms/!room/join", json={}, headers=self.headers)
        self.assertEqual(response.status, 200)
        self.matrix.join_room.assert_awaited_once_with("!room")
        response = await self.client.post("/api/direct", json={"user_id": "@alice:example.invalid"}, headers=self.headers)
        self.assertEqual(response.status, 200)
        self.matrix.create_direct.assert_awaited_once_with("@alice:example.invalid")
        response = await self.client.post("/api/rooms/!room/read", json={}, headers=self.headers)
        self.assertEqual(response.status, 200)
        self.matrix.mark_read.assert_awaited_once_with("!room")

    async def test_room_administration_routes_require_explicit_leave_confirmation(self):
        response = await self.client.post("/api/rooms/create", json={"name": "Einsatz", "invite": ["@alice:example.invalid"]}, headers=self.headers)
        self.assertEqual(response.status, 200)
        self.matrix.create_room.assert_awaited_once_with("Einsatz", ["@alice:example.invalid"], True)
        response = await self.client.post("/api/rooms/join", json={"room_id": "#einsatz:example.invalid"}, headers=self.headers)
        self.assertEqual(response.status, 200)
        self.matrix.join_room.assert_awaited_with("#einsatz:example.invalid")
        response = await self.client.post("/api/rooms/!room/invite", json={"user_id": "@bob:example.invalid"}, headers=self.headers)
        self.assertEqual(response.status, 200)
        self.matrix.invite_user.assert_awaited_once_with("!room", "@bob:example.invalid")
        response = await self.client.post("/api/rooms/!room/rename", json={"name": "Neuer Name"}, headers=self.headers)
        self.assertEqual(response.status, 200)
        self.matrix.rename_room.assert_awaited_once_with("!room", "Neuer Name")
        response = await self.client.post("/api/rooms/!room/leave", json={}, headers=self.headers)
        self.assertEqual(response.status, 400)
        self.matrix.leave_room.assert_not_awaited()
        response = await self.client.post("/api/rooms/!room/leave", json={"confirmed": True}, headers=self.headers)
        self.assertEqual(response.status, 200)
        self.matrix.leave_room.assert_awaited_once_with("!room")

    async def test_invalid_room_administration_inputs_are_rejected(self):
        cases = [
            ("/api/rooms/create", {"name": " ", "invite": []}),
            ("/api/rooms/create", {"name": "Room", "invite": ["alice"]}),
            ("/api/rooms/create", {"name": "Room", "encrypted": "false"}),
            ("/api/rooms/join", {"room_id": "room"}),
            ("/api/rooms/!room/invite", {"user_id": "bob"}),
            ("/api/rooms/!room/rename", {"name": ""}),
        ]
        for path, value in cases:
            response = await self.client.post(path, json=value, headers=self.headers)
            self.assertEqual(response.status, 400)
        self.matrix.create_room.assert_not_awaited()
        self.matrix.invite_user.assert_not_awaited()
        self.matrix.rename_room.assert_not_awaited()

    async def test_device_verification_requires_explicit_action(self):
        response = await self.client.post("/api/verification/start", json={"user_id": "@alice:example.invalid", "device_id": "ALICE"}, headers=self.headers)
        self.assertEqual(response.status, 200)
        self.matrix.start_verification.assert_awaited_once_with("@alice:example.invalid", "ALICE")
        for action in ("accept", "confirm", "cancel"):
            response = await self.client.post("/api/verification/verify-1", json={"action": action}, headers=self.headers)
            self.assertEqual(response.status, 200)
            self.matrix.verification_action.assert_awaited_with("verify-1", action)
        response = await self.client.post("/api/verification/verify-1", json={"action": "trust-everyone"}, headers=self.headers)
        self.assertEqual(response.status, 400)

    async def test_fingerprint_verification_requires_explicit_comparison(self):
        value = {"user_id": "@alice:example.invalid", "device_id": "ALICE", "fingerprint": "public-fingerprint"}
        response = await self.client.post("/api/verification/device", json=value, headers=self.headers)
        self.assertEqual(response.status, 400)
        self.matrix.verify_device_fingerprint.assert_not_awaited()
        value["confirmed"] = True
        response = await self.client.post("/api/verification/device", json=value, headers=self.headers)
        self.assertEqual(response.status, 200)
        self.matrix.verify_device_fingerprint.assert_awaited_once_with("@alice:example.invalid", "ALICE", "public-fingerprint", True)

    async def test_missing_keys_can_be_requested_for_selected_room(self):
        response = await self.client.post("/api/keys/retry", json={"room_id": "!room"}, headers=self.headers)
        self.assertEqual(response.status, 200)
        self.matrix.retry_keys.assert_awaited_once_with("!room")

    def key_upload(self, data=b"encrypted-matrix-key-export", passphrase="export-passphrase"):
        form = FormData()
        form.add_field("file", data, filename="matrix-keys.txt", content_type="application/octet-stream")
        form.add_field("passphrase", passphrase)
        return form

    async def test_key_import_accepts_only_bounded_explicit_multipart_upload(self):
        response = await self.client.post("/api/keys/import", data=self.key_upload(), headers=self.headers)
        self.assertEqual(response.status, 200)
        self.matrix.import_room_keys.assert_awaited_once_with(b"encrypted-matrix-key-export", "export-passphrase")
        self.assertNotIn("export-passphrase", await response.text())
        response = await self.client.post("/api/keys/import", data=self.key_upload())
        self.assertEqual(response.status, 403)
        response = await self.client.post("/api/rooms/!room/send", data=self.key_upload(), headers=self.headers)
        self.assertEqual(response.status, 403)
        response = await self.client.post("/api/keys/import", data=self.key_upload(passphrase=""), headers=self.headers)
        self.assertEqual(response.status, 400)
        response = await self.client.post("/api/keys/import", data=self.key_upload(data=b"x" * (10 * 1024 * 1024 + 1)), headers=self.headers)
        self.assertEqual(response.status, 413)
        self.assertEqual(self.matrix.import_room_keys.await_count, 1)

    async def test_json_mutations_keep_small_body_limit(self):
        response = await self.client.post("/api/settings", json={"padding": "x" * 65536}, headers=self.headers)
        self.assertEqual(response.status, 503)
        client = await self.management_client()
        response = await client.post("/api/settings", json={"padding": "x" * 65536}, headers=self.headers)
        self.assertEqual(response.status, 413)
        self.management.save_settings.assert_not_awaited()

    async def test_unverified_device_error_is_actionable_and_public(self):
        class ActionError(RuntimeError):
            def __init__(self):
                self.code = "unverified_devices"
                self.status = 409
                self.details = {"room_id": "!room", "secret": "private-detail", "devices": [{"user_id": "@alice:example.invalid", "device_id": "ALICE", "fingerprint": "public-fingerprint", "access_token": "private-token"}]}
                super().__init__("private-exception-text")

        self.matrix.send_room_text.side_effect = ActionError()
        with patch("bridge.matrix.MatrixActionError", ActionError, create=True):
            response = await self.client.post("/api/rooms/!room/send", json={"text": "Hello", "transaction_id": "id"}, headers=self.headers)
            self.assertEqual(response.status, 409)
            value = await response.json()
            self.assertEqual(value["code"], "unverified_devices")
            self.assertEqual(value["details"]["devices"][0]["fingerprint"], "public-fingerprint")
            self.assertNotIn("private-", await response.text())

    async def test_busy_key_import_reports_retry_guidance_without_export_details(self):
        from bridge.matrix import MatrixActionError

        self.matrix.import_room_keys.side_effect = MatrixActionError("key_import_busy", "private-import-detail")
        response = await self.client.post("/api/keys/import", data=self.key_upload(), headers=self.headers)
        self.assertEqual(response.status, 409)
        value = await response.json()
        self.assertEqual(value["code"], "key_import_busy")
        self.assertIn("erneut", value["error"])
        self.assertNotIn("private-import-detail", await response.text())

    async def test_backend_exception_does_not_expose_credentials(self):
        self.matrix.send_room_text.side_effect = RuntimeError("sensitive-token-in-exception")
        response = await self.client.post("/api/rooms/!room/send", json={"text": "Hi", "transaction_id": "id"}, headers=self.headers)
        self.assertEqual(response.status, 503)
        self.assertNotIn("sensitive-token", await response.text())

    async def test_previous_account_request_cannot_send_or_read_current_account(self):
        self.matrix.messenger_state.return_value["account_id"] = "current-account"
        for headers in (self.headers, {**self.headers, "X-Matrix-Account": "previous-account"}):
            response = await self.client.post("/api/rooms/!room/send", json={"text": "old", "transaction_id": "tx"}, headers=headers)
            self.assertEqual(response.status, 409)
            self.assertEqual((await response.json())["code"], "account_changed")
        response = await self.client.get("/api/rooms/!room/messages", headers={"X-Matrix-Account": "previous-account"})
        self.assertEqual(response.status, 409)
        self.matrix.send_room_text.assert_not_awaited()
        self.matrix.room_history.assert_not_awaited()
        response = await self.client.post("/api/rooms/!room/send", json={"text": "current", "transaction_id": "tx"}, headers={**self.headers, "X-Matrix-Account": "current-account"})
        self.assertEqual(response.status, 200)

    async def test_settings_validation_gives_concrete_safe_input_guidance(self):
        from bridge.matrix import MatrixActionError

        client = await self.management_client()
        message = "Die vollständige Matrix-Benutzer-ID wird benötigt."
        self.management.save_settings.side_effect = MatrixActionError("invalid_settings", message, status=400)
        response = await client.post("/api/settings", json={"matrix": {"user_id": "short"}}, headers=self.headers)
        self.assertEqual(response.status, 400)
        self.assertEqual((await response.json())["error"], message)

    async def test_frontend_assets_use_relative_urls_and_security_headers(self):
        response = await self.client.get("/")
        html = await response.text()
        self.assertIn('href="static/app.css"', html)
        self.assertIn('src="static/app.js"', html)
        self.assertIn("script-src 'self'", response.headers["Content-Security-Policy"])
        self.assertEqual(response.headers["Cache-Control"], "no-store")
        self.assertEqual(response.headers["X-Content-Type-Options"], "nosniff")
        response = await self.client.get("/static/app.js")
        javascript = await response.text()
        self.assertEqual(response.status, 200)
        self.assertNotIn("innerHTML", javascript)
        self.assertNotIn("localStorage", javascript)

    async def test_options_does_not_enable_cross_origin_access(self):
        response = await self.client.options("/api/state", headers={"Origin": "https://untrusted.invalid", "Access-Control-Request-Method": "POST"})
        self.assertEqual(response.status, 403)
        self.assertNotIn("Access-Control-Allow-Origin", response.headers)

    async def test_ingress_uses_supervisor_assigned_port(self):
        response = AsyncMock()
        response.raise_for_status = Mock()
        response.json.return_value = {"data": {"ingress_port": 62817}}
        request = AsyncMock()
        request.__aenter__.return_value = response
        session = AsyncMock()
        session.get = Mock(return_value=request)
        session_context = AsyncMock()
        session_context.__aenter__.return_value = session
        runner = SimpleNamespace(setup=AsyncMock(), cleanup=AsyncMock())
        site = SimpleNamespace(start=AsyncMock())
        with patch.dict("os.environ", {"SUPERVISOR_TOKEN": "private-test-token"}), patch("bridge.web.ClientSession", return_value=session_context), patch("bridge.web.web.AppRunner", return_value=runner), patch("bridge.web.web.TCPSite", return_value=site) as site_class:
            server = IngressServer(self.matrix, self.radio)
            await server.start()
            site_class.assert_called_once_with(runner, "0.0.0.0", 62817)
            self.assertEqual(server.port, 62817)
            await server.close()
        runner.cleanup.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
