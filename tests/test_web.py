import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

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
        )
        self.radio = lambda: {"enabled": False, "state": "disabled", "reason": "Matrix funktioniert eigenständig", "channel_psk": "must-not-reach-browser"}
        self.client = TestClient(TestServer(create_app(self.matrix, self.radio, allowed_peers={"127.0.0.1"})))
        await self.client.start_server()
        self.headers = {"X-Matrix-Messenger": "1"}

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

    async def test_backend_exception_does_not_expose_credentials(self):
        self.matrix.send_room_text.side_effect = RuntimeError("sensitive-token-in-exception")
        response = await self.client.post("/api/rooms/!room/send", json={"text": "Hi", "transaction_id": "id"}, headers=self.headers)
        self.assertEqual(response.status, 503)
        self.assertNotIn("sensitive-token", await response.text())

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
