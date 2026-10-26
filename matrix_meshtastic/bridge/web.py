import asyncio
import inspect
import json
import logging
import os
import re
from collections.abc import Callable
from pathlib import Path

from aiohttp import ClientSession, ClientTimeout, web


LOGGER = logging.getLogger(__name__)
STATIC = Path(__file__).with_name("static")
HEADERS = {
    "Cache-Control": "no-store",
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
    "Content-Security-Policy": "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'self'; base-uri 'self'; form-action 'self'",
}


def _json(value, status=200):
    return web.json_response(value, status=status, dumps=lambda item: json.dumps(item, ensure_ascii=False))


async def _await(value):
    return await value if inspect.isawaitable(value) else value


def create_app(matrix, radio_status: Callable[[], dict], allowed_peers=None) -> web.Application:
    peers = frozenset(("172.30.32.2",) if allowed_peers is None else allowed_peers)

    @web.middleware
    async def ingress_access(request, handler):
        if request.remote not in peers:
            response = _json({"error": "Zugriff nur über Home Assistant Ingress."}, 403)
        elif request.method not in ("GET", "HEAD") and (
            request.headers.get("X-Matrix-Messenger") != "1" or request.content_type != "application/json"
        ):
            response = _json({"error": "Ungültige Messenger-Anfrage."}, 403)
        else:
            try:
                response = await handler(request)
            except web.HTTPException as error:
                response = _json({"error": error.reason}, error.status)
            except (asyncio.TimeoutError, ConnectionError):
                response = _json({"error": "Matrix antwortet derzeit nicht. Bitte erneut versuchen."}, 503)
            except ValueError:
                response = _json({"error": "Die Eingabe ist ungültig."}, 400)
            except Exception as error:
                LOGGER.warning("Messenger-Anfrage fehlgeschlagen (%s).", type(error).__name__)
                response = _json({"error": "Die Aktion ist derzeit nicht möglich. Matrix-Verbindung und Gerätefreigabe prüfen."}, 503)
        response.headers.update(HEADERS)
        return response

    app = web.Application(middlewares=[ingress_access], client_max_size=65536)

    async def state(request):
        snapshot = await _await(matrix.messenger_state())
        value = {key: snapshot[key] for key in ("connected", "user_id", "device_id", "rooms", "verification", "devices", "error") if key in snapshot}
        radio = radio_status()
        value["radio"] = {key: radio[key] for key in ("enabled", "state", "reason") if key in radio}
        return _json(value)

    async def history(request):
        try:
            limit = int(request.query.get("limit", "50"))
        except ValueError:
            raise web.HTTPBadRequest(reason="Ungültige Nachrichtenanzahl.") from None
        if not 1 <= limit <= 100:
            raise web.HTTPBadRequest(reason="Ungültige Nachrichtenanzahl.")
        before = request.query.get("before") or None
        if before and len(before) > 2048:
            raise web.HTTPBadRequest(reason="Ungültige Verlaufsmarke.")
        value = await asyncio.wait_for(matrix.room_history(request.match_info["room_id"], before=before, limit=limit), 45)
        return _json(value)

    async def body(request):
        try:
            value = await request.json()
        except (ValueError, UnicodeError):
            raise web.HTTPBadRequest(reason="Ungültige JSON-Anfrage.") from None
        if not isinstance(value, dict):
            raise web.HTTPBadRequest(reason="Ungültige JSON-Anfrage.")
        return value

    async def send(request):
        value = await body(request)
        text = value.get("text")
        transaction = value.get("transaction_id")
        if not isinstance(text, str) or not text.strip() or len(text) > 16384:
            raise web.HTTPBadRequest(reason="Bitte einen Text mit höchstens 16384 Zeichen eingeben.")
        if not isinstance(transaction, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", transaction):
            raise web.HTTPBadRequest(reason="Eine gültige Nachrichten-ID ist erforderlich.")
        result = await asyncio.wait_for(matrix.send_room_text(request.match_info["room_id"], text, transaction), 45)
        return _json(result or {"ok": True})

    async def join(request):
        await body(request)
        return _json(await asyncio.wait_for(matrix.join_room(request.match_info["room_id"]), 45))

    async def direct(request):
        value = await body(request)
        user_id = value.get("user_id")
        if not isinstance(user_id, str) or not user_id.startswith("@") or ":" not in user_id or len(user_id) > 255:
            raise web.HTTPBadRequest(reason="Bitte eine vollständige Matrix-ID wie @name:server.org eingeben.")
        return _json(await asyncio.wait_for(matrix.create_direct(user_id), 45))

    async def read(request):
        await body(request)
        await asyncio.wait_for(matrix.mark_read(request.match_info["room_id"]), 30)
        return _json({"ok": True})

    async def verification(request):
        value = await body(request)
        action = value.get("action")
        if action not in ("accept", "confirm", "cancel"):
            raise web.HTTPBadRequest(reason="Unbekannte Bestätigungsaktion.")
        result = await asyncio.wait_for(matrix.verification_action(request.match_info["transaction_id"], action), 30)
        return _json(result or {"ok": True})

    async def start_verification(request):
        value = await body(request)
        user_id, device_id = value.get("user_id"), value.get("device_id")
        if not isinstance(user_id, str) or not user_id.startswith("@") or ":" not in user_id:
            raise web.HTTPBadRequest(reason="Ungültige Matrix-ID.")
        if not isinstance(device_id, str) or not 1 <= len(device_id) <= 255:
            raise web.HTTPBadRequest(reason="Ungültige Geräte-ID.")
        return _json(await asyncio.wait_for(matrix.start_verification(user_id, device_id), 30))

    async def index(request):
        return web.FileResponse(STATIC / "index.html")

    app.router.add_get("/", index)
    app.router.add_get("/api/state", state)
    app.router.add_get("/api/rooms/{room_id}/messages", history)
    app.router.add_post("/api/rooms/{room_id}/send", send)
    app.router.add_post("/api/rooms/{room_id}/join", join)
    app.router.add_post("/api/rooms/{room_id}/read", read)
    app.router.add_post("/api/direct", direct)
    app.router.add_post("/api/verification/start", start_verification)
    app.router.add_post("/api/verification/{transaction_id}", verification)
    app.router.add_static("/static/", STATIC, show_index=False, follow_symlinks=False)
    return app


class IngressServer:
    def __init__(self, matrix, radio_status: Callable[[], dict], port: int | None = None):
        self.app = create_app(matrix, radio_status)
        self.port = port
        self.runner = None

    async def start(self) -> None:
        port = self.port
        if not port:
            token = os.environ.get("SUPERVISOR_TOKEN")
            if token:
                try:
                    async with ClientSession(timeout=ClientTimeout(total=15)) as session:
                        async with session.get("http://supervisor/addons/self/info", headers={"Authorization": f"Bearer {token}"}) as response:
                            response.raise_for_status()
                            value = await response.json()
                            port = value["data"]["ingress_port"]
                    if type(port) is not int or not 1 <= port <= 65535:
                        raise ValueError("invalid ingress port")
                except Exception:
                    raise RuntimeError("Der Ingress-Port konnte nicht vom Supervisor abgerufen werden.") from None
            else:
                port = 8099
        self.port = port
        self.runner = web.AppRunner(self.app, access_log=None)
        await self.runner.setup()
        try:
            await web.TCPSite(self.runner, "0.0.0.0", self.port).start()
        except Exception:
            await self.runner.cleanup()
            self.runner = None
            raise
        LOGGER.info("Matrix-Messenger ist über Home Assistant Ingress verfügbar.")

    async def close(self) -> None:
        if self.runner is not None:
            await self.runner.cleanup()
            self.runner = None
