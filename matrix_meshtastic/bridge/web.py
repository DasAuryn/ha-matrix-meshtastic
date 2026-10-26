import asyncio
import inspect
import json
import logging
import os
import re
from collections.abc import Callable
from pathlib import Path

from aiohttp import ClientSession, ClientTimeout, web

from .request_context import requested_account


LOGGER = logging.getLogger(__name__)
STATIC = Path(__file__).with_name("static")
MAX_JSON = 65536
MAX_KEY_FILE = 10 * 1024 * 1024
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


def _safe_settings(snapshot):
    groups = {
        "matrix": ("homeserver", "user_id", "room_id", "device_id", "require_verified_devices", "has_password", "has_access_token"),
        "meshtastic": ("enabled", "connection", "channel_index", "tcp_host", "tcp_port", "serial_device", "ble_address", "mqtt_host", "mqtt_port", "mqtt_username", "mqtt_tls", "mqtt_topic", "mqtt_gateway_id", "mqtt_channel_name", "has_channel_psk", "has_mqtt_password"),
        "forwarding": ("include_sender", "max_message_age_seconds", "message_interval_seconds", "long_message", "max_parts"),
    }
    value = {key: snapshot[key] for key in ("configured", "config_valid", "setup_required", "validation_error", "saved", "applied", "logged_out", "server_session_revoked", "logout_notice") if key in snapshot}
    for name, allowed in groups.items():
        group = snapshot.get(name, {})
        value[name] = {key: group[key] for key in allowed if key in group}
    return value


def _public_state(snapshot):
    value = {key: snapshot[key] for key in ("connected", "user_id", "device_id", "account_id", "require_verified_devices", "error", "bridge_error", "configured", "config_valid", "setup_required") if key in snapshot}
    collections = {
        "rooms": ("room_id", "name", "encrypted", "membership", "is_direct", "unread_count", "last_message_ts", "last_message"),
        "devices": ("user_id", "device_id", "display_name", "fingerprint", "verified", "own_device"),
        "verification": ("transaction_id", "user_id", "device_id", "state", "emoji"),
    }
    for name, allowed in collections.items():
        value[name] = [
            {key: item[key] for key in allowed if key in item}
            for item in snapshot.get(name, []) if isinstance(item, dict)
        ]
    for verification in value["verification"]:
        verification["emoji"] = [
            {key: item[key] for key in ("emoji", "description") if key in item}
            for item in verification.get("emoji", []) if isinstance(item, dict)
        ]
    diagnostics = snapshot.get("diagnostics")
    if isinstance(diagnostics, dict):
        value["diagnostics"] = {key: diagnostics[key] for key in ("pending_encrypted_count", "unverified_device_count", "key_import_available") if key in diagnostics}
        value["diagnostics"]["rooms_missing_keys"] = [
            {key: room[key] for key in ("room_id", "name", "count") if key in room}
            for room in diagnostics.get("rooms_missing_keys", []) if isinstance(room, dict)
        ]
    return value


def _action_response(error):
    try:
        from .matrix import MatrixActionError
    except ImportError:
        return None

    messages = {
        "unverified_devices": "Die zusätzliche Geräteprüfung ist aktiviert. Die Geräte dieses Chats unter Geräte & Schlüssel bestätigen oder die Prüfung in den Matrix-Einstellungen ausschalten.",
        "encryption_not_ready": "Die Verschlüsselung ist noch nicht bereit. Bitte kurz warten und erneut senden. Fehlende Raumschlüssel können unter Geräte & Schlüssel erneut angefordert werden.",
        "key_import_failed": "Die Schlüsseldatei konnte nicht importiert werden. Datei und Export-Passphrase prüfen.",
        "key_import_busy": "Die Verschlüsselung verarbeitet gerade eine andere Anfrage. Bitte kurz warten und den Import erneut starten.",
        "fingerprint_mismatch": "Der Fingerabdruck stimmt nicht mehr überein. Das Gerät erneut vergleichen.",
        "confirmation_required": "Den Fingerabdruck zuerst mit dem anderen Gerät vergleichen und ausdrücklich bestätigen.",
        "setup_required": "Bitte zuerst das Matrix-Konto in den Einstellungen verbinden.",
        "invalid_settings": "Bitte die eingegebenen Einstellungen prüfen.",
        "permission_denied": "Deinem Matrix-Konto fehlt die Berechtigung für diese Aktion.",
        "account_changed": "Das Matrix-Konto wurde inzwischen gewechselt. Bitte die Aktion erneut öffnen.",
    }
    if not isinstance(error, MatrixActionError) or error.code not in messages:
        return None
    details = error.details if isinstance(error.details, dict) else {}
    safe = {key: details[key] for key in ("room_id", "user_id", "device_id") if key in details}
    if isinstance(details.get("devices"), list):
        safe["devices"] = [
            {key: device[key] for key in ("user_id", "device_id", "display_name", "fingerprint", "verified", "own_device") if key in device}
            for device in details["devices"] if isinstance(device, dict)
        ]
    status = error.status if error.status in (400, 403, 404, 409, 422, 503) else 409
    message = str(error) if error.code == "invalid_settings" else messages[error.code]
    return _json({"error": message, "code": error.code, "details": safe}, status)


def create_app(matrix, radio_status: Callable[[], dict], allowed_peers=None, management=None) -> web.Application:
    peers = frozenset(("172.30.32.2",) if allowed_peers is None else allowed_peers)

    @web.middleware
    async def ingress_access(request, handler):
        key_upload = request.path.endswith("/api/keys/import")
        valid_content_type = request.content_type == "application/json" or (key_upload and request.content_type == "multipart/form-data")
        if request.remote not in peers:
            response = _json({"error": "Zugriff nur über Home Assistant Ingress."}, 403)
        elif request.method not in ("GET", "HEAD") and (
            request.headers.get("X-Matrix-Messenger") != "1" or not valid_content_type
        ):
            response = _json({"error": "Ungültige Messenger-Anfrage."}, 403)
        else:
            context_token = None
            try:
                bound = any(part in request.path for part in ("/api/rooms/", "/api/direct", "/api/keys/", "/api/verification/", "/api/account/logout"))
                if bound:
                    snapshot = await _await(matrix.messenger_state())
                    account_id = snapshot.get("account_id")
                    expected = request.headers.get("X-Matrix-Account")
                    if account_id and expected != account_id:
                        from .matrix import MatrixActionError

                        raise MatrixActionError("account_changed", "Das Matrix-Konto wurde inzwischen gewechselt.", status=409)
                    context_token = requested_account.set(expected if account_id else None)
                response = await handler(request)
            except web.HTTPException as error:
                response = _json({"error": error.reason}, error.status)
            except (asyncio.TimeoutError, ConnectionError):
                response = _json({"error": "Matrix antwortet derzeit nicht. Bitte erneut versuchen."}, 503)
            except ValueError:
                response = _json({"error": "Die Eingabe ist ungültig."}, 400)
            except Exception as error:
                response = _action_response(error)
                if response is None:
                    LOGGER.warning("Messenger-Anfrage fehlgeschlagen (%s).", type(error).__name__)
                    response = _json({"error": "Die Aktion ist derzeit nicht möglich. Matrix-Verbindung und Gerätefreigabe prüfen."}, 503)
            finally:
                if context_token is not None:
                    requested_account.reset(context_token)
        response.headers.update(HEADERS)
        return response

    app = web.Application(middlewares=[ingress_access], client_max_size=MAX_KEY_FILE + MAX_JSON)

    async def state(request):
        snapshot = await _await(matrix.messenger_state())
        value = _public_state(snapshot)
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
            raw = bytearray()
            while chunk := await request.content.read(8192):
                raw.extend(chunk)
                if len(raw) > MAX_JSON:
                    raise web.HTTPRequestEntityTooLarge(max_size=MAX_JSON, actual_size=len(raw))
            value = json.loads(raw)
        except (ValueError, UnicodeError):
            raise web.HTTPBadRequest(reason="Ungültige JSON-Anfrage.") from None
        if not isinstance(value, dict):
            raise web.HTTPBadRequest(reason="Ungültige JSON-Anfrage.")
        return value

    def require_management():
        if management is None:
            raise web.HTTPServiceUnavailable(reason="Die Einstellungen sind noch nicht verfügbar.")
        return management

    async def settings(request):
        return _json(_safe_settings(await _await(require_management().get_settings())))

    async def save_settings(request):
        result = await asyncio.wait_for(_await(require_management().save_settings(await body(request))), 60)
        if isinstance(result, dict):
            return _json(_safe_settings(result))
        return _json({"saved": True})

    async def discover_devices(request):
        manager = require_management()
        result = await asyncio.wait_for(_await(manager.discover_devices()), 15)
        return _json({"serial": [
            {key: item[key] for key in ("path", "label") if key in item}
            for item in result.get("serial", []) if isinstance(item, dict)
        ]})

    async def logout(request):
        value = await body(request)
        if value.get("confirmed") is not True:
            raise web.HTTPBadRequest(reason="Das Abmelden dieses Matrix-Kontos muss bestätigt werden.")
        result = await asyncio.wait_for(_await(require_management().logout(True)), 60)
        return _json(_safe_settings(result))

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

    async def join_by_id(request):
        value = await body(request)
        room_id = value.get("room_id")
        if not isinstance(room_id, str) or not room_id.startswith(("!", "#")) or not 2 <= len(room_id) <= 255:
            raise web.HTTPBadRequest(reason="Bitte eine Raum-ID oder einen Raum-Alias eingeben.")
        return _json(await asyncio.wait_for(matrix.join_room(room_id), 45))

    def valid_user(user_id):
        return isinstance(user_id, str) and user_id.startswith("@") and ":" in user_id and len(user_id) <= 255

    async def create_room(request):
        value = await body(request)
        name, invite, encrypted = value.get("name"), value.get("invite", []), value.get("encrypted", True)
        if not isinstance(name, str) or not name.strip() or len(name) > 255:
            raise web.HTTPBadRequest(reason="Bitte einen Raumnamen mit höchstens 255 Zeichen eingeben.")
        if not isinstance(invite, list) or len(invite) > 50 or not all(valid_user(user_id) for user_id in invite):
            raise web.HTTPBadRequest(reason="Bitte vollständige Matrix-IDs für die Einladungen eingeben.")
        if not isinstance(encrypted, bool):
            raise web.HTTPBadRequest(reason="Ungültige Verschlüsselungseinstellung.")
        return _json(await asyncio.wait_for(matrix.create_room(name.strip(), invite, encrypted), 45))

    async def invite_user(request):
        value = await body(request)
        if not valid_user(value.get("user_id")):
            raise web.HTTPBadRequest(reason="Bitte eine vollständige Matrix-ID eingeben.")
        return _json(await asyncio.wait_for(matrix.invite_user(request.match_info["room_id"], value["user_id"]), 45))

    async def rename_room(request):
        value = await body(request)
        name = value.get("name")
        if not isinstance(name, str) or not name.strip() or len(name) > 255:
            raise web.HTTPBadRequest(reason="Bitte einen gültigen Raumnamen eingeben.")
        return _json(await asyncio.wait_for(matrix.rename_room(request.match_info["room_id"], name.strip()), 45))

    async def leave_room(request):
        value = await body(request)
        if value.get("confirmed") is not True:
            raise web.HTTPBadRequest(reason="Das Verlassen dieses Raums muss bestätigt werden.")
        return _json(await asyncio.wait_for(matrix.leave_room(request.match_info["room_id"]), 45))

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

    async def verify_device(request):
        value = await body(request)
        user_id, device_id, fingerprint = value.get("user_id"), value.get("device_id"), value.get("fingerprint")
        if not valid_user(user_id) or not isinstance(device_id, str) or not 1 <= len(device_id) <= 255:
            raise web.HTTPBadRequest(reason="Ungültiges Matrix-Gerät.")
        if not isinstance(fingerprint, str) or not 1 <= len(fingerprint) <= 255:
            raise web.HTTPBadRequest(reason="Der verglichene Fingerabdruck wird benötigt.")
        if value.get("confirmed") is not True:
            raise web.HTTPBadRequest(reason="Der Fingerabdruck muss ausdrücklich bestätigt werden.")
        return _json(await asyncio.wait_for(matrix.verify_device_fingerprint(user_id, device_id, fingerprint, True), 30))

    async def retry_keys(request):
        value = await body(request)
        room_id = value.get("room_id")
        if room_id is not None and (not isinstance(room_id, str) or not 2 <= len(room_id) <= 255):
            raise web.HTTPBadRequest(reason="Ungültige Raum-ID.")
        return _json(await asyncio.wait_for(matrix.retry_keys(room_id), 45))

    async def import_keys(request):
        if request.content_type != "multipart/form-data":
            raise web.HTTPBadRequest(reason="Eine verschlüsselte Matrix-Schlüsseldatei wird benötigt.")
        reader = await request.multipart()
        values = {}
        count = 0
        async for part in reader:
            count += 1
            if count > 2 or part.name not in ("file", "passphrase") or part.name in values:
                raise web.HTTPBadRequest(reason="Ungültiger Schlüsselimport.")
            limit = MAX_KEY_FILE if part.name == "file" else 4096
            chunks = bytearray()
            while True:
                chunk = await part.read_chunk()
                if not chunk:
                    break
                chunks.extend(chunk)
                if len(chunks) > limit:
                    raise web.HTTPRequestEntityTooLarge(max_size=limit, actual_size=len(chunks))
            values[part.name] = bytes(chunks)
        data = values.get("file", b"")
        try:
            passphrase = values.get("passphrase", b"").decode("utf-8")
        except UnicodeError:
            raise web.HTTPBadRequest(reason="Ungültige Export-Passphrase.") from None
        if not data or not passphrase or len(passphrase) > 1024:
            raise web.HTTPBadRequest(reason="Schlüsseldatei und Export-Passphrase eingeben.")
        return _json(await asyncio.wait_for(matrix.import_room_keys(data, passphrase), 300))

    async def index(request):
        return web.FileResponse(STATIC / "index.html")

    app.router.add_get("/", index)
    app.router.add_get("/api/state", state)
    app.router.add_get("/api/settings", settings)
    app.router.add_post("/api/settings", save_settings)
    app.router.add_post("/api/account/logout", logout)
    app.router.add_get("/api/devices/discover", discover_devices)
    app.router.add_post("/api/rooms/create", create_room)
    app.router.add_post("/api/rooms/join", join_by_id)
    app.router.add_get("/api/rooms/{room_id}/messages", history)
    app.router.add_post("/api/rooms/{room_id}/send", send)
    app.router.add_post("/api/rooms/{room_id}/join", join)
    app.router.add_post("/api/rooms/{room_id}/read", read)
    app.router.add_post("/api/rooms/{room_id}/invite", invite_user)
    app.router.add_post("/api/rooms/{room_id}/rename", rename_room)
    app.router.add_post("/api/rooms/{room_id}/leave", leave_room)
    app.router.add_post("/api/direct", direct)
    app.router.add_post("/api/verification/start", start_verification)
    app.router.add_post("/api/verification/device", verify_device)
    app.router.add_post("/api/verification/{transaction_id}", verification)
    app.router.add_post("/api/keys/retry", retry_keys)
    app.router.add_post("/api/keys/import", import_keys)
    app.router.add_static("/static/", STATIC, show_index=False, follow_symlinks=False)
    return app


class IngressServer:
    def __init__(self, matrix, radio_status: Callable[[], dict], port: int | None = None, management=None):
        self.app = create_app(matrix, radio_status, management=management)
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
