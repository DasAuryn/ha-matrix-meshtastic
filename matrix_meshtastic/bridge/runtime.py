import asyncio
import copy
import hashlib
import json
import logging
from pathlib import Path

from nio import ErrorResponse

from .matrix import MatrixActionError, _read_private, _write_private
from .request_context import requested_account
from .service import Bridge
from .settings_store import SettingsStore
from .web import IngressServer


LOGGER = logging.getLogger(__name__)


class MatrixProxy:
    def __init__(self, runtime: "RuntimeController"):
        self.runtime = runtime

    def account_id(self) -> str:
        public = self.runtime.store.public()
        account = public["matrix"]
        return hashlib.sha256(json.dumps([account.get("homeserver"), account.get("user_id")]).encode()).hexdigest()

    def messenger_state(self) -> dict:
        bridge = self.runtime.bridge
        account_id = self.account_id()
        if bridge is not None and self.runtime.bridge_task and not self.runtime.bridge_task.done():
            return {**bridge.matrix.messenger_state(), "account_id": account_id}
        return {
            "connected": False, "user_id": self.runtime.store.raw["matrix"].get("user_id", ""),
            "device_id": "", "rooms": [], "devices": [], "verification": [],
            "error": self.runtime.problem or "Bitte Matrix im Einrichtungsassistenten verbinden.",
            "setup_required": not self.runtime.store.public()["configured"],
            "account_id": account_id,
        }

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)

        async def call(*args, **kwargs):
            expected = requested_account.get()
            if expected is not None and expected != self.account_id():
                raise MatrixActionError("account_changed", "Das Matrix-Konto wurde inzwischen gewechselt. Bitte die Aktion erneut öffnen.", status=409)
            bridge = self.runtime.bridge
            if bridge is None or not self.runtime.bridge_task or self.runtime.bridge_task.done():
                raise MatrixActionError("setup_required", "Bitte zuerst das Matrix-Konto in den Einstellungen verbinden.")
            return await getattr(bridge.matrix, name)(*args, **kwargs)

        return call


class RuntimeController:
    def __init__(self, data_dir: Path, options_path: Path):
        self.data_dir = data_dir
        self.store = SettingsStore(options_path, data_dir)
        self.bridge = None
        self.bridge_task = None
        self.problem = None
        self.stop = asyncio.Event()
        self.lock = asyncio.Lock()
        self.matrix = MatrixProxy(self)
        self.ingress = IngressServer(self.matrix, self.radio_status, management=self)

    def get_settings(self) -> dict:
        return self.store.public()

    def radio_status(self) -> dict:
        if self.bridge is not None:
            return self.bridge.radio_status()
        return {"enabled": False, "state": "disabled", "reason": "Matrix zuerst einrichten. Meshtastic ist optional."}

    async def discover_devices(self) -> dict:
        from serial.tools import list_ports

        devices = await asyncio.to_thread(list_ports.comports)
        serial = [{"path": item.device, "label": item.description or item.device} for item in devices]
        by_id = Path("/dev/serial/by-id")
        if by_id.is_dir():
            serial = [{"path": str(path), "label": path.name} for path in sorted(by_id.iterdir())] + serial
        return {"serial": serial}

    async def save_settings(self, payload: dict) -> dict:
        async with self.lock:
            try:
                raw, settings = self.store.candidate(payload)
            except ValueError as error:
                raise MatrixActionError("invalid_settings", str(error), status=400) from None
            self.store.save(raw)
            await self._apply(settings)
            return {**self.store.public(), "saved": True}

    async def logout(self, confirmed: bool) -> dict:
        if confirmed is not True:
            raise MatrixActionError("invalid_settings", "Das Abmelden bitte ausdrücklich bestätigen.", status=400)
        async with self.lock:
            expected = requested_account.get()
            if expected is not None and expected != self.matrix.account_id():
                raise MatrixActionError("account_changed", "Das Matrix-Konto wurde inzwischen gewechselt. Bitte erneut bestätigen.", status=409)
            client = getattr(getattr(self.bridge, "matrix", None), "client", None)
            await self._close_bridge()
            revoked = False
            if client is not None and getattr(client, "access_token", ""):
                try:
                    response = await asyncio.wait_for(client.logout(), timeout=15)
                    revoked = not isinstance(response, ErrorResponse)
                except Exception:
                    LOGGER.warning("Server-Abmeldung konnte nicht bestätigt werden; die lokale Anmeldung wird entfernt.")
                finally:
                    await client.close()
            session_path = self.data_dir / "matrix" / "session.json"
            if session_path.exists():
                try:
                    previous = _read_private(session_path, {})
                    session = {name: previous.get(name, "") for name in ("homeserver", "user_id", "device_id")}
                    session["access_token"] = ""
                    _write_private(session_path, session)
                except Exception:
                    session_path.unlink(missing_ok=True)
            raw = copy.deepcopy(self.store.raw)
            for name in ("user_id", "password", "access_token", "device_id", "room_id"):
                raw["matrix"][name] = ""
            raw["meshtastic"]["enabled"] = False
            self.store.save(raw)
            self.problem = "Abgemeldet. Du kannst jetzt ein Matrix-Konto verbinden."
            return {
                **self.store.public(), "saved": True, "logged_out": True,
                "server_session_revoked": revoked,
                "logout_notice": "Abgemeldet." if revoked else "Lokal abgemeldet. Die Server-Sitzung bei Bedarf im bisherigen Matrix-Client beenden.",
            }

    async def _close_bridge(self) -> None:
        bridge, task = self.bridge, self.bridge_task
        self.bridge = None
        self.bridge_task = None
        if bridge is not None:
            bridge.stop.set()
        if task is not None:
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=15)
            except TimeoutError:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            except Exception as error:
                LOGGER.warning("Vorherige Verbindung wurde beendet (%s).", type(error).__name__)

    async def _apply(self, settings) -> None:
        await self._close_bridge()
        self.problem = None
        try:
            self.bridge = Bridge(settings, self.data_dir, manage_ingress=False)
            self.bridge_task = asyncio.create_task(self.bridge.run())
            self.bridge_task.add_done_callback(self._bridge_done)
        except Exception as error:
            self.problem = "Die Verbindung konnte nicht gestartet werden. Bitte die Einstellungen prüfen."
            LOGGER.warning("Verbindung konnte nicht gestartet werden (%s).", type(error).__name__)

    def _bridge_done(self, task) -> None:
        if task.cancelled() or task is not self.bridge_task:
            return
        error = task.exception()
        if error is not None:
            self.problem = "Die Verbindung wurde beendet. Bitte die Einstellungen prüfen und erneut speichern."
            LOGGER.warning("Verbindung wurde beendet (%s).", type(error).__name__)

    async def run(self) -> None:
        try:
            await self.ingress.start()
            try:
                settings = self.store.settings()
            except ValueError as error:
                self.problem = str(error)
                LOGGER.info("Einrichtung ist über Home Assistant Ingress verfügbar.")
            else:
                await self._apply(settings)
            await self.stop.wait()
        finally:
            async with self.lock:
                await self._close_bridge()
            await self.ingress.close()
