import copy
import json
import os
from dataclasses import asdict
from pathlib import Path
from urllib.parse import urlparse

from .config import ForwardingSettings, MatrixSettings, RadioSettings, Settings


SECRET_FIELDS = {
    "matrix": ("password", "access_token"),
    "meshtastic": ("channel_psk", "mqtt_password"),
    "forwarding": (),
}


def defaults() -> dict:
    return {
        "matrix": asdict(MatrixSettings()),
        "meshtastic": asdict(RadioSettings()),
        "forwarding": asdict(ForwardingSettings()),
    }


class SettingsStore:
    def __init__(self, options_path: Path, data_dir: Path):
        self.path = data_dir / "settings.json"
        self.options_path = options_path
        self.error = None
        self.raw = defaults()
        source = self.path if self.path.exists() else options_path
        if source.exists():
            try:
                value = json.loads(source.read_text())
                self.raw = self._merge(self.raw, value, preserve_secrets=False)
            except (OSError, ValueError, TypeError):
                self.error = "Die gespeicherten Einstellungen konnten nicht geladen werden. Bitte die Einrichtung prüfen."

    @staticmethod
    def _merge(original: dict, patch: dict, preserve_secrets: bool = True) -> dict:
        if not isinstance(patch, dict) or set(patch) - set(original):
            raise ValueError("Unbekannte Einstellungsgruppe.")
        result = copy.deepcopy(original)
        for group, values in patch.items():
            if not isinstance(values, dict):
                raise ValueError("Ungültige Einstellungsgruppe.")
            secrets = SECRET_FIELDS[group]
            allowed = set(result[group]) | {name + "_clear" for name in secrets}
            if set(values) - allowed:
                raise ValueError("Unbekanntes Einstellungsfeld.")
            for key, value in values.items():
                if key.endswith("_clear"):
                    if not isinstance(value, bool):
                        raise ValueError("Das Entfernen eines Geheimnisses muss bestätigt werden.")
                    continue
                if preserve_secrets and key in secrets and value == "":
                    continue
                result[group][key] = value
            for name in secrets:
                if values.get(name + "_clear"):
                    result[group][name] = ""
        return result

    def candidate(self, patch: dict) -> tuple[dict, Settings]:
        result = self._merge(self.raw, patch)
        matrix = result["matrix"]
        if isinstance(matrix.get("homeserver"), str):
            matrix["homeserver"] = matrix["homeserver"].strip().rstrip("/")
            if matrix["homeserver"] and "://" not in matrix["homeserver"]:
                matrix["homeserver"] = "https://" + matrix["homeserver"]
        if isinstance(matrix.get("user_id"), str):
            matrix["user_id"] = matrix["user_id"].strip()
        previous = copy.deepcopy(self.raw["matrix"])
        previous_server = previous.get("homeserver")
        if isinstance(previous_server, str):
            previous_server = previous_server.strip().rstrip("/")
            if previous_server and "://" not in previous_server:
                previous_server = "https://" + previous_server
            previous["homeserver"] = previous_server
        if isinstance(previous.get("user_id"), str):
            previous["user_id"] = previous["user_id"].strip()
        identity_changed = any(matrix.get(key) != previous.get(key) for key in ("homeserver", "user_id"))
        if identity_changed:
            supplied = patch.get("matrix", {})
            for name in ("password", "access_token", "device_id"):
                if not supplied.get(name):
                    matrix[name] = ""
        if matrix.get("access_token") and patch.get("matrix", {}).get("password") and not patch.get("matrix", {}).get("access_token"):
            matrix["access_token"] = ""
            matrix["device_id"] = ""
        if isinstance(matrix.get("homeserver"), str):
            server = urlparse(matrix["homeserver"])
            if server.username or server.password or server.query or server.fragment:
                raise ValueError("Bitte eine Matrix-Serveradresse ohne Zugangsdaten oder URL-Parameter eingeben.")
        settings = Settings.from_dict(result)
        return result, settings

    def settings(self) -> Settings:
        if self.error:
            raise ValueError(self.error)
        return Settings.from_dict(self.raw)

    def public(self) -> dict:
        result = copy.deepcopy(self.raw)
        server = result["matrix"].get("homeserver", "")
        if isinstance(server, str):
            try:
                parsed = urlparse(server)
                result["matrix"]["homeserver"] = parsed._replace(
                    netloc=parsed.netloc.rsplit("@", 1)[-1], query="", fragment=""
                ).geturl()
            except ValueError:
                result["matrix"]["homeserver"] = ""
        for group, fields in SECRET_FIELDS.items():
            for name in fields:
                result[group]["has_" + name] = bool(result[group].pop(name, ""))
        try:
            self.settings()
        except ValueError as error:
            result.update(configured=False, validation_error=str(error))
        else:
            result.update(configured=True, validation_error=None)
        return result

    def save(self, raw: dict) -> None:
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        temporary = self.path.with_suffix(".tmp")
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(raw, handle, ensure_ascii=False)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.chmod(0o600)
        temporary.replace(self.path)
        self.raw = copy.deepcopy(raw)
        self.error = None
