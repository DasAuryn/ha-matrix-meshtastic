import re

from .config import ForwardingSettings


def _take_bytes(text: str, budget: int) -> tuple[str, str]:
    prefix = text.encode("utf-8")[:budget].decode("utf-8", errors="ignore")
    return prefix, text[len(prefix):]


def mesh_messages(body: str, sender: str, settings: ForwardingSettings) -> list[str]:
    text = re.sub(r"\s+", " ", body).strip()
    if not text:
        return []
    name = sender.removeprefix("@").split(":", 1)[0]
    name = re.sub(r"[\s\[\]:]+", " ", name).strip()
    name, _ = _take_bytes(name, 32)
    prefix = f"[{name}] " if settings.include_sender and name else ""
    text = prefix + text
    if len(text.encode("utf-8")) <= 233:
        return [text]
    if settings.long_message == "truncate":
        part, _ = _take_bytes(text, 230)
        return [part + "…"]
    reserve = len(f"({settings.max_parts}/{settings.max_parts}) ".encode())
    parts = []
    remaining = text
    while remaining and len(parts) < settings.max_parts:
        part, remaining = _take_bytes(remaining, 233 - reserve)
        parts.append(part)
    if remaining:
        parts[-1], _ = _take_bytes(parts[-1], 233 - reserve - 3)
        parts[-1] += "…"
    return [f"({index}/{len(parts)}) {part}" for index, part in enumerate(parts, 1)]


def matrix_message(body: str, sender: str, settings: ForwardingSettings) -> str:
    return f"[{sender}] {body}" if settings.include_sender else body
