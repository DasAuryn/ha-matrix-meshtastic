import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PendingMessage:
    event_id: str
    direction: str
    part: str
    part_index: int
    timestamp_ms: int


class State:
    def __init__(self, path: Path, binding: str):
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript(
            "CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);"
            "CREATE TABLE IF NOT EXISTS outbox ("
            "event_id TEXT NOT NULL, direction TEXT NOT NULL, parts TEXT NOT NULL, "
            "part_index INTEGER NOT NULL DEFAULT 0, timestamp_ms INTEGER NOT NULL, "
            "status TEXT NOT NULL DEFAULT 'pending', PRIMARY KEY (event_id, direction));"
        )
        previous = self.get("binding")
        with self.db:
            if previous != binding:
                self.db.execute("DELETE FROM outbox")
                self.db.execute("DELETE FROM metadata WHERE key = 'sync_token'")
                self._set("binding", binding)
            self.db.execute("UPDATE outbox SET status='uncertain', parts='[]' WHERE status='sending' AND direction='mesh'")
            self.db.execute("UPDATE outbox SET status='pending' WHERE status='sending' AND direction='matrix'")

    def get(self, key: str) -> str | None:
        row = self.db.execute("SELECT value FROM metadata WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None

    def _set(self, key: str, value: str) -> None:
        self.db.execute("INSERT OR REPLACE INTO metadata VALUES (?, ?)", (key, value))

    def _enqueue(self, direction: str, event_id: str, parts: list[str], timestamp_ms: int) -> bool:
        if not parts:
            return False
        if self.db.execute("SELECT 1 FROM outbox WHERE event_id=? AND direction=?", (event_id, direction)).fetchone():
            return False
        pending = self.db.execute("SELECT count(*) FROM outbox WHERE status IN ('pending', 'sending')").fetchone()[0]
        if pending >= 1000:
            raise RuntimeError("Die dauerhafte Nachrichtenwarteschlange ist voll.")
        cursor = self.db.execute(
            "INSERT OR IGNORE INTO outbox (event_id, direction, parts, timestamp_ms) VALUES (?, ?, ?, ?)",
            (event_id, direction, json.dumps(parts, ensure_ascii=False), timestamp_ms),
        )
        return cursor.rowcount > 0

    def matrix_batch(self, messages: list[tuple[str, list[str], int]], next_batch: str) -> None:
        with self.db:
            for event_id, parts, timestamp_ms in messages:
                self._enqueue("mesh", event_id, parts, timestamp_ms)
            self._set("sync_token", next_batch)

    def radio_message(self, event_id: str, body: str, timestamp_ms: int) -> bool:
        with self.db:
            return self._enqueue("matrix", event_id, [body], timestamp_ms)

    def next(self, direction: str, max_age_seconds: int) -> PendingMessage | None:
        cutoff = int(time.time() * 1000) - max_age_seconds * 1000
        with self.db:
            self.db.execute("UPDATE outbox SET status='expired', parts='[]' WHERE status='pending' AND timestamp_ms < ?", (cutoff,))
            self.db.execute("DELETE FROM outbox WHERE status NOT IN ('pending', 'sending') AND timestamp_ms < ?", (cutoff - 604800000,))
        row = self.db.execute(
            "SELECT event_id, direction, parts, part_index, timestamp_ms FROM outbox "
            "WHERE direction=? AND status='pending' ORDER BY timestamp_ms, rowid LIMIT 1", (direction,)
        ).fetchone()
        if row is None:
            return None
        return PendingMessage(row[0], row[1], json.loads(row[2])[row[3]], row[3], row[4])

    def claim(self, message: PendingMessage) -> None:
        self._status(message, "sending")

    def retry(self, message: PendingMessage) -> None:
        self._status(message, "pending")

    def uncertain(self, message: PendingMessage) -> None:
        self._status(message, "uncertain", clear=True)

    def _status(self, message: PendingMessage, status: str, clear: bool = False) -> None:
        with self.db:
            self.db.execute("UPDATE outbox SET status=? WHERE event_id=? AND direction=?", (status, message.event_id, message.direction))
            if clear:
                self.db.execute("UPDATE outbox SET parts='[]' WHERE event_id=? AND direction=?", (message.event_id, message.direction))

    def complete(self, message: PendingMessage) -> None:
        row = self.db.execute("SELECT parts FROM outbox WHERE event_id=? AND direction=?", (message.event_id, message.direction)).fetchone()
        done = message.part_index + 1 >= len(json.loads(row[0]))
        with self.db:
            self.db.execute(
                "UPDATE outbox SET status=?, part_index=part_index+1, parts=? WHERE event_id=? AND direction=?",
                ("sent" if done else "pending", "[]" if done else row[0], message.event_id, message.direction),
            )

    def close(self) -> None:
        self.db.close()
