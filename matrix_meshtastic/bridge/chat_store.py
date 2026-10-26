import base64
import hashlib
import json
import sqlite3
from pathlib import Path


class ChatStore:
    def __init__(self, path: Path, homeserver: str, user_id: str):
        self.db = sqlite3.connect(path)
        path.chmod(0o600)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(
            "CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);"
            "CREATE TABLE IF NOT EXISTS rooms (room_id TEXT PRIMARY KEY, data TEXT NOT NULL, prev_batch TEXT);"
            "CREATE TABLE IF NOT EXISTS messages (room_id TEXT NOT NULL, event_id TEXT NOT NULL, "
            "sender TEXT NOT NULL, displayname TEXT NOT NULL, body TEXT NOT NULL, timestamp_ms INTEGER NOT NULL, "
            "encrypted INTEGER NOT NULL DEFAULT 0, pending INTEGER NOT NULL DEFAULT 0, "
            "PRIMARY KEY(room_id,event_id));"
            "CREATE INDEX IF NOT EXISTS chat_message_order ON messages(room_id,timestamp_ms,event_id);"
        )
        binding = hashlib.sha256(json.dumps([homeserver, user_id]).encode()).hexdigest()
        previous = self.db.execute("SELECT value FROM metadata WHERE key='account'").fetchone()
        self.account_changed = previous is not None and previous[0] != binding
        with self.db:
            if self.account_changed:
                self.db.execute("DELETE FROM rooms")
                self.db.execute("DELETE FROM messages")
                self.db.execute("DELETE FROM metadata")
            self.db.execute("INSERT OR REPLACE INTO metadata VALUES ('account',?)", (binding,))

    def set_metadata(self, key: str, value) -> None:
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO metadata VALUES (?,?)", (key, json.dumps(value)))

    def get_metadata(self, key: str, default=None):
        row = self.db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def upsert_room(self, room_id: str, **values) -> None:
        row = self.db.execute("SELECT data FROM rooms WHERE room_id=?", (room_id,)).fetchone()
        data = json.loads(row[0]) if row else {
            "room_id": room_id, "name": room_id, "encrypted": False,
            "membership": "join", "is_direct": False, "unread_count": 0,
            "last_message_ts": 0, "last_message": "",
        }
        data.update(values)
        with self.db:
            self.db.execute(
                "INSERT INTO rooms(room_id,data) VALUES (?,?) ON CONFLICT(room_id) DO UPDATE SET data=excluded.data",
                (room_id, json.dumps(data, ensure_ascii=False)),
            )

    def room(self, room_id: str) -> dict | None:
        row = self.db.execute("SELECT data FROM rooms WHERE room_id=?", (room_id,)).fetchone()
        return json.loads(row[0]) if row else None

    def snapshot_rooms(self) -> list[dict]:
        rooms = [json.loads(row[0]) for row in self.db.execute("SELECT data FROM rooms")]
        return sorted(rooms, key=lambda room: (room.get("membership") == "invite", room.get("last_message_ts", 0)), reverse=True)

    def set_history_cursor(self, room_id: str, cursor: str | None, only_if_empty: bool = False) -> None:
        self.upsert_room(room_id)
        with self.db:
            condition = " AND prev_batch IS NULL" if only_if_empty else ""
            self.db.execute(f"UPDATE rooms SET prev_batch=? WHERE room_id=?{condition}", (cursor, room_id))

    def save_message(self, room_id: str, event_id: str, sender: str, displayname: str,
                     body: str, timestamp_ms: int, encrypted: bool = False, pending: bool = False) -> dict | None:
        self.upsert_room(room_id)
        body = body[:65536]
        with self.db:
            self.db.execute(
                "INSERT INTO messages VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(room_id,event_id) DO UPDATE SET "
                "displayname=excluded.displayname,body=excluded.body,encrypted=excluded.encrypted,pending=excluded.pending",
                (room_id, event_id, sender, displayname, body, timestamp_ms, int(encrypted), int(pending)),
            )
            self.db.execute(
                "DELETE FROM messages WHERE room_id=? AND event_id NOT IN "
                "(SELECT event_id FROM messages WHERE room_id=? ORDER BY timestamp_ms DESC,event_id DESC LIMIT 2000)",
                (room_id, room_id),
            )
        room = self.room(room_id)
        if timestamp_ms >= room.get("last_message_ts", 0):
            self.upsert_room(room_id, last_message_ts=timestamp_ms, last_message=body[:160])
        return self.message(room_id, event_id)

    def message(self, room_id: str, event_id: str) -> dict | None:
        row = self.db.execute("SELECT * FROM messages WHERE room_id=? AND event_id=?", (room_id, event_id)).fetchone()
        return self._message(row) if row else None

    @staticmethod
    def _message(row) -> dict:
        return {"room_id": row[0], "event_id": row[1], "sender": row[2], "displayname": row[3],
                "body": row[4], "timestamp_ms": row[5], "encrypted": bool(row[6]), "pending": bool(row[7]),
                "decryption_error": bool(row[7])}

    def edit_message(self, room_id: str, event_id: str, sender: str, body: str) -> None:
        with self.db:
            self.db.execute("UPDATE messages SET body=? WHERE room_id=? AND event_id=? AND sender=?",
                            (body[:65536], room_id, event_id, sender))
        self._refresh_preview(room_id)

    def redact_message(self, room_id: str, event_id: str) -> None:
        with self.db:
            self.db.execute("UPDATE messages SET body='Nachricht entfernt',pending=0 WHERE room_id=? AND event_id=?",
                            (room_id, event_id))
        self._refresh_preview(room_id)

    def _refresh_preview(self, room_id: str) -> None:
        latest = self.db.execute("SELECT body,timestamp_ms FROM messages WHERE room_id=? ORDER BY timestamp_ms DESC,event_id DESC LIMIT 1", (room_id,)).fetchone()
        if latest:
            self.upsert_room(room_id, last_message=latest[0][:160], last_message_ts=latest[1])

    def get_messages(self, room_id: str, limit: int = 50, before: str | None = None) -> dict:
        limit = min(max(int(limit), 1), 200)
        parameters = [room_id]
        clause = ""
        if before:
            if not before.startswith("local:"):
                raise ValueError("Ungültiger lokaler Nachrichtenzeiger.")
            try:
                stamp, event_id = json.loads(base64.urlsafe_b64decode(before[6:].encode()).decode())
                if type(stamp) is not int or not isinstance(event_id, str):
                    raise ValueError
            except (ValueError, TypeError, UnicodeError) as exc:
                raise ValueError("Ungültiger Nachrichtenzeiger.") from exc
            clause = " AND (timestamp_ms < ? OR (timestamp_ms=? AND event_id<?))"
            parameters.extend([stamp, stamp, event_id])
        rows = self.db.execute(
            f"SELECT * FROM messages WHERE room_id=?{clause} ORDER BY timestamp_ms DESC,event_id DESC LIMIT ?",
            (*parameters, limit + 1),
        ).fetchall()
        has_more = len(rows) > limit
        rows = rows[:limit]
        if has_more:
            oldest = rows[-1]
            token = base64.urlsafe_b64encode(json.dumps([oldest[5], oldest[1]]).encode()).decode()
            cursor = "local:" + token
        else:
            room = self.db.execute("SELECT prev_batch FROM rooms WHERE room_id=?", (room_id,)).fetchone()
            cursor = room[0] if room else None
        return {"messages": [self._message(row) for row in reversed(rows)], "next_batch": cursor}

    def close(self) -> None:
        self.db.close()
