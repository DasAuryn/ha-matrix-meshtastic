from pathlib import Path

import pytest

from matrix_meshtastic.bridge.chat_store import ChatStore


@pytest.fixture
def store(tmp_path):
    database = ChatStore(tmp_path / "chats.sqlite3", "https://example.invalid", "@alice:example.invalid")
    yield database
    database.close()


def test_messages_and_room_names_survive_restart(tmp_path):
    path = tmp_path / "chats.sqlite3"
    store = ChatStore(path, "https://example.invalid", "@alice:example.invalid")
    store.upsert_room("!chat", name="Leitstelle", is_direct=True)
    store.save_message("!chat", "$event", "@bob:example.invalid", "Bob", "Hallo", 100)
    store.close()
    reopened = ChatStore(path, "https://example.invalid", "@alice:example.invalid")
    assert reopened.snapshot_rooms()[0]["name"] == "Leitstelle"
    assert reopened.get_messages("!chat")["messages"][0]["body"] == "Hallo"
    assert path.stat().st_mode & 0o777 == 0o600
    reopened.close()


def test_account_change_never_shows_previous_chats(tmp_path):
    path = tmp_path / "chats.sqlite3"
    store = ChatStore(path, "https://example.invalid", "@alice:example.invalid")
    store.save_message("!chat", "$event", "@alice:example.invalid", "Alice", "Privat", 100)
    store.set_metadata("direct_rooms", {"@bob:example.invalid": ["!chat"]})
    store.close()
    other_account = ChatStore(path, "https://example.invalid", "@bob:example.invalid")
    assert other_account.account_changed
    assert other_account.snapshot_rooms() == []
    assert other_account.get_messages("!chat")["messages"] == []
    assert other_account.get_metadata("direct_rooms", {}) == {}
    other_account.close()


def test_local_pagination_has_no_gaps_with_identical_timestamps(store):
    for index in range(121):
        store.save_message("!chat", f"${index:03}", "@alice:x", "Alice", str(index), 100)
    pages = []
    cursor = None
    while True:
        page = store.get_messages("!chat", 50, cursor)
        pages.extend(message["event_id"] for message in page["messages"])
        cursor = page["next_batch"]
        if cursor is None:
            break
    assert len(pages) == 121
    assert len(set(pages)) == 121


def test_edits_require_the_original_sender(store):
    store.save_message("!chat", "$event", "@alice:x", "Alice", "Original", 100)
    store.edit_message("!chat", "$event", "@mallory:x", "Fälschung")
    assert store.message("!chat", "$event")["body"] == "Original"
    store.edit_message("!chat", "$event", "@alice:x", "Korrektur")
    assert store.message("!chat", "$event")["body"] == "Korrektur"
    assert store.room("!chat")["last_message"] == "Korrektur"


def test_redacted_message_is_removed_from_sidebar_preview(store):
    store.save_message("!chat", "$event", "@alice:x", "Alice", "Vertraulicher Originaltext", 100)
    store.redact_message("!chat", "$event")
    assert store.room("!chat")["last_message"] == "Nachricht entfernt"
    assert store.message("!chat", "$event")["body"] == "Nachricht entfernt"


def test_decryption_replaces_placeholder_instead_of_duplicating(store):
    store.save_message("!chat", "$encrypted", "@alice:x", "Alice", "Verschlüsselt", 100, True, True)
    store.save_message("!chat", "$encrypted", "@alice:x", "Alice", "Entschlüsselt", 100, True, False)
    messages = store.get_messages("!chat")["messages"]
    assert len(messages) == 1
    assert messages[0]["body"] == "Entschlüsselt"
    assert not messages[0]["pending"]


def test_local_history_reaches_remote_cursor_after_cached_messages(store):
    store.upsert_room("!chat")
    store.set_history_cursor("!chat", "server-previous")
    for index in range(60):
        store.save_message("!chat", str(index), "@alice:x", "Alice", str(index), index)
    first = store.get_messages("!chat", 50)
    assert first["next_batch"].startswith("local:")
    second = store.get_messages("!chat", 50, first["next_batch"])
    assert len(second["messages"]) == 10
    assert second["next_batch"] == "server-previous"
