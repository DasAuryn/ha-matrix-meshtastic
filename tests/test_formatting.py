import pytest

from bridge.config import ForwardingSettings
from bridge.formatting import matrix_message, mesh_messages


@pytest.mark.parametrize("body", ["Grüße " * 300, "🔥🚒" * 400, "a" * 500, "界" * 500])
@pytest.mark.parametrize("max_parts", [1, 4, 16])
def test_mesh_payload_limit_includes_part_numbers_and_sender(body, max_parts):
    settings = ForwardingSettings(max_parts=max_parts)
    parts = mesh_messages(body, "@einsatzleitung:example.org", settings)
    assert 1 <= len(parts) <= max_parts
    assert all(len(part.encode("utf-8")) <= 233 for part in parts)
    assert parts[-1].endswith("…") or len(parts) < max_parts


def test_split_keeps_text_in_order_without_changing_unicode():
    body = "🚒 Grüße " * 40
    parts = mesh_messages(body, "@alice:example.org", ForwardingSettings(include_sender=False, max_parts=16))
    combined = "".join(part.split(") ", 1)[1] for part in parts)
    assert combined == body.strip()


def test_truncate_does_not_cut_a_unicode_codepoint():
    parts = mesh_messages("🚒" * 100, "@alice:example.org", ForwardingSettings(long_message="truncate"))
    assert len(parts) == 1
    assert parts[0].endswith("…")
    assert len(parts[0].encode()) <= 233
    assert "�" not in parts[0]


def test_single_message_keeps_local_matrix_name():
    assert mesh_messages("Hallo Mesh", "@alice:example.org", ForwardingSettings()) == ["[alice] Hallo Mesh"]


def test_empty_messages_are_not_sent():
    assert mesh_messages("\n  \t", "@alice:example.org", ForwardingSettings()) == []


def test_matrix_attribution_is_optional():
    assert matrix_message("Hallo", "!12345678", ForwardingSettings()) == "[!12345678] Hallo"
    assert matrix_message("Hallo", "!12345678", ForwardingSettings(include_sender=False)) == "Hallo"
