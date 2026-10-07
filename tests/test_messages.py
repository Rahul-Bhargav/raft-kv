import dataclasses
import json

import pytest

from raft.messages import (
    AppendEntries,
    AppendEntriesReply,
    Entry,
    RequestVote,
    RequestVoteReply,
    from_dict,
    to_dict,
)

ENTRIES = (
    Entry(2, {"op": "set", "key": "a", "value": "1"}),
    Entry(2, {"op": "delete", "key": "a", "client_id": "c1", "seq": 7}),
    Entry(3, {"op": "noop"}),
)

SAMPLES = {
    "RequestVote": RequestVote("n1", "n2", 4, last_log_index=9, last_log_term=3),
    "RequestVoteReply": RequestVoteReply("n2", "n1", 4, granted=True),
    "AppendEntries": AppendEntries(
        "n1", "n3", 4, prev_log_index=6, prev_log_term=2, entries=ENTRIES, leader_commit=5
    ),
    "AppendEntries (heartbeat)": AppendEntries(
        "n1", "n3", 4, prev_log_index=9, prev_log_term=3, entries=(), leader_commit=9
    ),
    "AppendEntriesReply": AppendEntriesReply(
        "n3", "n1", 4, success=False, match_index=0, last_log_index=5
    ),
}


@pytest.mark.parametrize("msg", SAMPLES.values(), ids=SAMPLES.keys())
def test_round_trips_through_json(msg):
    wire = json.dumps(to_dict(msg))
    assert from_dict(json.loads(wire)) == msg


@pytest.mark.parametrize("msg", SAMPLES.values(), ids=SAMPLES.keys())
def test_to_dict_is_json_native_and_tagged(msg):
    d = to_dict(msg)
    assert d["type"] == type(msg).__name__
    assert d == json.loads(json.dumps(d))  # only dicts, lists, str, int, bool


def test_to_dict_flattens_entries():
    d = to_dict(SAMPLES["AppendEntries"])
    assert d["entries"][0] == {"term": 2, "command": {"op": "set", "key": "a", "value": "1"}}
    assert [e["term"] for e in d["entries"]] == [2, 2, 3]


def test_from_dict_rebuilds_entries_as_tuple_of_entry():
    decoded = from_dict(json.loads(json.dumps(to_dict(SAMPLES["AppendEntries"]))))
    assert isinstance(decoded.entries, tuple)
    assert all(isinstance(e, Entry) for e in decoded.entries)
    assert decoded.entries[1].command["seq"] == 7  # client_id/seq survive


@pytest.mark.parametrize("bad", [{"type": "Nope", "src": "n1"}, {"src": "n1"}])
def test_unknown_or_missing_type_raises_value_error(bad):
    with pytest.raises(ValueError):
        from_dict(bad)


def test_entry_is_not_a_message():
    with pytest.raises(ValueError):
        from_dict({"type": "Entry", "term": 1, "command": {"op": "noop"}})


def test_missing_field_is_rejected():
    d = to_dict(SAMPLES["RequestVote"])
    del d["last_log_term"]
    with pytest.raises(TypeError):
        from_dict(d)


def test_messages_are_immutable():
    msg = SAMPLES["RequestVote"]
    with pytest.raises(dataclasses.FrozenInstanceError):
        msg.term = 5  # type: ignore[misc]
    assert not hasattr(msg, "__dict__")  # slots


def test_to_dict_does_not_alias_commands():
    msg = SAMPLES["AppendEntries"]
    to_dict(msg)["entries"][0]["command"]["value"] = "changed"
    assert msg.entries[0].command["value"] == "1"


def test_from_dict_does_not_alias_commands():
    d = to_dict(SAMPLES["AppendEntries"])
    decoded = from_dict(d)
    d["entries"][1]["command"]["key"] = "changed"
    assert decoded.entries[1].command["key"] == "a"
