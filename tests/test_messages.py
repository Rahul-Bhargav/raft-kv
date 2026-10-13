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

# The wire format, written out by hand. Each direction is tested against this table on its own,
# so a mistake that to_dict and from_dict share (a renamed key, say) can't pass by symmetry.
WIRE = {
    "RequestVote": (
        RequestVote("n1", "n2", 4, last_log_index=9, last_log_term=3),
        '{"type": "RequestVote", "src": "n1", "dst": "n2", "term": 4,'
        ' "last_log_index": 9, "last_log_term": 3}',
    ),
    "RequestVoteReply": (
        RequestVoteReply("n2", "n1", 4, granted=True),
        '{"type": "RequestVoteReply", "src": "n2", "dst": "n1", "term": 4, "granted": true}',
    ),
    "AppendEntries": (
        AppendEntries(
            "n1", "n3", 4, prev_log_index=6, prev_log_term=2, leader_commit=5,
            entries=(Entry(2, {"op": "set", "key": "a", "value": "1"}), Entry(3, {"op": "noop"})),
        ),
        '{"type": "AppendEntries", "src": "n1", "dst": "n3", "term": 4,'
        ' "prev_log_index": 6, "prev_log_term": 2,'
        ' "entries": [{"term": 2, "command": {"op": "set", "key": "a", "value": "1"}},'
        '             {"term": 3, "command": {"op": "noop"}}],'
        ' "leader_commit": 5}',
    ),
    "AppendEntries (heartbeat)": (
        AppendEntries("n1", "n3", 4, prev_log_index=9, prev_log_term=3, entries=(), leader_commit=9),
        '{"type": "AppendEntries", "src": "n1", "dst": "n3", "term": 4,'
        ' "prev_log_index": 9, "prev_log_term": 3, "entries": [], "leader_commit": 9}',
    ),
    "AppendEntriesReply": (
        AppendEntriesReply("n3", "n1", 4, success=True, match_index=8, last_log_index=8),
        '{"type": "AppendEntriesReply", "src": "n3", "dst": "n1", "term": 4,'
        ' "success": true, "match_index": 8, "last_log_index": 8}',
    ),
}


@pytest.mark.parametrize("msg, wire", WIRE.values(), ids=WIRE.keys())
def test_serialization_matches_wire_format(msg, wire):
    assert to_dict(msg) == json.loads(wire)


@pytest.mark.parametrize("msg, wire", WIRE.values(), ids=WIRE.keys())
def test_deserialization_parses_wire_format(msg, wire):
    assert from_dict(json.loads(wire)) == msg


@pytest.mark.parametrize("bad", [{"type": "Nope", "src": "n1"}, {"src": "n1"}])
def test_unknown_or_missing_type_raises_value_error(bad):
    with pytest.raises(ValueError):
        from_dict(bad)
