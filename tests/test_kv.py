import pytest

from raft.kv import KVStore


def set_cmd(key: str, value: str) -> dict:
    return {"op": "set", "key": key, "value": value}


def test_starts_empty():
    assert KVStore().data == {}


def test_set_then_overwrite():
    kv = KVStore()
    kv.apply(set_cmd("a", "1"))
    assert kv.data == {"a": "1"}
    kv.apply(set_cmd("a", "2"))
    assert kv.data == {"a": "2"}


def test_delete_removes_only_that_key():
    kv = KVStore()
    kv.apply(set_cmd("a", "1"))
    kv.apply(set_cmd("b", "2"))
    kv.apply({"op": "delete", "key": "a"})
    assert kv.data == {"b": "2"}


def test_delete_missing_key_is_fine():
    kv = KVStore()
    kv.apply({"op": "delete", "key": "ghost"})
    assert kv.data == {}


def test_noop_changes_nothing():
    kv = KVStore()
    kv.apply(set_cmd("a", "1"))
    kv.apply({"op": "noop"})
    assert kv.data == {"a": "1"}


@pytest.mark.parametrize("bad", [{"op": "incr", "key": "a"}, {"key": "a"}, {}])
def test_unknown_or_missing_op_raises_and_leaves_state_alone(bad):
    kv = KVStore()
    kv.apply(set_cmd("a", "1"))
    with pytest.raises(ValueError):
        kv.apply(bad)
    assert kv.data == {"a": "1"}


def test_client_id_and_seq_fields_are_accepted():
    kv = KVStore()
    kv.apply({**set_cmd("a", "1"), "client_id": "c1", "seq": 1})
    kv.apply({"op": "delete", "key": "a", "client_id": "c1", "seq": 2})
    assert kv.data == {}
