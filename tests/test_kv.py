import pytest

from raft.kv import KVStore


def set_cmd(key: str, value: str) -> dict:
    return {"op": "set", "key": key, "value": value}


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
    kv.apply({"op": "delete", "key": "ghost"})  # deleting a missing key is fine
    assert kv.data == {"b": "2"}


def test_noop_changes_nothing():
    kv = KVStore()
    kv.apply(set_cmd("a", "1"))
    kv.apply({"op": "noop"})
    assert kv.data == {"a": "1"}


def test_unknown_op_raises_and_leaves_state_alone():
    kv = KVStore()
    kv.apply(set_cmd("a", "1"))
    with pytest.raises(ValueError):
        kv.apply({"op": "incr", "key": "a"})
    assert kv.data == {"a": "1"}
