import pytest

from raft.messages import Entry
from raft.storage import MemoryStorage


def entries(*terms: int) -> list[Entry]:
    """One entry per term, each with a distinct command so they can be told apart."""
    return [Entry(t, {"op": "set", "key": f"k{i}", "value": "v"}) for i, t in enumerate(terms)]


def storage_with(*terms: int) -> MemoryStorage:
    s = MemoryStorage()
    s.append(entries(*terms))
    return s


def test_new_storage_is_empty():
    s = MemoryStorage()
    assert (s.term, s.voted_for, s.last_index(), s.term_at(0)) == (0, None, 0, 0)
    assert s.entries_from(1) == []


def test_append_and_read_back():
    s = MemoryStorage()
    first, second = entries(1, 1), entries(2)
    s.append(first)
    s.append(second)
    assert s.last_index() == 3
    assert [s.term_at(i) for i in range(4)] == [0, 1, 1, 2]
    assert [s.entry_at(i) for i in (1, 2, 3)] == first + second


def test_entries_from():
    s = storage_with(1, 1, 2, 3)
    log = [s.entry_at(i) for i in range(1, 5)]
    assert s.entries_from(1) == log
    assert s.entries_from(3) == log[2:]
    assert s.entries_from(5) == []  # one past the end


def test_truncate_then_append_reuses_the_indexes():
    s = storage_with(1, 1, 1)
    s.truncate_from(2)
    assert s.last_index() == 1
    s.append(entries(2, 2))
    assert [s.term_at(i) for i in range(4)] == [0, 1, 2, 2]
    s.truncate_from(4)  # past the end: a no-op
    assert s.last_index() == 3


def test_out_of_range_indexes_raise():
    s = storage_with(1, 2)
    out_of_range = [
        lambda: s.term_at(-1),
        lambda: s.term_at(3),
        lambda: s.entry_at(0),  # index 0 is the sentinel, not a real entry
        lambda: s.entry_at(3),
        lambda: s.entries_from(0),
        lambda: s.truncate_from(0),
    ]
    for call in out_of_range:
        with pytest.raises(IndexError):
            call()
    assert s.last_index() == 2  # nothing was deleted
