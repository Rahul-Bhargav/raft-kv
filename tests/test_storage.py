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
    assert s.term == 0
    assert s.voted_for is None
    assert s.last_index() == 0
    assert s.term_at(0) == 0
    assert s.entries_from(1) == []


def test_set_term_and_vote():
    s = MemoryStorage()
    s.set_term_and_vote(3, "n2")
    assert (s.term, s.voted_for) == (3, "n2")
    s.set_term_and_vote(4, None)  # a new term clears the vote
    assert (s.term, s.voted_for) == (4, None)


def test_append_and_read_back():
    s = MemoryStorage()
    first, second = entries(1, 1), entries(2)
    s.append(first)
    s.append(second)
    assert s.last_index() == 3
    assert [s.term_at(i) for i in range(4)] == [0, 1, 1, 2]
    assert [s.entry_at(i) for i in (1, 2, 3)] == first + second


def test_append_nothing_is_a_no_op():
    s = storage_with(1)
    s.append([])
    assert s.last_index() == 1


def test_append_copies_the_callers_sequence():
    s = MemoryStorage()
    batch = entries(1, 1)
    s.append(batch)
    batch.clear()
    assert s.last_index() == 2


def test_entries_from():
    s = storage_with(1, 1, 2, 3)
    log = [s.entry_at(i) for i in range(1, 5)]
    assert s.entries_from(1) == log
    assert s.entries_from(3) == log[2:]
    assert s.entries_from(4) == log[3:]
    assert s.entries_from(5) == []  # one past the end
    assert s.entries_from(99) == []


def test_entries_from_returns_a_copy():
    s = storage_with(1, 1)
    s.entries_from(1).clear()
    assert s.last_index() == 2


def test_truncate_from_middle():
    s = storage_with(1, 1, 2, 3)
    kept = s.entries_from(1)[:2]
    s.truncate_from(3)
    assert s.last_index() == 2
    assert s.entries_from(1) == kept


def test_truncate_from_last_entry():
    s = storage_with(1, 2, 3)
    s.truncate_from(3)
    assert s.last_index() == 2
    assert s.term_at(2) == 2


def test_truncate_past_the_end_is_a_no_op():
    s = storage_with(1, 2)
    s.truncate_from(3)
    s.truncate_from(50)
    assert s.last_index() == 2


def test_truncate_from_first_entry_empties_the_log():
    s = storage_with(1, 2)
    s.truncate_from(1)
    assert s.last_index() == 0
    assert s.term_at(0) == 0


def test_append_after_truncate_reuses_the_indexes():
    s = storage_with(1, 1, 1)
    s.truncate_from(2)
    s.append(entries(2, 2))
    assert [s.term_at(i) for i in range(4)] == [0, 1, 2, 2]


def test_term_and_vote_are_independent_of_the_log():
    s = storage_with(1, 2)
    s.set_term_and_vote(2, "n1")
    s.truncate_from(1)
    assert (s.term, s.voted_for) == (2, "n1")


@pytest.mark.parametrize("index", [-1, 3])
def test_term_at_out_of_range_raises(index):
    with pytest.raises(IndexError):
        storage_with(1, 2).term_at(index)


@pytest.mark.parametrize("index", [-1, 0, 3])
def test_entry_at_out_of_range_raises(index):
    # Index 0 is the sentinel, which is not a real entry.
    with pytest.raises(IndexError):
        storage_with(1, 2).entry_at(index)


@pytest.mark.parametrize("index", [-1, 0])
def test_entries_from_and_truncate_reject_non_positive_index(index):
    s = storage_with(1, 2)
    with pytest.raises(IndexError):
        s.entries_from(index)
    with pytest.raises(IndexError):
        s.truncate_from(index)
    assert s.last_index() == 2  # nothing was deleted
