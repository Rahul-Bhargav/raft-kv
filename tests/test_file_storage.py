from raft.messages import Entry
from server.file_storage import FileStorage

ENTRIES = [Entry(1, {"op": "set", "key": "a", "value": "1"}), Entry(2, {"op": "noop"})]


def test_reopening_restores_term_vote_and_log(tmp_path):
    path = str(tmp_path / "n1.json")
    storage = FileStorage(path)
    storage.set_term_and_vote(3, "n2")
    storage.append(ENTRIES + [Entry(3, {"op": "delete", "key": "a"})])
    storage.truncate_from(3)

    reopened = FileStorage(path)
    assert (reopened.term, reopened.voted_for) == (3, "n2")
    assert reopened.entries_from(1) == ENTRIES


def test_leftover_tmp_file_is_ignored(tmp_path):
    path = str(tmp_path / "n1.json")
    FileStorage(path).append(ENTRIES)
    (tmp_path / "n1.json.tmp").write_text('{"term": 9, "lo')  # a crash in the middle of a save

    assert FileStorage(path).entries_from(1) == ENTRIES
