import sys


def test_python_version():
    assert sys.version_info >= (3, 12)


def test_packages_import():
    import dashboard, raft, server, sim  # noqa: F401
