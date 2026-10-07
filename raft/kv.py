"""The replicated state machine: a key-value store. No I/O here."""


class KVStore:
    def __init__(self) -> None:
        self.data: dict[str, str] = {}

    def apply(self, command: dict) -> None:
        """Apply one committed command. Raises ValueError for an unknown op."""
        op = command.get("op")
        if op == "set":
            self.data[command["key"]] = command["value"]
        elif op == "delete":
            self.data.pop(command["key"], None)  # deleting a missing key is fine
        elif op == "noop":
            pass
        else:
            raise ValueError(f"unknown op: {op!r}")
