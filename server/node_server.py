"""One Raft node as an HTTP server: the real driver for the demo. Run: python -m server.node_server

One asyncio event loop, no threads, no locks. Every call into the node goes through step().
"""

import asyncio
import os
import random
import time
from collections.abc import AsyncIterator, Callable
from dataclasses import asdict
from typing import Any

import aiohttp
from aiohttp import web

from raft.kv import KVStore
from raft.messages import Message, from_dict, to_dict
from raft.node import RaftNode
from server.file_storage import FileStorage

SEND_TIMEOUT_S = 0.5
WRITE_TIMEOUT_S = 5.0
MAX_EVENTS = 20


class NodeServer:
    def __init__(self, node_id: str, peers: dict[str, str], data_dir: str, tick_s: float) -> None:
        os.makedirs(data_dir, exist_ok=True)
        self.peers = peers  # id -> "host:port", without this node
        self.tick_s = tick_s
        storage = FileStorage(os.path.join(data_dir, f"{node_id}.json"))
        # Unseeded on purpose: nodes must draw different election timeouts, also after a restart.
        self.node = RaftNode(node_id, list(peers), storage, random.Random())
        self.kv = KVStore()  # empty after a restart; refilled as entries are committed again
        self.blocked: set[str] = set()  # partition filter: peers we don't send to or hear from
        self.events: list[dict] = []
        self.sends: set[asyncio.Task] = set()  # the loop holds tasks weakly, so keep them here

    def step(self, call: Callable[..., Any], *args: Any) -> Any:
        """Make one call into the node, then collect everything it queued."""
        # No `await` in here: nothing else may run between the call and collecting its outputs.
        result = call(*args)
        for msg in self.node.take_messages():
            if msg.dst not in self.blocked:
                task = asyncio.create_task(self.send(msg))  # fire and forget
                self.sends.add(task)
                task.add_done_callback(self.sends.discard)
        for _index, entry in self.node.take_committed():
            self.kv.apply(entry.command)
        for event in self.node.take_events():
            self.events.append({"time": time.time(), "event": event})
        del self.events[:-MAX_EVENTS]
        return result

    async def send(self, msg: Message) -> None:
        try:
            async with self.session.post(f"http://{self.peers[msg.dst]}/raft", json=to_dict(msg)):
                pass
        except (aiohttp.ClientError, TimeoutError):
            pass  # a lost message is fine: Raft sends again at the next heartbeat or election

    async def run(self, app: web.Application) -> AsyncIterator[None]:
        """Lives as long as the app: the session for outbound messages and the tick loop."""
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=SEND_TIMEOUT_S))
        ticker = asyncio.create_task(self.tick_loop())
        yield
        ticker.cancel()
        await self.session.close()

    async def tick_loop(self) -> None:
        while True:
            await asyncio.sleep(self.tick_s)
            self.step(self.node.tick)

    # --- endpoints ---

    async def on_raft(self, request: web.Request) -> web.Response:
        msg = from_dict(await request.json())
        if msg.src not in self.blocked:
            self.step(self.node.receive, msg)
        return web.Response(status=204)

    async def on_write(self, request: web.Request) -> web.Response:
        body = await request.json()
        command = {"op": "set", "key": body["key"], "value": body["value"]}
        proposed = self.step(self.node.propose, command)
        if proposed is None:
            return web.json_response({"leader": self.node.leader_id}, status=421)
        index, term = proposed
        deadline = time.monotonic() + WRITE_TIMEOUT_S
        while self.node.last_applied < index and time.monotonic() < deadline:
            await asyncio.sleep(self.tick_s)
        # A committed entry never changes, so the term stored at our index says whether it's ours.
        if self.node.last_applied >= index and self.node.storage.term_at(index) == term:
            return web.json_response({"index": index, "term": term})
        return web.Response(status=504)  # timed out, or a later leader overwrote our entry

    async def on_state(self, request: web.Request) -> web.Response:
        node = self.node
        return web.json_response({
            "id": node.node_id,
            "role": node.role.value,
            "term": node.term,
            "voted_for": node.voted_for,
            "leader_id": node.leader_id,
            "commit_index": node.commit_index,
            "last_applied": node.last_applied,
            "log": [asdict(entry) for entry in node.storage.entries_from(1)],
            "kv": self.kv.data,
            "blocked": sorted(self.blocked),
            "events": self.events,
        })

    async def on_partition(self, request: web.Request) -> web.Response:
        self.blocked = set((await request.json())["blocked"])
        return web.Response(status=204)


def build_app(node_id: str, peers: dict[str, str], data_dir: str, tick_s: float) -> web.Application:
    server = NodeServer(node_id, peers, data_dir, tick_s)
    app = web.Application()
    app.cleanup_ctx.append(server.run)
    app.add_routes([
        web.post("/raft", server.on_raft),
        web.post("/client/write", server.on_write),
        web.get("/state", server.on_state),
        web.post("/admin/partition", server.on_partition),
    ])
    return app


def main() -> None:
    node_id = os.environ["NODE_ID"]
    peers = dict(p.split("=") for p in os.environ.get("PEERS", "").split(",") if p)
    peers.pop(node_id, None)  # so every node can be given the same full list
    tick_s = int(os.environ.get("TICK_MS", "100")) / 1000
    app = build_app(node_id, peers, os.environ.get("DATA_DIR", "/data"), tick_s)
    web.run_app(app, port=int(os.environ.get("PORT", "8000")))


if __name__ == "__main__":
    main()
