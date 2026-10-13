import asyncio
from collections.abc import Coroutine

from aiohttp.test_utils import TestClient, TestServer

from raft.messages import RequestVote, to_dict
from server.node_server import build_app

FAST = 0.001  # tick length in seconds: a lone node elects itself within 60 ticks
NEVER = 1000.0  # so long that the node never ticks during a test
PEER = {"n2": "127.0.0.1:1"}  # nobody listens there: sends to it fail and are ignored


def run(scenario: Coroutine) -> None:
    asyncio.run(asyncio.wait_for(scenario, timeout=10))


def client(data_dir, peers: dict[str, str], tick_s: float) -> TestClient:
    return TestClient(TestServer(build_app("n1", peers, str(data_dir), tick_s)))


async def state(c: TestClient) -> dict:
    return await (await c.get("/state")).json()


async def write_on_a_lone_node(data_dir) -> tuple[int, dict]:
    """Start a one-node server, write a=1, and return the write's status and the final /state."""
    async with client(data_dir, {}, FAST) as c:
        while (await state(c))["role"] != "leader":
            await asyncio.sleep(0.01)
        resp = await c.post("/client/write", json={"key": "a", "value": "1"})
        return resp.status, await state(c)


def test_one_node_accepts_a_write_and_shows_it_in_state(tmp_path):
    async def scenario():
        status, s = await write_on_a_lone_node(tmp_path)
        assert status == 200
        assert s["kv"] == {"a": "1"}
        assert s["log"][-1]["command"] == {"op": "set", "key": "a", "value": "1"}

    run(scenario())


def test_non_leader_answers_421(tmp_path):
    async def scenario():
        async with client(tmp_path, PEER, NEVER) as c:
            resp = await c.post("/client/write", json={"key": "a", "value": "1"})
            assert resp.status == 421
            assert await resp.json() == {"leader": None}

    run(scenario())


def test_message_from_blocked_sender_is_dropped(tmp_path):
    async def scenario():
        vote = to_dict(RequestVote("n2", "n1", term=5, last_log_index=0, last_log_term=0))
        async with client(tmp_path, PEER, NEVER) as c:
            await c.post("/admin/partition", json={"blocked": ["n2"]})
            assert (await c.post("/raft", json=vote)).status == 204
            assert (await state(c))["term"] == 0  # dropped: the higher term wasn't adopted
            await c.post("/admin/partition", json={"blocked": []})  # heal
            await c.post("/raft", json=vote)
            assert (await state(c))["term"] == 5

    run(scenario())


def test_restart_with_same_data_dir_keeps_term_and_log(tmp_path):
    async def scenario():
        _, before = await write_on_a_lone_node(tmp_path)
        async with client(tmp_path, {}, NEVER) as c:  # a new server on the same directory
            after = await state(c)
        assert before["term"] >= 1 and len(before["log"]) == 2  # the leader's noop and the write
        assert (after["term"], after["log"]) == (before["term"], before["log"])

    run(scenario())
