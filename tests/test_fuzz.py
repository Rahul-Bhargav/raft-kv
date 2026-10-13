"""Randomized fault schedules over many seeds: the main evidence that the Raft code is safe.

Each seed is one run of a 5-node cluster. What the seed randomizes:
- the network: delays of 1 to 2-8 ticks, 0-20% of messages dropped, 0-30% duplicated, so lost,
  repeated and reordered messages are all common;
- the batch cap (max_entries_per_append): none, 1 or 2. With a cap, a follower can confirm an
  older term's entry before the new leader's own entry, which is what makes Figure 8 reachable;
- faults at random ticks: crash a node (the leader half the time, and never more than 2 of 5 down
  at once), restart a crashed node, reboot a node (crash and restart at once: it keeps only its
  disk, and is back before a running election is over), split the network in two (often cutting
  the leader off), heal;
- strikes on new leaders: a new leader is fragile, with entries from older terms only partly
  copied and its own entry not yet on a majority. Per seed, none, half or all new leaders are
  crashed or cut off 1 to 15 ticks after they win. Without strikes, Figure 8 almost never happens;
- client writes through SimCluster.write, each to a new key. Faults keep happening while a write
  waits, so writes get lost, overwritten and recovered in every way the faults allow.

What is checked:
- after every tick, all five safety properties in tests/safety_checks.py (the cluster is a checked_cluster);
- at the end, after healing the network and restarting every crashed node: a leader is elected,
  every node has applied everything in the leader's log, every acknowledged write is in every KV
  store, nothing that was never written is, and all KV stores are equal.

FUZZ_SEEDS sets the number of seeds (default 1000; the whole suite then takes about 13 s here).
Each seed is its own test. A seed always gives the same run, and a failure says how to rerun just
that seed.
"""

import collections
import os
import random

import pytest

from raft.node import RaftConfig
from sim.cluster import SimCluster
from tests.safety_checks import checked_cluster

SEEDS = int(os.environ.get("FUZZ_SEEDS", "1000"))

FAULT_TICKS = 1500  # how long faults and writes go on in each run
WRITE_CHANCE = 0.3  # in each turn of the loop: start a client write (otherwise run one step)
WRITE_TICKS = 100  # how long a client waits for one write before giving up
# Fault chances per tick. Together they give a fault every 15 ticks or so, against elections that
# take 30-60 ticks: leaders change often, with time for writes to commit in between.
CRASH_CHANCE = 1 / 100
RESTART_CHANCE = 1 / 60  # while a node is down, so most stay down for tens of ticks
PARTITION_CHANCE = 1 / 150
HEAL_CHANCE = 1 / 100
REBOOT_CHANCE = 1 / 40
MAX_DOWN = 2  # a minority of 5, so the end can require a leader again
STRIKE_CHANCES = [0, 0.5, 1]  # per seed: the share of new leaders that are struck
STRIKE_WITHIN = 15  # ticks after a leader is first seen
# Ticks to settle once healed and restarted: 2x the worst measured over seeds 0-9999 (604).
SETTLE_DEADLINE = 1200


@pytest.mark.parametrize("seed", range(SEEDS))
def test_fuzz(seed: int) -> None:
    try:
        run_schedule(seed)
    except Exception as error:
        error.add_note(
            f"fuzz seed {seed} failed. Rerun just this seed with:\n"
            f"  FUZZ_SEEDS={seed + 1} uv run pytest -q 'tests/test_fuzz.py::test_fuzz[{seed}]'"
        )
        raise


def test_schedules_are_not_trivial():
    """The fuzz test could pass by doing nothing, so check that its runs really are eventful."""
    totals: collections.Counter[str] = collections.Counter()
    for seed in range(20):
        totals.update(run_schedule(seed))
    # Measured over these 20 seeds. Each total must be at least half of that.
    measured = {"acknowledged": 591, "leaders": 225, "crash": 278, "restart": 262, "reboot": 772}
    measured |= {"partition": 243, "heal": 264, "strike": 93}
    for what, count in measured.items():
        assert totals[what] >= count // 2, f"{what}: {totals[what]} over 20 seeds, measured {count}"


def run_schedule(seed: int) -> collections.Counter[str]:
    """One random run: faults and writes for FAULT_TICKS, then heal, restart and check the end.

    Returns counts of what happened: each kind of fault, acknowledged writes, and leaders.
    """
    rng = random.Random(seed)  # every choice in this schedule comes from here
    cluster = checked_cluster(
        seed=rng.getrandbits(32),  # the cluster's own Random: election timeouts and the network
        config=RaftConfig(max_entries_per_append=rng.choice([None, 1, 2])),
        max_delay=rng.randint(2, 8),
        drop_rate=rng.uniform(0, 0.2),
        dup_rate=rng.uniform(0, 0.3),
    )
    faults = Faults(rng, strike_chance=rng.choice(STRIKE_CHANCES))
    cluster.checkers.append(faults)  # after the safety checks: a fault lands between two steps
    written, acknowledged = {}, {}
    while cluster.now < FAULT_TICKS:
        if rng.random() < WRITE_CHANCE:
            key, value = f"k{len(written)}", f"v{len(written)}"
            written[key] = value
            if cluster.write(key, value, max_ticks=WRITE_TICKS):
                acknowledged[key] = value
        else:
            cluster.step()

    # The end: no more faults. Heal, restart everyone, and wait for one leader that all follow.
    cluster.checkers.remove(faults)
    cluster.heal()
    for node_id in cluster.ids:
        if cluster.nodes[node_id] is None:
            cluster.restart(node_id)
    assert cluster.run_until(lambda: settled(cluster), SETTLE_DEADLINE), (
        f"no settled leader within {SETTLE_DEADLINE} ticks of healing and restarting everyone"
    )
    leader_kv = cluster.kvs[cluster.leader()].data
    for node_id in cluster.ids:
        kv = cluster.kvs[node_id].data
        lost = {k: v for k, v in acknowledged.items() if kv.get(k) != v}
        assert not lost, f"{node_id} lost acknowledged writes: {lost}"
        assert kv.items() <= written.items(), f"{node_id} has writes nobody made"
        assert kv == leader_kv, f"{node_id}'s KV store differs from the leader's"

    faults.counts["acknowledged"] = len(acknowledged)
    faults.counts["leaders"] = len(faults.seen_leaders)
    return faults.counts


class Faults:
    """A SimCluster checker that injects random faults. It runs after every step, also while a
    client write waits, using the schedule's Random, so a seed always gives the same faults."""

    def __init__(self, rng: random.Random, strike_chance: float) -> None:
        self.rng = rng
        self.strike_chance = strike_chance
        # (leader, term) -> the tick to strike that leader, or None to leave it alone.
        self.seen_leaders: dict[tuple[str, int], int | None] = {}
        self.counts: collections.Counter[str] = collections.Counter()

    def __call__(self, cluster: SimCluster) -> None:
        rng = self.rng
        down = [node_id for node_id, node in cluster.nodes.items() if node is None]
        live = [node_id for node_id, node in cluster.nodes.items() if node is not None]
        leader = cluster.leader()
        if down and rng.random() < RESTART_CHANCE:
            self._restart(cluster, rng.choice(down))
        elif len(down) < MAX_DOWN and rng.random() < CRASH_CHANCE:
            # The leader half the time: losing a leader mid-replication is where the hard cases are.
            pick_leader = leader is not None and rng.random() < 0.5
            self._crash(cluster, leader if pick_leader else rng.choice(live))
        elif rng.random() < PARTITION_CHANCE:
            ids = list(cluster.ids)
            rng.shuffle(ids)
            if leader is not None and rng.random() < 0.5:
                ids.remove(leader)
                ids.insert(0, leader)
                side = ids[: rng.randint(1, 2)]  # the leader alone or with one follower
            else:
                side = ids[: rng.randint(1, len(ids) - 1)]
            self._partition(cluster, side)
        elif rng.random() < HEAL_CHANCE:
            cluster.heal()
            self.counts["heal"] += 1
        elif rng.random() < REBOOT_CHANCE:
            node_id = rng.choice(live)
            cluster.crash(node_id)
            cluster.restart(node_id)
            self.counts["reboot"] += 1
        self._strike_new_leaders(cluster)

    def _strike_new_leaders(self, cluster: SimCluster) -> None:
        """Crash or cut off a new leader soon after it wins, before it has settled in.

        In Figure 8, the damage needs a leader lost in a short window: after it has copied an older
        term's entry to a majority, before its own entry is on a majority. Random crashes rarely
        land there, so with strike_chance a new leader is struck 1 to STRIKE_WITHIN ticks after we
        first see it. Cutting it off also stops the messages it already sent; a crash doesn't.
        """
        leader = cluster.leader()
        if leader is None:
            return
        leadership = (leader, cluster.nodes[leader].term)
        if leadership not in self.seen_leaders:
            strike = self.rng.random() < self.strike_chance
            self.seen_leaders[leadership] = (
                cluster.now + self.rng.randint(1, STRIKE_WITHIN) if strike else None
            )
        elif self.seen_leaders[leadership] == cluster.now:
            self.counts["strike"] += 1
            down = sum(node is None for node in cluster.nodes.values())
            if down < MAX_DOWN and self.rng.random() < 0.5:
                self._crash(cluster, leader)
            else:
                self._partition(cluster, [leader])

    def _crash(self, cluster: SimCluster, node_id: str) -> None:
        cluster.crash(node_id)
        self.counts["crash"] += 1

    def _restart(self, cluster: SimCluster, node_id: str) -> None:
        cluster.restart(node_id)
        self.counts["restart"] += 1

    def _partition(self, cluster: SimCluster, side: list[str]) -> None:
        cluster.partition(side)  # the nodes not named form the other side
        self.counts["partition"] += 1


def settled(cluster: SimCluster) -> bool:
    """Every node is up and has applied everything in the current leader's log."""
    leader_id = cluster.leader()
    if leader_id is None:
        return False
    last = cluster.storages[leader_id].last_index()
    return all(node is not None and node.last_applied == last for node in cluster.nodes.values())
