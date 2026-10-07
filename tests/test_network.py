import random

import pytest

from raft.messages import RequestVote
from sim.network import SimNetwork


def msg(term: int = 1, src: str = "n1", dst: str = "n2") -> RequestVote:
    """The term doubles as an ID so tests can tell messages apart."""
    return RequestVote(src, dst, term, last_log_index=0, last_log_term=0)


class ScriptedDelays(random.Random):
    """A Random whose delays are scripted, so ordering tests don't depend on a seed."""

    def __init__(self, *delays: int) -> None:
        super().__init__(0)
        self._delays = iter(delays)

    def randint(self, a: int, b: int) -> int:
        return next(self._delays)


def drain(net: SimNetwork, until: int) -> list[tuple[int, int]]:
    """Deliver tick by tick; returns (tick, term) for every message that arrives."""
    return [(now, m.term) for now in range(until + 1) for m in net.deliver_due(now)]


def noisy_run(seed: int) -> list[tuple[int, int]]:
    net = SimNetwork(random.Random(seed), min_delay=1, max_delay=5, drop_rate=0.3, dup_rate=0.3)
    arrivals = []
    for now in range(40):
        net.send(msg(term=now), now)
        net.send(msg(term=1000 + now, src="n3", dst="n4"), now)
        arrivals += [(now, m.term) for m in net.deliver_due(now)]
    return arrivals + [(40 + now, m.term) for now in range(6) for m in net.deliver_due(40 + now)]


# --- delay and ordering ---


def test_delivered_only_after_the_delay():
    net = SimNetwork(random.Random(0), min_delay=2, max_delay=2)
    m = msg()
    net.send(m, now=10)
    assert net.deliver_due(10) == []
    assert net.deliver_due(11) == []
    assert net.deliver_due(12) == [m]


def test_a_message_is_delivered_once():
    net = SimNetwork(random.Random(0), min_delay=1, max_delay=1)
    net.send(msg(), now=0)
    assert len(net.deliver_due(1)) == 1
    assert net.deliver_due(1) == []
    assert net.deliver_due(100) == []


def test_a_late_call_returns_everything_that_is_due():
    net = SimNetwork(ScriptedDelays(1, 5, 3))
    for term in (1, 2, 3):
        net.send(msg(term), now=0)
    assert [m.term for m in net.deliver_due(10)] == [1, 3, 2]


def test_delays_stay_within_bounds_and_use_the_whole_range():
    net = SimNetwork(random.Random(1), min_delay=2, max_delay=4)
    for term in range(100):
        net.send(msg(term), now=0)
    arrival_ticks = {tick for tick, _ in drain(net, until=10)}
    assert arrival_ticks == {2, 3, 4}


def test_delivery_can_reorder_messages():
    net = SimNetwork(ScriptedDelays(3, 1))
    net.send(msg(term=1), now=0)  # sent first, slower
    net.send(msg(term=2), now=0)
    assert drain(net, until=5) == [(1, 2), (3, 1)]


def test_ties_are_broken_by_send_order():
    net = SimNetwork(ScriptedDelays(2, 1, 2, 1))
    net.send(msg(term=1), now=0)  # due 2
    net.send(msg(term=2), now=1)  # due 2 as well
    net.send(msg(term=3), now=1)  # due 3
    net.send(msg(term=4), now=2)  # due 3 as well
    assert drain(net, until=5) == [(2, 1), (2, 2), (3, 3), (3, 4)]


# --- drops and duplicates ---


def test_drop_rate_one_drops_everything():
    net = SimNetwork(random.Random(0), drop_rate=1.0)
    for term in range(50):
        net.send(msg(term), now=0)
    assert net.deliver_due(1000) == []


def test_drop_rate_zero_drops_nothing():
    net = SimNetwork(random.Random(0), drop_rate=0.0)
    for term in range(50):
        net.send(msg(term), now=0)
    assert len(net.deliver_due(1000)) == 50


def test_partial_drop_rate_drops_some():
    net = SimNetwork(random.Random(0), drop_rate=0.5)
    for term in range(200):
        net.send(msg(term), now=0)
    assert 50 < len(net.deliver_due(1000)) < 150


def test_dup_rate_one_delivers_every_message_twice():
    net = SimNetwork(random.Random(0), dup_rate=1.0)
    for term in range(20):
        net.send(msg(term), now=0)
    delivered = [m.term for m in net.deliver_due(1000)]
    assert sorted(delivered) == sorted(list(range(20)) * 2)


def test_dup_rate_zero_never_duplicates():
    net = SimNetwork(random.Random(0), dup_rate=0.0)
    for term in range(20):
        net.send(msg(term), now=0)
    assert sorted(m.term for m in net.deliver_due(1000)) == list(range(20))


def test_a_duplicate_gets_its_own_delay():
    net = SimNetwork(ScriptedDelays(1, 4), dup_rate=1.0)
    net.send(msg(), now=0)
    assert drain(net, until=5) == [(1, 1), (4, 1)]


# --- partitions ---


def test_partition_blocks_cross_group_messages_only():
    net = SimNetwork(random.Random(0), min_delay=1, max_delay=1)
    net.partition([{"n1", "n2"}, {"n3", "n4", "n5"}])
    inside_a, inside_b = msg(1, "n1", "n2"), msg(2, "n5", "n3")
    net.send(inside_a, now=0)
    net.send(inside_b, now=0)
    net.send(msg(3, "n1", "n3"), now=0)
    net.send(msg(4, "n4", "n2"), now=0)
    assert net.deliver_due(1) == [inside_a, inside_b]


def test_partition_is_checked_at_delivery_time():
    net = SimNetwork(random.Random(0), min_delay=2, max_delay=2)
    crossing = msg(1, "n1", "n3")
    net.send(crossing, now=0)
    net.partition([{"n1"}, {"n2", "n3"}])  # applied while the message is in flight
    assert net.deliver_due(2) == []


def test_message_sent_during_a_partition_arrives_if_healed_before_delivery():
    net = SimNetwork(random.Random(0), min_delay=2, max_delay=2)
    net.partition([{"n1"}, {"n2", "n3"}])
    crossing = msg(1, "n1", "n3")
    net.send(crossing, now=0)  # sending is not blocked
    net.heal()
    assert net.deliver_due(2) == [crossing]


def test_heal_restores_delivery_but_dropped_messages_stay_dropped():
    net = SimNetwork(random.Random(0), min_delay=1, max_delay=1)
    net.partition([{"n1"}, {"n2"}])
    net.send(msg(1, "n1", "n2"), now=0)
    assert net.deliver_due(1) == []  # dropped here
    net.heal()
    assert net.deliver_due(100) == []  # and gone for good
    after = msg(2, "n1", "n2")
    net.send(after, now=100)
    assert net.deliver_due(101) == [after]


def test_unlisted_nodes_are_grouped_together():
    net = SimNetwork(random.Random(0), min_delay=1, max_delay=1)
    net.partition([{"n1"}])  # n2..n5 are not listed
    rest = msg(1, "n2", "n5")
    net.send(rest, now=0)
    net.send(msg(2, "n1", "n2"), now=0)
    net.send(msg(3, "n4", "n1"), now=0)
    assert net.deliver_due(1) == [rest]


def test_a_new_partition_replaces_the_old_one():
    net = SimNetwork(random.Random(0), min_delay=1, max_delay=1)
    net.partition([{"n1"}, {"n2"}])
    net.partition([{"n1", "n2"}, {"n3"}])
    m = msg(1, "n1", "n2")
    net.send(m, now=0)
    assert net.deliver_due(1) == [m]


def test_a_node_in_two_groups_is_rejected():
    net = SimNetwork(random.Random(0))
    with pytest.raises(ValueError):
        net.partition([{"n1", "n2"}, {"n2", "n3"}])


# --- determinism and setup ---


def test_same_seed_gives_the_same_deliveries():
    first = noisy_run(seed=7)
    assert first == noisy_run(seed=7)
    assert first != noisy_run(seed=8)
    assert first  # the run actually delivered something


def test_invalid_delay_range_is_rejected():
    with pytest.raises(ValueError):
        SimNetwork(random.Random(0), min_delay=3, max_delay=1)
    with pytest.raises(ValueError):
        SimNetwork(random.Random(0), min_delay=-1, max_delay=1)
