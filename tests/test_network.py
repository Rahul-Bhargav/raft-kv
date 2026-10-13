import random

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


def test_each_message_is_delivered_once_after_a_delay_within_bounds():
    net = SimNetwork(random.Random(1), min_delay=2, max_delay=4)
    for term in range(100):
        net.send(msg(term), now=0)
    arrivals = drain(net, until=10)
    assert {tick for tick, _ in arrivals} == {2, 3, 4}  # the whole range, both ends included
    assert sorted(term for _, term in arrivals) == list(range(100))


def test_delivery_can_reorder_messages():
    net = SimNetwork(ScriptedDelays(3, 1))
    net.send(msg(term=1), now=0)  # sent first, slower
    net.send(msg(term=2), now=0)
    assert drain(net, until=5) == [(1, 2), (3, 1)]


def test_partial_drop_rate_drops_some():
    net = SimNetwork(random.Random(0), drop_rate=0.5)
    for term in range(200):
        net.send(msg(term), now=0)
    assert 50 < len(net.deliver_due(1000)) < 150


def test_a_duplicate_gets_its_own_delay():
    net = SimNetwork(ScriptedDelays(1, 4), dup_rate=1.0)
    net.send(msg(), now=0)
    assert drain(net, until=5) == [(1, 1), (4, 1)]


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
    net.send(msg(1, "n1", "n3"), now=0)
    net.partition([{"n1"}, {"n2", "n3"}])  # applied while the message is in flight
    assert net.deliver_due(2) == []


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
