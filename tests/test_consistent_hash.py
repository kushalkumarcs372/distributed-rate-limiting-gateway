"""
This test is your interview demo evidence: it proves that adding a node
to the ring only remaps ~1/N of keys, not all of them (unlike % N hashing).
Run this and keep the printed numbers -- put them in your README.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from gateway.consistent_hash import ConsistentHashRing


def test_basic_lookup_is_deterministic():
    ring = ConsistentHashRing(nodes=["node-a", "node-b", "node-c"])
    assert ring.get_node("client-123") == ring.get_node("client-123")


def test_naive_modulo_remaps_almost_everything():
    """Baseline to compare against: hash(key) % N."""
    clients = [f"client-{i}" for i in range(10_000)]
    n_before = 4
    n_after = 5

    before = {c: hash(c) % n_before for c in clients}
    after = {c: hash(c) % n_after for c in clients}

    remapped = sum(1 for c in clients if before[c] != after[c])
    pct = remapped / len(clients) * 100
    print(f"\n[naive % N] remapped on node add: {pct:.1f}% of {len(clients)} clients")
    assert pct > 70  # naive hashing remaps the vast majority


def test_consistent_hashing_remaps_roughly_1_over_n():
    clients = [f"client-{i}" for i in range(10_000)]

    ring = ConsistentHashRing(nodes=["node-a", "node-b", "node-c", "node-d"])
    before = {c: ring.get_node(c) for c in clients}

    ring.add_node("node-e")  # 4 -> 5 nodes
    after = {c: ring.get_node(c) for c in clients}

    remapped = sum(1 for c in clients if before[c] != after[c])
    pct = remapped / len(clients) * 100
    print(f"[consistent hash] remapped on node add: {pct:.1f}% of {len(clients)} clients "
          f"(expected ~{100/5:.0f}%)")

    # Expect close to 1/5 = 20%, allow generous margin for vnode randomness
    assert pct < 35


def test_node_removal_only_affects_its_own_keys():
    ring = ConsistentHashRing(nodes=["node-a", "node-b", "node-c"])
    clients = [f"client-{i}" for i in range(5_000)]
    before = {c: ring.get_node(c) for c in clients}

    ring.remove_node("node-b")
    after = {c: ring.get_node(c) for c in clients}

    # keys that were NOT on node-b should not move
    unaffected_before = {c: n for c, n in before.items() if n != "node-b"}
    for c, n in unaffected_before.items():
        assert after[c] == n, f"{c} moved from {n} to {after[c]} unnecessarily"
