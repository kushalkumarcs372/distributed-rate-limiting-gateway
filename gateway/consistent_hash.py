"""
Consistent Hashing Ring
-----------------------
WHY THIS EXISTS:
When we have N gateway nodes each holding rate-limit state for a subset of
clients, and we add/remove a node, naive hashing (hash(client_id) % N)
remaps almost ALL clients to new nodes -- every client's rate-limit history
resets, causing a thundering-herd of "fresh" rate limit windows.

Consistent hashing fixes this: adding/removing one node only remaps
~1/N of the keys, not all of them. We prove this with test_remapping().

HOW IT WORKS:
- Each real node is hashed to multiple points on a circular ring (virtual
  nodes / "vnodes") to smooth out load distribution.
- A key (client_id) is hashed to a point on the same ring.
- The key belongs to the first node found walking clockwise from the key's
  position.
"""
import bisect
import hashlib


class ConsistentHashRing:
    def __init__(self, nodes=None, vnodes=150):
        """
        vnodes: virtual nodes per physical node. Higher = smoother load
        distribution but more memory. 100-200 is a common production value.
        """
        self.vnodes = vnodes
        self.ring = {}          # hash_value -> node_name
        self.sorted_keys = []   # sorted list of hash values for bisect
        self.nodes = set()
        if nodes:
            for node in nodes:
                self.add_node(node)

    def _hash(self, key: str) -> int:
        # md5 is fine here -- we need uniform distribution, not security.
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def add_node(self, node: str):
        if node in self.nodes:
            return
        self.nodes.add(node)
        for i in range(self.vnodes):
            vnode_key = f"{node}#{i}"
            h = self._hash(vnode_key)
            self.ring[h] = node
            bisect.insort(self.sorted_keys, h)

    def remove_node(self, node: str):
        if node not in self.nodes:
            return
        self.nodes.discard(node)
        for i in range(self.vnodes):
            vnode_key = f"{node}#{i}"
            h = self._hash(vnode_key)
            del self.ring[h]
            idx = bisect.bisect_left(self.sorted_keys, h)
            self.sorted_keys.pop(idx)

    def get_node(self, key: str) -> str:
        if not self.ring:
            raise ValueError("Ring is empty, no nodes available")
        h = self._hash(key)
        idx = bisect.bisect(self.sorted_keys, h)
        if idx == len(self.sorted_keys):
            idx = 0  # wrap around the ring
        return self.ring[self.sorted_keys[idx]]
