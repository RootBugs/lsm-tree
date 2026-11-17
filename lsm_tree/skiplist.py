"""Deterministic skip list used as the in-memory memtable.

The skip list keeps entries sorted by key so a memtable can be flushed to an
SSTable without an external sort.  The RNG is seeded, giving fully
deterministic level assignment across runs.
"""
from __future__ import annotations

import random

MAX_LEVEL = 16


class _Node:
    __slots__ = ("key", "value", "forward")

    def __init__(self, key, value, height: int) -> None:
        self.key = key
        self.value = value
        self.forward: list = [None] * height


class SkipList:
    """Sorted key -> value map with expected O(log n) insert/lookup."""

    def __init__(self, max_level: int = MAX_LEVEL, seed: int = 0x5EED) -> None:
        if max_level < 1:
            raise ValueError("max_level must be >= 1")
        self.max_level = max_level
        self._rng = random.Random(seed)
        self._head = _Node(None, None, max_level)
        self._size = 0

    # ------------------------------------------------------------------ #
    def _random_level(self) -> int:
        level = 1
        while level < self.max_level and self._rng.random() < 0.5:
            level += 1
        return level

    def __len__(self) -> int:
        return self._size

    def __contains__(self, key: bytes) -> bool:
        try:
            self.get(key)
        except KeyError:
            return False
        return True

    # ------------------------------------------------------------------ #
    def insert(self, key: bytes, value) -> None:
        """Insert or overwrite ``key``."""
        update: list = [None] * self.max_level
        node = self._head
        for lvl in range(self.max_level - 1, -1, -1):
            while node.forward[lvl] is not None and node.forward[lvl].key < key:
                node = node.forward[lvl]
            update[lvl] = node
        nxt = node.forward[0]
        if nxt is not None and nxt.key == key:
            nxt.value = value
            return
        height = self._random_level()
        new_node = _Node(key, value, height)
        for lvl in range(height):
            new_node.forward[lvl] = update[lvl].forward[lvl]
            update[lvl].forward[lvl] = new_node
        self._size += 1

    def get(self, key: bytes):
        """Return the value stored under ``key`` or raise ``KeyError``."""
        node = self._head
        for lvl in range(self.max_level - 1, -1, -1):
            while node.forward[lvl] is not None and node.forward[lvl].key < key:
                node = node.forward[lvl]
        nxt = node.forward[0]
        if nxt is not None and nxt.key == key:
            return nxt.value
        raise KeyError(key)

    def items(self):
        """Yield ``(key, value)`` pairs in ascending key order."""
        node = self._head.forward[0]
        while node is not None:
            yield node.key, node.value
            node = node.forward[0]
