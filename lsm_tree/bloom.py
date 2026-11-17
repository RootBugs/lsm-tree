"""Bloom filter for SSTable key lookups (no false negatives, by construction)."""
from __future__ import annotations

import hashlib
import math


class BloomFilter:
    """Space-efficient probabilistic set with one-sided error."""

    def __init__(self, num_bits: int, num_hashes: int, bits: bytearray | None = None) -> None:
        if num_bits < 8:
            raise ValueError("num_bits must be >= 8")
        if num_hashes < 1:
            raise ValueError("num_hashes must be >= 1")
        self.num_bits = num_bits
        self.num_hashes = num_hashes
        self.bits = bits if bits is not None else bytearray((num_bits + 7) // 8)

    # ------------------------------------------------------------------ #
    @classmethod
    def create(cls, keys, fp_rate: float = 0.01) -> "BloomFilter":
        """Build a filter sized for ``keys`` at the target false-positive rate."""
        keys = list(keys)
        n = len(keys)
        if n == 0:
            return cls(64, 1)
        num_bits = max(64, int(-n * math.log(fp_rate) / (math.log(2) ** 2)))
        num_hashes = max(1, round((num_bits / n) * math.log(2)))
        bf = cls(num_bits, num_hashes)
        for key in keys:
            bf.add(key)
        return bf

    # ------------------------------------------------------------------ #
    def _indexes(self, key: bytes) -> list[int]:
        digest = hashlib.sha1(key).digest()
        h1 = int.from_bytes(digest[:8], "big")
        h2 = int.from_bytes(digest[8:16], "big") or 1
        out = []
        for i in range(self.num_hashes):
            out.append((h1 + i * h2) % self.num_bits)
        return out

    def add(self, key: bytes) -> None:
        for idx in self._indexes(key):
            self.bits[idx >> 3] |= 1 << (idx & 7)

    def might_contain(self, key: bytes) -> bool:
        """True if ``key`` was added (or a false positive); never a false negative."""
        for idx in self._indexes(key):
            if not (self.bits[idx >> 3] >> (idx & 7)) & 1:
                return False
        return True

    # ------------------------------------------------------------------ #
    def to_bytes(self) -> bytes:
        header = f"BLOOM:{self.num_bits}:{self.num_hashes}".encode("ascii")
        return header + b"\x00" + bytes(self.bits)

    @classmethod
    def from_bytes(cls, data: bytes) -> "BloomFilter":
        try:
            header, rest = data.split(b"\x00", 1)
            tag, bits_s, hashes_s = header.decode("ascii").split(":")
        except (ValueError, UnicodeDecodeError) as exc:
            raise ValueError("corrupt bloom filter encoding") from exc
        if tag != "BLOOM":
            raise ValueError("corrupt bloom filter tag")
        return cls(int(bits_s), int(hashes_s), bytearray(rest))
