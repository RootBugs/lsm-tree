"""SSTable (sorted string table) file format.

Layout (documented in README)::

    [data block]    entries sorted by key, each:
                    u16 klen | u64 seq | u8 kind | u32 vlen | key | value
    [sparse index]  every 16th key -> data offset:
                    u16 klen | key | u64 offset
    [bloom filter]  BloomFilter.to_bytes()
    [footer]        40 bytes, little-endian:
                    u64 index_off | u32 index_len | u32 bloom_off
                    | u32 bloom_len | u32 num_entries | i64 min_seq | u64 magic
"""
from __future__ import annotations

import struct
from pathlib import Path

from .bloom import BloomFilter

KIND_PUT = 0
KIND_DEL = 1

SPARSE_INTERVAL = 16
FOOTER_FMT = "<QIIIIqQ"
FOOTER_SIZE = struct.calcsize(FOOTER_FMT)
MAGIC = 0x4C534D54_41424C45

_ENTRY_HEAD = struct.Struct("<HQBI")
_SPARSE_HEAD = struct.Struct("<H")
_SPARSE_TAIL = struct.Struct("<Q")


def _encode_entry(key: bytes, seq: int, kind: int, value: bytes) -> bytes:
    return _ENTRY_HEAD.pack(len(key), seq & 0xFFFFFFFFFFFFFFFF, kind, len(value)) + key + value


class SSTableWriter:
    """Writes a sorted stream of entries to an SSTable file."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._fh = self.path.open("wb")
        self._index: list[tuple[bytes, int]] = []
        self._keys: list[bytes] = []
        self._offset = 0
        self._count = 0
        self._min_seq: int | None = None

    def add(self, key: bytes, seq: int, kind: int, value: bytes) -> None:
        if self._keys and key < self._keys[-1]:
            raise ValueError("SSTable entries must arrive in sorted key order")
        if self._count % SPARSE_INTERVAL == 0:
            self._index.append((key, self._offset))
        blob = _encode_entry(key, seq, kind, value)
        self._fh.write(blob)
        self._offset += len(blob)
        self._keys.append(key)
        self._count += 1
        if self._min_seq is None or seq < self._min_seq:
            self._min_seq = seq

    def finish(self) -> None:
        """Write sparse index, bloom filter and footer; close the file."""
        if self._fh.closed:
            return
        index_off = self._offset
        buf = bytearray()
        for key, off in self._index:
            buf += _SPARSE_HEAD.pack(len(key)) + key + _SPARSE_TAIL.pack(off)
        self._fh.write(bytes(buf))
        index_len = len(buf)
        bloom = BloomFilter.create(self._keys)
        bloom_bytes = bloom.to_bytes()
        bloom_off = index_off + index_len
        self._fh.write(bloom_bytes)
        footer = struct.pack(
            FOOTER_FMT,
            index_off,
            index_len,
            bloom_off,
            len(bloom_bytes),
            self._count,
            self._min_seq if self._min_seq is not None else 0,
            MAGIC,
        )
        self._fh.write(footer)
        self._fh.close()


class SSTableReader:
    """Read-only view over a finished SSTable."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        data = self.path.read_bytes()
        if len(data) < FOOTER_SIZE:
            raise ValueError(f"SSTable {self.path.name}: file too small")
        (
            self.index_off,
            self.index_len,
            self.bloom_off,
            self.bloom_len,
            self.num_entries,
            self.min_seq,
            magic,
        ) = struct.unpack_from(FOOTER_FMT, data, len(data) - FOOTER_SIZE)
        if magic != MAGIC:
            raise ValueError(f"SSTable {self.path.name}: bad magic")
        self._data = data
        self._index = self._parse_index()
        self._bloom = BloomFilter.from_bytes(
            data[self.bloom_off : self.bloom_off + self.bloom_len]
        )

    # ------------------------------------------------------------------ #
    def _parse_index(self) -> list[tuple[bytes, int]]:
        blob = self._data[self.index_off : self.index_off + self.index_len]
        out: list[tuple[bytes, int]] = []
        pos = 0
        while pos < len(blob):
            (klen,) = _SPARSE_HEAD.unpack_from(blob, pos)
            pos += _SPARSE_HEAD.size
            key = blob[pos : pos + klen]
            pos += klen
            (off,) = _SPARSE_TAIL.unpack_from(blob, pos)
            pos += _SPARSE_TAIL.size
            out.append((key, off))
        return out

    def _parse_entry(self, pos: int):
        klen, seq, kind, vlen = _ENTRY_HEAD.unpack_from(self._data, pos)
        pos += _ENTRY_HEAD.size
        key = self._data[pos : pos + klen]
        pos += klen
        value = self._data[pos : pos + vlen]
        return key, seq, kind, value, pos + vlen

    def _scan_from(self, offset: int):
        pos = offset
        end = self.index_off
        while pos < end:
            key, seq, kind, value, pos = self._parse_entry(pos)
            yield key, seq, kind, value

    # ------------------------------------------------------------------ #
    def _data_offset_for(self, key: bytes) -> int:
        """First data offset whose sparse key is <= ``key`` (0 if none)."""
        lo, hi = 0, len(self._index)
        best = 0
        while lo < hi:
            mid = (lo + hi) // 2
            if self._index[mid][0] <= key:
                best = self._index[mid][1]
                lo = mid + 1
            else:
                hi = mid
        return best

    def get(self, key: bytes):
        """Return ``(seq, kind, value)`` for ``key`` or ``None``."""
        if not self._bloom.might_contain(key):
            return None
        for k, seq, kind, value in self._scan_from(self._data_offset_for(key)):
            if k == key:
                return seq, kind, value
            if k > key:
                break
        return None

    def iter_range(self, start: bytes | None = None, end: bytes | None = None):
        """Yield ``(key, seq, kind, value)`` for keys in ``[start, end)``."""
        offset = 0 if start is None else self._data_offset_for(start)
        for k, seq, kind, value in self._scan_from(offset):
            if start is not None and k < start:
                continue
            if end is not None and k >= end:
                break
            yield k, seq, kind, value

    def all_entries(self):
        return self.iter_range()
