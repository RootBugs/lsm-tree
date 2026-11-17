"""LSM-tree key-value store: skip-list memtable + WAL + SSTables + compaction.

Writes: WAL record -> memtable.  When the memtable exceeds
``memtable_bytes`` it is flushed to a level-0 SSTable and the WAL is
truncated.  Reads consult the memtable then every SSTable (bloom filter +
sparse index), keeping the entry with the highest sequence number.
Size-tiered compaction merges runs of ``tier_size`` files at one level into
a single run at the next level.
"""
from __future__ import annotations

import heapq
import json
import os
from pathlib import Path

from .skiplist import SkipList
from .sstable import KIND_DEL, KIND_PUT, SSTableReader, SSTableWriter
from .wal import WriteAheadLog

MANIFEST_NAME = "MANIFEST.json"
WAL_NAME = "wal.log"


class LSMTree:
    """A single-directory LSM key-value store (bytes -> bytes)."""

    def __init__(
        self,
        directory: str | Path,
        memtable_bytes: int = 64 * 1024,
        tier_size: int = 4,
        seed: int = 0x5EED,
    ) -> None:
        if tier_size < 2:
            raise ValueError("tier_size must be >= 2")
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.memtable_bytes = memtable_bytes
        self.tier_size = tier_size
        self._seed = seed

        defaults = {
            "puts": 0,
            "deletes": 0,
            "gets": 0,
            "ranges": 0,
            "flushes": 0,
            "compactions": 0,
        }
        manifest = self._load_manifest()
        self._counters = {**defaults, **manifest.get("counters", {})}
        self._next_file_no = manifest.get("next_file_no", 1)
        self._next_seq = manifest.get("next_seq", 1)
        self._sst_meta = list(manifest.get("sstables", []))
        self._readers: dict[str, SSTableReader] = {}
        for meta in self._sst_meta:
            path = self.dir / meta["file"]
            if path.exists():
                self._readers[meta["file"]] = SSTableReader(path)
            else:
                raise FileNotFoundError(f"manifest references missing {meta['file']}")

        self._memtable = SkipList(seed=seed)
        self._mem_used = 0
        self._wal = WriteAheadLog(self.dir / WAL_NAME)
        self._recover_wal()
        self._write_manifest()

    # ------------------------------------------------------------------ #
    # manifest / recovery
    def _manifest_path(self) -> Path:
        return self.dir / MANIFEST_NAME

    def _load_manifest(self) -> dict:
        path = self._manifest_path()
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}

    def _write_manifest(self) -> None:
        payload = {
            "next_file_no": self._next_file_no,
            "next_seq": self._next_seq,
            "sstables": self._sst_meta,
            "counters": self._counters,
        }
        tmp = self.dir / (MANIFEST_NAME + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        os.replace(tmp, self._manifest_path())

    def _recover_wal(self) -> None:
        records, truncated = WriteAheadLog.recover(self.dir / WAL_NAME)
        for rec in records:
            seq = rec["seq"]
            if rec["op"] == "put":
                self._memtable_insert(rec["key"], seq, KIND_PUT, rec["value"])
            else:
                self._memtable_insert(rec["key"], seq, KIND_DEL, b"")
            if seq >= self._next_seq:
                self._next_seq = seq + 1
        self._wal_truncated = truncated

    def _memtable_insert(self, key: bytes, seq: int, kind: int, value: bytes) -> None:
        try:
            old = self._memtable.get(key)
            self._mem_used -= len(key) + len(old[2])
        except KeyError:
            pass
        self._memtable.insert(key, (seq, kind, value))
        self._mem_used += len(key) + len(value)

    # ------------------------------------------------------------------ #
    # writes
    def put(self, key: bytes, value: bytes) -> None:
        """Store ``value`` under ``key`` (overwrites any previous value)."""
        key, value = self._coerce(key, value)
        seq = self._next_seq
        self._next_seq += 1
        self._wal.append_put(key, value, seq)
        self._memtable_insert(key, seq, KIND_PUT, value)
        self._counters["puts"] += 1
        self._maybe_flush()

    def delete(self, key: bytes) -> bool:
        """Write a tombstone for ``key``.  Returns True if key existed."""
        key = self._coerce_key(key)
        existed = self._lookup(key) is not None
        seq = self._next_seq
        self._next_seq += 1
        self._wal.append_del(key, seq)
        self._memtable_insert(key, seq, KIND_DEL, b"")
        self._counters["deletes"] += 1
        self._maybe_flush()
        return existed

    @staticmethod
    def _coerce_key(key) -> bytes:
        if isinstance(key, str):
            return key.encode("utf-8")
        if isinstance(key, bytes):
            return key
        raise TypeError("key must be bytes or str")

    def _coerce(self, key, value) -> tuple[bytes, bytes]:
        if isinstance(value, str):
            value = value.encode("utf-8")
        if not isinstance(value, bytes):
            raise TypeError("value must be bytes or str")
        return self._coerce_key(key), value

    def _maybe_flush(self) -> None:
        if self._mem_used >= self.memtable_bytes and len(self._memtable) > 0:
            self.flush()

    def flush(self) -> None:
        """Persist the memtable to a new level-0 SSTable and reset the WAL."""
        if len(self._memtable) == 0:
            return
        file_no = self._next_file_no
        self._next_file_no += 1
        name = f"sst-{file_no:06d}.sst"
        path = self.dir / name
        writer = SSTableWriter(path)
        min_seq = None
        max_seq = None
        for key, (seq, kind, value) in self._memtable.items():
            writer.add(key, seq, kind, value)
            min_seq = seq if min_seq is None else min(min_seq, seq)
            max_seq = seq if max_seq is None else max(max_seq, seq)
        writer.finish()
        entries = len(self._memtable)
        self._sst_meta.append(
            {
                "file": name,
                "level": 0,
                "entries": entries,
                "min_seq": min_seq if min_seq is not None else 0,
                "max_seq": max_seq if max_seq is not None else 0,
            }
        )
        self._readers[name] = SSTableReader(path)
        self._memtable = SkipList(seed=self._seed)
        self._mem_used = 0
        self._wal.truncate()
        self._counters["flushes"] += 1
        self._write_manifest()
        self._maybe_compact()

    # ------------------------------------------------------------------ #
    # compaction
    def _maybe_compact(self) -> None:
        while True:
            by_level: dict[int, list[dict]] = {}
            for meta in self._sst_meta:
                by_level.setdefault(meta["level"], []).append(meta)
            target = None
            for level in sorted(by_level):
                if len(by_level[level]) >= self.tier_size:
                    target = level
                    break
            if target is None:
                break
            self._merge(by_level[target], target + 1)

    def compact(self, force: bool = False) -> int:
        """Merge runs.  Default: merge every level that is tier-sized.

        With ``force=True`` all SSTables are merged into one run (level 0).
        Returns the number of merge operations performed.
        """
        merges = 0
        if force and len(self._sst_meta) > 1:
            self._merge(list(self._sst_meta), 0)
            merges += 1
        before = self._counters["compactions"]
        self._maybe_compact()
        merges += self._counters["compactions"] - before
        return merges

    def _merge(self, victims: list[dict], new_level: int) -> None:
        """Merge ``victims`` into one sorted run at ``new_level``."""
        victim_names = {m["file"] for m in victims}
        outside_min = min(
            (m["min_seq"] for m in self._sst_meta if m["file"] not in victim_names),
            default=None,
        )
        file_no = self._next_file_no
        self._next_file_no += 1
        name = f"sst-{file_no:06d}.sst"
        path = self.dir / name

        sources = [
            (
                (k, (s, kind, v))
                for k, s, kind, v in self._readers[m["file"]].all_entries()
            )
            for m in victims
        ]
        writer = SSTableWriter(path)
        count = 0
        min_seq = None
        max_seq = None
        merged = heapq.merge(*sources, key=lambda item: item[0])
        cur_key = None
        best = None

        def emit() -> None:
            nonlocal count, min_seq, max_seq
            if cur_key is None or best is None:
                return
            seq, kind, value = best
            if kind == KIND_DEL and (outside_min is None or seq < outside_min):
                return  # tombstone shadows everything older; safe to drop
            writer.add(cur_key, seq, kind, value)
            count += 1
            min_seq = seq if min_seq is None else min(min_seq, seq)
            max_seq = seq if max_seq is None else max(max_seq, seq)

        for key, entry in merged:
            if key != cur_key:
                emit()
                cur_key, best = key, entry
            elif entry[0] > best[0]:
                best = entry
        emit()
        writer.finish()

        for m in victims:
            old = self.dir / m["file"]
            if old.exists():
                old.unlink()
            self._readers.pop(m["file"], None)
        self._sst_meta = [m for m in self._sst_meta if m["file"] not in victim_names]
        self._sst_meta.append(
            {
                "file": name,
                "level": new_level,
                "entries": count,
                "min_seq": min_seq if min_seq is not None else 0,
                "max_seq": max_seq if max_seq is not None else 0,
            }
        )
        self._readers[name] = SSTableReader(path)
        self._counters["compactions"] += 1
        self._write_manifest()

    # ------------------------------------------------------------------ #
    # reads
    def get(self, key: bytes):
        """Return the value for ``key`` or ``None`` if absent/deleted."""
        self._counters["gets"] += 1
        return self._lookup(self._coerce_key(key))

    def _lookup(self, key: bytes):
        best = None
        try:
            best = self._memtable.get(key)
        except KeyError:
            pass
        for reader in self._readers.values():
            hit = reader.get(key)
            if hit is not None and (best is None or hit[0] > best[0]):
                best = hit
        if best is None or best[1] == KIND_DEL:
            return None
        return best[2]

    def range(self, start: bytes | None = None, end: bytes | None = None):
        """Yield ``(key, value)`` for keys in ``[start, end)`` in key order.

        ``start``/``end`` are inclusive/exclusive respectively; ``None``
        means unbounded on that side.
        """
        self._counters["ranges"] += 1
        return self._range_iter(
            self._coerce_key(start) if start is not None else None,
            self._coerce_key(end) if end is not None else None,
        )

    def _range_iter(self, start: bytes | None, end: bytes | None):
        sources = []
        if len(self._memtable) > 0:
            sources.append(
                (kv for kv in self._memtable.items() if self._in_range(kv[0], start, end))
            )
        for reader in self._readers.values():
            sources.append(
                ((k, (s, kind, v)) for k, s, kind, v in reader.iter_range(start, end))
            )
        if not sources:
            return
        merged = heapq.merge(*sources, key=lambda kv: kv[0])
        cur_key = None
        best = None
        for key, entry in merged:
            if key != cur_key:
                if cur_key is not None and best[1] != KIND_DEL:
                    yield cur_key, best[2]
                cur_key, best = key, entry
            elif entry[0] > best[0]:
                best = entry
        if cur_key is not None and best[1] != KIND_DEL:
            yield cur_key, best[2]

    @staticmethod
    def _in_range(key: bytes, start: bytes | None, end: bytes | None) -> bool:
        if start is not None and key < start:
            return False
        if end is not None and key >= end:
            return False
        return True

    # ------------------------------------------------------------------ #
    def stats(self) -> dict:
        """Return operational counters and layout information."""
        wal_size = (self.dir / WAL_NAME).stat().st_size if (self.dir / WAL_NAME).exists() else 0
        return {
            **self._counters,
            "memtable_entries": len(self._memtable),
            "memtable_bytes": self._mem_used,
            "wal_bytes": wal_size,
            "sstables": [dict(m) for m in self._sst_meta],
            "levels": sorted({m["level"] for m in self._sst_meta}),
            "next_seq": self._next_seq,
            "torn_wal_tail_recovered": getattr(self, "_wal_truncated", False),
        }

    def close(self) -> None:
        """Flush the manifest and close the WAL (memtable stays in the WAL)."""
        self._write_manifest()
        self._wal.close()

    def __enter__(self) -> "LSMTree":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
