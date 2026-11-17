# Strata

A real **Log-Structured Merge-Tree key-value store** in pure Python: an
in-memory skip-list memtable, a write-ahead log for crash durability,
on-disk SSTables with bloom filters and sparse indexes, and size-tiered
compaction that actually merges sorted runs.

**Why it's special:** most "LSM" tutorials stop at the memtable. This one
ships the whole write path — WAL framing with CRC, a documented binary
SSTable layout, a bloom filter with no false negatives, tiered compaction
that provably preserves live keys and never resurrects deletes, plus a
CLI with `put/get/del/range/stats`.

**Zero dependencies** — Python standard library only.

## Architecture

```
put/del ──► WAL (append + flush per record) ──► skip-list memtable
                                                   │ memtable_bytes exceeded
                                                   ▼
                                    flush ──► level-0 SSTable + manifest
                                                   │ tier_size runs at one level
                                                   ▼
                                    compaction ──► merged run at next level
```

- **Memtable** (`lsm_tree/skiplist.py`): seeded, deterministic skip list
  with sorted iteration; a flush writes keys in order with no sort step.
- **WAL** (`lsm_tree/wal.py`): records framed as
  `u32 payload_len | u32 crc32 | JSON payload`. Replay stops at the first
  torn/corrupt record and truncates the tail, recovering everything before
  it. Each record is flushed to the OS before `put` returns.
- **SSTable** (`lsm_tree/sstable.py`), file layout:

  ```
  [data block]    u16 klen | u64 seq | u8 kind | u32 vlen | key | value   (sorted by key)
  [sparse index]  every 16th key -> u64 data offset
  [bloom filter]  "BLOOM:num_bits:num_hashes\0" + bit array
  [footer]        u64 index_off | u32 index_len | u32 bloom_off
                  | u32 bloom_len | u32 num_entries | i64 min_seq | u64 magic
  ```

  Reads consult the bloom filter first, binary-search the sparse index,
  then scan a small window of the data block. Every entry carries a global
  sequence number; the winner across memtable + all SSTables is the entry
  with the highest seq, so file order never matters.
- **Compaction** (`store.py:_merge`): when a level holds `tier_size`
  runs, they are k-way merged into one run at the next level. A tombstone
  is dropped only when no run *outside* the merge set can hold older data
  (checked via each run's `min_seq`) — deletes can never resurrect.
- **Manifest** `MANIFEST.json` (atomic tmp + rename): file numbering,
  next sequence, per-run metadata, cumulative counters.

## Runnable example

```console
> python -m lsm_tree C:\temp\demo put apple 1
OK
> python -m lsm_tree C:\temp\demo put banana 2
OK
> python -m lsm_tree C:\temp\demo put cherry 3
OK
> python -m lsm_tree C:\temp\demo del banana
OK
> python -m lsm_tree C:\temp\demo get apple
1
> python -m lsm_tree C:\temp\demo get banana
not found            (exit code 1)
> python -m lsm_tree C:\temp\demo range
apple	1
cherry	3
> python -m lsm_tree C:\temp\demo stats
{
  "compactions": 0,
  "deletes": 1,
  "flushes": 0,
  "gets": 2,
  "levels": [],
  "memtable_bytes": 19,
  "memtable_entries": 3,
  "next_seq": 5,
  "puts": 3,
  "ranges": 1,
  "sstables": [],
  "torn_wal_tail_recovered": false,
  "wal_bytes": 225
}
```

Library use:

```python
from lsm_tree import LSMTree

with LSMTree("C:/temp/demo2") as db:
    db.put("k", "v")
    db.get("k")            # b'v'
    db.range("a", "z")     # sorted (key, value) pairs, [start, end)
    db.compact(force=True)
```

Semantics: keys/values are `bytes` (or `str`, encoded UTF-8); `get`
returns `None` for missing **and** deleted keys; `range` bounds are
inclusive-start / exclusive-end; sequence numbers are monotonic across
reopens.

## Test command

```
python -m unittest discover -s tests -v
```

34 tests cover roundtrips, overwrite/shadowing, ordered range scans,
bloom no-false-negatives, simulated-crash WAL replay (`os._exit`),
torn-tail truncation, compaction key preservation, delete persistence
after compaction, the binary formats, and the CLI end to end.
