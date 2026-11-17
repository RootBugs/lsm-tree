"""Core LSM store behaviors: roundtrips, ranges, flush, crash, compaction."""
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from lsm_tree import LSMTree  # noqa: E402


class StoreTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="lsm-test-")
        self.addCleanup(self._tmp.cleanup)
        self.dir = Path(self._tmp.name)


class TestRoundtrip(StoreTestCase):
    def test_put_get_delete_roundtrip(self):
        tree = LSMTree(self.dir)
        tree.put("alpha", "one")
        tree.put("beta", "two")
        self.assertEqual(tree.get("alpha"), b"one")
        self.assertEqual(tree.get("beta"), b"two")
        self.assertTrue(tree.delete("alpha"))
        self.assertIsNone(tree.get("alpha"))
        self.assertEqual(tree.get("beta"), b"two")
        tree.close()

    def test_missing_key_returns_none(self):
        tree = LSMTree(self.dir)
        self.assertIsNone(tree.get("nope"))
        self.assertFalse(tree.delete("nope"))
        tree.close()

    def test_overwrite_semantics_latest_wins(self):
        tree = LSMTree(self.dir)
        tree.put("k", "v1")
        tree.put("k", "v2")
        tree.put("k", "v3")
        self.assertEqual(tree.get("k"), b"v3")
        tree.close()

    def test_overwrite_across_flush_boundary(self):
        tree = LSMTree(self.dir, memtable_bytes=1)
        tree.put("k", "old")
        tree.flush()
        tree.put("k", "new")
        self.assertEqual(tree.get("k"), b"new")
        # and the reverse order: memtable tombstone shadows older SST value
        tree.delete("k")
        self.assertIsNone(tree.get("k"))
        tree.close()

    def test_flush_then_reopen_reads_sstable(self):
        tree = LSMTree(self.dir, memtable_bytes=1)
        for i in range(10):
            tree.put(f"key-{i:03d}", f"val-{i}")
        tree.flush()
        tree.close()
        reopened = LSMTree(self.dir)
        for i in range(10):
            self.assertEqual(reopened.get(f"key-{i:03d}"), f"val-{i}".encode())
        self.assertIsNone(reopened.get("key-999"))
        reopened.close()


class TestRange(StoreTestCase):
    def test_range_scans_in_key_order(self):
        tree = LSMTree(self.dir, memtable_bytes=1)
        keys = ["delta", "alpha", "charlie", "bravo", "echo", "foxtrot"]
        for k in keys:
            tree.put(k, k.upper())
        tree.flush()  # half in SSTable, rest in memtable
        for k in ["golf", "hotel", "india"]:
            tree.put(k, k.upper())
        got = [k for k, _ in tree.range()]
        self.assertEqual(got, sorted(k.encode() for k in keys + ["golf", "hotel", "india"]))
        tree.close()

    def test_range_bounds_inclusive_start_exclusive_end(self):
        tree = LSMTree(self.dir, memtable_bytes=1)
        for ch in "abcdef":
            tree.put(ch, ch)
        tree.flush()
        got = lambda ks: [k.decode() for k, _ in ks]  # noqa: E731
        self.assertEqual(got(tree.range("b", "d")), ["b", "c"])
        self.assertEqual(got(tree.range("d", None)), ["d", "e", "f"])
        self.assertEqual(got(tree.range(None, "b")), ["a"])
        tree.close()

    def test_range_excludes_deleted_and_shadowed(self):
        tree = LSMTree(self.dir, memtable_bytes=1)
        tree.put("a", "1")
        tree.put("b", "2")
        tree.put("c", "3")
        tree.flush()
        tree.delete("b")
        tree.put("a", "10")
        items = {k.decode(): v for k, v in tree.range()}
        self.assertEqual(items, {"a": b"10", "c": b"3"})
        tree.close()


class TestBloomInStore(StoreTestCase):
    def test_flushed_keys_still_found_via_bloom_path(self):
        tree = LSMTree(self.dir, memtable_bytes=1)
        expected = {}
        for i in range(200):
            k = f"k{i:04d}"
            v = f"v{i}"
            expected[k] = v.encode()
            tree.put(k, v)
        tree.flush()
        tree.close()
        reopened = LSMTree(self.dir)
        for k, v in expected.items():
            self.assertEqual(reopened.get(k), v, f"false negative / miss for {k}")
        self.assertIsNone(reopened.get("k9999"))
        reopened.close()


class TestCrashRecovery(StoreTestCase):
    def test_recovery_after_simulated_crash_replays_wal(self):
        """Process dies via os._exit: no close, no flush — WAL must replay."""
        script = textwrap.dedent(
            f"""
            import os, sys
            sys.path.insert(0, {str(PROJECT_ROOT)!r})
            from lsm_tree import LSMTree
            t = LSMTree({str(self.dir)!r})
            t.put("alpha", "1")
            t.put("beta", "2")
            t.put("alpha", "3")
            t.delete("beta")
            os._exit(0)
            """
        )
        proc = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, timeout=60
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        recovered = LSMTree(self.dir)
        self.assertEqual(recovered.get("alpha"), b"3")
        self.assertIsNone(recovered.get("beta"))
        recovered.close()

    def test_torn_wal_tail_is_truncated_prior_records_intact(self):
        tree = LSMTree(self.dir)
        tree.put("a", "1")
        tree.put("b", "2")
        tree.close()
        wal = self.dir / "wal.log"
        with wal.open("ab") as fh:
            fh.write(b"\x10\x00\x00\x00partial-record-garbage")  # torn record
        recovered = LSMTree(self.dir)
        self.assertEqual(recovered.get("a"), b"1")
        self.assertEqual(recovered.get("b"), b"2")
        # torn tail was truncated away
        from lsm_tree import WriteAheadLog

        recs, good_bytes = WriteAheadLog.replay(wal)
        self.assertEqual(len(recs), 2)
        self.assertEqual(good_bytes, wal.stat().st_size)
        recovered.close()


class TestCompaction(StoreTestCase):
    def _build_tiered(self, tree, keys, deletions=()):
        for k in keys:
            tree.put(k, f"val-{k}")
        tree.flush()
        for k in keys:
            tree.put(k, f"val2-{k}")
        tree.flush()
        for k in deletions:
            tree.delete(k)
        tree.flush()
        return tree

    def test_compaction_preserves_all_live_keys(self):
        tree = LSMTree(self.dir, memtable_bytes=1, tier_size=2)
        keys = [f"k{i:03d}" for i in range(60)]
        self._build_tiered(tree, keys)
        self.assertGreaterEqual(len(tree.stats()["sstables"]), 3)
        merged = tree.compact(force=True)
        self.assertGreaterEqual(merged, 1)
        stats = tree.stats()
        self.assertEqual(len(stats["sstables"]), 1, stats)
        for k in keys:
            self.assertEqual(tree.get(k), f"val2-{k}".encode())
        tree.close()

    def test_deleted_keys_stay_deleted_after_compaction(self):
        tree = LSMTree(self.dir, memtable_bytes=1, tier_size=2)
        keys = [f"k{i:03d}" for i in range(40)]
        self._build_tiered(tree, keys, deletions=keys[:15])
        tree.compact(force=True)
        for k in keys[:15]:
            self.assertIsNone(tree.get(k), f"{k} resurrected after compaction")
        for k in keys[15:]:
            self.assertEqual(tree.get(k), f"val2-{k}".encode())
        # range agrees too
        self.assertEqual([k for k, _ in tree.range()], [k.encode() for k in keys[15:]])
        tree.close()

    def test_automatic_tiered_compaction_runs_on_flush(self):
        tree = LSMTree(self.dir, memtable_bytes=1, tier_size=2)
        for i in range(8):
            tree.put(f"x{i}", str(i))
            tree.flush()
        stats = tree.stats()
        self.assertGreaterEqual(stats["compactions"], 1)
        for i in range(8):
            self.assertEqual(tree.get(f"x{i}"), str(i).encode())
        tree.close()


class TestStats(StoreTestCase):
    def test_stats_counters_and_layout(self):
        tree = LSMTree(self.dir)
        tree.put("a", "1")
        tree.put("b", "2")
        tree.get("a")
        tree.get("zzz")
        tree.range()
        tree.delete("b")
        pre = tree.stats()
        self.assertGreater(pre["wal_bytes"], 0)
        self.assertEqual(pre["flushes"], 0)
        tree.flush()
        stats = tree.stats()
        self.assertEqual(stats["puts"], 2)
        self.assertEqual(stats["deletes"], 1)
        self.assertEqual(stats["gets"], 2)
        self.assertEqual(stats["ranges"], 1)
        self.assertEqual(stats["flushes"], 1)
        self.assertEqual(len(stats["sstables"]), 1)
        self.assertEqual(stats["sstables"][0]["level"], 0)
        self.assertEqual(stats["wal_bytes"], 0, "WAL truncated by flush")
        self.assertIn("next_seq", stats)
        tree.close()


if __name__ == "__main__":
    unittest.main()
