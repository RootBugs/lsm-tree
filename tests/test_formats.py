"""Component-level tests: skip list, bloom filter, SSTable format, WAL."""
import struct
import sys
import tempfile
import unittest
import zlib
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from lsm_tree import BloomFilter, SSTableReader, SSTableWriter, SkipList, WriteAheadLog  # noqa: E402
from lsm_tree.sstable import KIND_DEL, KIND_PUT, MAGIC  # noqa: E402


class TestSkipList(unittest.TestCase):
    def test_sorted_iteration_and_overwrite(self):
        sl = SkipList(seed=7)
        keys = ["m", "a", "z", "c", "b"]
        for k in keys:
            sl.insert(k, k.upper())
        sl.insert("a", "FIRST")
        self.assertEqual([k for k, _ in sl.items()], ["a", "b", "c", "m", "z"])
        self.assertEqual(dict(sl.items())["a"], "FIRST")
        self.assertEqual(len(sl), 5)

    def test_get_missing_raises_and_contains(self):
        sl = SkipList(seed=7)
        sl.insert(b"x", 1)
        with self.assertRaises(KeyError):
            sl.get(b"y")
        self.assertIn(b"x", sl)
        self.assertNotIn(b"y", sl)


class TestBloom(unittest.TestCase):
    def test_no_false_negatives_across_workloads(self):
        keys = [f"key-{i}".encode() for i in range(1000)]
        bf = BloomFilter.create(keys, fp_rate=0.01)
        for k in keys:
            self.assertTrue(bf.might_contain(k), f"false negative: {k}")

    def test_false_positive_rate_reasonable(self):
        keys = [f"key-{i}".encode() for i in range(1000)]
        bf = BloomFilter.create(keys, fp_rate=0.01)
        fp = sum(
            1 for i in range(5000) if bf.might_contain(f"absent-{i}".encode())
        )
        self.assertLess(fp / 5000, 0.05)

    def test_serialization_roundtrip(self):
        keys = [f"k{i}".encode() for i in range(50)]
        bf = BloomFilter.create(keys)
        restored = BloomFilter.from_bytes(bf.to_bytes())
        self.assertEqual(restored.num_bits, bf.num_bits)
        self.assertEqual(restored.num_hashes, bf.num_hashes)
        for k in keys:
            self.assertTrue(restored.might_contain(k))

    def test_empty_filter(self):
        bf = BloomFilter.create([])
        self.assertFalse(bf.might_contain(b"anything"))


class TestSSTable(unittest.TestCase):
    def _write(self, path, entries):
        w = SSTableWriter(path)
        for e in entries:
            w.add(*e)
        w.finish()

    def test_write_read_roundtrip_with_index_and_bloom(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "a.sst"
            entries = [
                (f"k{i:03d}".encode(), i + 1, KIND_PUT, f"v{i}".encode())
                for i in range(100)
            ]
            self._write(path, entries)
            r = SSTableReader(path)
            self.assertEqual(r.num_entries, 100)
            self.assertEqual(r.min_seq, 1)
            for k, seq, kind, v in entries:
                got = r.get(k)
                self.assertIsNotNone(got, k)
                self.assertEqual((got[0], got[1], got[2]), (seq, kind, v))
            self.assertIsNone(r.get(b"zzz"))

    def test_range_scan_matches_sorted_reference(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "b.sst"
            entries = [
                (f"k{i:03d}".encode(), i, KIND_PUT, b"v") for i in range(0, 200, 3)
            ]
            self._write(path, entries)
            r = SSTableReader(path)
            got = [k for k, *_ in r.iter_range(b"k050", b"k100")]
            expect = [k for k, *_ in entries if b"k050" <= k < b"k100"]
            self.assertEqual(got, expect)

    def test_corrupt_magic_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "c.sst"
            self._write(path, [(b"a", 1, KIND_PUT, b"1")])
            data = bytearray(path.read_bytes())
            data[-8:] = struct.pack("<Q", MAGIC ^ 0xFFFF)
            path.write_bytes(bytes(data))
            with self.assertRaises(ValueError):
                SSTableReader(path)

    def test_unsorted_input_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "d.sst"
            w = SSTableWriter(path)
            w.add(b"b", 1, KIND_PUT, b"1")
            with self.assertRaises(ValueError):
                w.add(b"a", 2, KIND_PUT, b"2")
            w.finish()


class TestWAL(unittest.TestCase):
    def test_append_iterate_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "wal.log"
            wal = WriteAheadLog(path)
            wal.append_put(b"alpha", b"1", 1)
            wal.append_del(b"beta", 2)
            wal.append_put(b"gamma", b"3", 3)
            wal.close()
            records, good = WriteAheadLog.replay(path)
            self.assertEqual(good, path.stat().st_size)
            self.assertEqual(len(records), 3)
            self.assertEqual(records[0], {"op": "put", "key": b"alpha", "seq": 1, "value": b"1"})
            self.assertEqual(records[1]["op"], "del")
            self.assertEqual(records[2]["value"], b"3")

    def test_crc_detects_single_bit_corruption(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "wal.log"
            wal = WriteAheadLog(path)
            wal.append_put(b"key", b"value", 1)
            wal.append_put(b"key2", b"value2", 2)
            wal.close()
            data = bytearray(path.read_bytes())
            data[10] ^= 0x01  # flip one bit inside first payload
            path.write_bytes(bytes(data))
            records, good = WriteAheadLog.replay(path)
            self.assertEqual(records, [])
            self.assertEqual(good, 0)

    def test_truncated_tail_keeps_prior_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "wal.log"
            wal = WriteAheadLog(path)
            wal.append_put(b"a", b"1", 1)
            wal.append_put(b"b", b"2", 2)
            wal.close()
            data = path.read_bytes()
            path.write_bytes(data[:-3])  # cut last record short
            records, truncated = WriteAheadLog.recover(path)
            self.assertTrue(truncated)
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]["key"], b"a")
            # after recover, file only holds intact records
            records2, good = WriteAheadLog.replay(path)
            self.assertEqual(len(records2), 1)
            self.assertEqual(good, path.stat().st_size)

    def test_empty_log_edge_case(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "wal.log"
            records, good = WriteAheadLog.replay(path)
            self.assertEqual((records, good), ([], 0))
            records, truncated = WriteAheadLog.recover(path)
            self.assertEqual(records, [])
            self.assertFalse(truncated)

    def test_single_record_and_large_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "wal.log"
            wal = WriteAheadLog(path)
            wal.append_put(b"only", b"1", 1)
            wal.close()
            records, _ = WriteAheadLog.replay(path)
            self.assertEqual(len(records), 1)
            big = bytes(range(256)) * 400  # 100 KB payload spanning buffers
            wal2 = WriteAheadLog(path)
            wal2.append_put(b"big", big, 2)
            wal2.close()
            records, good = WriteAheadLog.replay(path)
            self.assertEqual(len(records), 2)
            self.assertEqual(records[1]["value"], big)
            self.assertEqual(good, path.stat().st_size)


if __name__ == "__main__":
    unittest.main()
