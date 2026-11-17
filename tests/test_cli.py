"""CLI end-to-end tests via `python -m lsm_tree` subprocesses."""
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def run_cli(*args, cwd=None):
    return subprocess.run(
        [sys.executable, "-m", "lsm_tree", *args],
        cwd=cwd or PROJECT_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=60,
    )


class TestCLI(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="lsm-cli-")
        self.addCleanup(self._tmp.cleanup)
        self.dir = str(Path(self._tmp.name))

    def test_put_get_delete_flow(self):
        out = run_cli(self.dir, "put", "greeting", "hello world")
        self.assertEqual(out.returncode, 0, out.stderr)
        self.assertEqual(out.stdout.strip(), "OK")
        got = run_cli(self.dir, "get", "greeting")
        self.assertEqual(got.returncode, 0, got.stderr)
        self.assertEqual(got.stdout.strip(), "hello world")
        deleted = run_cli(self.dir, "del", "greeting")
        self.assertEqual(deleted.returncode, 0, deleted.stderr)
        missing = run_cli(self.dir, "get", "greeting")
        self.assertEqual(missing.returncode, 1)
        self.assertIn("not found", missing.stderr)

    def test_range_and_stats_output(self):
        for k, v in [("b", "2"), ("a", "1"), ("c", "3")]:
            self.assertEqual(run_cli(self.dir, "put", k, v).returncode, 0)
        ranged = run_cli(self.dir, "range")
        self.assertEqual(ranged.returncode, 0, ranged.stderr)
        lines = [ln.split("\t") for ln in ranged.stdout.splitlines()]
        self.assertEqual(lines, [["a", "1"], ["b", "2"], ["c", "3"]])
        bounded = run_cli(self.dir, "range", "a", "b")
        self.assertEqual([ln.split("\t")[0] for ln in bounded.stdout.splitlines()], ["a"])
        stats = run_cli(self.dir, "stats")
        self.assertEqual(stats.returncode, 0, stats.stderr)
        payload = json.loads(stats.stdout)
        self.assertEqual(payload["puts"], 3)
        self.assertIn("sstables", payload)
        self.assertIn("next_seq", payload)

    def test_compact_command(self):
        for i in range(5):
            run_cli(self.dir, "put", f"k{i}", str(i))
        compacted = run_cli(self.dir, "compact", "--force")
        self.assertEqual(compacted.returncode, 0, compacted.stderr)
        self.assertIn("compacted", compacted.stdout)
        for i in range(5):
            got = run_cli(self.dir, "get", f"k{i}")
            self.assertEqual(got.stdout.strip(), str(i))

    def test_bad_command_fails_cleanly(self):
        out = run_cli(self.dir, "frobnicate")
        self.assertNotEqual(out.returncode, 0)


if __name__ == "__main__":
    unittest.main()
