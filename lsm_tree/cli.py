"""Command-line interface for the lsm-tree store.

Usage::

    python -m lsm_tree <dir> put <key> <value>
    python -m lsm_tree <dir> get <key>
    python -m lsm_tree <dir> del <key>
    python -m lsm_tree <dir> range [start] [end]
    python -m lsm_tree <dir> stats
    python -m lsm_tree <dir> compact [--force]
"""
from __future__ import annotations

import argparse
import json
import sys

from .store import LSMTree


def _decode(raw: bytes) -> str:
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return repr(raw)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lsm_tree", description="LSM-tree KV store CLI")
    parser.add_argument("directory", help="store directory")
    sub = parser.add_subparsers(dest="command", required=True)

    p_put = sub.add_parser("put", help="store a key")
    p_put.add_argument("key")
    p_put.add_argument("value")

    p_get = sub.add_parser("get", help="fetch a key")
    p_get.add_argument("key")

    p_del = sub.add_parser("del", help="delete a key")
    p_del.add_argument("key")

    p_range = sub.add_parser("range", help="scan keys in order")
    p_range.add_argument("start", nargs="?", default=None)
    p_range.add_argument("end", nargs="?", default=None)

    sub.add_parser("stats", help="print store statistics")

    p_compact = sub.add_parser("compact", help="run compaction")
    p_compact.add_argument("--force", action="store_true", help="merge every run into one")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    tree = LSMTree(args.directory)
    try:
        if args.command == "put":
            tree.put(args.key, args.value)
            print("OK")
        elif args.command == "get":
            value = tree.get(args.key)
            if value is None:
                print("not found", file=sys.stderr)
                return 1
            print(_decode(value))
        elif args.command == "del":
            existed = tree.delete(args.key)
            print("OK" if existed else "OK (no-op, key was absent)")
        elif args.command == "range":
            for key, value in tree.range(args.start, args.end):
                print(f"{_decode(key)}\t{_decode(value)}")
        elif args.command == "stats":
            print(json.dumps(tree.stats(), indent=2, sort_keys=True))
        elif args.command == "compact":
            merges = tree.compact(force=args.force)
            print(f"compacted ({merges} merge(s))")
    finally:
        tree.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
