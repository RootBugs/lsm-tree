"""Write-ahead log with length + CRC32 framing and torn-tail recovery.

Record framing::

    u32 payload_len | u32 crc32(payload) | payload

Payloads are JSON objects with base64-encoded keys/values.  A truncated or
corrupted tail is detected on replay; everything before it is recovered and
the tail is truncated away.
"""
from __future__ import annotations

import base64
import json
import struct
from pathlib import Path

_HEAD = struct.Struct("<II")
HEAD_SIZE = _HEAD.size


def _encode_record(op: str, key: bytes, seq: int, value: bytes | None) -> bytes:
    obj = {
        "op": op,
        "key": base64.b64encode(key).decode("ascii"),
        "seq": seq,
    }
    if value is not None:
        obj["value"] = base64.b64encode(value).decode("ascii")
    return json.dumps(obj, separators=(",", ":")).encode("utf-8")


def _decode_record(payload: bytes) -> dict:
    obj = json.loads(payload.decode("utf-8"))
    out = {
        "op": obj["op"],
        "key": base64.b64decode(obj["key"]),
        "seq": int(obj["seq"]),
        "value": None,
    }
    if "value" in obj:
        out["value"] = base64.b64decode(obj["value"])
    return out


class WriteAheadLog:
    """Append-only log; every record is flushed before ``append`` returns."""

    def __init__(self, path: str | Path, fsync: bool = False) -> None:
        self.path = Path(path)
        self.fsync = fsync
        self._fh = self.path.open("ab")

    # ------------------------------------------------------------------ #
    def append_put(self, key: bytes, value: bytes, seq: int) -> None:
        self._append(_encode_record("put", key, seq, value))

    def append_del(self, key: bytes, seq: int) -> None:
        self._append(_encode_record("del", key, seq, None))

    def _append(self, payload: bytes) -> None:
        import zlib

        head = _HEAD.pack(len(payload), zlib.crc32(payload) & 0xFFFFFFFF)
        self._fh.write(head + payload)
        self._fh.flush()
        if self.fsync:
            import os

            os.fsync(self._fh.fileno())

    def close(self) -> None:
        if not self._fh.closed:
            self._fh.close()

    def truncate(self) -> None:
        """Reset the log to empty (called after the memtable is flushed)."""
        self._fh.close()
        self._fh = self.path.open("wb")

    # ------------------------------------------------------------------ #
    @staticmethod
    def replay(path: str | Path) -> tuple[list[dict], int]:
        """Replay intact records.

        Returns ``(records, good_bytes)``.  When ``good_bytes`` is smaller
        than the file, the tail is torn/corrupt and should be truncated.
        """
        import zlib

        p = Path(path)
        if not p.exists():
            return [], 0
        data = p.read_bytes()
        records: list[dict] = []
        pos = 0
        while pos + HEAD_SIZE <= len(data):
            length, crc = _HEAD.unpack_from(data, pos)
            end = pos + HEAD_SIZE + length
            if length > len(data) - pos - HEAD_SIZE:
                break  # truncated payload
            payload = data[pos + HEAD_SIZE : end]
            if zlib.crc32(payload) & 0xFFFFFFFF != crc:
                break  # corrupt payload
            try:
                records.append(_decode_record(payload))
            except (ValueError, KeyError, json.JSONDecodeError):
                break
            pos = end
        return records, pos

    @classmethod
    def recover(cls, path: str | Path) -> tuple[list[dict], bool]:
        """Replay and truncate any torn tail.  Returns ``(records, truncated)``."""
        records, good = cls.replay(path)
        p = Path(path)
        size = p.stat().st_size if p.exists() else 0
        if good < size:
            with p.open("r+b") as fh:
                fh.truncate(good)
            return records, True
        return records, False
