import hashlib

from typing import Optional
from aptos.util.timestamp import timestamp_pb2


def hash(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()


def truncate_str(s: str, max_len: int) -> str:
    if len(s) > max_len:
        return s[:max_len]
    else:
        return s


def standardize_address(address: str) -> str:
    address = address.removeprefix("0x")
    return "0x" + address.zfill(64)


def convert_timestamp_to_int64(timestamp: timestamp_pb2.Timestamp) -> int:
    return timestamp.seconds * 1000000 + int(timestamp.nanos / 1000)
