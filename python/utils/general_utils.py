import datetime
import hashlib

from aptos_protos.aptos.util.timestamp import timestamp_pb2


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


def parse_pb_timestamp(timestamp: timestamp_pb2.Timestamp):
    datetime_obj = datetime.datetime.fromtimestamp(
        timestamp.seconds + timestamp.nanos * 1e-9
    )
    return datetime_obj.strftime("%Y-%m-%d %H:%M:%S.%f")


def convert_pb_timestamp_to_datetime(
    timestamp: timestamp_pb2.Timestamp,
) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(timestamp.seconds + timestamp.nanos * 1e-9)


def convert_timestamp_to_int64(timestamp: timestamp_pb2.Timestamp) -> int:
    return timestamp.seconds * 1000000 + int(timestamp.nanos / 1000)
