import { aptos } from "@aptos-labs/aptos-indexer-protos";

export function grpcTimestampToDate(
  timestamp: aptos.util.timestamp.Timestamp,
): Date {
  const seconds = timestamp.seconds!;
  const nanos = timestamp.nanos!;
  const milliseconds = Number(seconds) * 1000 + nanos / 1000000;
  const date = new Date(milliseconds);
  return date;
}
