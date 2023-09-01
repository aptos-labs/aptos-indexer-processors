import { aptos } from "@aptos-labs/aptos-indexer-protos";
import { Event } from "./models/Event";

export const INDEXER_NAME = "ts_example_indexer";

function grpcTimestampToDate(timestamp: aptos.util.timestamp.Timestamp) {
  const seconds = timestamp.seconds!;
  const nanos = timestamp.nanos!;
  const milliseconds = Number(seconds) * 1000 + nanos / 1000000;
  const date = new Date(milliseconds);
  return date;
}

export function parse(transaction: aptos.transaction.v1.Transaction): Event[] {
  // Custom filtering: Filter out all transactions that are not User Transactions
  if (
    transaction.type !=
    aptos.transaction.v1.Transaction_TransactionType.TRANSACTION_TYPE_USER
  ) {
    return [];
  }

  // Parse Transaction object
  const transactionVersion = transaction.version!;
  const transactionBlockHeight = transaction.blockHeight!;
  const insertedAt = grpcTimestampToDate(transaction.timestamp!);

  const userTransaction = transaction.user!;

  const events = userTransaction.events!;

  return events.map((event, i) => {
    const eventEntity = new Event();
    eventEntity.sequenceNumber = event.sequenceNumber!.toString();
    eventEntity.creationNumber = event.key!.creationNumber!.toString();
    eventEntity.accountAddress = `0x${event.key!.accountAddress}`;
    eventEntity.type = event.typeStr!;
    eventEntity.data = event.data!;
    eventEntity.eventIndex = i.toString();
    eventEntity.transactionVersion = transactionVersion.toString();
    eventEntity.transactionBlockHeight = transactionBlockHeight.toString();
    eventEntity.inserted_at = insertedAt;
    return eventEntity;
  });
}
