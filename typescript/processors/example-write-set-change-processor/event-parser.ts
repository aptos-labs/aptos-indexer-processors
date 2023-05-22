import { Transaction } from "@/aptos/transaction/v1/transaction_pb";
import { Timestamp } from "@/aptos/util/timestamp/timestamp_pb";
import { Event } from "@/processors/example-write-set-change-processor/models/Event";

export const INDEXER_NAME = "ts_example_indexer";

function grpcTimestampToDate(timestamp: Timestamp) {
  const seconds = timestamp.getSeconds();
  const nanos = timestamp.getNanos();
  const milliseconds = seconds * 1000 + nanos / 1000000;
  const date = new Date(milliseconds);
  return date;
}

export function parse(transaction: Transaction): Event[] {
  // Custom filtering: Filter out all transactions that are not User Transactions
  if (
    transaction.getType() != Transaction.TransactionType.TRANSACTION_TYPE_USER
  ) {
    return [];
  }

  // Parse Transaction object
  const transactionVersion = transaction.getVersion();
  const transactionBlockHeight = transaction.getBlockHeight();
  const insertedAt = grpcTimestampToDate(transaction.getTimestamp());

  const userTransaction = transaction.getUser();

  const events = userTransaction.getEventsList();

  return events.map((event, i) => {
    const eventEntity = new Event();
    eventEntity.sequenceNumber = event.getSequenceNumber().toString();
    eventEntity.creationNumber = event.getKey().getCreationNumber().toString();
    eventEntity.accountAddress = `0x${event.getKey().getAccountAddress()}`;
    eventEntity.type = event.getTypeStr();
    eventEntity.data = event.getData();
    eventEntity.eventIndex = i.toString();
    eventEntity.transactionVersion = transactionVersion.toString();
    eventEntity.transactionBlockHeight = transactionBlockHeight.toString();
    eventEntity.inserted_at = insertedAt;
    return eventEntity;
  });
}
