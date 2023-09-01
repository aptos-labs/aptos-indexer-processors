import { aptos } from "@aptos-labs/aptos-indexer-protos";
import { DataSource } from "typeorm";

export type ProcessingResult = {
  startVersion: bigint;
  endVersion: bigint;
};

export abstract class TransactionsProcessor {
  // Name of the processor for status logging and tracking of the latest processed
  // version.
  abstract name(): string;

  // Process transactions. The function is given the start and end versions of the
  // given chunk of transactions. It is expected to process the transactions, write
  // to storage if appropriate, and return the range of transactions it processed.
  abstract processTransactions({
    transactions,
    startVersion,
    endVersion,
    dataSource,
  }: {
    transactions: aptos.transaction.v1.Transaction[];
    startVersion: bigint;
    endVersion: bigint;
    dataSource: DataSource; // DB connection
  }): Promise<ProcessingResult>;
}
