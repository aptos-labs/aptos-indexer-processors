import {
  DeleteModule,
  Transaction,
  WriteSetChange,
} from "./aptos/transaction/testing1/v1/transaction_pb";

export function parse(transaction: Transaction): void {
  // Custom filtering: Filter out all transactions that are not User Transactions
  if (
    transaction.getType() != Transaction.TransactionType.TRANSACTION_TYPE_USER
  ) {
    return;
  }

  // Parse Transaction object
  const transactionVersion = transaction.getVersion();
  const transactionBlockHeight = transaction.getBlockHeight();
  const transactionInfo = transaction.getInfo();

  if (transactionInfo == null) {
    return;
  }

  const writeSetChanges = transactionInfo.getChangesList();
  const parsedWriteSetChanges = [];

  // Parse WriteSetChanges
  writeSetChanges.forEach(function (writeSetChange, index) {
    const type = writeSetChange.getType();
    let address = "";
    let stateKeyHash = "";

    switch (type) {
      case WriteSetChange.Type.TYPE_DELETE_MODULE:
      case WriteSetChange.Type.TYPE_DELETE_RESOURCE:
      case WriteSetChange.Type.TYPE_WRITE_MODULE:
      case WriteSetChange.Type.TYPE_WRITE_RESOURCE:
        const change =
          writeSetChange.getDeleteModule() ||
          writeSetChange.getDeleteResource() ||
          writeSetChange.getWriteModule() ||
          writeSetChange.getWriteResource();

        if (change == null) {
          return;
        }

        address = change.getAddress();
        stateKeyHash = change.getStateKeyHash_asB64();
        break;
      case WriteSetChange.Type.TYPE_DELETE_TABLE_ITEM:
      case WriteSetChange.Type.TYPE_WRITE_TABLE_ITEM:
        const tableItemChange =
          writeSetChange.getDeleteTableItem() ||
          writeSetChange.getWriteTableItem();

        if (tableItemChange == null) {
          return;
        }
        stateKeyHash = tableItemChange.getStateKeyHash_asB64();
        break;
      case WriteSetChange.Type.TYPE_UNSPECIFIED:
        return;
    }
    console.log(
      transactionVersion,
      transactionBlockHeight,
      type,
      address,
      stateKeyHash,
      index
    );
  });
}
