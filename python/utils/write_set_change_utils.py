from aptos.transaction.v1 import transaction_pb2


def get_write_table_item(
    write_set_change: transaction_pb2.WriteSetChange,
) -> transaction_pb2.WriteTableItem | None:
    if write_set_change.type != transaction_pb2.WriteSetChange.TYPE_WRITE_TABLE_ITEM:
        return None
    return write_set_change.write_table_item
