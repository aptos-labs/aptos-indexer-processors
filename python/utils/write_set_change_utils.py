from aptos_protos.aptos.transaction.v1 import transaction_pb2


def get_write_table_item(
    write_set_change: transaction_pb2.WriteSetChange,
) -> transaction_pb2.WriteTableItem | None:
    if write_set_change.type != transaction_pb2.WriteSetChange.TYPE_WRITE_TABLE_ITEM:
        return None
    return write_set_change.write_table_item


def get_write_resource(
    write_set_change: transaction_pb2.WriteSetChange,
) -> transaction_pb2.WriteResource | None:
    if write_set_change.type != transaction_pb2.WriteSetChange.TYPE_WRITE_RESOURCE:
        return None
    return write_set_change.write_resource


def get_delete_resource(
    write_set_change: transaction_pb2.WriteSetChange,
) -> transaction_pb2.DeleteResource | None:
    if write_set_change.type != transaction_pb2.WriteSetChange.TYPE_DELETE_RESOURCE:
        return None
    return write_set_change.delete_resource


def get_move_type_short(
    move_struct: transaction_pb2.MoveStructTag,
) -> str:
    return f"{move_struct.module}::{move_struct.name}"
