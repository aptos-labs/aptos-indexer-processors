import argparse
import grpc
import ast
import json

from aptos.indexer.v1 import raw_data_pb2_grpc
from aptos.indexer.v1 import raw_data_pb2
from aptos.transaction.v1 import transaction_pb2
from utils.config import Config
from utils.general_utils import standardize_address

parser = argparse.ArgumentParser()
parser.add_argument(
    "-c", "--config", help="Path to config file", required=True)
args = parser.parse_args()
config = Config.from_yaml_file(args.config)
starting_version = 0
if config.starting_version_default != None:
    starting_version = config.starting_version_default
if config.starting_version_backfill != None:
    starting_version = config.starting_version_backfill
metadata = (
    ("x-aptos-data-authorization", config.grpc_data_stream_api_key),
    ("x-aptos-request-name", "ambassador token indexer"),
)
options = [("grpc.max_receive_message_length", -1)]

qualified_module_name = "0x9bfdd4efe15f4d8aa145bef5f64588c7c391bcddaf34f9e977f59bd93b498f2a::ambassador"
qualified_event_name = qualified_module_name + "::LevelUpdateEvent"
qualified_resource_name = qualified_module_name + "::AmbassadorLevel"

with grpc.insecure_channel(config.grpc_data_stream_endpoint, options=options) as channel:
    stub = raw_data_pb2_grpc.RawDataStub(channel)
    current_transaction_version = starting_version

    for response in stub.GetTransactions(
        raw_data_pb2.GetTransactionsRequest(starting_version=starting_version),
        metadata=metadata,
    ):
        for transaction in response.transactions:
            assert transaction.version == current_transaction_version

            found = False
            if transaction.type == transaction_pb2.Transaction.TRANSACTION_TYPE_USER:
                for event in transaction.user.events:
                    addr = standardize_address(event.key.account_address)
                    type = event.type_str
                    data = event.data
                    if type == qualified_event_name:
                        found = True
                        print('-------------')
                        print('Event emitted')
                        print('-------------')
                        print('ambassador token addr:', addr)
                        data_dic = ast.literal_eval(data)
                        print('old_level:', data_dic["old_level"])
                        print('new_level:', data_dic["new_level"])
                        print()

                for change in transaction.info.changes:
                    if change.type == transaction_pb2.WriteSetChange.TYPE_WRITE_RESOURCE:
                        addr = standardize_address(
                            change.write_resource.address)
                        type = change.write_resource.type_str
                        data = change.write_resource.data
                        if type == qualified_resource_name:
                            found = True
                            print('-------------')
                            print('State changed')
                            print('-------------')
                            print('ambassador token addr:', addr)
                            data_dic = ast.literal_eval(data)
                            print('ambassador_level:',
                                  data_dic["ambassador_level"])
                            print()

                if found:
                    print('Found from the transaction with:', json.dumps(
                        {
                            "version": transaction.version,
                            "epoch": transaction.epoch,
                            "block_height": transaction.block_height,
                        }
                    ))
                    print('\n\n')

            current_transaction_version += 1
