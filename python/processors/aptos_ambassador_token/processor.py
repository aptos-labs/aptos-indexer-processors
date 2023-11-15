import argparse
import grpc
import ast
import json

from aptos_protos.aptos.indexer.v1 import raw_data_pb2_grpc
from aptos_protos.aptos.indexer.v1 import raw_data_pb2
from aptos_protos.aptos.transaction.v1 import transaction_pb2
from utils.processor_name import ProcessorName
from utils.config import Config
from utils.general_utils import standardize_address
from utils.transactions_processor import TransactionsProcessor, ProcessingResult
from utils.models.schema_names import EXAMPLE
from time import perf_counter

qualified_module_name = (
    "0x9bfdd4efe15f4d8aa145bef5f64588c7c391bcddaf34f9e977f59bd93b498f2a::ambassador"
)
qualified_event_name = qualified_module_name + "::LevelUpdateEvent"
qualified_resource_name = qualified_module_name + "::AmbassadorLevel"


class AptosAmbassadorTokenProcessor(TransactionsProcessor):
    def name(self) -> str:
        return ProcessorName.EXAMPLE_AMBASSADOR_TOKEN_PROCESSOR.value

    def schema(self) -> str:
        return EXAMPLE

    def process_transactions(
        self,
        transactions: list[transaction_pb2.Transaction],
        start_version: int,
        end_version: int,
    ) -> ProcessingResult:
        start_time = perf_counter()
        for transaction in transactions:
            found = False
            if transaction.type == transaction_pb2.Transaction.TRANSACTION_TYPE_USER:
                for event in transaction.user.events:
                    addr = standardize_address(event.key.account_address)
                    type = event.type_str
                    data = event.data
                    if type == qualified_event_name:
                        found = True
                        print("-------------")
                        print("Event emitted")
                        print("-------------")
                        print("ambassador token addr:", addr)
                        data_dic = ast.literal_eval(data)
                        print("old_level:", data_dic["old_level"])
                        print("new_level:", data_dic["new_level"])
                        print()

                for change in transaction.info.changes:
                    if (
                        change.type
                        == transaction_pb2.WriteSetChange.TYPE_WRITE_RESOURCE
                    ):
                        addr = standardize_address(change.write_resource.address)
                        type = change.write_resource.type_str
                        data = change.write_resource.data
                        if type == qualified_resource_name:
                            found = True
                            print("-------------")
                            print("State changed")
                            print("-------------")
                            print("ambassador token addr:", addr)
                            data_dic = ast.literal_eval(data)
                            print("ambassador_level:", data_dic["ambassador_level"])
                            print()

                if found:
                    print(
                        "Found from the transaction with:",
                        json.dumps(
                            {
                                "version": transaction.version,
                                "epoch": transaction.epoch,
                                "block_height": transaction.block_height,
                            }
                        ),
                    )
                    print("\n\n")
        duration_in_secs = perf_counter() - start_time
        return ProcessingResult(
            start_version=start_version,
            end_version=end_version,
            processing_duration_in_secs=duration_in_secs,
            # No db insertion.
            db_insertion_duration_in_secs=0,
        )
