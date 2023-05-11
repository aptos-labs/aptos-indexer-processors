import argparse
import json
import grpc

from google.cloud.bigquery_storage_v1beta2 import BigQueryWriteClient
from models.proto_autogen import nft_marketplace_activities_pb2
from db_adapters.bigquery_stream_manager import BigqueryWriteManager
from aptos.indexer.v1 import raw_data_pb2_grpc
from aptos.indexer.v1 import raw_data_pb2
from utils.config import Config
from processors.nft_orderbooks import nft_orderbooks_parser
from models import nft_marketplace_activities

parser = argparse.ArgumentParser()
parser.add_argument("-c", "--config", help="Path to config file", required=True)
args = parser.parse_args()
config = Config.from_yaml_file(args.config)
processor_config = config.processors[nft_orderbooks_parser.INDEXER_NAME]

metadata = (("x-aptos-data-authorization", config.indexer_api_key),)
options = [("grpc.max_receive_message_length", -1)]
bq_write_manager = BigqueryWriteManager(
    project_id=processor_config.project_id,
    dataset_id=processor_config.database,
    table_id=processor_config.table_name,
    pb2_descriptor=nft_marketplace_activities_pb2.NFTMarketplaceActivityRow.DESCRIPTOR,
)

starting_version = 0
if processor_config.starting_version != None:
    # Start from config's starting version if set
    starting_version = processor_config.starting_version

print(
    json.dumps(
        {
            "message": "Connected to the indexer grpc",
            "starting_version": starting_version,
        }
    )
)

# Connect to grpc
with grpc.insecure_channel(
    processor_config.indexer_endpoint, options=options
) as channel:
    stub = raw_data_pb2_grpc.RawDataStub(channel)
    current_transaction_version = starting_version

    for response in stub.GetTransactions(
        raw_data_pb2.GetTransactionsRequest(starting_version=starting_version),
        metadata=metadata,
    ):
        chain_id = response.chain_id

        if chain_id != processor_config.chain_id:
            raise Exception(
                "Chain ID mismatch. Expected chain ID is: "
                + str(processor_config.chain_id)
                + ", but received chain ID is: "
                + str(chain_id)
            )
        print(
            json.dumps(
                {
                    "message": "Response received",
                    "starting_version": response.transactions[0].version,
                }
            )
        )
        transactions_output = response
        for transaction in transactions_output.transactions:
            transaction_version = transaction.version
            if transaction_version != current_transaction_version:
                raise Exception(
                    "Transaction version mismatch. Expected transaction version is: "
                    + str(current_transaction_version)
                    + ", but received transaction version is: "
                    + str(transaction_version)
                )

            try:
                parsed_objs = nft_orderbooks_parser.parse(transaction)
            except:
                raise Exception(
                    "Parser failed on transaction version " + str(transaction_version)
                )

            if len(parsed_objs) > 0:
                bq_write_manager.batch_rows(parsed_objs)

            if (current_transaction_version % 1000) == 0:
                print(
                    json.dumps(
                        {
                            "message": "Successfully processed transaction",
                            "last_success_transaction_version": current_transaction_version,
                        }
                    )
                )

            current_transaction_version += 1
