from google.cloud.bigquery_storage_v1beta2 import BigQueryWriteClient
from models.proto_autogen import nft_marketplace_activities_pb2
from db_adapters.bigquery_stream_manager import BigqueryWriteManager

project_id = "rtso-playground"
dataset = "custom_processor"
table = "nft_marketplace_activities"
write_stream = (
    "projects/{project_id}/datasets/{dataset}/tables/{table}/streams/_default"
)


def sample_append_rows():
    # Create a write manager
    write_manager = BigqueryWriteManager(
        project_id=project_id,
        dataset_id=dataset,
        table_id=table,
        pb2_descriptor=nft_marketplace_activities_pb2.NFTMarketplaceActivityRow.DESCRIPTOR,
    )

    sample_data = []
    row = nft_marketplace_activities_pb2.NFTMarketplaceActivityRow(
        transaction_version=1,
        event_index=0,
        event_type="test",
        standard_event_type="test",
        creator_address="test",
        collection="test",
        token_name="test",
        token_data_id="test",
        collection_id="test",
        price=1,
        amount=1,
        buyer="test",
        seller="test",
        json_data='{"test": "test"}',
        marketplace="test",
        contract_address="test",
        entry_function_id_str="test",
        transaction_timestamp=123,
        inserted_at=123,
        _CHANGE_TYPE="DELETE",
    )
    sample_data.append(row)
    write_manager.write_rows(sample_data)
