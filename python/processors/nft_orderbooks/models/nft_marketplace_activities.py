import argparse
from google.cloud.bigquery import Client, SchemaField, Table
from processors.nft_orderbooks.parsers import nft_orderbooks_parser
from utils.config import Config

schema = [
    SchemaField(name="transaction_version", field_type="INTEGER", mode="REQUIRED"),
    SchemaField(name="event_index", field_type="INTEGER", mode="REQUIRED"),
    SchemaField(name="event_type", field_type="STRING", mode="REQUIRED"),
    SchemaField(name="standard_event_type", field_type="STRING", mode="REQUIRED"),
    SchemaField(name="creator_address", field_type="STRING"),
    SchemaField(name="collection", field_type="STRING"),
    SchemaField(name="token_name", field_type="STRING"),
    SchemaField(name="token_data_id", field_type="STRING"),
    SchemaField(name="collection_id", field_type="STRING"),
    SchemaField(name="price", field_type="NUMERIC"),
    SchemaField(name="amount", field_type="NUMERIC"),
    SchemaField(name="buyer", field_type="STRING"),
    SchemaField(name="seller", field_type="STRING"),
    SchemaField(
        name="json_data",
        field_type="JSON",
    ),
    SchemaField(name="marketplace", field_type="STRING", mode="REQUIRED"),
    SchemaField(name="contract_address", field_type="STRING", mode="REQUIRED"),
    SchemaField(name="entry_function_id_str", field_type="STRING", mode="REQUIRED"),
    SchemaField(name="transaction_timestamp", field_type="TIMESTAMP", mode="REQUIRED"),
    SchemaField(
        name="inserted_at",
        field_type="TIMESTAMP",
        default_value_expression="CURRENT_TIMESTAMP()",
        mode="REQUIRED",
    ),
]


def create_table(project_id: str, database: str, table_name: str):
    client = Client()
    table = Table(
        f"{project_id}.{database}.{table_name}",
        schema=schema,
    )
    table.description = "Table of NFT marketplace events"
    table.clustering_fields = ["token_data_id", "collection_id"]
    table = client.create_table(table)

    table_update_query = f"""
        ALTER TABLE
            `{project_id}.{database}.{table_name}`
        ADD PRIMARY KEY
            (transaction_version,
                event_index) NOT ENFORCED
            ;

        ALTER TABLE
            `{project_id}.{database}.{table_name}`
        SET
            OPTIONS( max_staleness = INTERVAL 10 MINUTE );
    """
    query_job = client.query(table_update_query)
    return query_job.result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Path to config file", required=True)
    args = parser.parse_args()
    config = Config.from_yaml_file(args.config)
    processor_config = config.processors[nft_orderbooks_parser.INDEXER_NAME]
    create_table(
        project_id=processor_config.project_id,
        database=processor_config.database,
        table_name=processor_config.table_name,
    )
