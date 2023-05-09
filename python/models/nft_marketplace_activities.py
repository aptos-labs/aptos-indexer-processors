from google.cloud.bigquery import Client, SchemaField, Table

project_id = "rtso-playground"
database = "custom_processor"
table_name = "nft_marketplace_activities"

table_ref = "{project_id}.{database}.{table_name}"
stream_id = (
    "projects/{project_id}/datasets/{dataset}/tables/{table_name}/streams/_default"
)

schema = [
    SchemaField(name="transaction_version", field_type="INTEGER", mode="REQUIRED"),
    SchemaField(name="event_index", field_type="INTEGER", mode="REQUIRED"),
    SchemaField(name="event_type", field_type="STRING"),
    SchemaField(name="standard_event_type", field_type="STRING"),
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
    SchemaField(name="marketplace", field_type="STRING"),
    SchemaField(name="contract_address", field_type="STRING"),
    SchemaField(name="entry_function_id_str", field_type="STRING"),
    SchemaField(name="transaction_timestamp", field_type="TIMESTAMP"),
    SchemaField(
        name="inserted_at",
        field_type="TIMESTAMP",
        default_value_expression="CURRENT_TIMESTAMP",
    ),
]


def create_table():
    client = Client()
    table = Table(
        table_ref,
        schema=schema,
    )
    table.description = "Table of NFT marketplace events"
    table.clustering_fields = ["token_data_id", "collection_id"]
    table = client.create_table(table)

    table_update_query = """
        ALTER TABLE `{project_id}.{dataset}.{table_name}`
        ADD PRIMARY KEY (transaction_version, event_index) NOT ENFORCED;
        SET OPTIONS (
            max_staleness = INTERVAL 10 MINUTE
        );
    """
    query_job = client.query(table_update_query)
    return query_job.result

create_table()
