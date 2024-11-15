use crate::{
    config::db_config::DbConfig,
    steps::common::{
        gcs_uploader::{create_new_writer, GCSUploader},
        parquet_buffer_step::ParquetBufferStep,
    },
    utils::database::{new_db_pool, ArcDbPool},
};
use aptos_indexer_processor_sdk::utils::errors::ProcessorError;
use google_cloud_storage::client::{Client as GCSClient, ClientConfig as GcsClientConfig};
use parquet::schema::types::Type;
use processor::{
    db::parquet::models::default_models::{
        parquet_move_modules::MoveModule, parquet_move_resources::MoveResource,
        parquet_move_tables::TableItem, parquet_transactions::Transaction as ParquetTransaction,
        parquet_write_set_changes::WriteSetChangeModel,
    },
    worker::TableFlags,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc, time::Duration};
use strum::{Display, EnumIter};

pub mod parquet_default_processor;

const GOOGLE_APPLICATION_CREDENTIALS: &str = "GOOGLE_APPLICATION_CREDENTIALS";

/// Enum representing the different types of Parquet files that can be processed.
#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, Display, EnumIter)]
#[strum(serialize_all = "snake_case")]
#[cfg_attr(
    test,
    derive(strum::EnumDiscriminants),
    strum_discriminants(
        derive(
            strum::EnumVariantNames,
            Deserialize,
            Serialize,
            strum::IntoStaticStr,
            strum::Display,
            clap::ValueEnum
        ),
        name(ParquetTypeName),
        strum(serialize_all = "snake_case")
    )
)]
pub enum ParquetTypeEnum {
    MoveResource,
    WriteSetChange,
    Transaction,
    TableItem,
    MoveModule,
}

#[derive(Clone, Debug, strum::EnumDiscriminants)]
#[strum(serialize_all = "snake_case")]
#[strum_discriminants(
    derive(
        Deserialize,
        Serialize,
        strum::EnumVariantNames,
        strum::IntoStaticStr,
        strum::Display,
        clap::ValueEnum
    ),
    name(ParquetTypeStructName),
    clap(rename_all = "snake_case"),
    serde(rename_all = "snake_case"),
    strum(serialize_all = "snake_case")
)]
pub enum ParquetTypeStructs {
    MoveResource(Vec<MoveResource>),
    WriteSetChange(Vec<WriteSetChangeModel>),
    Transaction(Vec<ParquetTransaction>),
    TableItem(Vec<TableItem>),
    MoveModule(Vec<MoveModule>),
}

impl ParquetTypeStructs {
    pub fn default_for_type(parquet_type: &ParquetTypeEnum) -> Self {
        match parquet_type {
            ParquetTypeEnum::MoveResource => ParquetTypeStructs::MoveResource(Vec::new()),
            ParquetTypeEnum::WriteSetChange => ParquetTypeStructs::WriteSetChange(Vec::new()),
            ParquetTypeEnum::Transaction => ParquetTypeStructs::Transaction(Vec::new()),
            ParquetTypeEnum::TableItem => ParquetTypeStructs::TableItem(Vec::new()),
            ParquetTypeEnum::MoveModule => ParquetTypeStructs::MoveModule(Vec::new()),
        }
    }

    pub fn get_table_name(&self) -> &'static str {
        match self {
            ParquetTypeStructs::MoveResource(_) => "move_resources",
            ParquetTypeStructs::WriteSetChange(_) => "write_set_changes",
            ParquetTypeStructs::Transaction(_) => "transactions",
            ParquetTypeStructs::TableItem(_) => "table_items",
            ParquetTypeStructs::MoveModule(_) => "move_modules",
        }
    }

    pub fn calculate_size(&self) -> usize {
        match self {
            ParquetTypeStructs::MoveResource(data) => allocative::size_of_unique(data),
            ParquetTypeStructs::WriteSetChange(data) => allocative::size_of_unique(data),
            ParquetTypeStructs::Transaction(data) => allocative::size_of_unique(data),
            ParquetTypeStructs::TableItem(data) => allocative::size_of_unique(data),
            ParquetTypeStructs::MoveModule(data) => allocative::size_of_unique(data),
        }
    }

    /// Appends data to the current buffer within each ParquetTypeStructs variant.
    pub fn append(&mut self, other: ParquetTypeStructs) -> Result<(), ProcessorError> {
        match (self, other) {
            (ParquetTypeStructs::MoveResource(buf), ParquetTypeStructs::MoveResource(mut data)) => {
                buf.append(&mut data);
                Ok(())
            },
            (
                ParquetTypeStructs::WriteSetChange(buf),
                ParquetTypeStructs::WriteSetChange(mut data),
            ) => {
                buf.append(&mut data);
                Ok(())
            },
            (ParquetTypeStructs::Transaction(buf), ParquetTypeStructs::Transaction(mut data)) => {
                buf.append(&mut data);
                Ok(())
            },
            (ParquetTypeStructs::TableItem(buf), ParquetTypeStructs::TableItem(mut data)) => {
                buf.append(&mut data);
                Ok(())
            },
            (ParquetTypeStructs::MoveModule(buf), ParquetTypeStructs::MoveModule(mut data)) => {
                buf.append(&mut data);
                Ok(())
            },
            _ => Err(ProcessorError::ProcessError {
                message: "Mismatched buffer types in append operation".to_string(),
            }),
        }
    }
}

async fn initialize_gcs_client(credentials: Option<String>) -> Arc<GCSClient> {
    if let Some(credentials) = credentials {
        std::env::set_var(GOOGLE_APPLICATION_CREDENTIALS, credentials);
    }

    let gcs_config = GcsClientConfig::default()
        .with_auth()
        .await
        .expect("Failed to create GCS client config");

    Arc::new(GCSClient::new(gcs_config))
}

async fn initialize_database_pool(config: &DbConfig) -> anyhow::Result<ArcDbPool> {
    match config {
        DbConfig::ParquetConfig(ref parquet_config) => {
            let conn_pool = new_db_pool(
                &parquet_config.connection_string,
                Some(parquet_config.db_pool_size),
            )
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to create connection pool for PostgresConfig: {:?}",
                    e
                )
            })?;

            Ok(conn_pool)
        },
        _ => Err(anyhow::anyhow!("Invalid db config for Parquet Processor")),
    }
}

async fn initialize_parquet_buffer_step(
    gcs_client: Arc<GCSClient>,
    parquet_type_to_schemas: HashMap<ParquetTypeEnum, Arc<Type>>,
    upload_interval: u64,
    max_buffer_size: usize,
    bucket_name: String,
    bucket_root: String,
    processor_name: String,
) -> anyhow::Result<ParquetBufferStep> {
    let parquet_type_to_writer = parquet_type_to_schemas
        .iter()
        .map(|(key, schema)| {
            let writer = create_new_writer(schema.clone()).expect("Failed to create writer");
            (*key, writer)
        })
        .collect();

    let buffer_uploader = GCSUploader::new(
        gcs_client,
        parquet_type_to_schemas,
        parquet_type_to_writer,
        bucket_name,
        bucket_root,
        processor_name,
    )?;

    let default_size_buffer_step = ParquetBufferStep::new(
        Duration::from_secs(upload_interval),
        buffer_uploader,
        max_buffer_size,
    );

    Ok(default_size_buffer_step)
}

fn set_backfill_table_flag(table_names: Vec<String>) -> TableFlags {
    let mut backfill_table = TableFlags::empty();

    for table_name in table_names {
        if let Some(flag) = TableFlags::from_name(&table_name) {
            backfill_table |= flag;
        }
    }
    backfill_table
}

#[cfg(test)]
mod test {
    use super::*;
    use strum::VariantNames;

    /// This test exists to make sure that when a new processor is added, it is added
    /// to both Processor and ProcessorConfig.
    ///
    /// To make sure this passes, make sure the variants are in the same order
    /// (lexicographical) and the names match.
    #[test]
    fn test_parquet_type_names_complete() {
        assert_eq!(ParquetTypeStructName::VARIANTS, ParquetTypeName::VARIANTS);
    }
}
