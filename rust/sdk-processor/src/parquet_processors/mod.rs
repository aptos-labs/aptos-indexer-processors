use crate::{
    config::db_config::DbConfig,
    steps::common::{
        gcs_uploader::{create_new_writer, GCSUploader},
        parquet_buffer_step::ParquetBufferStep,
    },
    utils::database::{new_db_pool, ArcDbPool},
};
use aptos_indexer_processor_sdk::utils::errors::ProcessorError;
use async_trait::async_trait;
use enum_dispatch::enum_dispatch;
use google_cloud_storage::client::{Client as GCSClient, ClientConfig as GcsClientConfig};
use parquet::schema::types::Type;
use processor::{
    db::{
        parquet::models::{
            default_models::{
                parquet_block_metadata_transactions::BlockMetadataTransaction,
                parquet_move_modules::MoveModule,
                parquet_move_resources::MoveResource,
                parquet_move_tables::{CurrentTableItem, TableItem},
                parquet_table_metadata::TableMetadata,
                parquet_transactions::Transaction as ParquetTransaction,
                parquet_write_set_changes::WriteSetChangeModel,
            },
            event_models::parquet_events::Event,
            user_transaction_models::parquet_user_transactions::UserTransaction,
            fungible_asset_models::parquet_v2_fungible_asset_activities::FungibleAssetActivity,
            parquet_v2_fungible_metadata::FungibleAssetMetadataModel,
        },
        postgres::models::ans_models::parquet_ans_lookup_v2::AnsPrimaryNameV2,
    },
    worker::TableFlags,
};
#[allow(unused_imports)]
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use strum::{Display, EnumIter};

pub mod parquet_ans_processor;
pub mod parquet_default_processor;
pub mod parquet_events_processor;
pub mod parquet_user_transaction_processor;
pub mod parquet_fungible_asset_processor;

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

// TODO: Rename this to ParquetTableEnum as this reflects the table name rather than the type name
// which is written in plural form.
pub enum ParquetTypeEnum {
    // default
    MoveResources,
    WriteSetChanges,
    Transactions,
    TableItems,
    MoveModules,
    CurrentTableItems,
    BlockMetadataTransactions,
    TableMetadata,
    // events
    Events,
    // user transactions
    UserTransactions,
    // ANS types
    AnsPrimaryNameV2,
    // fa
    FungibleAssetActivity,
    FungibleAssetMetadata,
}

/// Trait for handling various Parquet types.
#[async_trait]
#[enum_dispatch]
pub trait ParquetTypeTrait: std::fmt::Debug + Send + Sync {
    fn parquet_type(&self) -> ParquetTypeEnum;
    fn calculate_size(&self) -> usize;

    async fn upload_to_gcs(
        &self,
        uploader: &mut GCSUploader,
        parquet_type: ParquetTypeEnum,
        table_name: &str,
    ) -> anyhow::Result<()>;
}

/// Macro for implementing ParquetTypeTrait for multiple types.
macro_rules! impl_parquet_trait {
    ($type:ty, $enum_variant:expr) => {
        #[async_trait]
        impl ParquetTypeTrait for Vec<$type> {
            fn parquet_type(&self) -> ParquetTypeEnum {
                $enum_variant
            }

            fn calculate_size(&self) -> usize {
                allocative::size_of_unique(self)
            }

            async fn upload_to_gcs(
                &self,
                uploader: &mut GCSUploader,
                parquet_type: ParquetTypeEnum,
                table_name: &str,
            ) -> anyhow::Result<()> {
                uploader
                    .upload_generic(self, parquet_type, table_name)
                    .await
            }
        }
    };
}

// Apply macro to supported types
impl_parquet_trait!(MoveResource, ParquetTypeEnum::MoveResources);
impl_parquet_trait!(WriteSetChangeModel, ParquetTypeEnum::WriteSetChanges);
impl_parquet_trait!(ParquetTransaction, ParquetTypeEnum::Transactions);
impl_parquet_trait!(TableItem, ParquetTypeEnum::TableItems);
impl_parquet_trait!(MoveModule, ParquetTypeEnum::MoveModules);
impl_parquet_trait!(CurrentTableItem, ParquetTypeEnum::CurrentTableItems);
impl_parquet_trait!(
    BlockMetadataTransaction,
    ParquetTypeEnum::BlockMetadataTransactions
);
impl_parquet_trait!(TableMetadata, ParquetTypeEnum::TableMetadata);
impl_parquet_trait!(Event, ParquetTypeEnum::Events);
impl_parquet_trait!(UserTransaction, ParquetTypeEnum::UserTransactions);
impl_parquet_trait!(AnsPrimaryNameV2, ParquetTypeEnum::AnsPrimaryNameV2);
impl_parquet_trait!(
    FungibleAssetActivity,
    ParquetTypeEnum::FungibleAssetActivity
);
impl_parquet_trait!(
    FungibleAssetMetadataModel,
    ParquetTypeEnum::FungibleAssetMetadata
);

#[derive(Debug, Clone)]
#[enum_dispatch(ParquetTypeTrait)]
pub enum ParquetTypeStructs {
    MoveResource(Vec<MoveResource>),
    WriteSetChange(Vec<WriteSetChangeModel>),
    Transaction(Vec<ParquetTransaction>),
    TableItem(Vec<TableItem>),
    MoveModule(Vec<MoveModule>),
    Event(Vec<Event>),
    CurrentTableItem(Vec<CurrentTableItem>),
    BlockMetadataTransaction(Vec<BlockMetadataTransaction>),
    UserTransaction(Vec<UserTransaction>),
    TableMetadata(Vec<TableMetadata>),
    AnsPrimaryNameV2(Vec<AnsPrimaryNameV2>),
    FungibleAssetActivity(Vec<FungibleAssetActivity>),
    FungibleAssetMetadata(Vec<FungibleAssetMetadataModel>),
}

impl ParquetTypeStructs {
    pub fn default_for_type(parquet_type: &ParquetTypeEnum) -> Self {
        match parquet_type {
            ParquetTypeEnum::MoveResources => ParquetTypeStructs::MoveResource(Vec::new()),
            ParquetTypeEnum::WriteSetChanges => ParquetTypeStructs::WriteSetChange(Vec::new()),
            ParquetTypeEnum::Transactions => ParquetTypeStructs::Transaction(Vec::new()),
            ParquetTypeEnum::TableItems => ParquetTypeStructs::TableItem(Vec::new()),
            ParquetTypeEnum::MoveModules => ParquetTypeStructs::MoveModule(Vec::new()),
            ParquetTypeEnum::CurrentTableItems => ParquetTypeStructs::CurrentTableItem(Vec::new()),
            ParquetTypeEnum::BlockMetadataTransactions => {
                ParquetTypeStructs::BlockMetadataTransaction(Vec::new())
            },
            ParquetTypeEnum::TableMetadata => ParquetTypeStructs::TableMetadata(Vec::new()),
            ParquetTypeEnum::Events => ParquetTypeStructs::Event(Vec::new()),
            ParquetTypeEnum::UserTransactions => ParquetTypeStructs::UserTransaction(Vec::new()),
            ParquetTypeEnum::AnsPrimaryNameV2 => ParquetTypeStructs::AnsPrimaryNameV2(Vec::new()),
            ParquetTypeEnum::FungibleAssetActivity => {
                ParquetTypeStructs::FungibleAssetActivity(Vec::new())
            },
            ParquetTypeEnum::FungibleAssetMetadata => {
                ParquetTypeStructs::FungibleAssetMetadata(Vec::new())
            },
        }
    }

    /// Appends data to the current buffer within each ParquetTypeStructs variant.
    pub fn append(&mut self, other: ParquetTypeStructs) -> Result<(), ProcessorError> {
        macro_rules! handle_append {
            ($self_data:expr, $other_data:expr) => {{
                $self_data.extend($other_data);
                Ok(())
            }};
        }

        match (self, other) {
            (
                ParquetTypeStructs::MoveResource(self_data),
                ParquetTypeStructs::MoveResource(other_data),
            ) => {
                handle_append!(self_data, other_data)
            },
            (
                ParquetTypeStructs::WriteSetChange(self_data),
                ParquetTypeStructs::WriteSetChange(other_data),
            ) => {
                handle_append!(self_data, other_data)
            },
            (
                ParquetTypeStructs::Transaction(self_data),
                ParquetTypeStructs::Transaction(other_data),
            ) => {
                handle_append!(self_data, other_data)
            },
            (
                ParquetTypeStructs::TableItem(self_data),
                ParquetTypeStructs::TableItem(other_data),
            ) => {
                handle_append!(self_data, other_data)
            },
            (
                ParquetTypeStructs::MoveModule(self_data),
                ParquetTypeStructs::MoveModule(other_data),
            ) => {
                handle_append!(self_data, other_data)
            },
            (ParquetTypeStructs::Event(self_data), ParquetTypeStructs::Event(other_data)) => {
                handle_append!(self_data, other_data)
            },
            (
                ParquetTypeStructs::CurrentTableItem(self_data),
                ParquetTypeStructs::CurrentTableItem(other_data),
            ) => {
                handle_append!(self_data, other_data)
            },
            (
                ParquetTypeStructs::BlockMetadataTransaction(self_data),
                ParquetTypeStructs::BlockMetadataTransaction(other_data),
            ) => {
                handle_append!(self_data, other_data)
            },
            (
                ParquetTypeStructs::TableMetadata(self_data),
                ParquetTypeStructs::TableMetadata(other_data),
            ) => {
                handle_append!(self_data, other_data)
            },
            (
                ParquetTypeStructs::UserTransaction(self_data),
                ParquetTypeStructs::UserTransaction(other_data),
            ) => {
                handle_append!(self_data, other_data)
            },
            (
                ParquetTypeStructs::AnsPrimaryNameV2(self_data),
                ParquetTypeStructs::AnsPrimaryNameV2(other_data),
            ) => {
                handle_append!(self_data, other_data)
            },
            (
                ParquetTypeStructs::FungibleAssetActivity(self_data),
                ParquetTypeStructs::FungibleAssetActivity(other_data),
            ) => {
                handle_append!(self_data, other_data)
            },
            (
                ParquetTypeStructs::FungibleAssetMetadata(self_data),
                ParquetTypeStructs::FungibleAssetMetadata(other_data),
            ) => {
                handle_append!(self_data, other_data)
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

/// Initializes the database connection pool.
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
                    "Failed to create connection pool for ParquetConfig: {:?}",
                    e
                )
            })?;

            Ok(conn_pool)
        },
        _ => Err(anyhow::anyhow!("Invalid db config for Parquet Processor")),
    }
}

/// Initializes the Parquet buffer step.
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

/// Sets the backfill table flag.
fn set_backfill_table_flag(table_names: HashSet<String>) -> TableFlags {
    let mut backfill_table = TableFlags::empty();

    for table_name in table_names.iter() {
        if let Some(flag) = TableFlags::from_name(table_name) {
            println!("Setting backfill table flag: {:?}", flag);
            backfill_table |= flag;
        }
    }
    backfill_table
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parquet_type_enum_matches_trait() {
        let types = vec![
            ParquetTypeEnum::MoveResources,
            ParquetTypeEnum::WriteSetChanges,
            ParquetTypeEnum::Transactions,
            ParquetTypeEnum::TableItems,
            ParquetTypeEnum::MoveModules,
            ParquetTypeEnum::CurrentTableItems,
            ParquetTypeEnum::AnsPrimaryNameV2,
        ];

        for t in types {
            // Use a type corresponding to the ParquetTypeEnum
            let default = ParquetTypeStructs::default_for_type(&t);

            assert_eq!(default.parquet_type(), t);
        }
    }
}
