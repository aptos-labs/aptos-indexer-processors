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
    db::parquet::models::{
        account_transaction_models::parquet_account_transactions::AccountTransaction,
        ans_models::{
            ans_lookup_v2::{AnsLookupV2, CurrentAnsLookupV2},
            ans_primary_name_v2::{AnsPrimaryNameV2, CurrentAnsPrimaryNameV2},
        },
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
        fungible_asset_models::{
            parquet_v2_fungible_asset_activities::FungibleAssetActivity,
            parquet_v2_fungible_asset_balances::{
                CurrentFungibleAssetBalance, CurrentUnifiedFungibleAssetBalance,
                FungibleAssetBalance,
            },
            parquet_v2_fungible_metadata::FungibleAssetMetadataModel,
        },
        object_models::v2_objects::{CurrentObject, Object},
        token_v2_models::{
            token_claims::CurrentTokenPendingClaim,
            v1_token_royalty::CurrentTokenRoyaltyV1,
            v2_token_activities::TokenActivityV2,
            v2_token_datas::{CurrentTokenDataV2, TokenDataV2},
            v2_token_metadata::CurrentTokenV2Metadata,
            v2_token_ownerships::{CurrentTokenOwnershipV2, TokenOwnershipV2},
        },
        transaction_metadata_model::parquet_write_set_size_info::WriteSetSize,
        user_transaction_models::parquet_user_transactions::UserTransaction,
    },
    utils::table_flags::TableFlags,
};
#[allow(unused_imports)]
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use strum::{Display, EnumIter};

pub mod parquet_account_transactions_processor;
pub mod parquet_ans_processor;
pub mod parquet_default_processor;
pub mod parquet_events_processor;
pub mod parquet_fungible_asset_processor;
pub mod parquet_objects_processor;
pub mod parquet_token_v2_processor;
pub mod parquet_transaction_metadata_processor;
pub mod parquet_user_transaction_processor;

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
    CurrentAnsPrimaryNameV2,
    AnsLookupV2,
    CurrentAnsLookupV2,
    // fa
    FungibleAssetActivities,
    FungibleAssetMetadata,
    FungibleAssetBalances,
    CurrentFungibleAssetBalances,
    CurrentFungibleAssetBalancesLegacy,
    // txn metadata,
    WriteSetSize,
    // account transactions
    AccountTransactions,
    // token v2
    CurrentTokenPendingClaims,
    CurrentTokenRoyaltiesV1,
    CurrentTokenV2Metadata,
    TokenActivitiesV2,
    TokenDatasV2,
    CurrentTokenDatasV2,
    TokenOwnershipsV2,
    CurrentTokenOwnershipsV2,
    // Objects
    Objects,
    CurrentObjects,
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
    ParquetTypeEnum::FungibleAssetActivities
);
impl_parquet_trait!(
    FungibleAssetMetadataModel,
    ParquetTypeEnum::FungibleAssetMetadata
);
impl_parquet_trait!(FungibleAssetBalance, ParquetTypeEnum::FungibleAssetBalances);
impl_parquet_trait!(
    CurrentUnifiedFungibleAssetBalance,
    ParquetTypeEnum::CurrentFungibleAssetBalances
);
impl_parquet_trait!(
    CurrentFungibleAssetBalance,
    ParquetTypeEnum::CurrentFungibleAssetBalancesLegacy
);
impl_parquet_trait!(WriteSetSize, ParquetTypeEnum::WriteSetSize);
impl_parquet_trait!(AccountTransaction, ParquetTypeEnum::AccountTransactions);
impl_parquet_trait!(
    CurrentTokenPendingClaim,
    ParquetTypeEnum::CurrentTokenPendingClaims
);
impl_parquet_trait!(
    CurrentTokenRoyaltyV1,
    ParquetTypeEnum::CurrentTokenRoyaltiesV1
);
impl_parquet_trait!(
    CurrentTokenV2Metadata,
    ParquetTypeEnum::CurrentTokenV2Metadata
);
impl_parquet_trait!(TokenActivityV2, ParquetTypeEnum::TokenActivitiesV2);
impl_parquet_trait!(
    CurrentAnsPrimaryNameV2,
    ParquetTypeEnum::CurrentAnsPrimaryNameV2
);
impl_parquet_trait!(AnsLookupV2, ParquetTypeEnum::AnsLookupV2);
impl_parquet_trait!(CurrentAnsLookupV2, ParquetTypeEnum::CurrentAnsLookupV2);
impl_parquet_trait!(TokenDataV2, ParquetTypeEnum::TokenDatasV2);
impl_parquet_trait!(CurrentTokenDataV2, ParquetTypeEnum::CurrentTokenDatasV2);
impl_parquet_trait!(TokenOwnershipV2, ParquetTypeEnum::TokenOwnershipsV2);
impl_parquet_trait!(
    CurrentTokenOwnershipV2,
    ParquetTypeEnum::CurrentTokenOwnershipsV2
);
impl_parquet_trait!(Object, ParquetTypeEnum::Objects);
impl_parquet_trait!(CurrentObject, ParquetTypeEnum::CurrentObjects);
#[derive(Debug, Clone)]
#[enum_dispatch(ParquetTypeTrait)]
pub enum ParquetTypeStructs {
    // Default
    MoveResource(Vec<MoveResource>),
    WriteSetChange(Vec<WriteSetChangeModel>),
    Transaction(Vec<ParquetTransaction>),
    TableItem(Vec<TableItem>),
    MoveModule(Vec<MoveModule>),
    CurrentTableItem(Vec<CurrentTableItem>),
    BlockMetadataTransaction(Vec<BlockMetadataTransaction>),
    TableMetadata(Vec<TableMetadata>),
    // Events
    Event(Vec<Event>),
    // User txn
    UserTransaction(Vec<UserTransaction>),
    // ANS types
    AnsPrimaryNameV2(Vec<AnsPrimaryNameV2>),
    CurrentAnsPrimaryNameV2(Vec<CurrentAnsPrimaryNameV2>),
    AnsLookupV2(Vec<AnsLookupV2>),
    CurrentAnsLookupV2(Vec<CurrentAnsLookupV2>),
    // FA
    FungibleAssetActivity(Vec<FungibleAssetActivity>),
    FungibleAssetMetadata(Vec<FungibleAssetMetadataModel>),
    FungibleAssetBalance(Vec<FungibleAssetBalance>),
    CurrentFungibleAssetBalance(Vec<CurrentFungibleAssetBalance>),
    CurrentUnifiedFungibleAssetBalance(Vec<CurrentUnifiedFungibleAssetBalance>),
    // Txn metadata
    WriteSetSize(Vec<WriteSetSize>),
    // account txn
    AccountTransaction(Vec<AccountTransaction>),
    // Token V2
    CurrentTokenPendingClaim(Vec<CurrentTokenPendingClaim>),
    CurrentTokenRoyaltyV1(Vec<CurrentTokenRoyaltyV1>),
    CurrentTokenV2Metadata(Vec<CurrentTokenV2Metadata>),
    TokenActivityV2(Vec<TokenActivityV2>),
    TokenDataV2(Vec<TokenDataV2>),
    CurrentTokenDataV2(Vec<CurrentTokenDataV2>),
    TokenOwnershipV2(Vec<TokenOwnershipV2>),
    CurrentTokenOwnershipV2(Vec<CurrentTokenOwnershipV2>),
    Object(Vec<Object>),
    CurrentObject(Vec<CurrentObject>),
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
            ParquetTypeEnum::FungibleAssetActivities => {
                ParquetTypeStructs::FungibleAssetActivity(Vec::new())
            },
            ParquetTypeEnum::FungibleAssetMetadata => {
                ParquetTypeStructs::FungibleAssetMetadata(Vec::new())
            },
            ParquetTypeEnum::FungibleAssetBalances => {
                ParquetTypeStructs::FungibleAssetBalance(Vec::new())
            },
            ParquetTypeEnum::CurrentFungibleAssetBalancesLegacy => {
                ParquetTypeStructs::CurrentFungibleAssetBalance(Vec::new())
            },
            ParquetTypeEnum::CurrentFungibleAssetBalances => {
                ParquetTypeStructs::CurrentUnifiedFungibleAssetBalance(Vec::new())
            },
            ParquetTypeEnum::WriteSetSize => ParquetTypeStructs::WriteSetSize(Vec::new()),
            ParquetTypeEnum::AccountTransactions => {
                ParquetTypeStructs::AccountTransaction(Vec::new())
            },
            ParquetTypeEnum::CurrentTokenPendingClaims => {
                ParquetTypeStructs::CurrentTokenPendingClaim(Vec::new())
            },
            ParquetTypeEnum::CurrentTokenRoyaltiesV1 => {
                ParquetTypeStructs::CurrentTokenRoyaltyV1(Vec::new())
            },
            ParquetTypeEnum::CurrentTokenV2Metadata => {
                ParquetTypeStructs::CurrentTokenV2Metadata(Vec::new())
            },
            ParquetTypeEnum::TokenActivitiesV2 => ParquetTypeStructs::TokenActivityV2(Vec::new()),
            ParquetTypeEnum::CurrentAnsPrimaryNameV2 => {
                ParquetTypeStructs::CurrentAnsPrimaryNameV2(Vec::new())
            },
            ParquetTypeEnum::AnsLookupV2 => ParquetTypeStructs::AnsLookupV2(Vec::new()),
            ParquetTypeEnum::CurrentAnsLookupV2 => {
                ParquetTypeStructs::CurrentAnsLookupV2(Vec::new())
            },
            ParquetTypeEnum::TokenDatasV2 => ParquetTypeStructs::TokenDataV2(Vec::new()),
            ParquetTypeEnum::CurrentTokenDatasV2 => {
                ParquetTypeStructs::CurrentTokenDataV2(Vec::new())
            },
            ParquetTypeEnum::TokenOwnershipsV2 => ParquetTypeStructs::TokenOwnershipV2(Vec::new()),
            ParquetTypeEnum::CurrentTokenOwnershipsV2 => {
                ParquetTypeStructs::CurrentTokenOwnershipV2(Vec::new())
            },
            ParquetTypeEnum::Objects => ParquetTypeStructs::Object(Vec::new()),
            ParquetTypeEnum::CurrentObjects => ParquetTypeStructs::CurrentObject(Vec::new()),
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
            (
                ParquetTypeStructs::FungibleAssetBalance(self_data),
                ParquetTypeStructs::FungibleAssetBalance(other_data),
            ) => {
                handle_append!(self_data, other_data)
            },
            (
                ParquetTypeStructs::CurrentFungibleAssetBalance(self_data),
                ParquetTypeStructs::CurrentFungibleAssetBalance(other_data),
            ) => {
                handle_append!(self_data, other_data)
            },
            (
                ParquetTypeStructs::CurrentUnifiedFungibleAssetBalance(self_data),
                ParquetTypeStructs::CurrentUnifiedFungibleAssetBalance(other_data),
            ) => {
                handle_append!(self_data, other_data)
            },
            (
                ParquetTypeStructs::WriteSetSize(self_data),
                ParquetTypeStructs::WriteSetSize(other_data),
            ) => {
                handle_append!(self_data, other_data)
            },
            (
                ParquetTypeStructs::AccountTransaction(self_data),
                ParquetTypeStructs::AccountTransaction(other_data),
            ) => {
                handle_append!(self_data, other_data)
            },
            (
                ParquetTypeStructs::CurrentTokenPendingClaim(self_data),
                ParquetTypeStructs::CurrentTokenPendingClaim(other_data),
            ) => {
                handle_append!(self_data, other_data)
            },
            (
                ParquetTypeStructs::CurrentTokenRoyaltyV1(self_data),
                ParquetTypeStructs::CurrentTokenRoyaltyV1(other_data),
            ) => {
                handle_append!(self_data, other_data)
            },
            (
                ParquetTypeStructs::CurrentTokenV2Metadata(self_data),
                ParquetTypeStructs::CurrentTokenV2Metadata(other_data),
            ) => {
                handle_append!(self_data, other_data)
            },
            (
                ParquetTypeStructs::TokenActivityV2(self_data),
                ParquetTypeStructs::TokenActivityV2(other_data),
            ) => {
                handle_append!(self_data, other_data)
            },
            // ANS types
            (
                ParquetTypeStructs::AnsPrimaryNameV2(self_data),
                ParquetTypeStructs::AnsPrimaryNameV2(other_data),
            ) => {
                handle_append!(self_data, other_data)
            },
            (
                ParquetTypeStructs::CurrentAnsPrimaryNameV2(self_data),
                ParquetTypeStructs::CurrentAnsPrimaryNameV2(other_data),
            ) => {
                handle_append!(self_data, other_data)
            },
            (
                ParquetTypeStructs::AnsLookupV2(self_data),
                ParquetTypeStructs::AnsLookupV2(other_data),
            ) => {
                handle_append!(self_data, other_data)
            },
            (
                ParquetTypeStructs::CurrentAnsLookupV2(self_data),
                ParquetTypeStructs::CurrentAnsLookupV2(other_data),
            ) => {
                handle_append!(self_data, other_data)
            },
            (
                ParquetTypeStructs::TokenDataV2(self_data),
                ParquetTypeStructs::TokenDataV2(other_data),
            ) => {
                handle_append!(self_data, other_data)
            },
            (
                ParquetTypeStructs::CurrentTokenDataV2(self_data),
                ParquetTypeStructs::CurrentTokenDataV2(other_data),
            ) => {
                handle_append!(self_data, other_data)
            },
            (
                ParquetTypeStructs::TokenOwnershipV2(self_data),
                ParquetTypeStructs::TokenOwnershipV2(other_data),
            ) => {
                handle_append!(self_data, other_data)
            },
            (
                ParquetTypeStructs::CurrentTokenOwnershipV2(self_data),
                ParquetTypeStructs::CurrentTokenOwnershipV2(other_data),
            ) => {
                handle_append!(self_data, other_data)
            },
            (ParquetTypeStructs::Object(self_data), ParquetTypeStructs::Object(other_data)) => {
                handle_append!(self_data, other_data)
            },
            (
                ParquetTypeStructs::CurrentObject(self_data),
                ParquetTypeStructs::CurrentObject(other_data),
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
