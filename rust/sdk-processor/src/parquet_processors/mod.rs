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
use downcast_rs::{impl_downcast, Downcast};
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
#[allow(unused_imports)]
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use strum::{Display, EnumIter};

pub mod parquet_default_processor;

const GOOGLE_APPLICATION_CREDENTIALS: &str = "GOOGLE_APPLICATION_CREDENTIALS";

/// Trait for providing default instances of supported types.
pub trait ParquetDefault {
    fn default_for_type() -> Self;
}

/// Macro to implement ParquetDefault for multiple types.
macro_rules! impl_parquet_default {
    ($($type:ty),*) => {
        $(
            impl ParquetDefault for Vec<$type> {
                fn default_for_type() -> Self {
                    Vec::new()
                }
            }
        )*
    };
}

// Apply the macro to all supported types
impl_parquet_default!(
    MoveResource,
    WriteSetChangeModel,
    ParquetTransaction,
    TableItem,
    MoveModule
);

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

/// Trait for handling various Parquet types.
#[async_trait]
pub trait ParquetTypeTrait: std::fmt::Debug + Send + Sync + Downcast {
    fn parquet_type(&self) -> ParquetTypeEnum;
    fn calculate_size(&self) -> usize;

    fn append(&mut self, other: Box<dyn ParquetTypeTrait>) -> Result<(), ProcessorError>;

    async fn upload_to_gcs(
        &self,
        uploader: &mut GCSUploader,
        parquet_type: ParquetTypeEnum,
        table_name: &str,
    ) -> anyhow::Result<()>;

    fn clone_box(&self) -> Box<dyn ParquetTypeTrait>;
}

impl_downcast!(ParquetTypeTrait);

/// Struct for handling Parquet types dynamically.
#[derive(Debug)]
pub struct ParquetTypeStructs {
    pub data: Box<dyn ParquetTypeTrait>,
}

impl ParquetTypeStructs {
    pub fn new<T: ParquetTypeTrait + 'static>(data: T) -> Self {
        Self {
            data: Box::new(data),
        }
    }

    pub fn calculate_size(&self) -> usize {
        self.data.calculate_size()
    }

    pub fn append(&mut self, other: ParquetTypeStructs) -> Result<(), ProcessorError> {
        self.data.append(other.data)
    }

    pub async fn upload_to_gcs(
        &self,
        uploader: &mut GCSUploader,
        parquet_type: ParquetTypeEnum,
        table_name: &str,
    ) -> anyhow::Result<()> {
        self.data
            .upload_to_gcs(uploader, parquet_type, table_name)
            .await
    }

    pub fn default_for_type(parquet_type: &ParquetTypeEnum) -> Self {
        match parquet_type {
            ParquetTypeEnum::MoveResource => {
                ParquetTypeStructs::new(Vec::<MoveResource>::default_for_type())
            },
            ParquetTypeEnum::WriteSetChange => {
                ParquetTypeStructs::new(Vec::<WriteSetChangeModel>::default_for_type())
            },
            ParquetTypeEnum::Transaction => {
                ParquetTypeStructs::new(Vec::<ParquetTransaction>::default_for_type())
            },
            ParquetTypeEnum::TableItem => {
                ParquetTypeStructs::new(Vec::<TableItem>::default_for_type())
            },
            ParquetTypeEnum::MoveModule => {
                ParquetTypeStructs::new(Vec::<MoveModule>::default_for_type())
            },
        }
    }
}

impl Clone for ParquetTypeStructs {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone_box(),
        }
    }
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

            fn append(&mut self, other: Box<dyn ParquetTypeTrait>) -> Result<(), ProcessorError> {
                if let Some(other_data) = other.downcast_ref::<Vec<$type>>() {
                    self.extend(other_data.clone());
                    Ok(())
                } else {
                    Err(ProcessorError::ProcessError {
                        message: "Mismatched buffer types in append operation".to_string(),
                    })
                }
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

            fn clone_box(&self) -> Box<dyn ParquetTypeTrait> {
                Box::new(self.clone())
            }
        }
    };
}

// Apply macro to supported types
impl_parquet_trait!(MoveResource, ParquetTypeEnum::MoveResource);
impl_parquet_trait!(WriteSetChangeModel, ParquetTypeEnum::WriteSetChange);
impl_parquet_trait!(ParquetTransaction, ParquetTypeEnum::Transaction);
impl_parquet_trait!(TableItem, ParquetTypeEnum::TableItem);
impl_parquet_trait!(MoveModule, ParquetTypeEnum::MoveModule);

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
            ParquetTypeEnum::MoveResource,
            ParquetTypeEnum::WriteSetChange,
            ParquetTypeEnum::Transaction,
            ParquetTypeEnum::TableItem,
            ParquetTypeEnum::MoveModule,
        ];
        for t in types {
            // Use a type corresponding to the ParquetTypeEnum
            let default = match t {
                ParquetTypeEnum::MoveResource => {
                    ParquetTypeStructs::new(Vec::<MoveResource>::default())
                },
                ParquetTypeEnum::WriteSetChange => {
                    ParquetTypeStructs::new(Vec::<WriteSetChangeModel>::default())
                },
                ParquetTypeEnum::Transaction => {
                    ParquetTypeStructs::new(Vec::<ParquetTransaction>::default())
                },
                ParquetTypeEnum::TableItem => ParquetTypeStructs::new(Vec::<TableItem>::default()),
                ParquetTypeEnum::MoveModule => {
                    ParquetTypeStructs::new(Vec::<MoveModule>::default())
                },
            };

            assert_eq!(default.data.parquet_type(), t);
        }
    }
}
