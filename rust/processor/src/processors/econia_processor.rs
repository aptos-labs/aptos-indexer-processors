use super::processor_trait::{ProcessingResult, ProcessorTrait};
use crate::{
    models::default_models::{
        block_metadata_transactions::BlockMetadataTransactionModel,
        events::EventModel,
        move_modules::MoveModule,
        move_resources::MoveResource,
        move_tables::{CurrentTableItem, TableItem, TableMetadata},
        signatures::Signature,
        transactions::{TransactionDetail, TransactionModel},
        user_transactions::UserTransactionModel,
        v2_objects::{CurrentObject, Object},
        write_set_changes::{WriteSetChangeDetail, WriteSetChangeModel},
    },
    schema,
    utils::database::{
        clean_data_for_db, execute_with_better_error, get_chunks, PgDbPool, PgPoolConnection,
    },
};
use anyhow::bail;
use aptos_indexer_protos::transaction::v1::{write_set_change::Change, Transaction};
use async_trait::async_trait;
use chrono::{NaiveDateTime, DateTime, Utc};
use diesel::{pg::upsert::excluded, result::Error, ExpressionMethods, PgConnection, RunQueryDsl};
use field_count::FieldCount;
use std::{collections::HashMap, fmt::Debug};
use tracing::error;
use serde_json::Value;
use std::str::FromStr;
use bigdecimal::BigDecimal;
use econia_db::schema::market_registration_events;
use econia_db::models::NewMarketRegistrationEvent;
// use bigdecimal::BigDecimal;
// use std::str::FromStr;

pub const NAME: &str = "econia_processor";
pub struct EconiaTransactionProcessor {
    connection_pool: PgDbPool,
    econia_address: String,
}

impl EconiaTransactionProcessor {
    pub fn new(connection_pool: PgDbPool, econia_address: String) -> Self {
        Self { connection_pool, econia_address }
    }
}

impl Debug for EconiaTransactionProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "DefaultTransactionProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

struct TypeInfo {
    address: String,
    module_name: String,
    struct_name: String,
}

fn hex_to_string(hex: &str) -> String {
    let hex_no_prefix = &hex[2..];
    let hex_bytes = hex::decode(hex_no_prefix).unwrap();
    String::from_utf8(hex_bytes).unwrap()
}

fn opt_value_to_big_decimal(value: Option<&Value>) -> BigDecimal {
    let lot_size_str = value
        .and_then(Value::as_str)
        .expect("key not found or not a string");
    BigDecimal::from_str(lot_size_str).expect("Failed to parse BigDecimal")
}

fn value_is_not_empty_string(val: &Value) -> bool {
    val.as_str()
        .expect("Value is not a string")
        .is_empty() == false
}

fn insert_market_registration_events(
    conn: &mut PgConnection,
    events: Vec<NewMarketRegistrationEvent>,
) -> Result<(), diesel::result::Error> {
    execute_with_better_error(
        conn,
        diesel::insert_into(market_registration_events::table)
            .values(&events)
            .on_conflict_do_nothing(),
        None,
    )?;
    Ok(())
}

#[async_trait]
impl ProcessorTrait for EconiaTransactionProcessor {
    fn name(&self) -> &'static str {
        NAME
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let mut conn = self.get_conn();
        let (txns, txn_details, events, write_set_changes, wsc_details) =
            TransactionModel::from_transactions(&transactions);

        // create a hashmap to store block_height to timestamp
        let mut block_height_to_timestamp: HashMap<i64, DateTime<Utc>> = HashMap::new();
        
        // iterate through the transactions and populate the map
        for txn_detail in txn_details {
            match txn_detail {
                TransactionDetail::User(user_transaction, _) => {
                    block_height_to_timestamp.insert(
                        user_transaction.block_height,
                        DateTime::from_utc(user_transaction.timestamp, Utc)
                    );
                }
                TransactionDetail::BlockMetadata(block_metadata_transaction) => {
                    block_height_to_timestamp.insert(
                        block_metadata_transaction.block_height,
                        DateTime::from_utc(block_metadata_transaction.timestamp, Utc)
                    );
                }
            }
        }

        let market_registration_type = format!("{}::registry::MarketRegistrationEvent", self.econia_address);
        let mut market_registration_events = vec![];
        for event in events {
            if event.type_ == market_registration_type {
                let market_id = opt_value_to_big_decimal(event.data.get("market_id"));
                let lot_size = opt_value_to_big_decimal(event.data.get("lot_size"));
                let tick_size = opt_value_to_big_decimal(event.data.get("tick_size"));
                let min_size = opt_value_to_big_decimal(event.data.get("min_size"));
                let underwriter_id = opt_value_to_big_decimal(event.data.get("underwriter_id"));
                let time = *block_height_to_timestamp.get(&event.transaction_block_height).unwrap();
                let base_name_generic;
                let base_account_address;
                let base_module_name;
                let base_struct_name;
                if value_is_not_empty_string(&event.data["base_name_generic"]) {
                    base_name_generic = Some(String::from(event.data["base_name_generic"].as_str().unwrap()));
                    base_account_address = None;
                    base_module_name = None;
                    base_struct_name = None;
                } else {
                    base_name_generic = None;
                    base_account_address = Some(String::from(event.data["base_type"]["account_address"].as_str().unwrap()));
                    let base_module_name_hex = event.data["base_type"]["module_name"].as_str().unwrap();
                    let base_struct_name_hex = event.data["base_type"]["struct_name"].as_str().unwrap();
                    base_module_name = Some(hex_to_string(base_module_name_hex));
                    base_struct_name = Some(hex_to_string(base_struct_name_hex));
                }
                let quote_account_address = String::from(event.data["quote_type"]["account_address"].as_str().unwrap());
                let quote_module_name_hex = event.data["quote_type"]["module_name"].as_str().unwrap();
                let quote_struct_name_hex = event.data["quote_type"]["struct_name"].as_str().unwrap();
                let quote_module_name = hex_to_string(quote_module_name_hex);
                let quote_struct_name = hex_to_string(quote_struct_name_hex);

                let market_registration_event = NewMarketRegistrationEvent {
                    market_id,
                    time,
                    base_name_generic,
                    base_account_address,
                    base_module_name,
                    base_struct_name,
                    quote_account_address,
                    quote_module_name,
                    quote_struct_name,
                    lot_size,
                    tick_size,
                    min_size,
                    underwriter_id,
                };
                market_registration_events.push(market_registration_event);
            }
        }

        conn
            .build_transaction()
            .read_write()
            .run::<_, Error, _>(|pg_conn| {
                insert_market_registration_events(
                    pg_conn,
                    market_registration_events,
                )
            })
            .expect("Expected a successful insertion into the table");

        panic!("I reached the end of times!");

        let mut signatures = vec![];
        let mut user_transactions = vec![];
        let mut block_metadata_transactions = vec![];
        for detail in txn_details {
            match detail {
                TransactionDetail::User(user_txn, sigs) => {
                    signatures.append(&mut sigs.clone());
                    user_transactions.push(user_txn.clone());
                },
                TransactionDetail::BlockMetadata(bmt) => {
                    block_metadata_transactions.push(bmt.clone())
                },
            }
        }
        let mut move_modules = vec![];
        let mut move_resources = vec![];
        let mut table_items = vec![];
        let mut current_table_items = HashMap::new();
        let mut table_metadata = HashMap::new();
        for detail in wsc_details {
            match detail {
                WriteSetChangeDetail::Module(module) => move_modules.push(module.clone()),
                WriteSetChangeDetail::Resource(resource) => move_resources.push(resource.clone()),
                WriteSetChangeDetail::Table(item, current_item, metadata) => {
                    table_items.push(item.clone());
                    current_table_items.insert(
                        (
                            current_item.table_handle.clone(),
                            current_item.key_hash.clone(),
                        ),
                        current_item.clone(),
                    );
                    if let Some(meta) = metadata {
                        table_metadata.insert(meta.handle.clone(), meta.clone());
                    }
                },
            }
        }

        // TODO, merge this loop with above
        // Moving object handling here because we need a single object
        // map through transactions for lookups
        let mut all_objects = vec![];
        let mut all_current_objects = HashMap::new();
        for txn in &transactions {
            let txn_version = txn.version as i64;
            let changes = &txn
                .info
                .as_ref()
                .unwrap_or_else(|| {
                    panic!(
                        "Transaction info doesn't exist! Transaction {}",
                        txn_version
                    )
                })
                .changes;
            for (index, wsc) in changes.iter().enumerate() {
                let index: i64 = index as i64;
                match wsc.change.as_ref().unwrap() {
                    Change::WriteResource(inner) => {
                        if let Some((object, current_object)) =
                            &Object::from_write_resource(inner, txn_version, index).unwrap()
                        {
                            all_objects.push(object.clone());
                            all_current_objects
                                .insert(object.object_address.clone(), current_object.clone());
                        }
                    },
                    Change::DeleteResource(inner) => {
                        // Passing all_current_objects into the function so that we can get the owner of the deleted
                        // resource if it was handled in the same batch
                        if let Some((object, current_object)) = Object::from_delete_resource(
                            inner,
                            txn_version,
                            index,
                            &all_current_objects,
                            &mut conn,
                        )
                        .unwrap()
                        {
                            all_objects.push(object.clone());
                            all_current_objects
                                .insert(object.object_address.clone(), current_object.clone());
                        }
                    },
                    _ => {},
                };
            }
        }
        // Getting list of values and sorting by pk in order to avoid postgres deadlock since we're doing multi threaded db writes
        let mut current_table_items = current_table_items
            .into_values()
            .collect::<Vec<CurrentTableItem>>();
        let mut table_metadata = table_metadata.into_values().collect::<Vec<TableMetadata>>();
        // Sort by PK
        let mut all_current_objects = all_current_objects
            .into_values()
            .collect::<Vec<CurrentObject>>();
        current_table_items
            .sort_by(|a, b| (&a.table_handle, &a.key_hash).cmp(&(&b.table_handle, &b.key_hash)));
        table_metadata.sort_by(|a, b| a.handle.cmp(&b.handle));
        all_current_objects.sort_by(|a, b| a.object_address.cmp(&b.object_address));

        Ok((start_version, end_version))
    }

    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}
