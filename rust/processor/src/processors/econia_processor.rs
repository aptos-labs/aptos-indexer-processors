use super::processor_trait::{ProcessingResult, ProcessorTrait};
use crate::{
    models::default_models::transactions::{TransactionDetail, TransactionModel},
    utils::database::{execute_with_better_error, PgDbPool},
};

use aptos_indexer_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use diesel::{result::Error, PgConnection};
use econia_db::models::MarketRegistrationEvent;
use econia_db::schema::market_registration_events;
use serde_json::Value;
use std::str::FromStr;
use std::{collections::HashMap, fmt::Debug};

pub const NAME: &str = "econia_processor";
pub struct EconiaTransactionProcessor {
    connection_pool: PgDbPool,
    econia_address: String,
}

impl EconiaTransactionProcessor {
    pub fn new(connection_pool: PgDbPool, econia_address: String) -> Self {
        Self {
            connection_pool,
            econia_address,
        }
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
    val.as_str().expect("Value is not a string").is_empty() == false
}

fn insert_market_registration_events(
    conn: &mut PgConnection,
    events: Vec<MarketRegistrationEvent>,
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
        let (_, txn_details, _, _, _) = TransactionModel::from_transactions(&transactions);

        // create a hashmap to store block_height to timestamp
        let mut block_height_to_timestamp: HashMap<i64, DateTime<Utc>> = HashMap::new();

        // iterate through the transactions and populate the map
        for txn_detail in txn_details {
            match txn_detail {
                TransactionDetail::User(user_transaction, _) => {
                    block_height_to_timestamp.insert(
                        user_transaction.block_height,
                        DateTime::from_utc(user_transaction.timestamp, Utc),
                    );
                },
                TransactionDetail::BlockMetadata(block_metadata_transaction) => {
                    block_height_to_timestamp.insert(
                        block_metadata_transaction.block_height,
                        DateTime::from_utc(block_metadata_transaction.timestamp, Utc),
                    );
                },
            }
        }

        let market_registration_type =
            format!("{}::registry::MarketRegistrationEvent", self.econia_address);
        let mut market_registration_events = vec![];
        for transaction in transactions {
            let (txn, _, events, _, _) = TransactionModel::from_transaction(&transaction);
            for (index, event) in events.iter().enumerate() {
                if event.type_ != market_registration_type {
                    continue;
                }
                let txn_version = BigDecimal::from(txn.version);
                let event_idx = BigDecimal::from(index as u64);
                let market_id = opt_value_to_big_decimal(event.data.get("market_id"));
                let lot_size = opt_value_to_big_decimal(event.data.get("lot_size"));
                let tick_size = opt_value_to_big_decimal(event.data.get("tick_size"));
                let min_size = opt_value_to_big_decimal(event.data.get("min_size"));
                let underwriter_id = opt_value_to_big_decimal(event.data.get("underwriter_id"));
                let time = *block_height_to_timestamp
                    .get(&event.transaction_block_height)
                    .unwrap();
                let base_name_generic;
                let base_account_address;
                let base_module_name;
                let base_struct_name;
                if value_is_not_empty_string(&event.data["base_name_generic"]) {
                    base_name_generic = Some(String::from(
                        event.data["base_name_generic"].as_str().unwrap(),
                    ));
                    base_account_address = None;
                    base_module_name = None;
                    base_struct_name = None;
                } else {
                    base_name_generic = None;
                    base_account_address = Some(String::from(
                        event.data["base_type"]["account_address"].as_str().unwrap(),
                    ));
                    let base_module_name_hex =
                        event.data["base_type"]["module_name"].as_str().unwrap();
                    let base_struct_name_hex =
                        event.data["base_type"]["struct_name"].as_str().unwrap();
                    base_module_name = Some(hex_to_string(base_module_name_hex));
                    base_struct_name = Some(hex_to_string(base_struct_name_hex));
                }
                let quote_account_address = String::from(
                    event.data["quote_type"]["account_address"]
                        .as_str()
                        .unwrap(),
                );
                let quote_module_name_hex =
                    event.data["quote_type"]["module_name"].as_str().unwrap();
                let quote_struct_name_hex =
                    event.data["quote_type"]["struct_name"].as_str().unwrap();
                let quote_module_name = hex_to_string(quote_module_name_hex);
                let quote_struct_name = hex_to_string(quote_struct_name_hex);

                let market_registration_event = MarketRegistrationEvent {
                    txn_version,
                    event_idx,
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

        conn.build_transaction()
            .read_write()
            .run::<_, Error, _>(|pg_conn| {
                insert_market_registration_events(pg_conn, market_registration_events)
            })
            .expect("Expected a successful insertion into the table");

        Ok((start_version, end_version))
    }

    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}
