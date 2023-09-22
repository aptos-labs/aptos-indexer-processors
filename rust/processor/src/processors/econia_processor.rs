use super::processor_trait::{ProcessingResult, ProcessorTrait};
use crate::{
    models::default_models::transactions::{TransactionDetail, TransactionModel},
    utils::database::{execute_with_better_error, PgDbPool},
};

use anyhow::anyhow;
use aptos_indexer_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use diesel::{result::Error, PgConnection};
use econia_db::models::CancelOrderEvent;
use econia_db::models::PlaceLimitOrderEvent;
use econia_db::models::FillEvent;
use econia_db::models::MarketRegistrationEvent;
use econia_db::schema::cancel_order_events;
use econia_db::schema::market_registration_events;
use econia_db::schema::fill_events;
use econia_db::schema::place_limit_order_events;
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

fn hex_to_string(hex: &str) -> anyhow::Result<String> {
    if !hex.starts_with("0x") {
        return Err(anyhow!("Hex string is not 0x-prefixed"));
    }

    let hex_no_prefix = &hex[2..];
    let hex_bytes =
        hex::decode(hex_no_prefix).map_err(|e| anyhow!("Failed to decode hex: {}", e))?;

    String::from_utf8(hex_bytes)
        .map_err(|e| anyhow!("Failed to convert hex bytes to utf-8 string: {}", e))
}

fn opt_value_to_big_decimal(value: Option<&Value>) -> anyhow::Result<BigDecimal> {
    match value {
        Some(Value::String(s)) => BigDecimal::from_str(s).map_err(anyhow::Error::new),
        Some(Value::Number(n)) if n.is_u64() => {
            Ok(BigDecimal::from(n.as_u64().unwrap()))
        },
        _ => Err(anyhow!("key not found or not a supported number type (i.e float)")),
    }
}

fn insert_market_registration_events(
    conn: &mut PgConnection,
    events: Vec<MarketRegistrationEvent>,
) -> Result<(), diesel::result::Error> {
    execute_with_better_error(
        conn,
        // If we try to insert an event twice, as according to its transaction
        // version and event index, the second insertion will just be dropped
        // and lost to the wind. It will not return an error.
        diesel::insert_into(market_registration_events::table)
            .values(&events)
            .on_conflict_do_nothing(),
        None,
    )?;
    Ok(())
}

fn insert_fill_events(
    conn: &mut PgConnection,
    events: Vec<FillEvent>,
) -> Result<(), diesel::result::Error> {
    execute_with_better_error(
        conn,
        // See comment above re: conflicts
        diesel::insert_into(fill_events::table)
            .values(&events)
            .on_conflict_do_nothing(),
        None,
    )?;
    Ok(())
}

fn insert_place_limit_order_events(
    conn: &mut PgConnection,
    events: Vec<PlaceLimitOrderEvent>,
) -> Result<(), diesel::result::Error> {
    execute_with_better_error(
        conn,
        // See comment above re: conflicts
        diesel::insert_into(place_limit_order_events::table)
            .values(&events)
            .on_conflict_do_nothing(),
        None,
    )?;
    Ok(())
}

fn insert_cancel_order_events(
    conn: &mut PgConnection,
    events: Vec<CancelOrderEvent>,
) -> Result<(), diesel::result::Error> {
    execute_with_better_error(
        conn,
        // See comment above re: conflicts
        diesel::insert_into(cancel_order_events::table)
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

        // Create a hashmap to store block_height to timestamp.
        let mut block_height_to_timestamp: HashMap<i64, DateTime<Utc>> = HashMap::new();

        // Iterate through the transactions and populate the map.
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

        let cancel_order_type =
            format!("{}::user::CancelOrderEvent", self.econia_address);
        let mut cancel_order_events = vec![];
        for transaction in &transactions {
            let (txn, _, events, _, _) = TransactionModel::from_transaction(&transaction);
            for (index, event) in events.iter().enumerate() {
                if event.type_ != cancel_order_type {
                    continue;
                }
                println!("{:?}", &event);
                let txn_version = BigDecimal::from(txn.version);
                let event_idx = BigDecimal::from(index as u64);
                let time = *block_height_to_timestamp
                    .get(&event.transaction_block_height)
                    // cannot panic because the loop beforehand populates the block height times
                    .unwrap();
                let market_id = opt_value_to_big_decimal(event.data.get("market_id")).unwrap();
                let maker_address = String::from(event.data["user"].as_str().unwrap());
                let maker_custodian_id = opt_value_to_big_decimal(event.data.get("custodian_id")).unwrap();
                let maker_order_id = opt_value_to_big_decimal(event.data.get("order_id")).unwrap();
                let reason = opt_value_to_big_decimal(event.data.get("reason")).unwrap();
                
                let place_limit_order_event = CancelOrderEvent {
                    txn_version,
                    event_idx,
                    time,
                    maker_address,
                    maker_custodian_id,
                    maker_order_id,
                    market_id,
                    reason,
                };
                cancel_order_events.push(place_limit_order_event);
            }
        }
        conn.build_transaction()
            .read_write()
            .run::<_, Error, _>(|pg_conn| {
                insert_cancel_order_events(pg_conn, cancel_order_events)
            })?;


        let place_limit_order_type =
            format!("{}::user::PlaceLimitOrderEvent", self.econia_address);
        let mut place_limit_order_events = vec![];
        for transaction in &transactions {
            let (txn, _, events, _, _) = TransactionModel::from_transaction(&transaction);
            for (index, event) in events.iter().enumerate() {
                if event.type_ != place_limit_order_type {
                    continue;
                }
                let txn_version = BigDecimal::from(txn.version);
                let event_idx = BigDecimal::from(index as u64);
                let time = *block_height_to_timestamp
                    .get(&event.transaction_block_height)
                    // cannot panic because the loop beforehand populates the block height times
                    .unwrap();
                let market_id = opt_value_to_big_decimal(event.data.get("market_id")).unwrap();
                let maker_address = String::from(event.data["user"].as_str().unwrap());
                let integrator_address = String::from(event.data["integrator"].as_str().unwrap());
                let maker_custodian_id = opt_value_to_big_decimal(event.data.get("custodian_id")).unwrap();
                let maker_order_id = opt_value_to_big_decimal(event.data.get("order_id")).unwrap();
                let maker_side = event.data.get("side").unwrap().as_bool().unwrap();
                let restriction = opt_value_to_big_decimal(event.data.get("restriction")).unwrap();
                let self_match_behavior = opt_value_to_big_decimal(event.data.get("self_match_behavior")).unwrap();
                let price = opt_value_to_big_decimal(event.data.get("price")).unwrap();
                let initial_size = opt_value_to_big_decimal(event.data.get("size")).unwrap();
                let posted_size = opt_value_to_big_decimal(event.data.get("remaining_size")).unwrap();
                
                let place_limit_order_event = PlaceLimitOrderEvent {
                    txn_version,
                    event_idx,
                    time,
                    maker_address,
                    integrator_address,
                    maker_custodian_id,
                    maker_order_id,
                    maker_side,
                    market_id,
                    price,
                    initial_size,
                    posted_size,
                    restriction,
                    self_match_behavior,
                };
                place_limit_order_events.push(place_limit_order_event);
            }
        }
        conn.build_transaction()
            .read_write()
            .run::<_, Error, _>(|pg_conn| {
                insert_place_limit_order_events(pg_conn, place_limit_order_events)
            })?;

        let fill_type =
            format!("{}::user::FillEvent", self.econia_address);
        let mut fill_events = vec![];
        for transaction in &transactions {
            let (txn, _, events, _, _) = TransactionModel::from_transaction(&transaction);
            for (index, event) in events.iter().enumerate() {
                if event.type_ != fill_type {
                    continue;
                }
                let txn_version = BigDecimal::from(txn.version);
                let event_idx = BigDecimal::from(index as u64);
                let time = *block_height_to_timestamp
                    .get(&event.transaction_block_height)
                    // cannot panic because the loop beforehand populates the block height times
                    .unwrap();
                let emit_address = event.account_address.to_string();
                let maker_address = String::from(event.data["maker"].as_str().unwrap());
                let maker_custodian_id = opt_value_to_big_decimal(event.data.get("maker_custodian_id"))?;
                let maker_order_id = opt_value_to_big_decimal(event.data.get("maker_order_id"))?;
                let maker_side = event.data.get("maker_side").unwrap().as_bool().unwrap();
                let market_id = opt_value_to_big_decimal(event.data.get("market_id"))?;
                let price = opt_value_to_big_decimal(event.data.get("price"))?;
                let trade_sequence_number = opt_value_to_big_decimal(event.data.get("sequence_number_for_trade"))?;
                let size = opt_value_to_big_decimal(event.data.get("size"))?;
                let taker_address = String::from(event.data["taker"].as_str().unwrap());
                let taker_custodian_id = opt_value_to_big_decimal(event.data.get("taker_custodian_id"))?;
                let taker_order_id = opt_value_to_big_decimal(event.data.get("taker_order_id"))?;
                let taker_quote_fees_paid = opt_value_to_big_decimal(event.data.get("taker_quote_fees_paid"))?;

                let fill_event = FillEvent {
                    txn_version,
                    event_idx,
                    emit_address,
                    time,
                    maker_address,
                    maker_custodian_id,
                    maker_order_id,
                    maker_side,
                    market_id,
                    price,
                    trade_sequence_number,
                    size,
                    taker_address,
                    taker_custodian_id,
                    taker_order_id,
                    taker_quote_fees_paid,
                };
                fill_events.push(fill_event);
            }
        }
        conn.build_transaction()
            .read_write()
            .run::<_, Error, _>(|pg_conn| {
                insert_fill_events(pg_conn, fill_events)
            })?;

        let market_registration_type =
            format!("{}::registry::MarketRegistrationEvent", self.econia_address);
        let mut market_registration_events = vec![];
        for transaction in &transactions {
            let (txn, _, events, _, _) = TransactionModel::from_transaction(&transaction);
            for (index, event) in events.iter().enumerate() {
                if event.type_ != market_registration_type {
                    continue;
                }
                let txn_version = BigDecimal::from(txn.version);
                let event_idx = BigDecimal::from(index as u64);
                let market_id = opt_value_to_big_decimal(event.data.get("market_id"))?;
                let lot_size = opt_value_to_big_decimal(event.data.get("lot_size"))?;
                let tick_size = opt_value_to_big_decimal(event.data.get("tick_size"))?;
                let min_size = opt_value_to_big_decimal(event.data.get("min_size"))?;
                let underwriter_id = opt_value_to_big_decimal(event.data.get("underwriter_id"))?;
                let time = *block_height_to_timestamp
                    .get(&event.transaction_block_height)
                    // cannot panic because the loop beforehand populates the block height times
                    .unwrap();
                let (base_name_generic, base_account_address, base_module_name, base_struct_name) =
                    // cannot panic because base_namme_generic is always present as a perhaps-empty string
                    if event.data["base_name_generic"].as_str().unwrap().is_empty() {
                        (
                            None,
                            // Unwrap to assert as Some, account_address must exist given empty base name
                            Some(String::from(event.data["base_type"]["account_address"].as_str().unwrap())),
                            // Unwrap to assert as Some, module_name must exist given empty base name
                            Some(hex_to_string(event.data["base_type"]["module_name"].as_str().unwrap())?),
                            // Unwrap to assert as Some, struct_name must exist given empty base name
                            Some(hex_to_string(event.data["base_type"]["struct_name"].as_str().unwrap())?),
                        )
                    } else {
                        (
                            // Unwrap to assert as Some, base_name_generic must exist given above condition
                            Some(String::from(event.data["base_name_generic"].as_str().unwrap())),
                            None,
                            None,
                            None,
                        )
                    };
                let quote_account_address = String::from(
                    event.data["quote_type"]["account_address"]
                        .as_str()
                        // cannot panic because account_address always exists as a string
                        .unwrap(),
                );
                let quote_module_name_hex =
                    // cannot panic because module_name always exists as a string
                    event.data["quote_type"]["module_name"].as_str().unwrap();
                let quote_struct_name_hex =
                    // cannot panic because struct_name always exists as a string
                    event.data["quote_type"]["struct_name"].as_str().unwrap();
                let quote_module_name = hex_to_string(quote_module_name_hex)?;
                let quote_struct_name = hex_to_string(quote_struct_name_hex)?;

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
            })?;

        Ok((start_version, end_version))
    }

    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}
