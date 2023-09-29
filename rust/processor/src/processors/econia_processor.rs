use super::{ProcessingResult, ProcessorTrait};
use crate::{
    models::default_models::transactions::TransactionModel,
    utils::{database::{execute_with_better_error, PgDbPool}, util::parse_timestamp},
};
use crate::models::events_models::events::EventModel;

use anyhow::anyhow;
use aptos_indexer_protos::transaction::v1::{Transaction, transaction::TxnData};
use async_trait::async_trait;
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use diesel::{result::Error, PgConnection};
use econia_db::models::CancelOrderEvent;
use econia_db::models::ChangeOrderSizeEvent;
use econia_db::models::FillEvent;
use econia_db::models::MarketRegistrationEvent;
use econia_db::models::PlaceLimitOrderEvent;
use econia_db::models::PlaceMarketOrderEvent;
use econia_db::models::PlaceSwapOrderEvent;
use econia_db::schema::cancel_order_events;
use econia_db::schema::change_order_size_events;
use econia_db::schema::fill_events;
use econia_db::schema::market_registration_events;
use econia_db::schema::place_limit_order_events;
use econia_db::schema::place_market_order_events;
use econia_db::schema::place_swap_order_events;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::str::FromStr;
use std::{collections::HashMap, fmt::Debug};

pub const NAME: &str = "econia_processor";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EconiaProcessorConfig {
    pub econia_address: String,
}

pub struct EconiaTransactionProcessor {
    connection_pool: PgDbPool,
    config: EconiaProcessorConfig,
}

impl EconiaTransactionProcessor {
    pub fn new(connection_pool: PgDbPool, config: EconiaProcessorConfig) -> Self {
        Self {
            connection_pool,
            config,
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

fn opt_value_to_bool(value: Option<&Value>) -> anyhow::Result<bool> {
    match value {
        Some(Value::Bool(b)) => Ok(b.clone()),
        _ => Err(anyhow!("key not found or not a supported type")),
    }
}

fn opt_value_to_big_decimal(value: Option<&Value>) -> anyhow::Result<BigDecimal> {
    match value {
        Some(Value::String(s)) => Ok(BigDecimal::from_str(s)?),
        Some(Value::Number(n)) if n.is_u64() => Ok(BigDecimal::from(n.as_u64().unwrap())),
        _ => Err(anyhow!(
            "key not found or not a supported number type (i.e float)"
        )),
    }
}

fn opt_value_to_string(value: Option<&Value>) -> anyhow::Result<String> {
    match value {
        Some(Value::String(s)) => Ok(s.clone()),
        _ => Err(anyhow!("key not found or not a supported type")),
    }
}

fn opt_value_to_i16(value: Option<&Value>) -> anyhow::Result<i16> {
    match value {
        Some(Value::String(s)) => Ok(s.parse()?),
        Some(Value::Number(n)) => {
            if n.is_u64() {
                Ok(n.as_u64().unwrap().try_into()?)
            } else if n.is_i64() {
                Ok(n.as_i64().unwrap().try_into()?)
            } else {
                Err(anyhow!(
                    "key not found or not a supported number type (i.e float)"
                ))
            }
        },
        _ => Err(anyhow!(
            "key not found or not a supported number type (i.e float)"
        )),
    }
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

fn insert_change_order_size_events(
    conn: &mut PgConnection,
    events: Vec<ChangeOrderSizeEvent>,
) -> Result<(), diesel::result::Error> {
    execute_with_better_error(
        conn,
        // See comment above re: conflicts
        diesel::insert_into(change_order_size_events::table)
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

fn insert_place_market_order_events(
    conn: &mut PgConnection,
    events: Vec<PlaceMarketOrderEvent>,
) -> Result<(), diesel::result::Error> {
    execute_with_better_error(
        conn,
        // See comment above re: conflicts
        diesel::insert_into(place_market_order_events::table)
            .values(&events)
            .on_conflict_do_nothing(),
        None,
    )?;
    Ok(())
}

fn insert_place_swap_order_events(
    conn: &mut PgConnection,
    events: Vec<PlaceSwapOrderEvent>,
) -> Result<(), diesel::result::Error> {
    execute_with_better_error(
        conn,
        // See comment above re: conflicts
        diesel::insert_into(place_swap_order_events::table)
            .values(&events)
            .on_conflict_do_nothing(),
        None,
    )?;
    Ok(())
}

fn event_data_to_cancel_order_event(
    event: &EventModel,
    txn_version: BigDecimal,
    event_idx: BigDecimal,
    time: DateTime<Utc>,
) -> anyhow::Result<CancelOrderEvent> {
    let market_id = opt_value_to_big_decimal(event.data.get("market_id"))?;
    let user = opt_value_to_string(event.data.get("user"))?;
    let custodian_id = opt_value_to_big_decimal(event.data.get("custodian_id"))?;
    let order_id = opt_value_to_big_decimal(event.data.get("order_id"))?;
    let reason = opt_value_to_i16(event.data.get("reason"))?;

    let cancel_order_event = CancelOrderEvent {
        txn_version,
        event_idx,
        time,
        user,
        custodian_id,
        order_id,
        market_id,
        reason,
    };

    Ok(cancel_order_event)
}

fn event_data_to_change_order_size_event(
    event: &EventModel,
    txn_version: BigDecimal,
    event_idx: BigDecimal,
    time: DateTime<Utc>,
) -> anyhow::Result<ChangeOrderSizeEvent> {
    let market_id = opt_value_to_big_decimal(event.data.get("market_id"))?;
    let user = opt_value_to_string(event.data.get("user"))?;
    let custodian_id = opt_value_to_big_decimal(event.data.get("custodian_id"))?;
    let order_id = opt_value_to_big_decimal(event.data.get("order_id"))?;
    let side = opt_value_to_bool(event.data.get("side"))?;
    let new_size = opt_value_to_big_decimal(event.data.get("new_size"))?;

    let change_order_size_event = ChangeOrderSizeEvent {
        txn_version,
        event_idx,
        time,
        user,
        custodian_id,
        order_id,
        market_id,
        side,
        new_size,
    };

    Ok(change_order_size_event)
}

fn event_data_to_fill_event(
    event: &EventModel,
    txn_version: BigDecimal,
    event_idx: BigDecimal,
    time: DateTime<Utc>,
) -> anyhow::Result<FillEvent> {
    let emit_address = event.account_address.to_string();
    let maker_address = opt_value_to_string(event.data.get("maker"))?;
    let maker_custodian_id = opt_value_to_big_decimal(event.data.get("maker_custodian_id"))?;
    let maker_order_id = opt_value_to_big_decimal(event.data.get("maker_order_id"))?;
    let maker_side = opt_value_to_bool(event.data.get("maker_side"))?;
    let market_id = opt_value_to_big_decimal(event.data.get("market_id"))?;
    let price = opt_value_to_big_decimal(event.data.get("price"))?;
    let sequence_number_for_trade =
        opt_value_to_big_decimal(event.data.get("sequence_number_for_trade"))?;
    let size = opt_value_to_big_decimal(event.data.get("size"))?;
    let taker_address = opt_value_to_string(event.data.get("taker"))?;
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
        sequence_number_for_trade,
        size,
        taker_address,
        taker_custodian_id,
        taker_order_id,
        taker_quote_fees_paid,
    };

    Ok(fill_event)
}

fn event_data_to_market_registration_event(
    event: &EventModel,
    txn_version: BigDecimal,
    event_idx: BigDecimal,
    time: DateTime<Utc>,
) -> anyhow::Result<MarketRegistrationEvent> {
    let market_id = opt_value_to_big_decimal(event.data.get("market_id"))?;
    let lot_size = opt_value_to_big_decimal(event.data.get("lot_size"))?;
    let tick_size = opt_value_to_big_decimal(event.data.get("tick_size"))?;
    let min_size = opt_value_to_big_decimal(event.data.get("min_size"))?;
    let underwriter_id = opt_value_to_big_decimal(event.data.get("underwriter_id"))?;
    let (base_name_generic, base_account_address, base_module_name, base_struct_name) =
        if opt_value_to_string(event.data.get("base_name_generic"))?.is_empty() {
            if let Some(base_type) = event.data.get("base_type") {
                (
                    None,
                    Some(opt_value_to_string(base_type.get("account_address"))?),
                    Some(opt_value_to_string(base_type.get("module_name"))?),
                    Some(opt_value_to_string(base_type.get("struct_name"))?),
                )
            } else {
                anyhow::bail!("could not determine base");
            }
        } else {
            (
                Some(opt_value_to_string(event.data.get("base_name_generic"))?),
                None,
                None,
                None,
            )
        };
    let (quote_account_address, quote_module_name_hex, quote_struct_name_hex) =
        if let Some(quote_type) = event.data.get("quote_type") {
            (
                opt_value_to_string(quote_type.get("account_address"))?,
                opt_value_to_string(quote_type.get("module_name"))?,
                opt_value_to_string(quote_type.get("struct_name"))?,
            )
        } else {
            anyhow::bail!("could not determine quote");
        };
    let quote_module_name = hex_to_string(&quote_module_name_hex)?;
    let quote_struct_name = hex_to_string(&quote_struct_name_hex)?;

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

    Ok(market_registration_event)
}

fn event_data_to_place_market_order_event(
    event: &EventModel,
    txn_version: BigDecimal,
    event_idx: BigDecimal,
    time: DateTime<Utc>,
) -> anyhow::Result<PlaceMarketOrderEvent> {
    let custodian_id = opt_value_to_big_decimal(event.data.get("custodian_id"))?;
    let order_id = opt_value_to_big_decimal(event.data.get("order_id"))?;
    let direction = event.data.get("direction").unwrap().as_bool().unwrap();
    let market_id = opt_value_to_big_decimal(event.data.get("market_id"))?;
    let size = opt_value_to_big_decimal(event.data.get("size"))?;
    let self_match_behavior = opt_value_to_i16(event.data.get("self_match_behavior"))?;
    let user = opt_value_to_string(event.data.get("user"))?;
    let integrator = opt_value_to_string(event.data.get("integrator"))?;

    let place_market_order_event = PlaceMarketOrderEvent {
        txn_version,
        event_idx,
        market_id,
        time,
        user,
        custodian_id,
        order_id,
        direction,
        size,
        self_match_behavior,
        integrator,
    };

    Ok(place_market_order_event)
}

fn event_data_to_place_limit_order_event(
    event: &EventModel,
    txn_version: BigDecimal,
    event_idx: BigDecimal,
    time: DateTime<Utc>,
) -> anyhow::Result<PlaceLimitOrderEvent> {
    let market_id = opt_value_to_big_decimal(event.data.get("market_id"))?;
    let user = opt_value_to_string(event.data.get("user"))?;
    let integrator = opt_value_to_string(event.data.get("integrator"))?;
    let custodian_id = opt_value_to_big_decimal(event.data.get("custodian_id"))?;
    let order_id = opt_value_to_big_decimal(event.data.get("order_id"))?;
    let side = opt_value_to_bool(event.data.get("side"))?;
    let restriction = opt_value_to_i16(event.data.get("restriction"))?;
    let self_match_behavior = opt_value_to_i16(event.data.get("self_match_behavior"))?;
    let price = opt_value_to_big_decimal(event.data.get("price"))?;
    let initial_size = opt_value_to_big_decimal(event.data.get("size"))?;
    let size = opt_value_to_big_decimal(event.data.get("remaining_size"))?;

    let place_limit_order_event = PlaceLimitOrderEvent {
        txn_version,
        event_idx,
        time,
        user,
        integrator,
        custodian_id,
        order_id,
        side,
        market_id,
        price,
        initial_size,
        size,
        restriction,
        self_match_behavior,
    };

    Ok(place_limit_order_event)
}

fn event_data_to_place_swap_order_event(
    event: &EventModel,
    txn_version: BigDecimal,
    event_idx: BigDecimal,
    time: DateTime<Utc>,
) -> anyhow::Result<PlaceSwapOrderEvent> {
    let market_id = opt_value_to_big_decimal(event.data.get("market_id"))?;
    let order_id = opt_value_to_big_decimal(event.data.get("order_id"))?;
    let direction = opt_value_to_bool(event.data.get("direction"))?;
    let integrator = opt_value_to_string(event.data.get("integrator"))?;
    let min_base = opt_value_to_big_decimal(event.data.get("min_base"))?;
    let max_base = opt_value_to_big_decimal(event.data.get("max_base"))?;
    let min_quote = opt_value_to_big_decimal(event.data.get("min_quote"))?;
    let max_quote = opt_value_to_big_decimal(event.data.get("max_quote"))?;
    let limit_price = opt_value_to_big_decimal(event.data.get("limit_price"))?;
    let signing_account = opt_value_to_string(event.data.get("signing_account"))?;

    let place_swap_order_event = PlaceSwapOrderEvent {
        txn_version,
        event_idx,
        time,
        integrator,
        order_id,
        market_id,
        min_base,
        max_base,
        min_quote,
        max_quote,
        direction,
        limit_price,
        signing_account,
    };

    Ok(place_swap_order_event)
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
        
        // Create a hashmap to store block_height to timestamp.
        let mut block_height_to_timestamp: HashMap<i64, DateTime<Utc>> = HashMap::new();
        let mut user_transactions = vec![];
        for txn in &transactions {
            let txn_version = txn.version as i64;
            let block_height = txn.block_height as i64;
            let txn_data = txn.txn_data.as_ref().expect("Txn Data doesn't exit!");
            if let TxnData::User(_) = txn_data {
                block_height_to_timestamp.insert(
                    block_height,
                    DateTime::from_utc(
                        parse_timestamp(
                            txn.timestamp.as_ref().unwrap(),
                            txn_version
                        ),
                        Utc
                    ),
                );
                user_transactions.push(txn);
            }
        }

        let econia_address = &self.config.econia_address;

        let cancel_order_type = format!("{}::user::CancelOrderEvent", econia_address);
        let change_order_size_type = format!("{}::user::ChangeOrderSizeEvent", econia_address);
        let fill_type = format!("{}::user::FillEvent", econia_address);
        let market_registration_type =
            format!("{}::registry::MarketRegistrationEvent", econia_address);
        let place_limit_order_type = format!("{}::user::PlaceLimitOrderEvent", econia_address);
        let place_market_order_type =
            format!("{}::user::PlaceMarketOrderEvent", econia_address);
        let place_swap_order_type = format!("{}::user::PlaceSwapOrderEvent", econia_address);

        let mut cancel_order_events = vec![];
        let mut change_order_size_events = vec![];
        let mut fill_events = vec![];
        let mut market_registration_events = vec![];
        let mut place_limit_order_events = vec![];
        let mut place_market_order_events = vec![];
        let mut place_swap_order_events = vec![];

        for txn in user_transactions {
            let txn_version = txn.version as i64;
            let block_height = txn.block_height as i64;
            let txn_data = txn.txn_data.as_ref().expect("Txn Data doesn't exit!");
            let default = vec![];
            let raw_events = match txn_data {
                TxnData::BlockMetadata(tx_inner) => &tx_inner.events,
                TxnData::Genesis(tx_inner) => &tx_inner.events,
                TxnData::User(tx_inner) => &tx_inner.events,
                _ => &default,
            };
            let events = EventModel::from_events(raw_events, txn_version, block_height);
            for (index, event) in events.iter().enumerate() {
                let txn_version = BigDecimal::from(txn.version);
                let event_idx = BigDecimal::from(index as u64);
                let time = *block_height_to_timestamp
                    .get(&event.transaction_block_height)
                    // cannot panic because the loop beforehand populates the block height times
                    .unwrap();

                if event.type_ == cancel_order_type {
                    cancel_order_events.push(event_data_to_cancel_order_event(
                        event,
                        txn_version,
                        event_idx,
                        time,
                    )?);
                } else if event.type_ == change_order_size_type {
                    change_order_size_events.push(event_data_to_change_order_size_event(
                        event,
                        txn_version,
                        event_idx,
                        time,
                    )?);
                } else if event.type_ == fill_type {
                    fill_events.push(event_data_to_fill_event(
                        event,
                        txn_version,
                        event_idx,
                        time,
                    )?);
                } else if event.type_ == market_registration_type {
                    market_registration_events.push(event_data_to_market_registration_event(
                        event,
                        txn_version,
                        event_idx,
                        time,
                    )?);
                } else if event.type_ == place_limit_order_type {
                    place_limit_order_events.push(event_data_to_place_limit_order_event(
                        event,
                        txn_version,
                        event_idx,
                        time,
                    )?);
                } else if event.type_ == place_market_order_type {
                    place_market_order_events.push(event_data_to_place_market_order_event(
                        event,
                        txn_version,
                        event_idx,
                        time,
                    )?);
                } else if event.type_ == place_swap_order_type {
                    place_swap_order_events.push(event_data_to_place_swap_order_event(
                        event,
                        txn_version,
                        event_idx,
                        time,
                    )?);
                }
            }
        }

        conn.build_transaction()
            .read_write()
            .run::<_, Error, _>(|pg_conn| {
                insert_cancel_order_events(pg_conn, cancel_order_events)
            })?;
        conn.build_transaction()
            .read_write()
            .run::<_, Error, _>(|pg_conn| {
                insert_change_order_size_events(pg_conn, change_order_size_events)
            })?;
        conn.build_transaction()
            .read_write()
            .run::<_, Error, _>(|pg_conn| insert_fill_events(pg_conn, fill_events))?;
        conn.build_transaction()
            .read_write()
            .run::<_, Error, _>(|pg_conn| {
                insert_market_registration_events(pg_conn, market_registration_events)
            })?;
        conn.build_transaction()
            .read_write()
            .run::<_, Error, _>(|pg_conn| {
                insert_place_limit_order_events(pg_conn, place_limit_order_events)
            })?;
        conn.build_transaction()
            .read_write()
            .run::<_, Error, _>(|pg_conn| {
                insert_place_market_order_events(pg_conn, place_market_order_events)
            })?;
        conn.build_transaction()
            .read_write()
            .run::<_, Error, _>(|pg_conn| {
                insert_place_swap_order_events(pg_conn, place_swap_order_events)
            })?;

        Ok((start_version, end_version))
    }

    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}
