use crate::{
    errors::FilterError,
    filters::{
        EventFilter, TransactionRootFilter, UserTransactionRequestFilter, WriteSetChangeFilter,
    },
    traits::Filterable,
};
use aptos_protos::transaction::v1::{transaction::TxnData, Transaction};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// These are filters we would expect to be exposed via API
#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "type")]
pub enum APIFilter {
    TransactionRootFilter(TransactionRootFilter),
    UserTransactionRequestFilter(UserTransactionRequestFilter),
    EventFilter(EventFilter),
    WriteSetChangeFilter(WriteSetChangeFilter),
}

impl Filterable<Transaction> for APIFilter {
    fn validate_state(&self) -> Result<(), FilterError> {
        match self {
            APIFilter::TransactionRootFilter(filter) => filter.is_valid(),
            APIFilter::UserTransactionRequestFilter(filter) => filter.is_valid(),
            APIFilter::EventFilter(filter) => filter.is_valid(),
            APIFilter::WriteSetChangeFilter(filter) => filter.is_valid(),
        }
    }

    fn is_allowed(&self, txn: &Transaction) -> bool {
        match self {
            APIFilter::TransactionRootFilter(filter) => filter.is_allowed(txn),
            APIFilter::UserTransactionRequestFilter(ut_filter) => txn
                .txn_data
                .as_ref()
                .map(|txn_data| {
                    if let TxnData::User(u) = txn_data {
                        u.request
                            .as_ref()
                            .map(|req| ut_filter.is_allowed(req))
                            .unwrap_or(false)
                    } else {
                        false
                    }
                })
                .unwrap_or(false),
            APIFilter::EventFilter(events_filter) => {
                if let Some(txn_data) = &txn.txn_data {
                    let events = match txn_data {
                        TxnData::BlockMetadata(bm) => &bm.events,
                        TxnData::Genesis(g) => &g.events,
                        TxnData::StateCheckpoint(_) => return false,
                        TxnData::User(u) => &u.events,
                        TxnData::Validator(_) => return false,
                    };
                    events_filter.is_allowed_vec(events)
                } else {
                    false
                }
            },
            APIFilter::WriteSetChangeFilter(changes_filter) => {
                changes_filter.is_allowed_opt_vec(&txn.info.as_ref().map(|inf| &inf.changes))
            },
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum FilterOperator {
    And(LogicalAnd),
    Or(LogicalOr),
    Not(LogicalNot),
    Filter(APIFilter),
}

impl FilterOperator {
    pub fn new_and(and: Vec<FilterOperator>) -> Self {
        FilterOperator::And(LogicalAnd { and })
    }

    pub fn new_or(or: Vec<FilterOperator>) -> Self {
        FilterOperator::Or(LogicalOr { or })
    }

    pub fn new_not(not: Vec<FilterOperator>) -> Self {
        FilterOperator::Not(LogicalNot { not })
    }

    pub fn new_filter(filter: APIFilter) -> Self {
        FilterOperator::Filter(filter)
    }
}

impl Filterable<Transaction> for FilterOperator {
    fn validate_state(&self) -> Result<(), FilterError> {
        match self {
            FilterOperator::And(and) => and.is_valid(),
            FilterOperator::Or(or) => or.is_valid(),
            FilterOperator::Not(not) => not.is_valid(),
            FilterOperator::Filter(filter) => filter.is_valid(),
        }
    }

    fn is_allowed(&self, item: &Transaction) -> bool {
        match self {
            FilterOperator::And(and) => and.is_allowed(item),
            FilterOperator::Or(or) => or.is_allowed(item),
            FilterOperator::Not(not) => not.is_allowed(item),
            FilterOperator::Filter(filter) => filter.is_allowed(item),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct LogicalAnd {
    and: Vec<FilterOperator>,
}

impl Filterable<Transaction> for LogicalAnd {
    fn validate_state(&self) -> Result<(), FilterError> {
        for filter in &self.and {
            filter.is_valid()?;
        }
        Ok(())
    }

    fn is_allowed(&self, item: &Transaction) -> bool {
        self.and.iter().all(|filter| filter.is_allowed(item))
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct LogicalOr {
    or: Vec<FilterOperator>,
}

impl Filterable<Transaction> for LogicalOr {
    fn validate_state(&self) -> Result<(), FilterError> {
        for filter in &self.or {
            filter.is_valid()?;
        }
        Ok(())
    }

    fn is_allowed(&self, item: &Transaction) -> bool {
        self.or.iter().any(|filter| filter.is_allowed(item))
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct LogicalNot {
    not: Vec<FilterOperator>,
}

impl Filterable<Transaction> for LogicalNot {
    fn validate_state(&self) -> Result<(), FilterError> {
        for filter in &self.not {
            filter.is_valid()?;
        }
        Ok(())
    }

    fn is_allowed(&self, item: &Transaction) -> bool {
        !self.not.iter().any(|filter| filter.is_allowed(item))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        filters::{
            user_transaction_request::EntryFunctionFilter,
            write_set_change_filter::{
                ChangeItemFilter, ModuleChangeFilter, ResourceChangeFilter, TableChangeFilter,
            },
            MoveStructTagFilter, PositionalFilter, UserTransactionPayloadFilter,
        },
        json_search::{JsonOrStringSearch, JsonSearchTerm},
        test_lib::load_graffio_fixture,
    };

    #[test]
    pub fn test_query_parsing() {
        let trf = TransactionRootFilter {
            success: Some(true),
            txn_type: Some(aptos_protos::transaction::v1::transaction::TransactionType::User),
        };

        let utrf = UserTransactionRequestFilter {
            sender: Some("0x0011".into()),
            payload: Some(UserTransactionPayloadFilter {
                function: Some(EntryFunctionFilter {
                    address: Some("0x001".into()),
                    module: Some("module".into()),
                    function: Some("F".into()),
                }),
                arguments: Some(vec![PositionalFilter {
                    index: 0,
                    value: "0x0011".into(),
                }]),
            }),
        };

        let ef = EventFilter {
            data: Some(JsonSearchTerm::new("spins".into(), 5.into()).unwrap()),
            struct_type: Some(MoveStructTagFilter {
                address: Some("0x0077".into()),
                module: Some("roulette".into()),
                name: Some("spin".into()),
            }),
        };

        let wscf_res = WriteSetChangeFilter {
            change: Some(ChangeItemFilter::ResourceChange(ResourceChangeFilter {
                resource_type: Some(MoveStructTagFilter {
                    address: Some("0x001af32".into()),
                    module: Some("airport".into()),
                    name: Some("airplane".into()),
                }),
                address: Some("0x001af32".into()),
                data: Some(JsonSearchTerm::new("takeoff".into(), true.into()).unwrap()),
            })),
        };
        let wscf_table = WriteSetChangeFilter {
            change: Some(ChangeItemFilter::TableChange(TableChangeFilter {
                handle: Some("0x796857465434253644536475453432453".into()),
                key: Some(JsonOrStringSearch::String("table_key".into())),
                key_type_str: Some("0x423453466345::some_module::SomeStruct".into()),
            })),
        };
        let wscf_mod = WriteSetChangeFilter {
            change: Some(ChangeItemFilter::ModuleChange(ModuleChangeFilter {
                address: Some("0x0000098".into()),
            })),
        };

        let write_set_ors = FilterOperator::new_or(vec![
            FilterOperator::Filter(APIFilter::WriteSetChangeFilter(wscf_res)),
            FilterOperator::Filter(APIFilter::WriteSetChangeFilter(wscf_table)),
            FilterOperator::Filter(APIFilter::WriteSetChangeFilter(wscf_mod)),
        ]);

        let event_filter_or_write_set = FilterOperator::new_or(vec![
            FilterOperator::Filter(APIFilter::EventFilter(ef)),
            write_set_ors,
        ]);

        let transaction_root_and_request_filter = FilterOperator::new_or(vec![
            FilterOperator::Filter(APIFilter::TransactionRootFilter(trf)),
            FilterOperator::Filter(APIFilter::UserTransactionRequestFilter(utrf)),
        ]);

        let query = FilterOperator::new_or(vec![
            transaction_root_and_request_filter,
            event_filter_or_write_set,
        ]);

        println!(
            "JSON RESULT: \n {}",
            serde_json::to_string_pretty(&query).unwrap()
        );

        let txns = load_graffio_fixture();

        // Benchmark how long it takes to do this 100 times
        let start = std::time::Instant::now();
        const LOOPS: i32 = 1000;
        for _ in 0..LOOPS {
            for txn in &txns.transactions {
                query.is_allowed(txn);
            }
        }
        let elapsed = start.elapsed();

        let total_txn = LOOPS * txns.transactions.len() as i32;
        println!(
            "BENCH: Took {:?} for {} transactions ({:?} each)",
            elapsed,
            total_txn,
            elapsed / total_txn as u32
        );

        let ef_econia = EventFilter {
            data: None,
            struct_type: Some(MoveStructTagFilter {
                address: Some("0x00ECONIA".into()),
                module: None,
                name: None,
            }),
        };
        let ef_aries = EventFilter {
            data: None,
            struct_type: Some(MoveStructTagFilter {
                address: Some("0x00ARIES".into()),
                module: None,
                name: None,
            }),
        };
        let query = FilterOperator::new_or(vec![
            FilterOperator::Filter(APIFilter::EventFilter(ef_econia)),
            FilterOperator::Filter(APIFilter::EventFilter(ef_aries)),
        ]);
        println!(
            "JSON RESULT: \n {}",
            serde_json::to_string_pretty(&query).unwrap()
        );

        //println!("Filter result for u32: {}", filter.is_allowed(&item_u32)); // true
        //println!("Filter result for String: {}", filter.is_allowed(&item_s)); // false
    }
}
