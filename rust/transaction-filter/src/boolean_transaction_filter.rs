use crate::{
    errors::FilterError,
    filters::{EventFilter, TransactionRootFilter, UserTransactionFilter},
    traits::Filterable,
};
use aptos_protos::transaction::v1::{transaction::TxnData, Transaction};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// BooleanTransactionFilter is the top level filter

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum BooleanTransactionFilter {
    And(LogicalAnd),
    Or(LogicalOr),
    Not(LogicalNot),
    Filter(APIFilter),
}

impl From<APIFilter> for BooleanTransactionFilter {
    fn from(filter: APIFilter) -> Self {
        BooleanTransactionFilter::Filter(filter)
    }
}

impl From<TransactionRootFilter> for BooleanTransactionFilter {
    fn from(filter: TransactionRootFilter) -> Self {
        BooleanTransactionFilter::Filter(APIFilter::TransactionRootFilter(filter))
    }
}

impl From<UserTransactionFilter> for BooleanTransactionFilter {
    fn from(filter: UserTransactionFilter) -> Self {
        BooleanTransactionFilter::Filter(APIFilter::UserTransactionFilter(filter))
    }
}

impl From<EventFilter> for BooleanTransactionFilter {
    fn from(filter: EventFilter) -> Self {
        BooleanTransactionFilter::Filter(APIFilter::EventFilter(filter))
    }
}

impl BooleanTransactionFilter {
    pub fn and<Other: Into<BooleanTransactionFilter>>(self, other: Other) -> Self {
        BooleanTransactionFilter::And(LogicalAnd {
            and: vec![self, other.into()],
        })
    }

    pub fn or<Other: Into<BooleanTransactionFilter>>(self, other: Other) -> Self {
        BooleanTransactionFilter::Or(LogicalOr {
            or: vec![self, other.into()],
        })
    }

    #[allow(clippy::should_implement_trait)]
    pub fn not(self) -> Self {
        BooleanTransactionFilter::Not(LogicalNot {
            not: Box::new(self),
        })
    }

    pub fn new_or(or: Vec<BooleanTransactionFilter>) -> Self {
        BooleanTransactionFilter::Or(LogicalOr { or })
    }

    pub fn new_not(not: BooleanTransactionFilter) -> Self {
        BooleanTransactionFilter::Not(LogicalNot { not: Box::new(not) })
    }

    pub fn new_filter(filter: APIFilter) -> Self {
        BooleanTransactionFilter::Filter(filter)
    }
}

impl Filterable<Transaction> for BooleanTransactionFilter {
    fn validate_state(&self) -> Result<(), FilterError> {
        match self {
            BooleanTransactionFilter::And(and) => and.is_valid(),
            BooleanTransactionFilter::Or(or) => or.is_valid(),
            BooleanTransactionFilter::Not(not) => not.is_valid(),
            BooleanTransactionFilter::Filter(filter) => filter.is_valid(),
        }
    }

    fn is_allowed(&self, item: &Transaction) -> bool {
        match self {
            BooleanTransactionFilter::And(and) => and.is_allowed(item),
            BooleanTransactionFilter::Or(or) => or.is_allowed(item),
            BooleanTransactionFilter::Not(not) => not.is_allowed(item),
            BooleanTransactionFilter::Filter(filter) => filter.is_allowed(item),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct LogicalAnd {
    and: Vec<BooleanTransactionFilter>,
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
    or: Vec<BooleanTransactionFilter>,
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
    not: Box<BooleanTransactionFilter>,
}

impl Filterable<Transaction> for LogicalNot {
    fn validate_state(&self) -> Result<(), FilterError> {
        self.not.is_valid()
    }

    fn is_allowed(&self, item: &Transaction) -> bool {
        !self.not.is_allowed(item)
    }
}

/// These are filters we would expect to be exposed via API
#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "type")]
pub enum APIFilter {
    TransactionRootFilter(TransactionRootFilter),
    UserTransactionFilter(UserTransactionFilter),
    EventFilter(EventFilter),
}

impl From<TransactionRootFilter> for APIFilter {
    fn from(filter: TransactionRootFilter) -> Self {
        APIFilter::TransactionRootFilter(filter)
    }
}

impl From<UserTransactionFilter> for APIFilter {
    fn from(filter: UserTransactionFilter) -> Self {
        APIFilter::UserTransactionFilter(filter)
    }
}

impl From<EventFilter> for APIFilter {
    fn from(filter: EventFilter) -> Self {
        APIFilter::EventFilter(filter)
    }
}

impl Filterable<Transaction> for APIFilter {
    fn validate_state(&self) -> Result<(), FilterError> {
        match self {
            APIFilter::TransactionRootFilter(filter) => filter.is_valid(),
            APIFilter::UserTransactionFilter(filter) => filter.is_valid(),
            APIFilter::EventFilter(filter) => filter.is_valid(),
        }
    }

    fn is_allowed(&self, txn: &Transaction) -> bool {
        match self {
            APIFilter::TransactionRootFilter(filter) => filter.is_allowed(txn),
            APIFilter::UserTransactionFilter(ut_filter) => ut_filter.is_allowed(txn),
            APIFilter::EventFilter(events_filter) => {
                if let Some(txn_data) = &txn.txn_data {
                    let events = match txn_data {
                        TxnData::BlockMetadata(bm) => &bm.events,
                        TxnData::Genesis(g) => &g.events,
                        TxnData::BlockEpilogue(_) => return false,
                        TxnData::StateCheckpoint(_) => return false,
                        TxnData::User(u) => &u.events,
                        TxnData::Validator(_) => return false,
                    };
                    events_filter.is_allowed_vec(events)
                } else {
                    false
                }
            },
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        filters::{
            event::EventFilterBuilder, move_module::MoveStructTagFilterBuilder,
            user_transaction::EntryFunctionFilter, TransactionRootFilterBuilder,
            UserTransactionFilterBuilder, UserTransactionPayloadFilterBuilder,
        },
        test_lib::load_graffio_fixture,
    };

    #[test]
    pub fn test_query_parsing() {
        let trf = TransactionRootFilter {
            success: Some(true),
            txn_type: Some(aptos_protos::transaction::v1::transaction::TransactionType::User),
        };

        let utrf = UserTransactionFilterBuilder::default()
            .sender("0x0011")
            .payload(
                UserTransactionPayloadFilterBuilder::default()
                    .function(EntryFunctionFilter {
                        address: Some("0x007".into()),
                        module: Some("roulette".into()),
                        function: None,
                    })
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        let ef = EventFilterBuilder::default()
            .struct_type(
                MoveStructTagFilterBuilder::default()
                    .address("0x0077")
                    .module("roulette")
                    .name("spin")
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        // (trf OR utrf)
        let trf_or_utrf = BooleanTransactionFilter::from(trf).or(utrf);
        // ((trf OR utrf) AND ef)
        let query = trf_or_utrf.and(ef);

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

        let ef_econia = EventFilterBuilder::default()
            .struct_type(
                MoveStructTagFilterBuilder::default()
                    .address("0x00ECONIA")
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        let ef_aries = EventFilterBuilder::default()
            .struct_type(
                MoveStructTagFilterBuilder::default()
                    .address("0x00ARIES")
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        let query = BooleanTransactionFilter::from(ef_econia).or(ef_aries);
        println!(
            "JSON RESULT: \n {}",
            serde_json::to_string_pretty(&query).unwrap()
        );
    }

    #[test]
    fn test_serialization() {
        let trf = TransactionRootFilterBuilder::default()
            .success(true)
            .build()
            .unwrap();

        let utrf = UserTransactionFilterBuilder::default()
            .sender("0x0011")
            .build()
            .unwrap();

        let ef = EventFilterBuilder::default()
            .struct_type(
                MoveStructTagFilterBuilder::default()
                    .address("0x0077")
                    .module("roulette")
                    .name("spin")
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        // Combine filters using logical operators!
        // (trf OR utrf)
        let trf_or_utrf = BooleanTransactionFilter::from(trf).or(utrf);
        // ((trf OR utrf) AND ef)
        let query = trf_or_utrf.and(ef);

        let yaml = serde_yaml::to_string(&query).unwrap();
        println!("YAML: \n{}", yaml);
    }
}
