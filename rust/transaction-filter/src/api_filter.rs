// use crate::traits::Filterable;
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
/// If any of these filters match, the transaction returns true
#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PublicOrApiFilter {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_filter: Option<TransactionRootFilter>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_transaction_filter: Option<UserTransactionRequestFilter>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_filter: Option<Vec<EventFilter>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_set_change_filter: Option<Vec<WriteSetChangeFilter>>,
}

impl Filterable<Transaction> for PublicOrApiFilter {
    fn validate_state(&self) -> Result<(), FilterError> {
        if self.root_filter.is_none()
            && self.user_transaction_filter.is_none()
            && self.event_filter.is_none()
            && self.write_set_change_filter.is_none()
        {
            return Err(anyhow::anyhow!(
                        "At least one of root_filter, user_transaction_filter, event_filter, or write_set_change_filter must be set"
                ).into());
        };

        self.root_filter.is_valid()?;
        self.user_transaction_filter.is_valid()?;
        if let Some(event_filters) = &self.event_filter {
            for event_filter in event_filters {
                event_filter.is_valid()?;
            }
        }
        if let Some(write_set_change_filters) = &self.write_set_change_filter {
            for write_set_change_filter in write_set_change_filters {
                write_set_change_filter.is_valid()?;
            }
        }

        Ok(())
    }

    fn is_allowed(&self, txn: &Transaction) -> bool {
        if self.root_filter.is_allowed(txn) {
            return true;
        }

        if let Some(ut_filter) = &self.user_transaction_filter {
            let txn_filter_res = txn.txn_data.as_ref().map(|txn_data| {
                if let TxnData::User(u) = txn_data {
                    u.request
                        .as_ref()
                        .map(|req| ut_filter.is_allowed(req))
                        .unwrap_or(false)
                } else {
                    false
                }
            });

            if let Some(txn_filter_res) = txn_filter_res {
                if txn_filter_res {
                    return true;
                }
            }
        }

        if let Some(events_filter) = &self.event_filter {
            if let Some(txn_data) = &txn.txn_data {
                let events = match txn_data {
                    TxnData::BlockMetadata(bm) => Some(&bm.events),
                    TxnData::Genesis(g) => Some(&g.events),
                    TxnData::StateCheckpoint(_) => None,
                    TxnData::User(u) => Some(&u.events),
                    TxnData::Validator(_) => None,
                };
                if let Some(events) = events {
                    for event_filter in events_filter {
                        if event_filter.is_allowed_vec(events) {
                            return true;
                        }
                    }
                }
            }
        }

        if let Some(changes_filter) = &self.write_set_change_filter {
            let changes = &txn.info.as_ref().map(|inf| &inf.changes);
            for change_filter in changes_filter {
                if change_filter.is_allowed_opt_vec(changes) {
                    return true;
                }
            }
        }

        false
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
        test_lib::{load_graffio_fixture, load_random_april_3mb_fixture, load_taptos_fixture},
    };
    use aptos_protos::indexer::v1::TransactionsInStorage;

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

        let ef1 = EventFilter {
            data: Some(JsonSearchTerm::new("spins".into(), 5.into()).unwrap()),
            struct_type: Some(MoveStructTagFilter {
                address: Some("0x0077".into()),
                module: Some("roulette".into()),
                name: Some("spin".into()),
            }),
        };

        let ef2 = EventFilter {
            data: Some(JsonSearchTerm::new("debt".into(), 12.into()).unwrap()),
            struct_type: Some(MoveStructTagFilter {
                address: Some("0x0052".into()),
                module: Some("lending".into()),
                name: Some("borrow_to_gamble".into()),
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

        let query = PublicOrApiFilter {
            root_filter: Some(trf),
            user_transaction_filter: Some(utrf),
            event_filter: Some(vec![ef1, ef2]),
            write_set_change_filter: Some(vec![wscf_res, wscf_table, wscf_mod]),
        };

        let tapos_txns = load_taptos_fixture();
        let random3mb_txns = load_random_april_3mb_fixture();
        let graffio_txns = load_graffio_fixture();

        test_filter(&query, &tapos_txns, "graffio");
        test_filter(&query, &random3mb_txns, "random3mb");
        test_filter(&query, &graffio_txns, "tapos");
    }

    fn test_filter(query: &PublicOrApiFilter, txns: &TransactionsInStorage, set_name: &str) {
        println!(
            "SET {}:> Json Query Representation: \n {}",
            set_name,
            serde_json::to_string_pretty(query).unwrap()
        );
        const LOOPS: usize = 1000;
        let start = std::time::Instant::now();
        for _ in 0..LOOPS {
            for txn in &txns.transactions {
                query.is_allowed(txn);
            }
        }
        let elapsed = start.elapsed();
        let total_txn = LOOPS * txns.transactions.len();
        println!(
            "BENCH SET {}:> Took {:?} for {} transactions ({:?} each)\n\n",
            set_name,
            elapsed,
            total_txn,
            elapsed / total_txn as u32
        );
    }
}
