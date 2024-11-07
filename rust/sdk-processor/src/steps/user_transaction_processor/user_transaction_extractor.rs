use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::{transaction::TxnData, Transaction},
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use processor::{
    db::common::models::user_transactions_models::{
        signatures::Signature, user_transactions::UserTransactionModel,
    },
    utils::counters::PROCESSOR_UNKNOWN_TYPE_COUNT,
    worker::TableFlags,
};
pub struct UserTransactionExtractor
where
    Self: Sized + Send + 'static,
{
    deprecated_tables: TableFlags,
}

impl UserTransactionExtractor {
    pub fn new(deprecated_tables: TableFlags) -> Self {
        Self { deprecated_tables }
    }
}

#[async_trait]
impl Processable for UserTransactionExtractor {
    type Input = Vec<Transaction>;
    type Output = (Vec<UserTransactionModel>, Vec<Signature>);
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        item: TransactionContext<Vec<Transaction>>,
    ) -> Result<
        Option<TransactionContext<(Vec<UserTransactionModel>, Vec<Signature>)>>,
        ProcessorError,
    > {
        let mut signatures = vec![];
        let mut user_transactions = vec![];
        for txn in item.data {
            let txn_version = txn.version as i64;
            let block_height = txn.block_height as i64;
            let txn_data = match txn.txn_data.as_ref() {
                Some(txn_data) => txn_data,
                None => {
                    PROCESSOR_UNKNOWN_TYPE_COUNT
                        .with_label_values(&["UserTransactionProcessor"])
                        .inc();
                    tracing::warn!(
                        transaction_version = txn_version,
                        "Transaction data doesn't exist"
                    );
                    continue;
                },
            };
            if let TxnData::User(inner) = txn_data {
                let (user_transaction, sigs) = UserTransactionModel::from_transaction(
                    inner,
                    txn.timestamp.as_ref().unwrap(),
                    block_height,
                    txn.epoch as i64,
                    txn_version,
                );
                signatures.extend(sigs);
                user_transactions.push(user_transaction);
            }
        }

        if self.deprecated_tables.contains(TableFlags::SIGNATURES) {
            signatures.clear();
        }

        Ok(Some(TransactionContext {
            data: (user_transactions, signatures),
            metadata: item.metadata,
        }))
    }
}

impl AsyncStep for UserTransactionExtractor {}

impl NamedStep for UserTransactionExtractor {
    fn name(&self) -> String {
        "UserTransactionExtractor".to_string()
    }
}
