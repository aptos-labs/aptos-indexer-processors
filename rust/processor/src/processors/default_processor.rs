use super::{ProcessingResult, ProcessorName, ProcessorTrait, token_v2_processor::parse_v2_token};
use crate::{
    models::{default_models::{
        block_metadata_transactions::BlockMetadataTransactionModel,
        move_tables::{CurrentTableItem, TableMetadata},
        transactions::TransactionModel,
        v2_objects::{CurrentObject, Object},
        write_set_changes::{WriteSetChangeDetail},
    }, events_models::events::EventModel, token_models::{tokens::{CurrentTokenOwnershipPK, TokenDataIdHash, CurrentTokenPendingClaimPK, Token, TableMetadataForToken}, token_ownerships::CurrentTokenOwnership, token_claims::CurrentTokenPendingClaim, token_datas::CurrentTokenData, collection_datas::CurrentCollectionData, token_activities::TokenActivity, nft_points::NftPoints}, user_transactions_models::user_transactions::UserTransactionModel},
    schema,
    utils::database::{
        clean_data_for_db, execute_with_better_error, get_chunks, MyDbConnection, PgDbPool,
        PgPoolConnection,
    }, processors::{events_processor::insert_events_to_db, token_processor::insert_tokens_to_db, token_v2_processor::insert_token_v2_to_db, user_transaction_processor::insert_user_transactions_to_db},
};
use aptos_protos::transaction::v1::{write_set_change::Change, Transaction, transaction::TxnData};
use async_trait::async_trait;
use diesel::{result::Error};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Debug};
use tracing::error;


#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DefaultProcessorConfig {
    pub nft_points_contract: Option<String>,
}

pub struct DefaultProcessor {
    connection_pool: PgDbPool,
    config: DefaultProcessorConfig,
}

impl DefaultProcessor {
    pub fn new(connection_pool: PgDbPool, config: DefaultProcessorConfig) -> Self {
        Self {
            connection_pool,
            config,
        }
    }
}

impl Debug for DefaultProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "DefaultTransactionProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

fn fail(start_version: u64, end_version: u64, additional_message: &str, e: Error) {
    error!(
        start_version = start_version,
        end_version = end_version,
        processor_name = "default_processor",
        error = ?e,
        additional_message,
    );
}

async fn insert_to_db_impl(
    conn: &mut MyDbConnection,
    txns: &[TransactionModel],
    block_metadata_transactions: &[BlockMetadataTransactionModel],
) -> Result<(), diesel::result::Error> {
    insert_transactions(conn, txns).await?;
    insert_block_metadata_transactions(conn, block_metadata_transactions).await?;
    Ok(())
}

async fn insert_to_db(
    conn: &mut PgPoolConnection<'_>,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    txns: Vec<TransactionModel>,
    block_metadata_transactions: Vec<BlockMetadataTransactionModel>,
) -> Result<(), diesel::result::Error> {
    tracing::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Inserting to db",
    );
    match conn
        .build_transaction()
        .read_write()
        .run::<_, Error, _>(|pg_conn| {
            Box::pin(insert_to_db_impl(
                pg_conn,
                &txns,
                &block_metadata_transactions,
            ))
        })
        .await
    {
        Ok(_) => Ok(()),
        Err(_) => {
            conn.build_transaction()
                .read_write()
                .run::<_, Error, _>(|pg_conn| {
                    Box::pin(async move {
                        let txns = clean_data_for_db(txns, true);
                        let block_metadata_transactions =
                            clean_data_for_db(block_metadata_transactions, true);
                        insert_to_db_impl(
                            pg_conn,
                            &txns,
                            &block_metadata_transactions,
                        )
                        .await
                    })
                })
                .await
        },
    }
}

async fn insert_transactions(
    conn: &mut MyDbConnection,
    items_to_insert: &[TransactionModel],
) -> Result<(), diesel::result::Error> {
    use schema::transactions::dsl::*;
    let chunks = get_chunks(items_to_insert.len(), TransactionModel::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::transactions::table)
                .values(&items_to_insert[start_ind..end_ind])
                .on_conflict(version)
                .do_nothing(),
            None,
        )
        .await?;
    }
    Ok(())
}

async fn insert_block_metadata_transactions(
    conn: &mut MyDbConnection,
    items_to_insert: &[BlockMetadataTransactionModel],
) -> Result<(), diesel::result::Error> {
    use schema::block_metadata_transactions::dsl::*;
    let chunks = get_chunks(
        items_to_insert.len(),
        BlockMetadataTransactionModel::field_count(),
    );
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::block_metadata_transactions::table)
                .values(&items_to_insert[start_ind..end_ind])
                .on_conflict(version)
                .do_nothing(),
            None,
        )
        .await?;
    }
    Ok(())
}

#[async_trait]
impl ProcessorTrait for DefaultProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::DefaultProcessor.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let mut conn = self.get_conn().await;
        let (txns, block_metadata_txns, _, wsc_details) =
            TransactionModel::from_transactions(&transactions);

        let mut block_metadata_transactions = vec![];
        for block_metadata_txn in block_metadata_txns {
            block_metadata_transactions.push(block_metadata_txn.clone());
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

        let mut all_objects = vec![];
        let mut all_current_objects = HashMap::new();
        let mut events = vec![];
        let mut all_tokens = vec![];
        let mut all_token_ownerships = vec![];
        let mut all_token_datas = vec![];
        let mut all_collection_datas = vec![];
        let mut all_token_activities = vec![];

        let mut all_current_token_ownerships: HashMap<
            CurrentTokenOwnershipPK,
            CurrentTokenOwnership,
        > = HashMap::new();
        let mut all_current_token_datas: HashMap<TokenDataIdHash, CurrentTokenData> =
            HashMap::new();
        let mut all_current_collection_datas: HashMap<TokenDataIdHash, CurrentCollectionData> =
            HashMap::new();
        let mut all_current_token_claims: HashMap<
            CurrentTokenPendingClaimPK,
            CurrentTokenPendingClaim,
        > = HashMap::new();
        let mut all_nft_points = vec![];

        let mut signatures = vec![];
        let mut user_transactions = vec![];

        let table_handle_to_owner = TableMetadataForToken::get_table_handle_to_owner_from_transactions(&transactions);
        
        for txn in &transactions {
            let txn_version = txn.version as i64;
            let block_height = txn.block_height as i64;
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
                        .await
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
            let txn_data = txn.txn_data.as_ref().expect("Txn Data doesn't exit!");
            let default = vec![];
            let raw_events = match txn_data {
                TxnData::BlockMetadata(tx_inner) => &tx_inner.events,
                TxnData::Genesis(tx_inner) => &tx_inner.events,
                TxnData::User(tx_inner) => &tx_inner.events,
                _ => &default,
            };

            let txn_events = EventModel::from_events(raw_events, txn_version, block_height);
            events.extend(txn_events);

            let (
                mut tokens,
                mut token_ownerships,
                mut token_datas,
                mut collection_datas,
                current_token_ownerships,
                current_token_datas,
                current_collection_datas,
                current_token_claims,
            ) = Token::from_transaction(txn, &table_handle_to_owner, &mut conn).await;
            all_tokens.append(&mut tokens);
            all_token_ownerships.append(&mut token_ownerships);
            all_token_datas.append(&mut token_datas);
            all_collection_datas.append(&mut collection_datas);
            // Given versions will always be increasing here (within a single batch), we can just override current values
            all_current_token_ownerships.extend(current_token_ownerships);
            all_current_token_datas.extend(current_token_datas);
            all_current_collection_datas.extend(current_collection_datas);

            // Track token activities
            let mut activities = TokenActivity::from_transaction(txn);
            all_token_activities.append(&mut activities);

            // claims
            all_current_token_claims.extend(current_token_claims);

            // NFT points
            let nft_points_txn =
                NftPoints::from_transaction(txn, self.config.nft_points_contract.clone());
            if let Some(nft_points) = nft_points_txn {
                all_nft_points.push(nft_points);
            }

            if let TxnData::User(inner) = txn_data {
                let (user_transaction, sigs) = UserTransactionModel::from_transaction(
                    inner,
                    &txn.timestamp.as_ref().unwrap(),
                    block_height,
                    txn.epoch as i64,
                    txn_version,
                );
                signatures.extend(sigs);
                user_transactions.push(user_transaction);
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

        let all_current_token_ownerships = all_current_token_ownerships
            .into_values()
            .collect::<Vec<CurrentTokenOwnership>>();
        let all_current_token_datas = all_current_token_datas
            .into_values()
            .collect::<Vec<CurrentTokenData>>();
        let all_current_collection_datas = all_current_collection_datas
            .into_values()
            .collect::<Vec<CurrentCollectionData>>();
        let all_current_token_claims = all_current_token_claims
            .into_values()
            .collect::<Vec<CurrentTokenPendingClaim>>();
        
        let (
            collections_v2,
            token_datas_v2,
            token_ownerships_v2,
            current_collections_v2,
            current_token_ownerships_v2,
            current_token_datas_v2,
            token_activities_v2,
            current_token_v2_metadata,
        ) = parse_v2_token(&transactions, &table_handle_to_owner, &mut conn).await;

        // Transaction metadata 
        match insert_to_db(
            &mut conn,
            self.name(),
            start_version,
            end_version,
            txns,
            block_metadata_transactions,
        ).await {
            Ok(_) => (),
            Err(e) => fail(start_version, end_version, "Error on processing transaction metadata", e),
        };

        // Events 
        match insert_events_to_db(&mut conn, self.name(), start_version, end_version, events).await {
            Ok(_) => (),
            Err(e) => fail(start_version, end_version, "Error on processing events", e),
        };

        // User transactions
        match insert_user_transactions_to_db(
            &mut conn,
            self.name(),
            start_version,
            end_version,
            user_transactions,
            signatures,
        ).await {
            Ok(_) => (),
            Err(e) => fail(start_version, end_version, "Error on processing user transactions", e),
        };

        // Tokens v2
        match insert_token_v2_to_db(
            &mut conn,
            self.name(),
            start_version,
            end_version,
            collections_v2,
            token_datas_v2,
            token_ownerships_v2,
            current_collections_v2,
            current_token_ownerships_v2,
            current_token_datas_v2,
            token_activities_v2,
            current_token_v2_metadata,
        ).await {
            Ok(_) => (),
            Err(e) => fail(start_version, end_version, "Error on processing tokens v2", e),
        };
        
        // Tokens
        match insert_tokens_to_db(
            &mut conn,
            self.name(),
            start_version,
            end_version,
            (
                all_tokens,
                all_token_ownerships,
                all_token_datas,
                all_collection_datas,
            ),
            (
                all_current_token_ownerships,
                all_current_token_datas,
                all_current_collection_datas,
            ),
            all_token_activities,
            all_current_token_claims,
            all_nft_points,
        ).await {
            Ok(_) => (),
            Err(e) => fail(start_version, end_version, "Error on processing tokens v1", e),
        };
        
        Ok(ProcessingResult {
            start_version,
            end_version,
        })
    }


    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}
