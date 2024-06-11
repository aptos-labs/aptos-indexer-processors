// @generated automatically by Diesel CLI.

diesel::table! {
    account_transactions (account_address, transaction_version) {
        transaction_version -> Int8,
        #[max_length = 0]
        account_address -> Varchar,
        inserted_at -> Timestamp,
        rowid -> Int8,
    }
}

diesel::table! {
    ans_lookup_v2 (transaction_version, write_set_change_index) {
        transaction_version -> Int8,
        write_set_change_index -> Int8,
        #[max_length = 0]
        domain -> Varchar,
        #[max_length = 0]
        subdomain -> Varchar,
        #[max_length = 0]
        token_standard -> Varchar,
        #[max_length = 0]
        registered_address -> Nullable<Varchar>,
        expiration_timestamp -> Nullable<Timestamp>,
        #[max_length = 0]
        token_name -> Varchar,
        is_deleted -> Bool,
        inserted_at -> Timestamp,
        subdomain_expiration_policy -> Nullable<Int8>,
        rowid -> Int8,
    }
}

diesel::table! {
    block_metadata_transactions (version) {
        version -> Int8,
        block_height -> Int8,
        #[max_length = 0]
        id -> Varchar,
        round -> Int8,
        epoch -> Int8,
        previous_block_votes_bitvec -> Jsonb,
        #[max_length = 0]
        proposer -> Varchar,
        failed_proposer_indices -> Jsonb,
        timestamp -> Timestamp,
        inserted_at -> Timestamp,
        rowid -> Int8,
    }
}

diesel::table! {
    coin_infos (coin_type_hash) {
        #[max_length = 0]
        coin_type_hash -> Varchar,
        #[max_length = 0]
        coin_type -> Varchar,
        transaction_version_created -> Int8,
        #[max_length = 0]
        creator_address -> Varchar,
        #[max_length = 0]
        name -> Varchar,
        #[max_length = 0]
        symbol -> Varchar,
        decimals -> Int8,
        transaction_created_timestamp -> Timestamp,
        inserted_at -> Timestamp,
        #[max_length = 0]
        supply_aggregator_table_handle -> Nullable<Varchar>,
        supply_aggregator_table_key -> Nullable<Text>,
        rowid -> Int8,
    }
}

diesel::table! {
    current_ans_lookup_v2 (domain, subdomain, token_standard) {
        #[max_length = 0]
        domain -> Varchar,
        #[max_length = 0]
        subdomain -> Varchar,
        #[max_length = 0]
        token_standard -> Varchar,
        #[max_length = 0]
        token_name -> Nullable<Varchar>,
        #[max_length = 0]
        registered_address -> Nullable<Varchar>,
        expiration_timestamp -> Timestamp,
        last_transaction_version -> Int8,
        is_deleted -> Bool,
        inserted_at -> Timestamp,
        subdomain_expiration_policy -> Nullable<Int8>,
        rowid -> Int8,
    }
}

diesel::table! {
    current_ans_primary_name_v2 (registered_address, token_standard) {
        #[max_length = 0]
        registered_address -> Varchar,
        #[max_length = 0]
        token_standard -> Varchar,
        #[max_length = 0]
        domain -> Nullable<Varchar>,
        #[max_length = 0]
        subdomain -> Nullable<Varchar>,
        #[max_length = 0]
        token_name -> Nullable<Varchar>,
        is_deleted -> Bool,
        last_transaction_version -> Int8,
        inserted_at -> Timestamp,
        rowid -> Int8,
    }
}

diesel::table! {
    current_coin_balances (rowid) {
        #[max_length = 0]
        owner_address -> Varchar,
        #[max_length = 0]
        coin_type_hash -> Varchar,
        #[max_length = 0]
        coin_type -> Varchar,
        amount -> Numeric,
        last_transaction_version -> Int8,
        last_transaction_timestamp -> Timestamp,
        inserted_at -> Timestamp,
        rowid -> Int8,
    }
}

diesel::table! {
    current_collection_datas (collection_data_id_hash) {
        #[max_length = 0]
        collection_data_id_hash -> Varchar,
        #[max_length = 0]
        creator_address -> Varchar,
        #[max_length = 0]
        collection_name -> Varchar,
        description -> Text,
        #[max_length = 0]
        metadata_uri -> Varchar,
        supply -> Numeric,
        maximum -> Numeric,
        maximum_mutable -> Bool,
        uri_mutable -> Bool,
        description_mutable -> Bool,
        last_transaction_version -> Int8,
        inserted_at -> Timestamp,
        #[max_length = 0]
        table_handle -> Varchar,
        last_transaction_timestamp -> Timestamp,
        rowid -> Int8,
    }
}

diesel::table! {
    current_collections_v2 (collection_id) {
        #[max_length = 0]
        collection_id -> Varchar,
        #[max_length = 0]
        creator_address -> Varchar,
        #[max_length = 0]
        collection_name -> Varchar,
        description -> Text,
        #[max_length = 0]
        uri -> Varchar,
        current_supply -> Numeric,
        max_supply -> Nullable<Numeric>,
        total_minted_v2 -> Nullable<Numeric>,
        mutable_description -> Nullable<Bool>,
        mutable_uri -> Nullable<Bool>,
        #[max_length = 0]
        table_handle_v1 -> Nullable<Varchar>,
        #[max_length = 0]
        token_standard -> Varchar,
        last_transaction_version -> Int8,
        last_transaction_timestamp -> Timestamp,
        inserted_at -> Timestamp,
        rowid -> Int8,
    }
}

diesel::table! {
    current_delegated_staking_pool_balances (staking_pool_address) {
        #[max_length = 0]
        staking_pool_address -> Varchar,
        total_coins -> Numeric,
        total_shares -> Numeric,
        last_transaction_version -> Int8,
        inserted_at -> Timestamp,
        operator_commission_percentage -> Numeric,
        #[max_length = 0]
        inactive_table_handle -> Varchar,
        #[max_length = 0]
        active_table_handle -> Varchar,
        rowid -> Int8,
    }
}

diesel::table! {
    current_delegated_voter (delegation_pool_address, delegator_address) {
        #[max_length = 0]
        delegation_pool_address -> Varchar,
        #[max_length = 0]
        delegator_address -> Varchar,
        #[max_length = 0]
        table_handle -> Nullable<Varchar>,
        #[max_length = 0]
        voter -> Nullable<Varchar>,
        #[max_length = 0]
        pending_voter -> Nullable<Varchar>,
        last_transaction_version -> Int8,
        last_transaction_timestamp -> Timestamp,
        inserted_at -> Timestamp,
        rowid -> Int8,
    }
}

diesel::table! {
    current_delegator_balances (delegator_address, pool_address, pool_type, table_handle) {
        #[max_length = 0]
        delegator_address -> Varchar,
        #[max_length = 0]
        pool_address -> Varchar,
        #[max_length = 0]
        pool_type -> Varchar,
        #[max_length = 0]
        table_handle -> Varchar,
        last_transaction_version -> Int8,
        inserted_at -> Timestamp,
        shares -> Numeric,
        #[max_length = 0]
        parent_table_handle -> Varchar,
        rowid -> Int8,
    }
}

diesel::table! {
    current_fungible_asset_balances (storage_id) {
        #[max_length = 0]
        storage_id -> Varchar,
        #[max_length = 0]
        owner_address -> Varchar,
        #[max_length = 0]
        asset_type -> Varchar,
        is_primary -> Bool,
        is_frozen -> Bool,
        amount -> Numeric,
        last_transaction_timestamp -> Timestamp,
        last_transaction_version -> Int8,
        #[max_length = 0]
        token_standard -> Varchar,
        inserted_at -> Timestamp,
        rowid -> Int8,
    }
}

diesel::table! {
    current_objects (object_address) {
        #[max_length = 0]
        object_address -> Varchar,
        #[max_length = 0]
        owner_address -> Varchar,
        #[max_length = 0]
        state_key_hash -> Varchar,
        allow_ungated_transfer -> Bool,
        last_guid_creation_num -> Numeric,
        last_transaction_version -> Int8,
        is_deleted -> Bool,
        inserted_at -> Timestamp,
        rowid -> Int8,
    }
}

diesel::table! {
    current_staking_pool_voter (staking_pool_address) {
        #[max_length = 0]
        staking_pool_address -> Varchar,
        #[max_length = 0]
        voter_address -> Varchar,
        last_transaction_version -> Int8,
        inserted_at -> Timestamp,
        #[max_length = 0]
        operator_address -> Varchar,
        rowid -> Int8,
    }
}

diesel::table! {
    current_table_items (table_handle, key_hash) {
        #[max_length = 0]
        table_handle -> Varchar,
        #[max_length = 0]
        key_hash -> Varchar,
        key -> Text,
        decoded_key -> Jsonb,
        decoded_value -> Nullable<Jsonb>,
        is_deleted -> Bool,
        last_transaction_version -> Int8,
        inserted_at -> Timestamp,
        rowid -> Int8,
    }
}

diesel::table! {
    current_token_datas_v2 (token_data_id) {
        #[max_length = 0]
        token_data_id -> Varchar,
        #[max_length = 0]
        collection_id -> Varchar,
        #[max_length = 0]
        token_name -> Varchar,
        maximum -> Nullable<Numeric>,
        supply -> Nullable<Numeric>,
        largest_property_version_v1 -> Nullable<Numeric>,
        #[max_length = 0]
        token_uri -> Varchar,
        description -> Text,
        token_properties -> Jsonb,
        #[max_length = 0]
        token_standard -> Varchar,
        is_fungible_v2 -> Nullable<Bool>,
        last_transaction_version -> Int8,
        last_transaction_timestamp -> Timestamp,
        inserted_at -> Timestamp,
        decimals -> Nullable<Int8>,
        is_deleted_v2 -> Nullable<Bool>,
        rowid -> Int8,
    }
}

diesel::table! {
    current_token_ownerships_v2 (token_data_id, property_version_v1, owner_address, storage_id) {
        #[max_length = 0]
        token_data_id -> Varchar,
        property_version_v1 -> Numeric,
        #[max_length = 0]
        owner_address -> Varchar,
        #[max_length = 0]
        storage_id -> Varchar,
        amount -> Numeric,
        #[max_length = 0]
        table_type_v1 -> Nullable<Varchar>,
        token_properties_mutated_v1 -> Nullable<Jsonb>,
        is_soulbound_v2 -> Nullable<Bool>,
        #[max_length = 0]
        token_standard -> Varchar,
        is_fungible_v2 -> Nullable<Bool>,
        last_transaction_version -> Int8,
        last_transaction_timestamp -> Timestamp,
        inserted_at -> Timestamp,
        non_transferrable_by_owner -> Nullable<Bool>,
        rowid -> Int8,
    }
}

diesel::table! {
    current_token_pending_claims (token_data_id_hash, property_version, from_address, to_address) {
        #[max_length = 0]
        token_data_id_hash -> Varchar,
        property_version -> Numeric,
        #[max_length = 0]
        from_address -> Varchar,
        #[max_length = 0]
        to_address -> Varchar,
        #[max_length = 0]
        collection_data_id_hash -> Varchar,
        #[max_length = 0]
        creator_address -> Varchar,
        #[max_length = 0]
        collection_name -> Varchar,
        #[max_length = 0]
        name -> Varchar,
        amount -> Numeric,
        #[max_length = 0]
        table_handle -> Varchar,
        last_transaction_version -> Int8,
        inserted_at -> Timestamp,
        last_transaction_timestamp -> Timestamp,
        #[max_length = 0]
        token_data_id -> Varchar,
        #[max_length = 0]
        collection_id -> Varchar,
        rowid -> Int8,
    }
}

diesel::table! {
    current_token_royalty_v1 (token_data_id) {
        #[max_length = 0]
        token_data_id -> Varchar,
        #[max_length = 0]
        payee_address -> Varchar,
        royalty_points_numerator -> Numeric,
        royalty_points_denominator -> Numeric,
        last_transaction_version -> Int8,
        last_transaction_timestamp -> Timestamp,
        inserted_at -> Timestamp,
        rowid -> Int8,
    }
}

diesel::table! {
    current_unified_fungible_asset_balances (storage_id) {
        #[max_length = 0]
        storage_id -> Varchar,
        #[max_length = 0]
        owner_address -> Varchar,
        #[max_length = 0]
        asset_type -> Varchar,
        #[max_length = 0]
        coin_type -> Nullable<Varchar>,
        is_primary -> Nullable<Bool>,
        is_frozen -> Bool,
        amount_v1 -> Nullable<Numeric>,
        amount_v2 -> Nullable<Numeric>,
        amount -> Nullable<Numeric>,
        last_transaction_version_v1 -> Nullable<Int8>,
        last_transaction_version_v2 -> Nullable<Int8>,
        last_transaction_version -> Nullable<Int8>,
        last_transaction_timestamp_v1 -> Nullable<Timestamp>,
        last_transaction_timestamp_v2 -> Nullable<Timestamp>,
        last_transaction_timestamp -> Nullable<Timestamp>,
        inserted_at -> Timestamp,
        rowid -> Int8,
    }
}

diesel::table! {
    delegated_staking_activities (transaction_version, event_index) {
        transaction_version -> Int8,
        event_index -> Int8,
        #[max_length = 0]
        delegator_address -> Varchar,
        #[max_length = 0]
        pool_address -> Varchar,
        event_type -> Text,
        amount -> Numeric,
        inserted_at -> Timestamp,
        rowid -> Int8,
    }
}

diesel::table! {
    delegated_staking_pool_balances (transaction_version, staking_pool_address) {
        transaction_version -> Int8,
        #[max_length = 0]
        staking_pool_address -> Varchar,
        total_coins -> Numeric,
        total_shares -> Numeric,
        inserted_at -> Timestamp,
        operator_commission_percentage -> Numeric,
        #[max_length = 0]
        inactive_table_handle -> Varchar,
        #[max_length = 0]
        active_table_handle -> Varchar,
        rowid -> Int8,
    }
}

diesel::table! {
    delegated_staking_pools (staking_pool_address) {
        #[max_length = 0]
        staking_pool_address -> Varchar,
        first_transaction_version -> Int8,
        inserted_at -> Timestamp,
        rowid -> Int8,
    }
}

diesel::table! {
    delegator_balances (transaction_version, write_set_change_index) {
        transaction_version -> Int8,
        write_set_change_index -> Int8,
        #[max_length = 0]
        delegator_address -> Varchar,
        #[max_length = 0]
        pool_address -> Varchar,
        #[max_length = 0]
        pool_type -> Varchar,
        #[max_length = 0]
        table_handle -> Varchar,
        shares -> Numeric,
        #[max_length = 0]
        parent_table_handle -> Varchar,
        inserted_at -> Timestamp,
        rowid -> Int8,
    }
}

diesel::table! {
    events (transaction_version, event_index) {
        sequence_number -> Int8,
        creation_number -> Int8,
        #[max_length = 0]
        account_address -> Varchar,
        transaction_version -> Int8,
        transaction_block_height -> Int8,
        #[sql_name = "type"]
        type_ -> Text,
        data -> Jsonb,
        inserted_at -> Timestamp,
        event_index -> Int8,
        #[max_length = 0]
        indexed_type -> Varchar,
        rowid -> Int8,
    }
}

diesel::table! {
    fungible_asset_activities (transaction_version, event_index) {
        transaction_version -> Int8,
        event_index -> Int8,
        #[max_length = 0]
        owner_address -> Varchar,
        #[max_length = 0]
        storage_id -> Varchar,
        #[max_length = 0]
        asset_type -> Varchar,
        is_frozen -> Nullable<Bool>,
        amount -> Nullable<Numeric>,
        #[sql_name = "type"]
        type_ -> Varchar,
        is_gas_fee -> Bool,
        #[max_length = 0]
        gas_fee_payer_address -> Nullable<Varchar>,
        is_transaction_success -> Bool,
        #[max_length = 0]
        entry_function_id_str -> Nullable<Varchar>,
        block_height -> Int8,
        #[max_length = 0]
        token_standard -> Varchar,
        transaction_timestamp -> Timestamp,
        inserted_at -> Timestamp,
        storage_refund_amount -> Numeric,
        rowid -> Int8,
    }
}

diesel::table! {
    fungible_asset_metadata (asset_type) {
        #[max_length = 0]
        asset_type -> Varchar,
        #[max_length = 0]
        creator_address -> Varchar,
        #[max_length = 0]
        name -> Varchar,
        #[max_length = 0]
        symbol -> Varchar,
        decimals -> Int8,
        #[max_length = 0]
        icon_uri -> Nullable<Varchar>,
        #[max_length = 0]
        project_uri -> Nullable<Varchar>,
        last_transaction_version -> Int8,
        last_transaction_timestamp -> Timestamp,
        #[max_length = 0]
        supply_aggregator_table_handle_v1 -> Nullable<Varchar>,
        supply_aggregator_table_key_v1 -> Nullable<Text>,
        #[max_length = 0]
        token_standard -> Varchar,
        inserted_at -> Timestamp,
        is_token_v2 -> Nullable<Bool>,
        supply_v2 -> Nullable<Numeric>,
        maximum_v2 -> Nullable<Numeric>,
        rowid -> Int8,
    }
}

diesel::table! {
    ledger_infos (chain_id) {
        chain_id -> Int8,
        rowid -> Int8,
    }
}

diesel::table! {
    processor_status (processor) {
        #[max_length = 0]
        processor -> Varchar,
        last_success_version -> Int8,
        last_updated -> Timestamp,
        last_transaction_timestamp -> Nullable<Timestamp>,
        rowid -> Int8,
    }
}

diesel::table! {
    proposal_votes (transaction_version, proposal_id, voter_address) {
        transaction_version -> Int8,
        proposal_id -> Int8,
        #[max_length = 0]
        voter_address -> Varchar,
        #[max_length = 0]
        staking_pool_address -> Varchar,
        num_votes -> Numeric,
        should_pass -> Bool,
        transaction_timestamp -> Timestamp,
        inserted_at -> Timestamp,
        rowid -> Int8,
    }
}

diesel::table! {
    spam_assets (asset) {
        #[max_length = 0]
        asset -> Varchar,
        is_spam -> Bool,
        last_updated -> Timestamp,
        rowid -> Int8,
    }
}

diesel::table! {
    token_activities_v2 (transaction_version, event_index) {
        transaction_version -> Int8,
        event_index -> Int8,
        #[max_length = 0]
        event_account_address -> Varchar,
        #[max_length = 0]
        token_data_id -> Varchar,
        property_version_v1 -> Numeric,
        #[sql_name = "type"]
        type_ -> Varchar,
        #[max_length = 0]
        from_address -> Nullable<Varchar>,
        #[max_length = 0]
        to_address -> Nullable<Varchar>,
        token_amount -> Numeric,
        before_value -> Nullable<Text>,
        after_value -> Nullable<Text>,
        #[max_length = 0]
        entry_function_id_str -> Nullable<Varchar>,
        #[max_length = 0]
        token_standard -> Varchar,
        is_fungible_v2 -> Nullable<Bool>,
        transaction_timestamp -> Timestamp,
        inserted_at -> Timestamp,
        rowid -> Int8,
    }
}

diesel::table! {
    user_transactions (version) {
        version -> Int8,
        block_height -> Int8,
        #[max_length = 0]
        parent_signature_type -> Varchar,
        #[max_length = 0]
        sender -> Varchar,
        sequence_number -> Int8,
        max_gas_amount -> Numeric,
        expiration_timestamp_secs -> Timestamp,
        gas_unit_price -> Numeric,
        timestamp -> Timestamp,
        #[max_length = 0]
        entry_function_id_str -> Varchar,
        inserted_at -> Timestamp,
        epoch -> Int8,
        rowid -> Int8,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    account_transactions,
    ans_lookup_v2,
    block_metadata_transactions,
    coin_infos,
    current_ans_lookup_v2,
    current_ans_primary_name_v2,
    current_coin_balances,
    current_collection_datas,
    current_collections_v2,
    current_delegated_staking_pool_balances,
    current_delegated_voter,
    current_delegator_balances,
    current_fungible_asset_balances,
    current_objects,
    current_staking_pool_voter,
    current_table_items,
    current_token_datas_v2,
    current_token_ownerships_v2,
    current_token_pending_claims,
    current_token_royalty_v1,
    current_unified_fungible_asset_balances,
    delegated_staking_activities,
    delegated_staking_pool_balances,
    delegated_staking_pools,
    delegator_balances,
    events,
    fungible_asset_activities,
    fungible_asset_metadata,
    ledger_infos,
    processor_status,
    proposal_votes,
    spam_assets,
    token_activities_v2,
    user_transactions,
);
