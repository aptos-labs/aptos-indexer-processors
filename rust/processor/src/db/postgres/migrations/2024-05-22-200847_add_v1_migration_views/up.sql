-- Your SQL goes here
-- Create the schema
CREATE SCHEMA IF NOT EXISTS legacy_migration_v1;
-- Replace `move_resources` with account transactions
CREATE OR REPLACE VIEW legacy_migration_v1.move_resources AS
SELECT transaction_version,
    account_address as address
FROM account_transactions at2;
CREATE OR REPLACE VIEW legacy_migration_v1.address_version_from_move_resources AS
SELECT transaction_version,
    account_address as address
FROM account_transactions at2;
-- replace `coin_activities` with `fungible_asset_activities`
CREATE OR REPLACE VIEW legacy_migration_v1.coin_activities AS
SElECT transaction_version,
    owner_address as event_account_address,
    -- these two below are mildly concerning
    0 as event_creation_number,
    0 as event_sequence_number,
    owner_address,
    asset_type AS coin_type,
    amount,
    "type" AS activity_type,
    is_gas_fee,
    is_transaction_success,
    entry_function_id_str,
    block_height,
    transaction_timestamp,
    inserted_at,
    event_index,
    gas_fee_payer_address,
    storage_refund_amount
FROM public.fungible_asset_activities
WHERE token_standard = 'v1';
-- replace `coin_balances` with `fungible_asset_balances`
CREATE OR REPLACE VIEW legacy_migration_v1.coin_balances AS
SELECT transaction_version,
    owner_address,
    -- this is mainly for hashing the coin type for primary key
    encode(sha256(asset_type::bytea), 'hex') as coin_type_hash,
    asset_type as coin_type,
    amount,
    transaction_timestamp,
    inserted_at
FROM public.fungible_asset_balances
WHERE token_standard = 'v1';
-- replace `coin_infos` with `fungible_asset_metadata`
CREATE OR REPLACE VIEW legacy_migration_v1.coin_infos AS
SELECT encode(sha256(asset_type::bytea), 'hex') as coin_type_hash,
    asset_type as coin_type,
    last_transaction_version as transaction_version_created,
    creator_address,
    name,
    symbol,
    decimals,
    last_transaction_timestamp as transaction_created_timestamp,
    inserted_at,
    supply_aggregator_table_handle_v1 as supply_aggregator_table_handle,
    supply_aggregator_table_key_v1 as supply_aggregator_table_key
FROM public.fungible_asset_metadata
WHERE token_standard = 'v1';
-- replace `current_coin_balances` with `current_fungible_asset_balances`
CREATE OR REPLACE VIEW legacy_migration_v1.current_coin_balances AS
SELECT owner_address,
    encode(sha256(asset_type::bytea), 'hex') as coin_type_hash,
    asset_type as coin_type,
    amount,
    last_transaction_version,
    last_transaction_timestamp,
    inserted_at
FROM public.current_fungible_asset_balances
WHERE token_standard = 'v1';
-- replace `token_activities` with `token_activities_v2`
-- token_activities_v2.token_data_id is 0x prefixed, but token_activities.token_data_id is not. We need to create an index on the substring
CREATE OR REPLACE VIEW legacy_migration_v1.token_activities AS
SELECT tav.transaction_version,
    event_account_address,
    -- These were only used for hashing pk in v1 table
    0 as event_creation_number,
    0 as event_sequence_number,
    tdv.collection_id as collection_data_id_hash,
    ltrim(tav.token_data_id, '0x') as token_data_id_hash,
    property_version_v1 AS property_version,
    cv.creator_address,
    cv.collection_name,
    tdv.token_name AS "name",
    "type" AS transfer_type,
    from_address,
    to_address,
    token_amount,
    -- These are not columns in v2
    NULL AS coin_type,
    NULL AS coin_amount,
    tav.inserted_at,
    tav.transaction_timestamp,
    event_index
FROM public.token_activities_v2 tav
    JOIN token_datas_v2 tdv ON tav.token_data_id = tdv.token_data_id
    AND tav.transaction_version = tdv.transaction_version
    JOIN collections_v2 cv ON tdv.collection_id = cv.collection_id
    AND tdv.transaction_version = cv.transaction_version
WHERE tav.token_standard = 'v1';
-- replace `token_ownerships` with `token_ownerships_v2`
CREATE OR REPLACE VIEW legacy_migration_v1.token_ownerships AS
SELECT tov.token_data_id AS token_data_id_hash,
    property_version_v1 AS property_version,
    tov.transaction_version,
    -- this is a bit concerning
    '' AS table_handle,
    creator_address,
    collection_name,
    tdv.token_name AS name,
    owner_address,
    amount,
    table_type_v1 AS table_type,
    tov.inserted_at,
    tdv.collection_id AS collection_data_id_hash,
    tov.transaction_timestamp
FROM public.token_ownerships_v2 tov
    JOIN public.token_datas_v2 tdv ON tov.token_data_id = tdv.token_data_id
    AND tov.transaction_version = tdv.transaction_version
    JOIN public.collections_v2 cv ON tdv.collection_id = cv.collection_id
    AND tdv.transaction_version = cv.transaction_version
WHERE tov.token_standard = 'v1';
-- replace `current_token_ownerships` with `current_token_ownerships_v2`
CREATE OR REPLACE VIEW legacy_migration_v1.current_token_ownerships AS
SELECT ctov.token_data_id AS token_data_id_hash,
    ctov.property_version_v1 AS property_version,
    ctov.owner_address,
    ccv.creator_address,
    ccv.collection_name,
    ctdv.token_name AS "name",
    ctov.amount,
    ctov.token_properties_mutated_v1 AS token_properties,
    ctov.last_transaction_version,
    ctov.inserted_at,
    ctdv.collection_id AS collection_data_id_hash,
    ctov.table_type_v1 AS table_type,
    ctov.last_transaction_timestamp
FROM current_token_ownerships_v2 ctov
    JOIN current_token_datas_v2 ctdv ON ctov.token_data_id = ctdv.token_data_id
    JOIN current_collections_v2 ccv ON ctdv.collection_id = ccv.collection_id
WHERE ctov.token_standard = 'v1';
-- replace `tokens` with `current_token_datas_v2`
CREATE OR REPLACE VIEW legacy_migration_v1.tokens AS
SELECT tdv.token_data_id AS token_data_id_hash,
    tdv.largest_property_version_v1 AS property_version,
    tdv.transaction_version,
    ccv.creator_address,
    ccv.collection_name,
    tdv.token_name AS "name",
    tdv.token_properties,
    tdv.inserted_at,
    tdv.collection_id AS collection_data_id_hash,
    tdv.transaction_timestamp
FROM token_datas_v2 tdv
    JOIN current_collections_v2 ccv ON tdv.collection_id = ccv.collection_id
WHERE tdv.token_standard = 'v1';
-- replace `token_datas` with `token_datas_v2`
CREATE OR REPLACE VIEW legacy_migration_v1.token_datas AS
SELECT token_data_id AS token_data_id_hash,
    tdv.transaction_version,
    creator_address,
    collection_name,
    token_name AS "name",
    maximum,
    supply,
    largest_property_version_v1 AS largest_property_version,
    token_uri AS metadata_uri,
    -- Null b/c we're not tracking royalty on transaction level
    '' as payee_address,
    null as royalty_points_numerator,
    null as royalty_points_denominator,
    -- Validated this is fine, since most are true anyway
    TRUE AS maximum_mutable,
    TRUE AS uri_mutable,
    TRUE AS description_mutable,
    TRUE AS properties_mutable,
    TRUE AS royalty_mutable,
    token_properties AS default_properties,
    tdv.inserted_at,
    tdv.collection_id AS collection_data_id_hash,
    tdv.transaction_timestamp,
    tdv.description
FROM token_datas_v2 tdv
    JOIN collections_v2 cv ON tdv.collection_id = cv.collection_id
    AND tdv.transaction_version = cv.transaction_version
WHERE tdv.token_standard = 'v1';
-- replace `current_token_datas` with `current_token_datas_v2`
CREATE OR REPLACE VIEW legacy_migration_v1.current_token_datas AS
SELECT ctdv.token_data_id AS token_data_id_hash,
    creator_address,
    collection_name,
    token_name AS "name",
    COALESCE(maximum, 0) AS maximum,
    COALESCE(supply, 0) AS supply,
    largest_property_version_v1 AS largest_property_version,
    token_uri AS metadata_uri,
    COALESCE(payee_address, '') as payee_address,
    royalty_points_numerator,
    royalty_points_denominator,
    -- Validated this is fine, since most are true anyway
    TRUE AS maximum_mutable,
    TRUE AS uri_mutable,
    TRUE AS description_mutable,
    TRUE AS properties_mutable,
    TRUE AS royalty_mutable,
    token_properties AS default_properties,
    ctdv.last_transaction_version,
    ctdv.inserted_at,
    ctdv.collection_id AS collection_data_id_hash,
    ctdv.last_transaction_timestamp,
    ctdv."description" AS "description"
FROM current_token_datas_v2 ctdv
    JOIN current_collections_v2 ccv ON ctdv.collection_id = ccv.collection_id
    LEFT JOIN current_token_royalty_v1 ctrv on ctdv.token_data_id = ctrv.token_data_id
WHERE ctdv.token_standard = 'v1';
-- replace `collection_datas` with `collection_v2`
CREATE OR REPLACE VIEW legacy_migration_v1.collection_datas AS
SELECT collection_id AS collection_data_id_hash,
    transaction_version,
    creator_address,
    collection_name,
    description,
    uri AS metadata_uri,
    current_supply AS supply,
    max_supply AS maximum,
    -- Validated this is fine, since most are true anyway
    TRUE AS maximum_mutable,
    TRUE AS uri_mutable,
    TRUE AS description_mutable,
    inserted_at,
    table_handle_v1 AS table_handle,
    transaction_timestamp
FROM collections_v2
WHERE token_standard = 'v1';
-- replace `current_ans_primary_name` with `current_ans_primary_name_v2`
CREATE OR REPLACE VIEW legacy_migration_v1.current_ans_primary_name AS
SELECT registered_address,
    domain,
    subdomain,
    token_name,
    is_deleted,
    last_transaction_version,
    0 AS last_transaction_timestamp
FROM current_ans_primary_name_v2
WHERE token_standard = 'v1';
-- replace `current_ans_lookup` with `current_ans_lookup_v2`
CREATE OR REPLACE VIEW legacy_migration_v1.current_ans_lookup AS
SELECT domain,
    subdomain,
    registered_address,
    expiration_timestamp,
    last_transaction_version,
    inserted_at,
    token_name,
    is_deleted
FROM current_ans_lookup_v2
WHERE token_standard = 'v1';
-----
-----
-----
-- If you would like to run these indices, please do it outside of diesel migration since it will be blocking processing
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS lm1_ca_ct_a_index ON public.fungible_asset_activities USING btree (asset_type, amount);
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS lm1_ca_ct_at_a_index ON public.fungible_asset_activities USING btree (asset_type, "type", amount);
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS lm1_ca_oa_ct_at_index ON public.fungible_asset_activities USING btree (owner_address, asset_type, "type", amount);
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS lm1_ca_oa_igf_index ON public.fungible_asset_activities USING btree (owner_address, is_gas_fee);
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS lm1_cb_tv_oa_ct_index ON public.fungible_asset_balances USING btree (transaction_version, owner_address, asset_type);
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS lm1_ccb_ct_a_index ON public.current_fungible_asset_balances USING btree (asset_type, amount);
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS lm1_tdv_tdi_tv_index ON public.token_datas_v2 USING btree (token_data_id, transaction_version);
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS lm1_cv_ci_tv_index ON public.collections_v2 USING btree (collection_id, transaction_version);
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS lm1_ta_tdih_pv_index ON public.token_activities_v2 USING btree (token_data_id, property_version_v1);
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS lm1_ans_d_s_et_index ON public.current_ans_lookup_v2 USING btree (domain, subdomain, expiration_timestamp);
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS lm1_ans_ra_et_index ON public.current_ans_lookup_v2 USING btree (registered_address, expiration_timestamp);
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS lm1_curr_to_oa_tt_am_ltv_index ON current_token_ownerships_v2 USING btree (
--     owner_address,
--     table_type_v1,
--     amount,
--     last_transaction_version DESC
-- );
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS lm1_curr_to_oa_tt_ltv_index ON current_token_ownerships_v2 USING btree (
--     owner_address,
--     table_type_v1,
--     last_transaction_version DESC
-- );