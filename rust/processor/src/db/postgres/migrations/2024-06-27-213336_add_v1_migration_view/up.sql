-- Your SQL goes here
CREATE OR REPLACE VIEW legacy_migration_v1.current_collection_datas AS
SELECT collection_id as collection_data_id_hash,
    creator_address,
    collection_name,
    description,
    uri as metadata_uri,
    current_supply as supply,
    max_supply as maximum,
    TRUE AS maximum_mutable,
    mutable_uri as uri_mutable,
    mutable_description as description_mutable,
    last_transaction_version,
    inserted_at,
    table_handle_v1 as table_handle,
    last_transaction_timestamp
FROM current_collections_v2
WHERE token_standard = 'v1';

-- If you would like to run these indices, please do it outside of diesel migration since it will be blocking processing
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS lm1_curr_cd_th_index ON public.current_collections_v2 USING btree (table_handle_v1);