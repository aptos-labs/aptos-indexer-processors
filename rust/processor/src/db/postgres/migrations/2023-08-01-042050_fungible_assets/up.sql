-- Your SQL goes here
-- Redo the view for collection view
DROP VIEW IF EXISTS current_collection_ownership_v2_view;
CREATE OR REPLACE VIEW current_collection_ownership_v2_view as
select owner_address,
  creator_address,
  collection_name,
  b.collection_id,
  max(a.last_transaction_version) as last_transaction_version,
  count(distinct a.token_data_id) as distinct_tokens,
  min(c.uri) as collection_uri,
  min(token_uri) as single_token_uri
from current_token_ownerships_v2 a
  join token_datas_v2 b on a.token_data_id = b.token_data_id
  join current_collections_v2 c on b.collection_id = c.collection_id
where amount > 0
group by 1,
  2,
  3,
  4;
-- fungible asset activities
CREATE TABLE IF NOT EXISTS fungible_asset_activities (
  transaction_version BIGINT NOT NULL,
  event_index BIGINT NOT NULL,
  owner_address VARCHAR(66) NOT NULL,
  storage_id VARCHAR(66) NOT NULL,
  asset_type VARCHAR(1000) NOT NULL,
  is_frozen BOOLEAN,
  amount NUMERIC,
  type VARCHAR NOT NULL,
  is_gas_fee BOOLEAN NOT NULL,
  gas_fee_payer_address VARCHAR(66),
  is_transaction_success BOOLEAN NOT NULL,
  entry_function_id_str VARCHAR(1000),
  block_height BIGINT NOT NULL,
  token_standard VARCHAR(10) NOT NULL,
  transaction_timestamp TIMESTAMP NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  -- constraints
  PRIMARY KEY (transaction_version, event_index)
);
CREATE INDEX IF NOT EXISTS faa_owner_type_index ON fungible_asset_activities (owner_address, type);
CREATE INDEX IF NOT EXISTS faa_si_index ON fungible_asset_activities (storage_id);
CREATE INDEX IF NOT EXISTS faa_at_index ON fungible_asset_activities (asset_type);
CREATE INDEX IF NOT EXISTS faa_gfpa_index ON fungible_asset_activities (gas_fee_payer_address);
CREATE INDEX IF NOT EXISTS faa_insat_idx ON fungible_asset_activities (inserted_at);
-- current fungible asset balances
CREATE TABLE IF NOT EXISTS current_fungible_asset_balances (
  storage_id VARCHAR(66) UNIQUE PRIMARY KEY NOT NULL,
  owner_address VARCHAR(66) NOT NULL,
  asset_type VARCHAR(1000) NOT NULL,
  is_primary BOOLEAN NOT NULL,
  is_frozen BOOLEAN NOT NULL,
  amount NUMERIC NOT NULL,
  last_transaction_timestamp TIMESTAMP NOT NULL,
  last_transaction_version BIGINT NOT NULL,
  token_standard VARCHAR(10) NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS cfab_owner_at_index ON current_fungible_asset_balances (owner_address, asset_type);
CREATE INDEX IF NOT EXISTS cfab_insat_index ON current_fungible_asset_balances (inserted_at);
-- balances
CREATE TABLE IF NOT EXISTS fungible_asset_balances (
  transaction_version BIGINT NOT NULL,
  write_set_change_index BIGINT NOT NULL,
  storage_id VARCHAR(66) NOT NULL,
  owner_address VARCHAR(66) NOT NULL,
  asset_type VARCHAR(1000) NOT NULL,
  is_primary BOOLEAN NOT NULL,
  is_frozen BOOLEAN NOT NULL,
  amount NUMERIC NOT NULL,
  transaction_timestamp TIMESTAMP NOT NULL,
  token_standard VARCHAR(10) NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  -- constraints
  PRIMARY KEY (transaction_version, write_set_change_index)
);
CREATE INDEX IF NOT EXISTS fab_owner_at_index ON fungible_asset_balances (owner_address, asset_type);
CREATE INDEX IF NOT EXISTS fab_insat_index ON fungible_asset_balances (inserted_at);
-- fungible asset metadata
CREATE TABLE IF NOT EXISTS fungible_asset_metadata (
  asset_type VARCHAR(1000) UNIQUE PRIMARY KEY NOT NULL,
  creator_address VARCHAR(66) NOT NULL,
  "name" VARCHAR(32) NOT NULL,
  symbol VARCHAR(10) NOT NULL,
  decimals INT NOT NULL,
  icon_uri VARCHAR(512),
  project_uri VARCHAR(512),
  last_transaction_version BIGINT NOT NULL,
  last_transaction_timestamp TIMESTAMP NOT NULL,
  supply_aggregator_table_handle_v1 VARCHAR(66),
  supply_aggregator_table_key_v1 text,
  token_standard VARCHAR(10) NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS fam_creator_index ON fungible_asset_metadata (creator_address);
CREATE INDEX IF NOT EXISTS fam_insat_index ON fungible_asset_metadata (inserted_at);
-- adding fee payer handling to old coin activities
ALTER TABLE coin_activities
ADD COLUMN IF NOT EXISTS gas_fee_payer_address VARCHAR(66) DEFAULT NULL;
CREATE INDEX IF NOT EXISTS ca_gfpa_index ON coin_activities (gas_fee_payer_address);