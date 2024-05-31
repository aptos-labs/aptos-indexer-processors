-- current fungible asset balances
CREATE TABLE IF NOT EXISTS current_unified_fungible_asset_balances (
    storage_id VARCHAR(66) PRIMARY KEY NOT NULL,
    owner_address VARCHAR(66) NOT NULL,
    asset_type VARCHAR(66) NOT NULL,
    coin_type VARCHAR(1000),
    is_primary BOOLEAN,
    is_frozen BOOLEAN NOT NULL,
    amount_v1 NUMERIC,
    amount_v2 NUMERIC,
    amount NUMERIC GENERATED ALWAYS AS (COALESCE(amount_v1, 0) + COALESCE(amount_v2, 0)) STORED,
    last_transaction_version_v1 BIGINT,
    last_transaction_version_v2 BIGINT,
    last_transaction_version BIGINT GENERATED ALWAYS AS (GREATEST(last_transaction_version_v1, last_transaction_version_v2)) STORED,
    last_transaction_timestamp_v1 TIMESTAMP,
    last_transaction_timestamp_v2 TIMESTAMP,
    last_transaction_timestamp TIMESTAMP GENERATED ALWAYS AS (GREATEST(last_transaction_timestamp_v1, last_transaction_timestamp_v2)) STORED,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS cufab_owner_at_index ON current_unified_fungible_asset_balances (owner_address, asset_type);
CREATE INDEX IF NOT EXISTS cufab_insat_index ON current_unified_fungible_asset_balances (inserted_at);
