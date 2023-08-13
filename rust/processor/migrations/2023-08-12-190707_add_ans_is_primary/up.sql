ALTER TABLE current_ans_lookup
ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN NOT NULL DEFAULT FALSE;
-- Tracks current primary name, deleted means that address no longer has a primary name
CREATE TABLE IF NOT EXISTS current_ans_primary_name (
    registered_address VARCHAR(66) UNIQUE PRIMARY KEY NOT NULL,
    domain VARCHAR(64),
    subdomain VARCHAR(64),
    token_name VARCHAR(140),
    is_deleted BOOLEAN NOT NULL,
    last_transaction_version BIGINT NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS capn_tn_index on current_ans_primary_name (token_name);
CREATE INDEX IF NOT EXISTS capn_insat_index on current_ans_primary_name (inserted_at);
-- Tracks primary name, deleted means that address no longer has a primary name
CREATE TABLE IF NOT EXISTS ans_primary_name (
    transaction_version BIGINT NOT NULL,
    write_set_change_index BIGINT NOT NULL,
    registered_address VARCHAR(66) NOT NULL,
    domain VARCHAR(64),
    subdomain VARCHAR(64),
    token_name VARCHAR(140),
    is_deleted BOOLEAN NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
    -- Constraints 
    PRIMARY KEY (
        transaction_version,
        write_set_change_index
    )
);
CREATE INDEX IF NOT EXISTS apn_tn_index on ans_primary_name (token_name);
CREATE INDEX IF NOT EXISTS apn_insat_index on ans_primary_name (inserted_at);
-- Tracks full history of the ans records table
CREATE TABLE IF NOT EXISTS ans_lookup (
    transaction_version BIGINT NOT NULL,
    write_set_change_index BIGINT NOT NULL,
    domain VARCHAR(64) NOT NULL,
    -- if subdomain is null set to empty string
    subdomain VARCHAR(64) NOT NULL,
    registered_address VARCHAR(66),
    expiration_timestamp TIMESTAMP,
    token_name VARCHAR(140) NOT NULL,
    is_deleted BOOLEAN NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
    -- Constraints 
    PRIMARY KEY (transaction_version, write_set_change_index)
);
CREATE INDEX IF NOT EXISTS al_tn_index on ans_lookup (token_name);
CREATE INDEX IF NOT EXISTS al_insat_index on ans_lookup (inserted_at);