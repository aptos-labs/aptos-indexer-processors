-- Your SQL goes here
CREATE TABLE IF NOT EXISTS current_ans_lookup_v2 (
    domain VARCHAR(64) NOT NULL,
    -- if subdomain is null set to empty string
    subdomain VARCHAR(64) NOT NULL,
    token_name VARCHAR(140) NOT NULL,
    registered_address VARCHAR(66),
    expiration_timestamp TIMESTAMP NOT NULL,
    is_primary BOOLEAN NOT NULL,
    is_deleted BOOLEAN NOT NULL,
    last_transaction_version BIGINT NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
    -- Constraints 
    PRIMARY KEY (domain, subdomain)
);
CREATE TABLE IF NOT EXISTS ans_lookup_v2 (
    transaction_version BIGINT NOT NULL,
    write_set_change_index BIGINT NOT NULL,
    domain VARCHAR(64) NOT NULL,
    -- if subdomain is null set to empty string
    subdomain VARCHAR(64) NOT NULL,
    token_name VARCHAR(140) NOT NULL,
    registered_address VARCHAR(66),
    expiration_timestamp TIMESTAMP NOT NULL,
    is_primary BOOLEAN NOT NULL,
    is_deleted BOOLEAN NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
    -- Constraints 
    PRIMARY KEY (transaction_version, write_set_change_index)
);

-- TODO: Create view for current_aptos_names