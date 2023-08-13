ALTER TABLE current_ans_lookup
ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS current_ans_primary_name (
    registered_address VARCHAR(66),
    domain VARCHAR(64) NOT NULL,
    -- if subdomain is null set to empty string
    subdomain VARCHAR(64) NOT NULL,
    token_name VARCHAR(140) NOT NULL,
    is_primary BOOLEAN NOT NULL,
    last_transaction_version BIGINT NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
    -- Constraints
    -- We need domain and subdomain name in the PK to support soft deletes of primary name
    PRIMARY KEY (registered_address, domain, subdomain)
);

CREATE TABLE IF NOT EXISTS ans_primary_name (
    transaction_version BIGINT NOT NULL,
    wsc_index BIGINT NOT NULL,
    domain VARCHAR(64) NOT NULL,
    -- if subdomain is null set to empty string
    subdomain VARCHAR(64) NOT NULL,
    registered_address VARCHAR(66),
    token_name VARCHAR(140) NOT NULL,
    is_primary BOOLEAN NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
    -- Constraints 
    -- Need registered_address, domain, subdomain in PK because multiple updates can happen in the same wsc
    PRIMARY KEY (transaction_version, wsc_index, domain, subdomain)
);

CREATE TABLE IF NOT EXISTS ans_lookup (
    transaction_version BIGINT NOT NULL,
    wsc_index BIGINT NOT NULL,
    domain VARCHAR(64) NOT NULL,
    -- if subdomain is null set to empty string
    subdomain VARCHAR(64) NOT NULL,
    registered_address VARCHAR(66),
    expiration_timestamp TIMESTAMP,
    token_name VARCHAR(140) NOT NULL,
    is_primary BOOLEAN NOT NULL,
    is_deleted BOOLEAN NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
    -- Constraints 
    PRIMARY KEY (transaction_version, wsc_index)
);