ALTER TABLE current_ans_lookup
ADD COLUMN is_primary BOOLEAN NOT NULL DEFAULT FALSE,
ADD COLUMN is_deleted BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE ans_lookup (
    transaction_version BIGINT NOT NULL,
    domain VARCHAR(64) NOT NULL,
    -- if subdomain is null set to empty string
    subdomain VARCHAR(64) NOT NULL,
    registered_address VARCHAR(66),
    expiration_timestamp TIMESTAMP,
    token_name VARCHAR(128) NOT NULL,
    is_primary BOOLEAN,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    transaction_timestamp TIMESTAMP NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
    -- Constraints 
    PRIMARY KEY (transaction_version, domain, subdomain)
);