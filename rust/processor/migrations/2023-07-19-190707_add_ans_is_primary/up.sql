ALTER TABLE current_ans_lookup
ADD COLUMN IF NOT EXISTS is_primary BOOLEAN NOT NULL DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS ans_lookup (
    transaction_version BIGINT NOT NULL,
    domain VARCHAR(64) NOT NULL,
    -- if subdomain is null set to empty string
    subdomain VARCHAR(64) NOT NULL,
    registered_address VARCHAR(66),
    expiration_timestamp TIMESTAMP,
    token_name VARCHAR(140) NOT NULL,
    is_primary BOOLEAN,
    is_deleted BOOLEAN,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
    -- Constraints 
    PRIMARY KEY (transaction_version, domain, subdomain)
);