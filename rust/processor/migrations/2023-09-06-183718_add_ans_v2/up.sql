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
DROP VIEW IF EXISTS current_aptos_names;
CREATE OR REPLACE VIEW current_aptos_names AS 
WITH current_aptos_names_v1 AS (
    SELECT
        current_ans_lookup.domain,
        current_ans_lookup.subdomain,
        current_ans_lookup.token_name,
        current_ans_lookup.registered_address,
        current_ans_lookup.expiration_timestamp,
        greatest(
            current_ans_lookup.last_transaction_version,
            current_ans_primary_name.last_transaction_version
        ) AS last_transaction_version,
        coalesce(NOT current_ans_primary_name.is_deleted, false) AS is_primary
    FROM
        current_ans_lookup
        LEFT JOIN current_ans_primary_name ON current_ans_lookup.token_name = current_ans_primary_name.token_name
    WHERE
        current_ans_lookup.expiration_timestamp > CURRENT_TIMESTAMP
        AND current_ans_lookup.is_deleted IS false
),
current_aptos_names_v2 AS (
    SELECT
        domain,
        subdomain,
        token_name,
        registered_address,
        expiration_timestamp,
        last_transaction_version,
        is_primary
    FROM
        current_ans_lookup_v2
    WHERE
        expiration_timestamp > CURRENT_TIMESTAMP
        AND is_deleted IS false
)
SELECT
    *
FROM
    current_aptos_names_v2
UNION
SELECT
    *
FROM
    current_aptos_names_v1;