-- Your SQL goes here
-- This'll only work with royalty v1 because royalty_v2 requires collection id
CREATE TABLE IF NOT EXISTS current_token_royalty_v1 (
    token_data_id VARCHAR(66) UNIQUE PRIMARY KEY NOT NULL,
    payee_address VARCHAR(66) NOT NULL,
    royalty_points_numerator NUMERIC NOT NULL,
    royalty_points_denominator NUMERIC NOT NULL,
    last_transaction_version BIGINT NOT NULL,
    last_transaction_timestamp TIMESTAMP NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW()
);