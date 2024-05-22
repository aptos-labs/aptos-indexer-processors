-- Your SQL goes here
CREATE TABLE token_royalty (
    transaction_version BIGINT NOT NULL,
    token_data_id VARCHAR(66) NOT NULL,
    payee_address VARCHAR(66) NOT NULL,
    royalty_points_numerator NUMERIC NOT NULL,
    royalty_points_denominator NUMERIC NOT NULL,
    token_standard TEXT NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (transaction_version, token_data_id) 
);

CREATE TABLE current_token_royalty (
    token_data_id VARCHAR(66) NOT NULL,
    payee_address VARCHAR(66) NOT NULL,
    royalty_points_numerator NUMERIC NOT NULL,
    royalty_points_denominator NUMERIC NOT NULL,
    token_standard TEXT NOT NULL,
    last_transaction_version BIGINT NOT NULL,
    last_transaction_timestamp TIMESTAMP NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (token_data_id)
);
