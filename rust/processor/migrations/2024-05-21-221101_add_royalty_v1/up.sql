-- Your SQL goes here
CREATE TABLE token_royalty (
    transaction_version BIGINT NOT NULL,
    token_data_id VARCHAR(64) NOT NULL,
    payee_address VARCHAR(64) NOT NULL,
    royalty_points_numerator_v1 INT NOT NULL,
    royalty_points_denominator_v1 INT NOT NULL,
    token_standard TEXT NOT NULL,
    (transaction_version, token_data_id) PRIMARY KEY
);

CREATE TABLE current_token_royalty (
    token_data_id VARCHAR(64) NOT NULL,
    payee_address VARCHAR(64) NOT NULL,
    royalty_points_numerator_v1 INT NOT NULL,
    royalty_points_denominator_v1 INT NOT NULL,
    token_standard TEXT NOT NULL,
    token_data_id PRIMARY KEY
);
