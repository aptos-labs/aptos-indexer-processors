-- Your SQL goes here
CREATE TABLE events (
    sequence_number BIGINT NOT NULL,
    creation_number BIGINT NOT NULL,
    account_address VARCHAR(66) NOT NULL,
    transaction_version BIGINT NOT NULL,
    transaction_block_height BIGINT NOT NULL,
    type TEXT NOT NULL,
    data JSONB NOT NULL,
    inserted_at TIMESTAMP NOT NULL,
    event_index BIGINT NOT NULL,
    indexed_type VARCHAR(300) NOT NULL,
    PRIMARY KEY (transaction_version, event_index)
);