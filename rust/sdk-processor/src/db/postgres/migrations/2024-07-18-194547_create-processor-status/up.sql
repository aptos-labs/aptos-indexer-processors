-- Your SQL goes here
CREATE TABLE processor_status (
    processor VARCHAR(50) NOT NULL,
    last_success_version BIGINT NOT NULL,
    last_updated TIMESTAMP NOT NULL,
    last_transaction_timestamp TIMESTAMP NULL,
    PRIMARY KEY (processor)
);