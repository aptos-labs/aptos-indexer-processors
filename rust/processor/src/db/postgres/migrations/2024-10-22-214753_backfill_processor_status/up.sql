-- Your SQL goes here
CREATE TABLE backfill_processor_status (
    backfill_alias VARCHAR(50) NOT NULL,
    backfill_status VARCHAR(50) NOT NULL,
    last_success_version BIGINT NOT NULL,
    last_updated TIMESTAMP NOT NULL DEFAULT NOW(),
    last_transaction_timestamp TIMESTAMP NULL,
    backfill_start_version BIGINT NOT NULL,
    backfill_end_version BIGINT NOT NULL,
    PRIMARY KEY (backfill_alias)
);