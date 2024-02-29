-- Your SQL goes here
CREATE TABLE IF NOT EXISTS roulette (
  room_address VARCHAR(66) PRIMARY KEY NOT NULL,
  result_index jsonb NOT NULL,
  revealed_index BIGINT NOT NULL,
  last_transaction_version BIGINT NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW()
);