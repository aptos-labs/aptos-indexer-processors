-- Your SQL goes here
CREATE TABLE IF NOT EXISTS transaction_size_info (
  transaction_version BIGINT UNIQUE PRIMARY KEY NOT NULL,
  size_bytes BIGINT NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS event_size_info (
  transaction_version BIGINT NOT NULL,
  index BIGINT NOT NULL,
  type_tag_bytes BIGINT NOT NULL,
  total_bytes BIGINT NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY (transaction_version, index)
);
CREATE TABLE IF NOT EXISTS write_set_size_info (
  transaction_version BIGINT NOT NULL,
  index BIGINT NOT NULL,
  key_bytes BIGINT NOT NULL,
  value_bytes BIGINT NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY (transaction_version, index)
);
CREATE INDEX IF NOT EXISTS tsi_insat_index ON transaction_size_info (inserted_at);
CREATE INDEX IF NOT EXISTS esi_insat_index ON event_size_info (inserted_at);
CREATE INDEX IF NOT EXISTS wsi_insat_index ON write_set_size_info (inserted_at);