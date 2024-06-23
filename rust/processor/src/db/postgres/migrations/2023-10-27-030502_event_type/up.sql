-- Your SQL goes here
-- p99 currently is 303 so using 300 as a safe max length 
ALTER TABLE events ADD COLUMN IF NOT EXISTS indexed_type VARCHAR(300) NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS ev_itype_index ON events (indexed_type);
CREATE TABLE IF NOT EXISTS spam_assets (
  asset VARCHAR(1100) PRIMARY KEY NOT NULL,
  is_spam BOOLEAN NOT NULL DEFAULT TRUE,
  last_updated TIMESTAMP NOT NULL DEFAULT NOW()
);