-- Your SQL goes here

ALTER TABLE events DROP CONSTRAINT IF EXISTS fk_transaction_versions;

ALTER TABLE events
ALTER COLUMN "event_index" SET NOT NULL;
