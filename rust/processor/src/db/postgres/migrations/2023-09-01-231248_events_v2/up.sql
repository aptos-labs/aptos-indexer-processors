-- Your SQL goes here
ALTER TABLE events DROP CONSTRAINT events_pkey;
ALTER TABLE events DROP CONSTRAINT IF EXISTS fk_transaction_versions;
ALTER TABLE events
ADD CONSTRAINT events_pkey PRIMARY KEY (transaction_version, event_index);