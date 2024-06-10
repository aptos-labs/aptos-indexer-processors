-- Your SQL goes here

ALTER TABLE events DROP CONSTRAINT events_pkey;
ALTER TABLE events
ADD CONSTRAINT events_pkey PRIMARY KEY (transaction_version, event_index);

