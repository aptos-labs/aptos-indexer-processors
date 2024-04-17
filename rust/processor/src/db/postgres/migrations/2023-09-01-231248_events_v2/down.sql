-- This file should undo anything in `up.sql`
ALTER TABLE events DROP CONSTRAINT events_pkey;
ALTER TABLE events
ADD CONSTRAINT events_pkey PRIMARY KEY (
    account_address,
    creation_number,
    sequence_number
  );