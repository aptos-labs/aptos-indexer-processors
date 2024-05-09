-- Your SQL goes here
CREATE TABLE inbox_events AS
TABLE events
WITH NO DATA;

ALTER TABLE inbox_events
ADD PRIMARY KEY (transaction_version, event_index);
