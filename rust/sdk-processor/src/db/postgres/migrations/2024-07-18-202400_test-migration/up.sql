-- Your SQL goes here
ALTER TABLE IF EXISTS events ALTER COLUMN inserted_at SET DEFAULT NOW();
ALTER TABLE IF EXISTS processor_status ALTER COLUMN last_updated SET DEFAULT NOW();