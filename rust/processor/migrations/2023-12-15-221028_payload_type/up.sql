-- Your SQL goes here
ALTER TABLE transactions
ADD COLUMN IF NOT EXISTS payload_type VARCHAR(50);