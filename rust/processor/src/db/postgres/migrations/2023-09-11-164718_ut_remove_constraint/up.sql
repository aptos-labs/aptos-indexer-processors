-- Your SQL goes here
ALTER TABLE user_transactions DROP CONSTRAINT IF EXISTS fk_versions;
ALTER TABLE signatures DROP CONSTRAINT IF EXISTS fk_transaction_versions;