-- This file should undo anything in `up.sql`
ALTER TABLE user_transactions DROP CONSTRAINT IF EXISTS fk_versions;
ALTER TABLE signatures DROP CONSTRAINT IF EXISTS fk_transaction_versions;
ALTER TABLE user_transactions
ADD CONSTRAINT fk_versions FOREIGN KEY (version) REFERENCES transactions (version);
ALTER TABLE signatures
ADD CONSTRAINT fk_transaction_versions FOREIGN KEY (transaction_version) REFERENCES transactions (version);