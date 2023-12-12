-- This file should undo anything in `up.sql`
ALTER TABLE objects DROP COLUMN IF EXISTS is_token;
ALTER TABLE objects DROP COLUMN IF EXISTS is_fungible_asset;
ALTER TABLE current_objects DROP COLUMN IF EXISTS is_token;
ALTER TABLE current_objects DROP COLUMN IF EXISTS is_fungible_asset;

ALTER TABLE block_metadata_transactions
ADD CONSTRAINT fk_versions FOREIGN KEY (version) REFERENCES transactions (version);
ALTER TABLE move_modules
ADD CONSTRAINT fk_transaction_versions FOREIGN KEY (transaction_version) REFERENCES transactions (version);
ALTER TABLE move_resources 
ADD CONSTRAINT fk_transaction_versions FOREIGN KEY (transaction_version) REFERENCES transactions (version);
ALTER TABLE table_items 
ADD CONSTRAINT fk_transaction_versions FOREIGN KEY (transaction_version) REFERENCES transactions (version);
ALTER TABLE write_set_changes 
ADD CONSTRAINT fk_transaction_versions FOREIGN KEY (transaction_version) REFERENCES transactions (version);