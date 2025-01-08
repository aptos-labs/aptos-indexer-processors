-- Your SQL goes here
ALTER TABLE user_transactions
ADD COLUMN contract_address VARCHAR(66) NOT NULL,
ADD COLUMN module_name VARCHAR(255) NOT NULL,
ADD COLUMN function_name VARCHAR(255) NOT NULL;
CREATE INDEX IF NOT EXISTS user_transactions_contract_info_index ON user_transactions (contract_address, module_name, function_name);
