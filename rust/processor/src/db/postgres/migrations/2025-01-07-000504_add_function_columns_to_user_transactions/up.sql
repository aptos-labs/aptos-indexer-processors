-- Your SQL goes here
ALTER TABLE user_transactions
ADD COLUMN entry_function_contract_address VARCHAR(66),
ADD COLUMN entry_function_module_name VARCHAR(255),
ADD COLUMN entry_function_function_name VARCHAR(255);

-- If you would like to run these indices, please do it outside of diesel migration since it will be blocking processing
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS user_transactions_contract_info_index ON user_transactions (entry_function_contract_address, version);
