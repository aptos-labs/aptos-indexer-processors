-- This file should undo anything in `up.sql`
ALTER TABLE user_transactions
DROP COLUMN IF EXISTS entry_function_contract_address,
DROP COLUMN IF EXISTS entry_function_module_name,
DROP COLUMN IF EXISTS entry_function_function_name;
