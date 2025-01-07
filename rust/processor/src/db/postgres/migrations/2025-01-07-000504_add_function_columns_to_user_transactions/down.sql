-- This file should undo anything in `up.sql`
ALTER TABLE user_transactions
DROP COLUMN IF EXISTS contract_address,
DROP COLUMN IF EXISTS module_name,
DROP COLUMN IF EXISTS function_name;
