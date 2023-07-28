-- This file should undo anything in `up.sql`
ALTER TABLE user_transactions
ALTER COLUMN entry_function_id_str TYPE TEXT;
ALTER TABLE coin_activities
ALTER COLUMN entry_function_id_str TYPE VARCHAR(100);
ALTER TABLE token_activities_v2
ALTER COLUMN entry_function_id_str TYPE VARCHAR(100);