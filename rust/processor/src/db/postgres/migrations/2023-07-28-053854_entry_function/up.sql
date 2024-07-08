-- Your SQL goes here
ALTER TABLE user_transactions
ALTER COLUMN entry_function_id_str TYPE VARCHAR(1000);
ALTER TABLE coin_activities
ALTER COLUMN entry_function_id_str TYPE VARCHAR(1000);
ALTER TABLE token_activities_v2
ALTER COLUMN entry_function_id_str TYPE VARCHAR(1000);
