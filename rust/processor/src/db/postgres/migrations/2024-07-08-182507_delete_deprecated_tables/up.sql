-- Drop dependent views and indexes first
DROP VIEW IF EXISTS move_resources_view;
DROP VIEW IF EXISTS address_version_from_move_resources;

-- Drop the move_resources table
DROP TABLE IF EXISTS move_resources;

-- Drop the transactions table and related objects
DROP VIEW IF EXISTS transactions_view;
DROP INDEX IF EXISTS txn_insat_index;
DROP TABLE IF EXISTS transactions;

-- Drop the write_set_changes table and related indexes
DROP TABLE IF EXISTS write_set_changes;
