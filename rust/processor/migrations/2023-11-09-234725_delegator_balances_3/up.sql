-- Your SQL goes here
CREATE INDEX IF NOT EXISTS cdb_insat_index ON current_delegator_balances (inserted_at);
