-- This file should undo anything in `up.sql`
ALTER TABLE current_delegator_balances
ADD CONSTRAINT current_delegator_balances_pkey PRIMARY KEY (
    delegator_address,
    pool_address,
    pool_type
  );