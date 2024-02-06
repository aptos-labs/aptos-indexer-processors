
-- add new field to composite primary key because technically a user could have inactive pools
ALTER TABLE current_delegator_balances DROP CONSTRAINT current_delegator_balances_pkey;
ALTER TABLE current_delegator_balances
    ADD CONSTRAINT current_delegator_balances_pkey PRIMARY KEY (
                                                                delegator_address,
                                                                pool_address,
                                                                pool_type,
                                                                table_handle
        );