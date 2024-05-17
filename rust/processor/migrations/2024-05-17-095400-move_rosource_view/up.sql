-- Your SQL goes here
-- need this to query transactions that touch an account's move resources,
-- previously used move_resources, and now it's using account_transactions to deprecate move_resources
CREATE OR REPLACE VIEW address_version_from_account_transactions AS
SELECT account_address,
  transaction_version
FROM account_transactions
GROUP BY 1,
  2;