-- Create index for account transaction to speed up queries
CREATE INDEX at_account_addr_index ON account_transactions (account_address);