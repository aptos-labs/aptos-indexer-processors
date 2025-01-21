-- need this for delegation staking
CREATE OR REPLACE VIEW num_active_delegator_per_pool AS
SELECT pool_address,
  COUNT(DISTINCT delegator_address) AS num_active_delegator
FROM current_delegator_balances
WHERE shares > 0
  AND delegator_address  != '0x0000000000000000000000000000000000000000000000000000000000000000'
  AND pool_type = 'active_shares'
GROUP BY 1;
