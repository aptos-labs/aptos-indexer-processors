DROP VIEW IF EXISTS current_aptos_names;
CREATE OR REPLACE VIEW current_aptos_names AS
SELECT
	current_ans_lookup.domain,
	current_ans_lookup.subdomain,
	current_ans_lookup.token_name,
	current_ans_lookup.registered_address,
	current_ans_lookup.expiration_timestamp,
	GREATEST(current_ans_lookup.last_transaction_version, current_ans_primary_name.last_transaction_version) as last_transaction_version,
	COALESCE(NOT current_ans_primary_name.is_deleted, false) AS is_primary
FROM current_ans_lookup
LEFT JOIN current_ans_primary_name
ON current_ans_lookup.token_name = current_ans_primary_name.token_name
WHERE current_ans_lookup.expiration_timestamp > current_timestamp and current_ans_lookup.is_deleted is false;