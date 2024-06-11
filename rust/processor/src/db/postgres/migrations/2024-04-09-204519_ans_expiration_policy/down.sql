-- This file should undo anything in `up.sql`
DROP VIEW IF EXISTS current_aptos_names;
CREATE OR REPLACE VIEW current_aptos_names AS
SELECT cal.domain,
  cal.subdomain,
  cal.token_name,
  cal.token_standard,
  cal.registered_address,
  cal.expiration_timestamp,
  greatest(
    cal.last_transaction_version,
    capn.last_transaction_version
  ) as last_transaction_version,
  coalesce(not capn.is_deleted, false) as is_primary,
  concat(cal.domain, '.apt') as domain_with_suffix,
  c.owner_address as owner_address,
  cal.expiration_timestamp >= CURRENT_TIMESTAMP as is_active
FROM current_ans_lookup_v2 cal
  LEFT JOIN current_ans_primary_name_v2 capn ON cal.token_name = capn.token_name
  AND cal.token_standard = capn.token_standard
  JOIN current_token_datas_v2 b ON cal.token_name = b.token_name
  AND cal.token_standard = b.token_standard
  JOIN current_token_ownerships_v2 c ON b.token_data_id = c.token_data_id
  AND b.token_standard = c.token_standard
WHERE cal.is_deleted IS false
  AND c.amount > 0
  AND b.collection_id IN (
    '0x1c380887f0cfcc8a82c0df44b24116985a92c58e686a0ea4a441c9f423a72b47',
    -- Testnet ANS v1 domain collection
    '0x56654f4bf4e528bfef33094d11a3475f0638e949b0976ec831ca0d66a2efb673',
    -- Testnet ANS v2 domain collection 
    '0x3a2c902067bb4f0e37a2a89675d5cbceb07cf1a27479229b269fb1afffa62230',
    -- Testnet ANS v2 subdomain collection
    '0x09e63a48047b1c2bc51c0abc4b67ffcd9922e0adc99a6cc36532662172976a4b',
    -- Mainnet ANS v1 domain collection
    '0x63d26a4e3a8aeececf9b878e46bad78997fb38e50936efeabb2c4453f4d7f746',
    -- Mainnet ANS v2 domain collection
    '0x63d26a4e3a8aeececf9b878e46bad78997fb38e50936efeabb2c4453f4d7f746' -- Mainnet ANS v2 subdomain collection
  );
ALTER TABLE current_ans_lookup_v2 DROP COLUMN IF EXISTS subdomain_expiration_policy;