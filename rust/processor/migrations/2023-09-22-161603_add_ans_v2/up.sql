-- Tracks ans v1 and v2 records
CREATE TABLE IF NOT EXISTS current_ans_lookup_v2 (
  domain VARCHAR(64) NOT NULL,
  -- if subdomain is null set to empty string
  subdomain VARCHAR(64) NOT NULL,
  token_standard VARCHAR(10) NOT NULL,
  token_name VARCHAR(140),
  registered_address VARCHAR(66),
  expiration_timestamp TIMESTAMP NOT NULL,
  last_transaction_version BIGINT NOT NULL,
  is_deleted BOOLEAN NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  -- Constraints
  PRIMARY KEY (domain, subdomain, token_standard)
);
CREATE INDEX IF NOT EXISTS ans_v2_tn_index on current_ans_lookup_v2 (token_name, token_standard);
CREATE INDEX IF NOT EXISTS ans_v2_et_index ON current_ans_lookup_v2 (expiration_timestamp);
CREATE INDEX IF NOT EXISTS ans_v2_ra_index ON current_ans_lookup_v2 (registered_address);
CREATE INDEX IF NOT EXISTS ans_v2_insat_index ON current_ans_lookup_v2 (inserted_at);

-- Tracks current ans v1 and v2 primary names,
CREATE TABLE IF NOT EXISTS current_ans_primary_name_v2 (
    registered_address VARCHAR(66) NOT NULL,
    token_standard VARCHAR(10) NOT NULL,
    domain VARCHAR(64),
    subdomain VARCHAR(64),
    token_name VARCHAR(140),
    --  Deleted means registered_address no longer has a primary name
    is_deleted BOOLEAN NOT NULL,
    last_transaction_version BIGINT NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
    -- Constraints
    PRIMARY KEY (registered_address, token_standard)
);
CREATE INDEX IF NOT EXISTS capn_v2_tn_index on current_ans_primary_name_v2 (token_name, token_standard);
CREATE INDEX IF NOT EXISTS capn_v2_insat_index ON current_ans_primary_name_v2 (inserted_at);

-- Tracks full history of the ans v1 and v2 records table
CREATE TABLE IF NOT EXISTS ans_lookup_v2 (
    transaction_version BIGINT NOT NULL,
    write_set_change_index BIGINT NOT NULL,
    domain VARCHAR(64) NOT NULL,
    -- if subdomain is null set to empty string
    subdomain VARCHAR(64) NOT NULL,
    token_standard VARCHAR(10) NOT NULL,
    registered_address VARCHAR(66),
    expiration_timestamp TIMESTAMP,
    token_name VARCHAR(140) NOT NULL,
    is_deleted BOOLEAN NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
    -- Constraints 
    PRIMARY KEY (transaction_version, write_set_change_index)
);
CREATE INDEX IF NOT EXISTS al_v2_name_index on ans_lookup_v2 (domain, subdomain, token_standard);
CREATE INDEX IF NOT EXISTS al_v2_ra_index on ans_lookup_v2 (registered_address);
CREATE INDEX IF NOT EXISTS al_v2_insat_index on ans_lookup_v2 (inserted_at);

-- Tracks full history of ans v1 and v2 primary names
CREATE TABLE IF NOT EXISTS ans_primary_name_v2 (
    transaction_version BIGINT NOT NULL,
    write_set_change_index BIGINT NOT NULL,
    registered_address VARCHAR(66) NOT NULL,
    domain VARCHAR(64),
    subdomain VARCHAR(64),
    token_standard VARCHAR(10) NOT NULL,
    token_name VARCHAR(140),
    is_deleted BOOLEAN NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
    -- Constraints 
    PRIMARY KEY (
        transaction_version,
        write_set_change_index
    )
);
CREATE INDEX IF NOT EXISTS apn_v2_name_index on ans_primary_name_v2 (domain, subdomain, token_standard);
CREATE INDEX IF NOT EXISTS apn_v2_ra_index on ans_primary_name_v2 (registered_address);
CREATE INDEX IF NOT EXISTS apn_v2_insat_index on ans_primary_name_v2 (inserted_at);

DROP VIEW IF EXISTS current_aptos_names;
CREATE OR REPLACE VIEW current_aptos_names AS 
SELECT 
    cal.domain,
	cal.subdomain,
	cal.token_name,
	cal.token_standard,
	cal.registered_address,
	cal.expiration_timestamp,
	greatest(cal.last_transaction_version,
	capn.last_transaction_version) as last_transaction_version,
	coalesce(not capn.is_deleted,
	false) as is_primary,
	concat(cal.domain, '.apt') as domain_with_suffix,
	c.owner_address as owner_address,
	cal.expiration_timestamp >= CURRENT_TIMESTAMP as is_active
FROM current_ans_lookup_v2 cal
LEFT JOIN current_ans_primary_name_v2 capn
ON 
    cal.token_name = capn.token_name 
    AND cal.token_standard = capn.token_standard
JOIN current_token_datas_v2 b
ON
	cal.token_name = b.token_name
    AND cal.token_standard = b.token_standard
JOIN current_token_ownerships_v2 c
ON
    b.token_data_id = c.token_data_id
    AND b.token_standard = c.token_standard
WHERE
	cal.is_deleted IS false
	AND c.amount > 0
	AND b.collection_id IN (
		'0x1c380887f0cfcc8a82c0df44b24116985a92c58e686a0ea4a441c9f423a72b47', -- Testnet ANS v1 domain collection
		'0x56654f4bf4e528bfef33094d11a3475f0638e949b0976ec831ca0d66a2efb673', -- Testnet ANS v2 domain collection 
		'0x3a2c902067bb4f0e37a2a89675d5cbceb07cf1a27479229b269fb1afffa62230', -- Testnet ANS v2 subdomain collection
        '0x09e63a48047b1c2bc51c0abc4b67ffcd9922e0adc99a6cc36532662172976a4b', -- Mainnet ANS v1 domain collection
        '0x63d26a4e3a8aeececf9b878e46bad78997fb38e50936efeabb2c4453f4d7f746', -- Mainnet ANS v2 domain collection
        '0x63d26a4e3a8aeececf9b878e46bad78997fb38e50936efeabb2c4453f4d7f746' -- Mainnet ANS v2 subdomain collection
	)