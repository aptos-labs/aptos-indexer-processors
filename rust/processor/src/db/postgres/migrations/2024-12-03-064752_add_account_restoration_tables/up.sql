-- Public key to associated multikey auth keys
CREATE TABLE public_key_auth_keys (
  public_key VARCHAR(200) NOT NULL,
  public_key_type VARCHAR(50) NOT NULL,
  auth_key VARCHAR(66) NOT NULL,
  verified BOOLEAN NOT NULL,
  last_transaction_version BIGINT NOT NULL,
  -- Constraints
  PRIMARY KEY (
    public_key,
    public_key_type,
    auth_key
  )
);

-- Auth key to its corresponding multikey layout
CREATE TABLE auth_key_multikey_layout (
  auth_key VARCHAR(66) PRIMARY KEY NOT NULL,
  signatures_required BIGINT NOT NULL,
  multikey_layout_with_prefixes jsonb NOT NULL,
  multikey_type VARCHAR(50) NOT NULL,
  last_transaction_version BIGINT NOT NULL
);

-- Auth key to account addresses
CREATE TABLE auth_key_account_addresses (
  auth_key VARCHAR(66) NOT NULL,
  address VARCHAR(66) PRIMARY KEY NOT NULL,
  verified BOOLEAN NOT NULL,
  last_transaction_version BIGINT NOT NULL
);
