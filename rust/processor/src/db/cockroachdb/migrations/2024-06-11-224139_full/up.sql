CREATE TABLE account_transactions (
    transaction_version bigint NOT NULL,
    account_address character varying(66) NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (account_address, transaction_version)
);
-- CREATE INDEX at_insat_index ON account_transactions (inserted_at);
CREATE INDEX at_version_index ON account_transactions (transaction_version DESC);

CREATE TABLE current_ans_lookup_v2 (
    domain character varying(64) NOT NULL,
    subdomain character varying(64) NOT NULL,
    token_standard character varying(10) NOT NULL,
    token_name character varying(140),
    registered_address character varying(66),
    expiration_timestamp timestamp without time zone NOT NULL,
    last_transaction_version bigint NOT NULL,
    is_deleted boolean NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    subdomain_expiration_policy bigint,
    PRIMARY KEY (domain, subdomain, token_standard)
);
CREATE INDEX ans_v2_et_index ON current_ans_lookup_v2 (expiration_timestamp);
-- CREATE INDEX ans_v2_insat_index ON current_ans_lookup_v2 (inserted_at);
CREATE INDEX ans_v2_ra_index ON current_ans_lookup_v2 (registered_address);
CREATE INDEX ans_v2_tn_index ON current_ans_lookup_v2 (token_name, token_standard);
CREATE INDEX lm1_ans_d_s_et_index ON current_ans_lookup_v2 (domain, subdomain, expiration_timestamp);
CREATE INDEX lm1_ans_ra_et_index ON current_ans_lookup_v2 (registered_address, expiration_timestamp);

CREATE TABLE current_ans_primary_name_v2 (
    registered_address character varying(66) NOT NULL,
    token_standard character varying(10) NOT NULL,
    domain character varying(64),
    subdomain character varying(64),
    token_name character varying(140),
    is_deleted boolean NOT NULL,
    last_transaction_version bigint NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (registered_address, token_standard)
);
-- CREATE INDEX capn_v2_insat_index ON current_ans_primary_name_v2 (inserted_at);
CREATE INDEX capn_v2_tn_index ON current_ans_primary_name_v2 (token_name, token_standard);

CREATE TABLE current_fungible_asset_balances (
    storage_id character varying(66) NOT NULL,
    owner_address character varying(66) NOT NULL,
    asset_type character varying(1000) NOT NULL,
    is_primary boolean NOT NULL,
    is_frozen boolean NOT NULL,
    amount numeric NOT NULL,
    last_transaction_timestamp timestamp without time zone NOT NULL,
    last_transaction_version bigint NOT NULL,
    token_standard character varying(10) NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (storage_id)
);
-- CREATE INDEX cfab_insat_index ON current_fungible_asset_balances (inserted_at);
CREATE INDEX cfab_owner_at_index ON current_fungible_asset_balances (owner_address, asset_type);
CREATE INDEX lm1_ccb_ct_a_index ON current_fungible_asset_balances (asset_type, amount);

CREATE TABLE current_collections_v2 (
    collection_id character varying(66) NOT NULL,
    creator_address character varying(66) NOT NULL,
    collection_name character varying(128) NOT NULL,
    description text NOT NULL,
    uri character varying(512) NOT NULL,
    current_supply numeric NOT NULL,
    max_supply numeric,
    total_minted_v2 numeric,
    mutable_description boolean,
    mutable_uri boolean,
    table_handle_v1 character varying(66),
    token_standard character varying(10) NOT NULL,
    last_transaction_version bigint NOT NULL,
    last_transaction_timestamp timestamp without time zone NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (collection_id)
);
CREATE INDEX cur_col2_crea_cn_index ON current_collections_v2 (creator_address, collection_name);
-- CREATE INDEX cur_col2_insat_index ON current_collections_v2 (inserted_at);

CREATE TABLE current_token_datas_v2 (
    token_data_id character varying(66) NOT NULL,
    collection_id character varying(66) NOT NULL,
    token_name character varying(128) NOT NULL,
    maximum numeric,
    supply numeric,
    largest_property_version_v1 numeric,
    token_uri character varying(512) NOT NULL,
    description text NOT NULL,
    token_properties jsonb NOT NULL,
    token_standard character varying(10) NOT NULL,
    is_fungible_v2 boolean,
    last_transaction_version bigint NOT NULL,
    last_transaction_timestamp timestamp without time zone NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    decimals bigint DEFAULT 0,
    is_deleted_v2 boolean,
    PRIMARY KEY (token_data_id)
);
CREATE INDEX cur_td2_cid_name_index ON current_token_datas_v2 (collection_id, token_name);
-- CREATE INDEX cur_td2_insat_index ON current_token_datas_v2 (inserted_at);

CREATE TABLE current_token_royalty_v1 (
    token_data_id character varying(66) NOT NULL,
    payee_address character varying(66) NOT NULL,
    royalty_points_numerator numeric NOT NULL,
    royalty_points_denominator numeric NOT NULL,
    last_transaction_version bigint NOT NULL,
    last_transaction_timestamp timestamp without time zone NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (token_data_id)
);

CREATE TABLE current_token_ownerships_v2 (
    token_data_id character varying(66) NOT NULL,
    property_version_v1 numeric NOT NULL,
    owner_address character varying(66) NOT NULL,
    storage_id character varying(66) NOT NULL,
    amount numeric NOT NULL,
    table_type_v1 character varying(66),
    token_properties_mutated_v1 jsonb,
    is_soulbound_v2 boolean,
    token_standard character varying(10) NOT NULL,
    is_fungible_v2 boolean,
    last_transaction_version bigint NOT NULL,
    last_transaction_timestamp timestamp without time zone NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    non_transferrable_by_owner boolean,
    PRIMARY KEY (token_data_id, property_version_v1, owner_address, storage_id)
);
-- CREATE INDEX curr_to2_insat_index ON current_token_ownerships_v2 (inserted_at);
CREATE INDEX curr_to2_owner_index ON current_token_ownerships_v2 (owner_address);
CREATE INDEX curr_to2_wa_index ON current_token_ownerships_v2 (storage_id);
CREATE INDEX lm1_curr_to_oa_tt_am_ltv_index ON current_token_ownerships_v2 (owner_address, table_type_v1, amount, last_transaction_version DESC);
CREATE INDEX lm1_curr_to_oa_tt_ltv_index ON current_token_ownerships_v2 (owner_address, table_type_v1, last_transaction_version DESC);

CREATE TABLE events (
    sequence_number bigint NOT NULL,
    creation_number bigint NOT NULL,
    account_address character varying(66) NOT NULL,
    transaction_version bigint NOT NULL,
    transaction_block_height bigint NOT NULL,
    type text NOT NULL,
    data jsonb NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    event_index bigint NOT NULL,
    indexed_type character varying(300) DEFAULT ''::character varying NOT NULL,
    PRIMARY KEY (transaction_version, event_index)
);
CREATE INDEX ev_addr_type_index ON events (account_address);
-- CREATE INDEX ev_insat_index ON events (inserted_at);
CREATE INDEX ev_itype_index ON events (indexed_type);

CREATE VIEW address_events_summary AS
 SELECT events.account_address,
    min(events.transaction_block_height) AS min_block_height,
    count(DISTINCT events.transaction_version) AS num_distinct_versions
   FROM events
  GROUP BY events.account_address;
CREATE VIEW address_version_from_events AS
 SELECT events.account_address,
    events.transaction_version
   FROM events
  GROUP BY events.account_address, events.transaction_version;

CREATE TABLE fungible_asset_activities (
    transaction_version bigint NOT NULL,
    event_index bigint NOT NULL,
    owner_address character varying(66) NOT NULL,
    storage_id character varying(66) NOT NULL,
    asset_type character varying(1000) NOT NULL,
    is_frozen boolean,
    amount numeric,
    type character varying NOT NULL,
    is_gas_fee boolean NOT NULL,
    gas_fee_payer_address character varying(66),
    is_transaction_success boolean NOT NULL,
    entry_function_id_str character varying(1000),
    block_height bigint NOT NULL,
    token_standard character varying(10) NOT NULL,
    transaction_timestamp timestamp without time zone NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    storage_refund_amount numeric DEFAULT 0 NOT NULL,
    PRIMARY KEY (transaction_version, event_index)
);
CREATE INDEX faa_at_index ON fungible_asset_activities (asset_type);
CREATE INDEX faa_gfpa_index ON fungible_asset_activities (gas_fee_payer_address);
-- CREATE INDEX faa_insat_idx ON fungible_asset_activities (inserted_at);
CREATE INDEX faa_owner_type_index ON fungible_asset_activities (owner_address, type);
CREATE INDEX faa_si_index ON fungible_asset_activities (storage_id);
CREATE INDEX lm1_ca_ct_a_index ON fungible_asset_activities (asset_type, amount);
CREATE INDEX lm1_ca_ct_at_a_index ON fungible_asset_activities (asset_type, type, amount);
CREATE INDEX lm1_ca_oa_ct_at_index ON fungible_asset_activities (owner_address, asset_type, type, amount);
CREATE INDEX lm1_ca_oa_igf_index ON fungible_asset_activities (owner_address, is_gas_fee);

CREATE TABLE fungible_asset_metadata (
    asset_type character varying(1000) NOT NULL,
    creator_address character varying(66) NOT NULL,
    name character varying(32) NOT NULL,
    symbol character varying(10) NOT NULL,
    decimals int4 NOT NULL,
    icon_uri character varying(512),
    project_uri character varying(512),
    last_transaction_version bigint NOT NULL,
    last_transaction_timestamp timestamp without time zone NOT NULL,
    supply_aggregator_table_handle_v1 character varying(66),
    supply_aggregator_table_key_v1 text,
    token_standard character varying(10) NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    is_token_v2 boolean,
    supply_v2 numeric,
    maximum_v2 numeric,
    PRIMARY KEY (asset_type)
);
CREATE INDEX fam_creator_index ON fungible_asset_metadata (creator_address);
-- CREATE INDEX fam_insat_index ON fungible_asset_metadata (inserted_at);

CREATE TABLE token_activities_v2 (
    transaction_version bigint NOT NULL,
    event_index bigint NOT NULL,
    event_account_address character varying(66) NOT NULL,
    token_data_id character varying(66) NOT NULL,
    property_version_v1 numeric NOT NULL,
    type character varying NOT NULL,
    from_address character varying(66),
    to_address character varying(66),
    token_amount numeric NOT NULL,
    before_value text,
    after_value text,
    entry_function_id_str character varying(1000),
    token_standard character varying(10) NOT NULL,
    is_fungible_v2 boolean,
    transaction_timestamp timestamp without time zone NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (transaction_version, event_index)
);
CREATE INDEX ta2_from_type_index ON token_activities_v2 (from_address, type);
-- CREATE INDEX ta2_insat_index ON token_activities_v2 (inserted_at);
CREATE INDEX ta2_owner_type_index ON token_activities_v2 (event_account_address, type);
CREATE INDEX ta2_tid_index ON token_activities_v2 (token_data_id);
CREATE INDEX ta2_to_type_index ON token_activities_v2 (to_address, type);
CREATE INDEX lm1_ta_tdih_pv_index ON token_activities_v2 (token_data_id, property_version_v1);

CREATE TABLE ans_lookup_v2 (
    transaction_version bigint NOT NULL,
    write_set_change_index bigint NOT NULL,
    domain character varying(64) NOT NULL,
    subdomain character varying(64) NOT NULL,
    token_standard character varying(10) NOT NULL,
    registered_address character varying(66),
    expiration_timestamp timestamp without time zone,
    token_name character varying(140) NOT NULL,
    is_deleted boolean NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    subdomain_expiration_policy bigint,
    PRIMARY KEY (transaction_version, write_set_change_index)
);
-- CREATE INDEX al_v2_insat_index ON ans_lookup_v2 (inserted_at);
CREATE INDEX al_v2_name_index ON ans_lookup_v2 (domain, subdomain, token_standard);
CREATE INDEX al_v2_ra_index ON ans_lookup_v2 (registered_address);

CREATE TABLE block_metadata_transactions (
    version bigint NOT NULL,
    block_height bigint NOT NULL,
    id character varying(66) NOT NULL,
    round bigint NOT NULL,
    epoch bigint NOT NULL,
    previous_block_votes_bitvec jsonb NOT NULL,
    proposer character varying(66) NOT NULL,
    failed_proposer_indices jsonb NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (version)
);
-- ALTER TABLE ONLY block_metadata_transactions ADD CONSTRAINT block_metadata_transactions_block_height_key UNIQUE (block_height);
-- CREATE INDEX bmt_insat_index ON block_metadata_transactions (inserted_at);

CREATE VIEW current_aptos_names AS
 SELECT cal.domain,
    cal.subdomain,
    cal.token_name,
    cal.token_standard,
    cal.registered_address,
    cal.expiration_timestamp,
    GREATEST(cal.last_transaction_version, capn.last_transaction_version) AS last_transaction_version,
    COALESCE((NOT capn.is_deleted), false) AS is_primary,
    concat(cal.domain, '.apt') AS domain_with_suffix,
    c.owner_address,
    (cal.expiration_timestamp >= CURRENT_TIMESTAMP) AS is_active,
    cal2.expiration_timestamp AS domain_expiration_timestamp,
    b.token_data_id,
    cal.subdomain_expiration_policy
   FROM ((((current_ans_lookup_v2 cal
     LEFT JOIN current_ans_primary_name_v2 capn ON ((((cal.token_name)::text = (capn.token_name)::text) AND ((cal.token_standard)::text = (capn.token_standard)::text))))
     JOIN current_token_datas_v2 b ON ((((cal.token_name)::text = (b.token_name)::text) AND ((cal.token_standard)::text = (b.token_standard)::text))))
     JOIN current_token_ownerships_v2 c ON ((((b.token_data_id)::text = (c.token_data_id)::text) AND ((b.token_standard)::text = (c.token_standard)::text))))
     LEFT JOIN current_ans_lookup_v2 cal2 ON ((((cal.domain)::text = (cal2.domain)::text) AND ((cal2.subdomain)::text = ''::text) AND ((cal.token_standard)::text = (cal2.token_standard)::text))))
  WHERE ((cal.is_deleted IS FALSE) AND (c.amount > (0)::numeric) AND ((b.collection_id)::text = ANY ((ARRAY['0x1c380887f0cfcc8a82c0df44b24116985a92c58e686a0ea4a441c9f423a72b47'::character varying, '0x56654f4bf4e528bfef33094d11a3475f0638e949b0976ec831ca0d66a2efb673'::character varying, '0x3a2c902067bb4f0e37a2a89675d5cbceb07cf1a27479229b269fb1afffa62230'::character varying, '0x09e63a48047b1c2bc51c0abc4b67ffcd9922e0adc99a6cc36532662172976a4b'::character varying, '0x63d26a4e3a8aeececf9b878e46bad78997fb38e50936efeabb2c4453f4d7f746'::character varying, '0x30fbc956f0f38db2d314bd9c018d34be3e047a804a71e30a4e5d43d8b7c539eb'::character varying])::text[])));

CREATE TABLE current_collection_datas (
    collection_data_id_hash character varying(64) NOT NULL,
    creator_address character varying(66) NOT NULL,
    collection_name character varying(128) NOT NULL,
    description text NOT NULL,
    metadata_uri character varying(512) NOT NULL,
    supply numeric NOT NULL,
    maximum numeric NOT NULL,
    maximum_mutable boolean NOT NULL,
    uri_mutable boolean NOT NULL,
    description_mutable boolean NOT NULL,
    last_transaction_version bigint NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    table_handle character varying(66) NOT NULL,
    last_transaction_timestamp timestamp without time zone NOT NULL,
    PRIMARY KEY (collection_data_id_hash)
);
CREATE INDEX curr_cd_crea_cn_index ON current_collection_datas (creator_address, collection_name);
-- CREATE INDEX curr_cd_insat_index ON current_collection_datas (inserted_at);
CREATE INDEX curr_cd_th_index ON current_collection_datas (table_handle);

CREATE TABLE current_delegated_staking_pool_balances (
    staking_pool_address character varying(66) NOT NULL,
    total_coins numeric NOT NULL,
    total_shares numeric NOT NULL,
    last_transaction_version bigint NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    operator_commission_percentage numeric NOT NULL,
    inactive_table_handle character varying(66) NOT NULL,
    active_table_handle character varying(66) NOT NULL,
    PRIMARY KEY (staking_pool_address)
);
CREATE INDEX cdspb_inactive_index ON current_delegated_staking_pool_balances (inactive_table_handle);
-- CREATE INDEX cdspb_insat_index ON current_delegated_staking_pool_balances (inserted_at);

CREATE TABLE current_delegated_voter (
    delegation_pool_address character varying(66) NOT NULL,
    delegator_address character varying(66) NOT NULL,
    table_handle character varying(66),
    voter character varying(66),
    pending_voter character varying(66),
    last_transaction_version bigint NOT NULL,
    last_transaction_timestamp timestamp without time zone NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (delegation_pool_address, delegator_address)
);
CREATE INDEX cdv_da_index ON current_delegated_voter (delegator_address);
-- CREATE INDEX cdv_insat_index ON current_delegated_voter (inserted_at);
CREATE INDEX cdv_pv_index ON current_delegated_voter (pending_voter);
CREATE INDEX cdv_th_index ON current_delegated_voter (table_handle);
CREATE INDEX cdv_v_index ON current_delegated_voter (voter);

CREATE TABLE current_delegator_balances (
    delegator_address character varying(66) NOT NULL,
    pool_address character varying(66) NOT NULL,
    pool_type character varying(100) NOT NULL,
    table_handle character varying(66) NOT NULL,
    last_transaction_version bigint NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    shares numeric NOT NULL,
    parent_table_handle character varying(66) NOT NULL,
    PRIMARY KEY (delegator_address, pool_address, pool_type, table_handle)
);
-- CREATE INDEX cdb_insat_index ON current_delegator_balances (inserted_at);

CREATE TABLE current_objects (
    object_address character varying(66) NOT NULL,
    owner_address character varying(66) NOT NULL,
    state_key_hash character varying(66) NOT NULL,
    allow_ungated_transfer boolean NOT NULL,
    last_guid_creation_num numeric NOT NULL,
    last_transaction_version bigint NOT NULL,
    is_deleted boolean NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (object_address)
);
-- CREATE INDEX co_insat_idx ON current_objects (inserted_at);
CREATE INDEX co_object_skh_idx ON current_objects (object_address, state_key_hash);
CREATE INDEX co_owner_idx ON current_objects (owner_address);
CREATE INDEX co_skh_idx ON current_objects (state_key_hash);

CREATE TABLE current_staking_pool_voter (
    staking_pool_address character varying(66) NOT NULL,
    voter_address character varying(66) NOT NULL,
    last_transaction_version bigint NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    operator_address character varying(66) NOT NULL,
    PRIMARY KEY (staking_pool_address)
);
-- CREATE INDEX ctpv_insat_index ON current_staking_pool_voter (inserted_at);
CREATE INDEX ctpv_va_index ON current_staking_pool_voter (voter_address);

CREATE TABLE current_table_items (
    table_handle character varying(66) NOT NULL,
    key_hash character varying(64) NOT NULL,
    key text NOT NULL,
    decoded_key jsonb NOT NULL,
    decoded_value jsonb,
    is_deleted boolean NOT NULL,
    last_transaction_version bigint NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (table_handle, key_hash)
);
-- CREATE INDEX cti_insat_index ON current_table_items (inserted_at);

CREATE VIEW current_table_items_view AS
 SELECT current_table_items.key,
    current_table_items.table_handle,
    current_table_items.key_hash,
    (current_table_items.decoded_key #>> '{}'::text[]) AS json_decoded_key,
    (current_table_items.decoded_value #>> '{}'::text[]) AS json_decoded_value,
    current_table_items.is_deleted,
    current_table_items.last_transaction_version,
    current_table_items.inserted_at
   FROM current_table_items;

CREATE TABLE current_token_pending_claims (
    token_data_id_hash character varying(64) NOT NULL,
    property_version numeric NOT NULL,
    from_address character varying(66) NOT NULL,
    to_address character varying(66) NOT NULL,
    collection_data_id_hash character varying(64) NOT NULL,
    creator_address character varying(66) NOT NULL,
    collection_name character varying(128) NOT NULL,
    name character varying(128) NOT NULL,
    amount numeric NOT NULL,
    table_handle character varying(66) NOT NULL,
    last_transaction_version bigint NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    last_transaction_timestamp timestamp without time zone NOT NULL,
    token_data_id character varying(66) DEFAULT ''::character varying NOT NULL,
    collection_id character varying(66) DEFAULT ''::character varying NOT NULL,
    PRIMARY KEY (token_data_id_hash, property_version, from_address, to_address)
);
CREATE INDEX ctpc_from_am_index ON current_token_pending_claims (from_address, amount);
-- CREATE INDEX ctpc_insat_index ON current_token_pending_claims (inserted_at);
CREATE INDEX ctpc_th_index ON current_token_pending_claims (table_handle);
CREATE INDEX ctpc_to_am_index ON current_token_pending_claims (to_address, amount);

CREATE TABLE current_unified_fungible_asset_balances (
    storage_id character varying(66) NOT NULL,
    owner_address character varying(66) NOT NULL,
    asset_type character varying(66) NOT NULL,
    coin_type character varying(1000),
    is_primary boolean,
    is_frozen boolean NOT NULL,
    amount_v1 numeric,
    amount_v2 numeric,
    amount numeric GENERATED ALWAYS AS ((COALESCE(amount_v1, (0)::numeric) + COALESCE(amount_v2, (0)::numeric))) STORED,
    last_transaction_version_v1 bigint,
    last_transaction_version_v2 bigint,
    last_transaction_version bigint GENERATED ALWAYS AS (GREATEST(last_transaction_version_v1, last_transaction_version_v2)) STORED,
    last_transaction_timestamp_v1 timestamp without time zone,
    last_transaction_timestamp_v2 timestamp without time zone,
    last_transaction_timestamp timestamp without time zone GENERATED ALWAYS AS (GREATEST(last_transaction_timestamp_v1, last_transaction_timestamp_v2)) STORED,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (storage_id)
);
-- CREATE INDEX cufab_insat_index ON current_unified_fungible_asset_balances (inserted_at);
CREATE INDEX cufab_owner_at_index ON current_unified_fungible_asset_balances (owner_address, asset_type);

CREATE TABLE delegated_staking_activities (
    transaction_version bigint NOT NULL,
    event_index bigint NOT NULL,
    delegator_address character varying(66) NOT NULL,
    pool_address character varying(66) NOT NULL,
    event_type text NOT NULL,
    amount numeric NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (transaction_version, event_index)
);
-- CREATE INDEX dsa_insat_index ON delegated_staking_activities (inserted_at);
CREATE INDEX dsa_pa_da_index ON delegated_staking_activities (pool_address, delegator_address, transaction_version, event_index);

CREATE TABLE delegated_staking_pool_balances (
    transaction_version bigint NOT NULL,
    staking_pool_address character varying(66) NOT NULL,
    total_coins numeric NOT NULL,
    total_shares numeric NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    operator_commission_percentage numeric NOT NULL,
    inactive_table_handle character varying(66) NOT NULL,
    active_table_handle character varying(66) NOT NULL,
    PRIMARY KEY (transaction_version, staking_pool_address)
);
-- CREATE INDEX dspb_insat_index ON delegated_staking_pool_balances (inserted_at);

CREATE TABLE delegated_staking_pools (
    staking_pool_address character varying(66) NOT NULL,
    first_transaction_version bigint NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (staking_pool_address)
);
-- CREATE INDEX dsp_insat_index ON delegated_staking_pools (inserted_at);

CREATE TABLE delegator_balances (
    transaction_version bigint NOT NULL,
    write_set_change_index bigint NOT NULL,
    delegator_address character varying(66) NOT NULL,
    pool_address character varying(66) NOT NULL,
    pool_type character varying(100) NOT NULL,
    table_handle character varying(66) NOT NULL,
    shares numeric NOT NULL,
    parent_table_handle character varying(66) NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (transaction_version, write_set_change_index)
);
CREATE INDEX db_da_index ON delegator_balances (delegator_address);
-- CREATE INDEX db_insat_index ON delegator_balances (inserted_at);

CREATE VIEW delegator_distinct_pool AS
 SELECT current_delegator_balances.delegator_address,
    current_delegator_balances.pool_address
   FROM current_delegator_balances
  WHERE (current_delegator_balances.shares > (0)::numeric)
  GROUP BY current_delegator_balances.delegator_address, current_delegator_balances.pool_address;

CREATE VIEW events_view AS
 SELECT events.sequence_number,
    events.creation_number,
    events.account_address,
    events.transaction_version,
    events.transaction_block_height,
    events.type,
    (events.data #>> '{}'::text[]) AS json_data,
    events.inserted_at
   FROM events;
CREATE TABLE indexer_status (
    db character varying(50) NOT NULL,
    is_indexer_up boolean NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (db)
);
CREATE TABLE ledger_infos (
    chain_id bigint NOT NULL,
    PRIMARY KEY (chain_id)
);

CREATE VIEW num_active_delegator_per_pool AS
 SELECT current_delegator_balances.pool_address,
    count(DISTINCT current_delegator_balances.delegator_address) AS num_active_delegator
   FROM current_delegator_balances
  WHERE ((current_delegator_balances.shares > (0)::numeric) AND ((current_delegator_balances.pool_type)::text = 'active_shares'::text))
  GROUP BY current_delegator_balances.pool_address;

CREATE TABLE processor_status (
    processor character varying(50) NOT NULL,
    last_success_version bigint NOT NULL,
    last_updated timestamp without time zone DEFAULT now() NOT NULL,
    last_transaction_timestamp timestamp without time zone,
    PRIMARY KEY (processor)
);
CREATE TABLE proposal_votes (
    transaction_version bigint NOT NULL,
    proposal_id bigint NOT NULL,
    voter_address character varying(66) NOT NULL,
    staking_pool_address character varying(66) NOT NULL,
    num_votes numeric NOT NULL,
    should_pass boolean NOT NULL,
    transaction_timestamp timestamp without time zone NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (transaction_version, proposal_id, voter_address)
);
-- CREATE INDEX pv_ia_index ON proposal_votes (inserted_at);
CREATE INDEX pv_pi_va_index ON proposal_votes (proposal_id, voter_address);
CREATE INDEX pv_spa_index ON proposal_votes (staking_pool_address);
CREATE INDEX pv_va_index ON proposal_votes (voter_address);

CREATE TABLE spam_assets (
    asset character varying(1100) NOT NULL,
    is_spam boolean DEFAULT true NOT NULL,
    last_updated timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (asset)
);

CREATE TABLE user_transactions (
    version bigint NOT NULL,
    block_height bigint NOT NULL,
    parent_signature_type character varying(50) NOT NULL,
    sender character varying(66) NOT NULL,
    sequence_number bigint NOT NULL,
    max_gas_amount numeric NOT NULL,
    expiration_timestamp_secs timestamp without time zone NOT NULL,
    gas_unit_price numeric NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    entry_function_id_str character varying(1000) NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    epoch bigint NOT NULL,
    PRIMARY KEY (version)
);
-- ALTER TABLE ONLY user_transactions ADD CONSTRAINT user_transactions_sender_sequence_number_key UNIQUE (sender, sequence_number);
CREATE INDEX ut_epoch_index ON user_transactions (epoch);
-- CREATE INDEX ut_insat_index ON user_transactions (inserted_at);
CREATE INDEX ut_sender_seq_index ON user_transactions (sender, sequence_number);

-- Deprecated tables 

CREATE TABLE collections_v2 (
    transaction_version bigint NOT NULL,
    write_set_change_index bigint NOT NULL,
    collection_id character varying(66) NOT NULL,
    creator_address character varying(66) NOT NULL,
    collection_name character varying(128) NOT NULL,
    description text NOT NULL,
    uri character varying(512) NOT NULL,
    current_supply numeric NOT NULL,
    max_supply numeric,
    total_minted_v2 numeric,
    mutable_description boolean,
    mutable_uri boolean,
    table_handle_v1 character varying(66),
    token_standard character varying(10) NOT NULL,
    transaction_timestamp timestamp without time zone NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (transaction_version, write_set_change_index)
);
CREATE INDEX col2_crea_cn_index ON collections_v2 (creator_address, collection_name);
CREATE INDEX col2_id_index ON collections_v2 (collection_id);
-- CREATE INDEX col2_insat_index ON collections_v2 (inserted_at);
CREATE INDEX lm1_cv_ci_tv_index ON collections_v2 (collection_id, transaction_version);

CREATE TABLE fungible_asset_balances (
    transaction_version bigint NOT NULL,
    write_set_change_index bigint NOT NULL,
    storage_id character varying(66) NOT NULL,
    owner_address character varying(66) NOT NULL,
    asset_type character varying(1000) NOT NULL,
    is_primary boolean NOT NULL,
    is_frozen boolean NOT NULL,
    amount numeric NOT NULL,
    transaction_timestamp timestamp without time zone NOT NULL,
    token_standard character varying(10) NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (transaction_version, write_set_change_index)
);
-- CREATE INDEX fab_insat_index ON fungible_asset_balances (inserted_at);
CREATE INDEX fab_owner_at_index ON fungible_asset_balances (owner_address, asset_type);
CREATE INDEX lm1_cb_tv_oa_ct_index ON fungible_asset_balances (transaction_version, owner_address, asset_type);

CREATE TABLE move_resources (
    transaction_version bigint NOT NULL,
    write_set_change_index bigint NOT NULL,
    transaction_block_height bigint NOT NULL,
    name text NOT NULL,
    address character varying(66) NOT NULL,
    type text NOT NULL,
    module text NOT NULL,
    generic_type_params jsonb,
    data jsonb,
    is_deleted boolean NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    state_key_hash character varying(66) DEFAULT ''::character varying NOT NULL,
    PRIMARY KEY (transaction_version, write_set_change_index)
);
CREATE INDEX mr_addr_mod_name_ver_index ON move_resources (address, module, name, transaction_version);
-- CREATE INDEX mr_insat_index ON move_resources (inserted_at);
CREATE INDEX mr_ver_index ON move_resources (transaction_version DESC);

CREATE VIEW address_version_from_move_resources AS
 SELECT move_resources.address,
    move_resources.transaction_version
   FROM move_resources
  GROUP BY move_resources.address, move_resources.transaction_version;

CREATE TABLE token_datas_v2 (
    transaction_version bigint NOT NULL,
    write_set_change_index bigint NOT NULL,
    token_data_id character varying(66) NOT NULL,
    collection_id character varying(66) NOT NULL,
    token_name character varying(128) NOT NULL,
    maximum numeric,
    supply numeric,
    largest_property_version_v1 numeric,
    token_uri character varying(512) NOT NULL,
    token_properties jsonb NOT NULL,
    description text NOT NULL,
    token_standard character varying(10) NOT NULL,
    is_fungible_v2 boolean,
    transaction_timestamp timestamp without time zone NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    decimals bigint DEFAULT 0,
    is_deleted_v2 boolean,
    PRIMARY KEY (transaction_version, write_set_change_index)
);
CREATE INDEX td2_cid_name_index ON token_datas_v2 (collection_id, token_name);
CREATE INDEX td2_id_index ON token_datas_v2 (token_data_id);
-- CREATE INDEX td2_insat_index ON token_datas_v2 (inserted_at);
CREATE INDEX lm1_tdv_tdi_tv_index ON token_datas_v2 (token_data_id, transaction_version);

CREATE TABLE token_ownerships_v2 (
    transaction_version bigint NOT NULL,
    write_set_change_index bigint NOT NULL,
    token_data_id character varying(66) NOT NULL,
    property_version_v1 numeric NOT NULL,
    owner_address character varying(66),
    storage_id character varying(66) NOT NULL,
    amount numeric NOT NULL,
    table_type_v1 character varying(66),
    token_properties_mutated_v1 jsonb,
    is_soulbound_v2 boolean,
    token_standard character varying(10) NOT NULL,
    is_fungible_v2 boolean,
    transaction_timestamp timestamp without time zone NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    non_transferrable_by_owner boolean,
    PRIMARY KEY (transaction_version, write_set_change_index)
);
CREATE INDEX to2_id_index ON token_ownerships_v2 (token_data_id);
-- CREATE INDEX to2_insat_index ON token_ownerships_v2 (inserted_at);
CREATE INDEX to2_owner_index ON token_ownerships_v2 (owner_address);

CREATE TABLE ans_lookup (
    transaction_version bigint NOT NULL,
    write_set_change_index bigint NOT NULL,
    domain character varying(64) NOT NULL,
    subdomain character varying(64) NOT NULL,
    registered_address character varying(66),
    expiration_timestamp timestamp without time zone,
    token_name character varying(140) NOT NULL,
    is_deleted boolean NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (transaction_version, write_set_change_index)
);
-- CREATE INDEX al_insat_index ON ans_lookup (inserted_at);
CREATE INDEX al_tn_index ON ans_lookup (token_name);

CREATE TABLE ans_primary_name (
    transaction_version bigint NOT NULL,
    write_set_change_index bigint NOT NULL,
    registered_address character varying(66) NOT NULL,
    domain character varying(64),
    subdomain character varying(64),
    token_name character varying(140),
    is_deleted boolean NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (transaction_version, write_set_change_index)
);
-- CREATE INDEX apn_insat_index ON ans_primary_name (inserted_at);
CREATE INDEX apn_tn_index ON ans_primary_name (token_name);

CREATE TABLE ans_primary_name_v2 (
    transaction_version bigint NOT NULL,
    write_set_change_index bigint NOT NULL,
    registered_address character varying(66) NOT NULL,
    domain character varying(64),
    subdomain character varying(64),
    token_standard character varying(10) NOT NULL,
    token_name character varying(140),
    is_deleted boolean NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (transaction_version, write_set_change_index)
);
-- CREATE INDEX apn_v2_insat_index ON ans_primary_name_v2 (inserted_at);
CREATE INDEX apn_v2_name_index ON ans_primary_name_v2 (domain, subdomain, token_standard);
CREATE INDEX apn_v2_ra_index ON ans_primary_name_v2 (registered_address);

CREATE TABLE coin_activities (
    transaction_version bigint NOT NULL,
    event_account_address character varying(66) NOT NULL,
    event_creation_number bigint NOT NULL,
    event_sequence_number bigint NOT NULL,
    owner_address character varying(66) NOT NULL,
    coin_type character varying(5000) NOT NULL,
    amount numeric NOT NULL,
    activity_type character varying(200) NOT NULL,
    is_gas_fee boolean NOT NULL,
    is_transaction_success boolean NOT NULL,
    entry_function_id_str character varying(1000),
    block_height bigint NOT NULL,
    transaction_timestamp timestamp without time zone NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    event_index bigint,
    gas_fee_payer_address character varying(66) DEFAULT NULL::character varying,
    storage_refund_amount numeric DEFAULT 0 NOT NULL,
    PRIMARY KEY (transaction_version, event_account_address, event_creation_number, event_sequence_number)
);
CREATE INDEX ca_ct_a_index ON coin_activities (coin_type, amount);
CREATE INDEX ca_ct_at_a_index ON coin_activities (coin_type, activity_type, amount);
CREATE INDEX ca_gfpa_index ON coin_activities (gas_fee_payer_address);
-- CREATE INDEX ca_insat_index ON coin_activities (inserted_at);
CREATE INDEX ca_oa_ct_at_index ON coin_activities (owner_address, coin_type, activity_type, amount);
CREATE INDEX ca_oa_igf_index ON coin_activities (owner_address, is_gas_fee);

CREATE TABLE coin_balances (
    transaction_version bigint NOT NULL,
    owner_address character varying(66) NOT NULL,
    coin_type_hash character varying(64) NOT NULL,
    coin_type character varying(5000) NOT NULL,
    amount numeric NOT NULL,
    transaction_timestamp timestamp without time zone NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (transaction_version, owner_address, coin_type_hash)
);
CREATE INDEX cb_ct_a_index ON coin_balances (coin_type, amount);
-- CREATE INDEX cb_insat_index ON coin_balances (inserted_at);
CREATE INDEX cb_oa_ct_index ON coin_balances (owner_address, coin_type);
CREATE INDEX cb_tv_oa_ct_index ON coin_balances (transaction_version, owner_address, coin_type);

CREATE TABLE coin_infos (
    coin_type_hash character varying(64) NOT NULL,
    coin_type character varying(5000) NOT NULL,
    transaction_version_created bigint NOT NULL,
    creator_address character varying(66) NOT NULL,
    name character varying(32) NOT NULL,
    symbol character varying(10) NOT NULL,
    decimals int4 NOT NULL,
    transaction_created_timestamp timestamp without time zone NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    supply_aggregator_table_handle character varying(66),
    supply_aggregator_table_key text,
    PRIMARY KEY (coin_type_hash)
);
CREATE INDEX ci_ca_name_symbol_index ON coin_infos (creator_address, name, symbol);
CREATE INDEX ci_ct_index ON coin_infos (coin_type);
-- CREATE INDEX ci_insat_index ON coin_infos (inserted_at);

CREATE TABLE coin_supply (
    transaction_version bigint NOT NULL,
    coin_type_hash character varying(64) NOT NULL,
    coin_type character varying(5000) NOT NULL,
    supply numeric NOT NULL,
    transaction_timestamp timestamp without time zone NOT NULL,
    transaction_epoch bigint NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (transaction_version, coin_type_hash)
);
CREATE INDEX cs_ct_tv_index ON coin_supply (coin_type, transaction_version DESC);
CREATE INDEX cs_epoch_index ON coin_supply (transaction_epoch);

CREATE TABLE collection_datas (
    collection_data_id_hash character varying(64) NOT NULL,
    transaction_version bigint NOT NULL,
    creator_address character varying(66) NOT NULL,
    collection_name character varying(128) NOT NULL,
    description text NOT NULL,
    metadata_uri character varying(512) NOT NULL,
    supply numeric NOT NULL,
    maximum numeric NOT NULL,
    maximum_mutable boolean NOT NULL,
    uri_mutable boolean NOT NULL,
    description_mutable boolean NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    table_handle character varying(66) NOT NULL,
    transaction_timestamp timestamp without time zone NOT NULL,
    PRIMARY KEY (collection_data_id_hash, transaction_version)
);
CREATE INDEX cd_crea_cn_index ON collection_datas (creator_address, collection_name);
-- CREATE INDEX cd_insat_index ON collection_datas (inserted_at);

CREATE TABLE current_ans_lookup (
    domain character varying(64) NOT NULL,
    subdomain character varying(64) NOT NULL,
    registered_address character varying(66),
    expiration_timestamp timestamp without time zone NOT NULL,
    last_transaction_version bigint NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    token_name character varying(140) DEFAULT ''::character varying NOT NULL,
    is_deleted boolean DEFAULT false NOT NULL,
    PRIMARY KEY (domain, subdomain)
);
CREATE INDEX ans_d_s_et_index ON current_ans_lookup (domain, subdomain, expiration_timestamp);
CREATE INDEX ans_et_index ON current_ans_lookup (expiration_timestamp);
-- CREATE INDEX ans_insat_index ON current_ans_lookup (inserted_at);
CREATE INDEX ans_ra_et_index ON current_ans_lookup (registered_address, expiration_timestamp);
CREATE INDEX ans_tn_index ON current_ans_lookup (token_name);

CREATE TABLE current_ans_primary_name (
    registered_address character varying(66) NOT NULL,
    domain character varying(64),
    subdomain character varying(64),
    token_name character varying(140),
    is_deleted boolean NOT NULL,
    last_transaction_version bigint NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (registered_address)
);
-- CREATE INDEX capn_insat_index ON current_ans_primary_name (inserted_at);
CREATE INDEX capn_tn_index ON current_ans_primary_name (token_name);

CREATE TABLE current_coin_balances (
    owner_address character varying(66) NOT NULL,
    coin_type_hash character varying(64) NOT NULL,
    coin_type character varying(5000) NOT NULL,
    amount numeric NOT NULL,
    last_transaction_version bigint NOT NULL,
    last_transaction_timestamp timestamp without time zone NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (owner_address, coin_type)
);
CREATE INDEX ccb_ct_a_index ON current_coin_balances (coin_type, amount);
-- CREATE INDEX ccb_insat_index ON current_coin_balances (inserted_at);
CREATE INDEX ccb_oa_ct_index ON current_coin_balances (owner_address, coin_type);

CREATE TABLE current_token_ownerships (
    token_data_id_hash character varying(64) NOT NULL,
    property_version numeric NOT NULL,
    owner_address character varying(66) NOT NULL,
    creator_address character varying(66) NOT NULL,
    collection_name character varying(128) NOT NULL,
    name character varying(128) NOT NULL,
    amount numeric NOT NULL,
    token_properties jsonb NOT NULL,
    last_transaction_version bigint NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    collection_data_id_hash character varying(64) NOT NULL,
    table_type text NOT NULL,
    last_transaction_timestamp timestamp without time zone NOT NULL,
    PRIMARY KEY (token_data_id_hash, property_version, owner_address)
);
CREATE INDEX curr_to_collection_hash_owner_index ON current_token_ownerships (collection_data_id_hash, owner_address);
CREATE INDEX curr_to_crea_cn_name_index ON current_token_ownerships (creator_address, collection_name, name);
-- CREATE INDEX curr_to_insat_index ON current_token_ownerships (inserted_at);
CREATE INDEX curr_to_oa_tt_am_ltv_index ON current_token_ownerships (owner_address, table_type, amount, last_transaction_version DESC);
CREATE INDEX curr_to_oa_tt_ltv_index ON current_token_ownerships (owner_address, table_type, last_transaction_version DESC);
CREATE INDEX curr_to_owner_index ON current_token_ownerships (owner_address);
CREATE INDEX curr_to_owner_tt_am_index ON current_token_ownerships (owner_address, table_type, amount);

CREATE TABLE current_token_datas (
    token_data_id_hash character varying(64) NOT NULL,
    creator_address character varying(66) NOT NULL,
    collection_name character varying(128) NOT NULL,
    name character varying(128) NOT NULL,
    maximum numeric NOT NULL,
    supply numeric NOT NULL,
    largest_property_version numeric NOT NULL,
    metadata_uri character varying(512) NOT NULL,
    payee_address character varying(66) NOT NULL,
    royalty_points_numerator numeric NOT NULL,
    royalty_points_denominator numeric NOT NULL,
    maximum_mutable boolean NOT NULL,
    uri_mutable boolean NOT NULL,
    description_mutable boolean NOT NULL,
    properties_mutable boolean NOT NULL,
    royalty_mutable boolean NOT NULL,
    default_properties jsonb NOT NULL,
    last_transaction_version bigint NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    collection_data_id_hash character varying(64) NOT NULL,
    last_transaction_timestamp timestamp without time zone NOT NULL,
    description text NOT NULL,
    PRIMARY KEY (token_data_id_hash)
);
CREATE INDEX curr_td_crea_cn_name_index ON current_token_datas (creator_address, collection_name, name);
-- CREATE INDEX curr_td_insat_index ON current_token_datas (inserted_at);

CREATE TABLE current_token_v2_metadata (
    object_address character varying(66) NOT NULL,
    resource_type character varying(128) NOT NULL,
    data jsonb NOT NULL,
    state_key_hash character varying(66) NOT NULL,
    last_transaction_version bigint NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (object_address, resource_type)
);

CREATE TABLE event_size_info (
    transaction_version bigint NOT NULL,
    index bigint NOT NULL,
    type_tag_bytes bigint NOT NULL,
    total_bytes bigint NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (transaction_version, index)
);
-- CREATE INDEX esi_insat_index ON event_size_info (inserted_at);

CREATE TABLE move_modules (
    transaction_version bigint NOT NULL,
    write_set_change_index bigint NOT NULL,
    transaction_block_height bigint NOT NULL,
    name text NOT NULL,
    address character varying(66) NOT NULL,
    bytecode bytea,
    friends jsonb,
    exposed_functions jsonb,
    structs jsonb,
    is_deleted boolean NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (transaction_version, write_set_change_index)
);
CREATE INDEX mm_addr_name_ver_index ON move_modules (address, name, transaction_version);
-- CREATE INDEX mm_insat_index ON move_modules (inserted_at);

CREATE VIEW move_resources_view AS
 SELECT move_resources.transaction_version,
    move_resources.write_set_change_index,
    move_resources.transaction_block_height,
    move_resources.name,
    move_resources.address,
    move_resources.type,
    move_resources.module,
    move_resources.generic_type_params,
    (move_resources.data #>> '{}'::text[]) AS json_data,
    move_resources.is_deleted,
    move_resources.inserted_at
   FROM move_resources;

CREATE TABLE nft_points (
    transaction_version bigint NOT NULL,
    owner_address character varying(66) NOT NULL,
    token_name text NOT NULL,
    point_type text NOT NULL,
    amount numeric NOT NULL,
    transaction_timestamp timestamp without time zone NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (transaction_version)
);
-- CREATE INDEX np_insat_idx ON nft_points (inserted_at);
CREATE INDEX np_oa_idx ON nft_points (owner_address);
CREATE INDEX np_tt_oa_idx ON nft_points (transaction_timestamp, owner_address);

CREATE TABLE objects (
    transaction_version bigint NOT NULL,
    write_set_change_index bigint NOT NULL,
    object_address character varying(66) NOT NULL,
    owner_address character varying(66) NOT NULL,
    state_key_hash character varying(66) NOT NULL,
    guid_creation_num numeric NOT NULL,
    allow_ungated_transfer boolean NOT NULL,
    is_deleted boolean NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (transaction_version, write_set_change_index)
);
-- CREATE INDEX o_insat_idx ON objects (inserted_at);
CREATE INDEX o_object_skh_idx ON objects (object_address, state_key_hash);
CREATE INDEX o_owner_idx ON objects (owner_address);
CREATE INDEX o_skh_idx ON objects (state_key_hash);

CREATE TABLE signatures (
    transaction_version bigint NOT NULL,
    multi_agent_index bigint NOT NULL,
    multi_sig_index bigint NOT NULL,
    transaction_block_height bigint NOT NULL,
    signer character varying(66) NOT NULL,
    is_sender_primary boolean NOT NULL,
    type character varying NOT NULL,
    public_key character varying(136) NOT NULL,
    signature text NOT NULL,
    threshold bigint NOT NULL,
    public_key_indices jsonb NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (transaction_version, multi_agent_index, multi_sig_index, is_sender_primary)
);
-- CREATE INDEX sig_insat_index ON signatures (inserted_at);

CREATE TABLE table_items (
    key text NOT NULL,
    transaction_version bigint NOT NULL,
    write_set_change_index bigint NOT NULL,
    transaction_block_height bigint NOT NULL,
    table_handle character varying(66) NOT NULL,
    decoded_key jsonb NOT NULL,
    decoded_value jsonb,
    is_deleted boolean NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (transaction_version, write_set_change_index)
);
CREATE INDEX ti_hand_ver_key_index ON table_items (table_handle, transaction_version);
-- CREATE INDEX ti_insat_index ON table_items (inserted_at);

CREATE VIEW table_items_view AS
 SELECT table_items.key,
    table_items.transaction_version,
    table_items.write_set_change_index,
    table_items.transaction_block_height,
    table_items.table_handle,
    (table_items.decoded_key #>> '{}'::text[]) AS json_decoded_key,
    (table_items.decoded_value #>> '{}'::text[]) AS json_decoded_value,
    table_items.is_deleted,
    table_items.inserted_at
   FROM table_items;

CREATE TABLE table_metadatas (
    handle character varying(66) NOT NULL,
    key_type text NOT NULL,
    value_type text NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (handle)
);
-- CREATE INDEX tm_insat_index ON table_metadatas (inserted_at);

CREATE TABLE token_activities (
    transaction_version bigint NOT NULL,
    event_account_address character varying(66) NOT NULL,
    event_creation_number bigint NOT NULL,
    event_sequence_number bigint NOT NULL,
    collection_data_id_hash character varying(64) NOT NULL,
    token_data_id_hash character varying(64) NOT NULL,
    property_version numeric NOT NULL,
    creator_address character varying(66) NOT NULL,
    collection_name character varying(128) NOT NULL,
    name character varying(128) NOT NULL,
    transfer_type character varying(50) NOT NULL,
    from_address character varying(66),
    to_address character varying(66),
    token_amount numeric NOT NULL,
    coin_type text,
    coin_amount numeric,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    transaction_timestamp timestamp without time zone NOT NULL,
    event_index bigint,
    PRIMARY KEY (transaction_version, event_account_address, event_creation_number, event_sequence_number)
);
CREATE INDEX ta_addr_coll_name_pv_index ON token_activities (creator_address, collection_name, name, property_version);
CREATE INDEX ta_from_ttyp_index ON token_activities (from_address, transfer_type);
-- CREATE INDEX ta_insat_index ON token_activities (inserted_at);
CREATE INDEX ta_tdih_pv_index ON token_activities (token_data_id_hash, property_version);
CREATE INDEX ta_to_ttyp_index ON token_activities (to_address, transfer_type);
CREATE INDEX ta_version_index ON token_activities (transaction_version);

CREATE TABLE token_datas (
    token_data_id_hash character varying(64) NOT NULL,
    transaction_version bigint NOT NULL,
    creator_address character varying(66) NOT NULL,
    collection_name character varying(128) NOT NULL,
    name character varying(128) NOT NULL,
    maximum numeric NOT NULL,
    supply numeric NOT NULL,
    largest_property_version numeric NOT NULL,
    metadata_uri character varying(512) NOT NULL,
    payee_address character varying(66) NOT NULL,
    royalty_points_numerator numeric NOT NULL,
    royalty_points_denominator numeric NOT NULL,
    maximum_mutable boolean NOT NULL,
    uri_mutable boolean NOT NULL,
    description_mutable boolean NOT NULL,
    properties_mutable boolean NOT NULL,
    royalty_mutable boolean NOT NULL,
    default_properties jsonb NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    collection_data_id_hash character varying(64) NOT NULL,
    transaction_timestamp timestamp without time zone NOT NULL,
    description text NOT NULL,
    PRIMARY KEY (token_data_id_hash, transaction_version)
);
CREATE INDEX td_crea_cn_name_index ON token_datas (creator_address, collection_name, name);
-- CREATE INDEX td_insat_index ON token_datas (inserted_at);

CREATE TABLE token_ownerships (
    token_data_id_hash character varying(64) NOT NULL,
    property_version numeric NOT NULL,
    transaction_version bigint NOT NULL,
    table_handle character varying(66) NOT NULL,
    creator_address character varying(66) NOT NULL,
    collection_name character varying(128) NOT NULL,
    name character varying(128) NOT NULL,
    owner_address character varying(66),
    amount numeric NOT NULL,
    table_type text,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    collection_data_id_hash character varying(64) NOT NULL,
    transaction_timestamp timestamp without time zone NOT NULL,
    PRIMARY KEY (token_data_id_hash, property_version, transaction_version, table_handle)
);
CREATE INDEX to_crea_cn_name_index ON token_ownerships (creator_address, collection_name, name);
-- CREATE INDEX to_insat_index ON token_ownerships (inserted_at);
CREATE INDEX to_owner_index ON token_ownerships (owner_address);

CREATE TABLE tokens (
    token_data_id_hash character varying(64) NOT NULL,
    property_version numeric NOT NULL,
    transaction_version bigint NOT NULL,
    creator_address character varying(66) NOT NULL,
    collection_name character varying(128) NOT NULL,
    name character varying(128) NOT NULL,
    token_properties jsonb NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    collection_data_id_hash character varying(64) NOT NULL,
    transaction_timestamp timestamp without time zone NOT NULL,
    PRIMARY KEY (token_data_id_hash, property_version, transaction_version)
);
-- CREATE INDEX token_insat_index ON tokens (inserted_at);
CREATE INDEX token_crea_cn_name_index ON tokens (creator_address, collection_name, name);

CREATE TABLE transaction_size_info (
    transaction_version bigint NOT NULL,
    size_bytes bigint NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (transaction_version)
);
-- CREATE INDEX tsi_insat_index ON transaction_size_info (inserted_at);

CREATE TABLE transactions (
    version bigint NOT NULL,
    block_height bigint NOT NULL,
    hash character varying(66) NOT NULL,
    type character varying NOT NULL,
    payload jsonb,
    state_change_hash character varying(66) NOT NULL,
    event_root_hash character varying(66) NOT NULL,
    state_checkpoint_hash character varying(66),
    gas_used numeric NOT NULL,
    success boolean NOT NULL,
    vm_status text NOT NULL,
    accumulator_root_hash character varying(66) NOT NULL,
    num_events bigint NOT NULL,
    num_write_set_changes bigint NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    epoch bigint NOT NULL,
    payload_type character varying(50),
    PRIMARY KEY (version)
);
-- ALTER TABLE ONLY transactions ADD CONSTRAINT transactions_hash_key UNIQUE (hash);
CREATE INDEX txn_epoch_index ON transactions (epoch);
-- CREATE INDEX txn_insat_index ON transactions (inserted_at);
CREATE VIEW transactions_view AS
 SELECT transactions.version,
    transactions.block_height,
    transactions.hash,
    transactions.type,
    (transactions.payload #>> '{}'::text[]) AS json_payload,
    transactions.state_change_hash,
    transactions.event_root_hash,
    transactions.state_checkpoint_hash,
    transactions.gas_used,
    transactions.success,
    transactions.vm_status,
    transactions.accumulator_root_hash,
    transactions.num_events,
    transactions.num_write_set_changes,
    transactions.inserted_at
   FROM transactions;

CREATE TABLE write_set_changes (
    transaction_version bigint NOT NULL,
    index bigint NOT NULL,
    hash character varying(66) NOT NULL,
    transaction_block_height bigint NOT NULL,
    type text NOT NULL,
    address character varying(66) NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (transaction_version, index)
);
-- CREATE INDEX wsc_insat_index ON write_set_changes (inserted_at);
CREATE INDEX wsc_addr_type_ver_index ON write_set_changes (address, transaction_version DESC);

CREATE TABLE write_set_size_info (
    transaction_version bigint NOT NULL,
    index bigint NOT NULL,
    key_bytes bigint NOT NULL,
    value_bytes bigint NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (transaction_version, index)
);
-- CREATE INDEX wsi_insat_index ON write_set_size_info (inserted_at);

CREATE VIEW current_collection_ownership_v2_view AS
 SELECT a.owner_address,
    c.creator_address,
    c.collection_name,
    b.collection_id,
    max(a.last_transaction_version) AS last_transaction_version,
    count(DISTINCT a.token_data_id) AS distinct_tokens,
    min((c.uri)::text) AS collection_uri,
    min((b.token_uri)::text) AS single_token_uri
   FROM ((current_token_ownerships_v2 a
     JOIN token_datas_v2 b ON (((a.token_data_id)::text = (b.token_data_id)::text)))
     JOIN current_collections_v2 c ON (((b.collection_id)::text = (c.collection_id)::text)))
  WHERE (a.amount > (0)::numeric)
  GROUP BY a.owner_address, c.creator_address, c.collection_name, b.collection_id;

CREATE VIEW current_collection_ownership_view AS
 SELECT current_token_ownerships.owner_address,
    current_token_ownerships.creator_address,
    current_token_ownerships.collection_name,
    current_token_ownerships.collection_data_id_hash,
    max(current_token_ownerships.last_transaction_version) AS last_transaction_version,
    count(DISTINCT current_token_ownerships.name) AS distinct_tokens
   FROM current_token_ownerships
  WHERE (current_token_ownerships.amount > (0)::numeric)
  GROUP BY current_token_ownerships.owner_address, current_token_ownerships.creator_address, current_token_ownerships.collection_name, current_token_ownerships.collection_data_id_hash;

-- lagecy_migration_v1 views

CREATE SCHEMA legacy_migration_v1;

CREATE VIEW legacy_migration_v1.address_version_from_move_resources AS
 SELECT at2.transaction_version,
    at2.account_address AS address
   FROM account_transactions at2;

CREATE VIEW legacy_migration_v1.coin_activities AS
 SELECT fungible_asset_activities.transaction_version,
    fungible_asset_activities.owner_address AS event_account_address,
    0 AS event_creation_number,
    0 AS event_sequence_number,
    fungible_asset_activities.owner_address,
    fungible_asset_activities.asset_type AS coin_type,
    fungible_asset_activities.amount,
    fungible_asset_activities.type AS activity_type,
    fungible_asset_activities.is_gas_fee,
    fungible_asset_activities.is_transaction_success,
    fungible_asset_activities.entry_function_id_str,
    fungible_asset_activities.block_height,
    fungible_asset_activities.transaction_timestamp,
    fungible_asset_activities.inserted_at,
    fungible_asset_activities.event_index,
    fungible_asset_activities.gas_fee_payer_address,
    fungible_asset_activities.storage_refund_amount
   FROM fungible_asset_activities
  WHERE ((fungible_asset_activities.token_standard)::text = 'v1'::text);

/*
CREATE VIEW legacy_migration_v1.coin_balances AS
 SELECT fungible_asset_balances.transaction_version,
    fungible_asset_balances.owner_address,
    encode(sha256((fungible_asset_balances.asset_type)::bytea), 'hex'::text) AS coin_type_hash,
    fungible_asset_balances.asset_type AS coin_type,
    fungible_asset_balances.amount,
    fungible_asset_balances.transaction_timestamp,
    fungible_asset_balances.inserted_at
   FROM fungible_asset_balances
  WHERE ((fungible_asset_balances.token_standard)::text = 'v1'::text);
*/

/*
CREATE VIEW legacy_migration_v1.coin_infos AS
 SELECT encode(sha256((fungible_asset_metadata.asset_type)::bytea), 'hex'::text) AS coin_type_hash,
    fungible_asset_metadata.asset_type AS coin_type,
    fungible_asset_metadata.last_transaction_version AS transaction_version_created,
    fungible_asset_metadata.creator_address,
    fungible_asset_metadata.name,
    fungible_asset_metadata.symbol,
    fungible_asset_metadata.decimals,
    fungible_asset_metadata.last_transaction_timestamp AS transaction_created_timestamp,
    fungible_asset_metadata.inserted_at,
    fungible_asset_metadata.supply_aggregator_table_handle_v1 AS supply_aggregator_table_handle,
    fungible_asset_metadata.supply_aggregator_table_key_v1 AS supply_aggregator_table_key
   FROM fungible_asset_metadata
  WHERE ((fungible_asset_metadata.token_standard)::text = 'v1'::text);*/

/*
CREATE VIEW legacy_migration_v1.collection_datas AS
 SELECT collections_v2.collection_id AS collection_data_id_hash,
    collections_v2.transaction_version,
    collections_v2.creator_address,
    collections_v2.collection_name,
    collections_v2.description,
    collections_v2.uri AS metadata_uri,
    collections_v2.current_supply AS supply,
    collections_v2.max_supply AS maximum,
    true AS maximum_mutable,
    true AS uri_mutable,
    true AS description_mutable,
    collections_v2.inserted_at,
    collections_v2.table_handle_v1 AS table_handle,
    collections_v2.transaction_timestamp
   FROM collections_v2
  WHERE ((collections_v2.token_standard)::text = 'v1'::text);*/

CREATE VIEW legacy_migration_v1.current_ans_lookup AS
 SELECT current_ans_lookup_v2.domain,
    current_ans_lookup_v2.subdomain,
    current_ans_lookup_v2.registered_address,
    current_ans_lookup_v2.expiration_timestamp,
    current_ans_lookup_v2.last_transaction_version,
    current_ans_lookup_v2.inserted_at,
    current_ans_lookup_v2.token_name,
    current_ans_lookup_v2.is_deleted
   FROM current_ans_lookup_v2
  WHERE ((current_ans_lookup_v2.token_standard)::text = 'v1'::text);

CREATE VIEW legacy_migration_v1.current_ans_primary_name AS
 SELECT current_ans_primary_name_v2.registered_address,
    current_ans_primary_name_v2.domain,
    current_ans_primary_name_v2.subdomain,
    current_ans_primary_name_v2.token_name,
    current_ans_primary_name_v2.is_deleted,
    current_ans_primary_name_v2.last_transaction_version,
    0 AS last_transaction_timestamp
   FROM current_ans_primary_name_v2
  WHERE ((current_ans_primary_name_v2.token_standard)::text = 'v1'::text);

/*
CREATE VIEW legacy_migration_v1.current_coin_balances AS
 SELECT current_fungible_asset_balances.owner_address,
    encode(sha256((current_fungible_asset_balances.asset_type)::bytea), 'hex'::text) AS coin_type_hash,
    current_fungible_asset_balances.asset_type AS coin_type,
    current_fungible_asset_balances.amount,
    current_fungible_asset_balances.last_transaction_version,
    current_fungible_asset_balances.last_transaction_timestamp,
    current_fungible_asset_balances.inserted_at
   FROM current_fungible_asset_balances
  WHERE ((current_fungible_asset_balances.token_standard)::text = 'v1'::text);*/

CREATE VIEW legacy_migration_v1.current_token_datas AS
 SELECT ctdv.token_data_id AS token_data_id_hash,
    ccv.creator_address,
    ccv.collection_name,
    ctdv.token_name AS name,
    COALESCE(ctdv.maximum, (0)::numeric) AS maximum,
    COALESCE(ctdv.supply, (0)::numeric) AS supply,
    ctdv.largest_property_version_v1 AS largest_property_version,
    ctdv.token_uri AS metadata_uri,
    COALESCE(ctrv.payee_address, ''::character varying) AS payee_address,
    ctrv.royalty_points_numerator,
    ctrv.royalty_points_denominator,
    true AS maximum_mutable,
    true AS uri_mutable,
    true AS description_mutable,
    true AS properties_mutable,
    true AS royalty_mutable,
    ctdv.token_properties AS default_properties,
    ctdv.last_transaction_version,
    ctdv.inserted_at,
    ctdv.collection_id AS collection_data_id_hash,
    ctdv.last_transaction_timestamp,
    ctdv.description
   FROM ((current_token_datas_v2 ctdv
     JOIN current_collections_v2 ccv ON (((ctdv.collection_id)::text = (ccv.collection_id)::text)))
     LEFT JOIN current_token_royalty_v1 ctrv ON (((ctdv.token_data_id)::text = (ctrv.token_data_id)::text)))
  WHERE ((ctdv.token_standard)::text = 'v1'::text);

CREATE VIEW legacy_migration_v1.current_token_ownerships AS
 SELECT ctov.token_data_id AS token_data_id_hash,
    ctov.property_version_v1 AS property_version,
    ctov.owner_address,
    ccv.creator_address,
    ccv.collection_name,
    ctdv.token_name AS name,
    ctov.amount,
    ctov.token_properties_mutated_v1 AS token_properties,
    ctov.last_transaction_version,
    ctov.inserted_at,
    ctdv.collection_id AS collection_data_id_hash,
    ctov.table_type_v1 AS table_type,
    ctov.last_transaction_timestamp
   FROM ((current_token_ownerships_v2 ctov
     JOIN current_token_datas_v2 ctdv ON (((ctov.token_data_id)::text = (ctdv.token_data_id)::text)))
     JOIN current_collections_v2 ccv ON (((ctdv.collection_id)::text = (ccv.collection_id)::text)))
  WHERE ((ctov.token_standard)::text = 'v1'::text);
CREATE VIEW legacy_migration_v1.move_resources AS
 SELECT at2.transaction_version,
    at2.account_address AS address
   FROM account_transactions at2;

CREATE VIEW legacy_migration_v1.token_activities AS
 SELECT tav.transaction_version,
    tav.event_account_address,
    0 AS event_creation_number,
    0 AS event_sequence_number,
    tdv.collection_id AS collection_data_id_hash,
    ltrim((tav.token_data_id)::text, '0x'::text) AS token_data_id_hash,
    tav.property_version_v1 AS property_version,
    cv.creator_address,
    cv.collection_name,
    tdv.token_name AS name,
    tav.type AS transfer_type,
    tav.from_address,
    tav.to_address,
    tav.token_amount,
    NULL::text AS coin_type,
    NULL::text AS coin_amount,
    tav.inserted_at,
    tav.transaction_timestamp,
    tav.event_index
   FROM ((token_activities_v2 tav
     JOIN token_datas_v2 tdv ON ((((tav.token_data_id)::text = (tdv.token_data_id)::text) AND (tav.transaction_version = tdv.transaction_version))))
     JOIN collections_v2 cv ON ((((tdv.collection_id)::text = (cv.collection_id)::text) AND (tdv.transaction_version = cv.transaction_version))))
  WHERE ((tav.token_standard)::text = 'v1'::text);

CREATE VIEW legacy_migration_v1.token_datas AS
 SELECT tdv.token_data_id AS token_data_id_hash,
    tdv.transaction_version,
    cv.creator_address,
    cv.collection_name,
    tdv.token_name AS name,
    tdv.maximum,
    tdv.supply,
    tdv.largest_property_version_v1 AS largest_property_version,
    tdv.token_uri AS metadata_uri,
    ''::text AS payee_address,
    NULL::text AS royalty_points_numerator,
    NULL::text AS royalty_points_denominator,
    true AS maximum_mutable,
    true AS uri_mutable,
    true AS description_mutable,
    true AS properties_mutable,
    true AS royalty_mutable,
    tdv.token_properties AS default_properties,
    tdv.inserted_at,
    tdv.collection_id AS collection_data_id_hash,
    tdv.transaction_timestamp,
    tdv.description
   FROM (token_datas_v2 tdv
     JOIN collections_v2 cv ON ((((tdv.collection_id)::text = (cv.collection_id)::text) AND (tdv.transaction_version = cv.transaction_version))))
  WHERE ((tdv.token_standard)::text = 'v1'::text);

CREATE VIEW legacy_migration_v1.token_ownerships AS
 SELECT tov.token_data_id AS token_data_id_hash,
    tov.property_version_v1 AS property_version,
    tov.transaction_version,
    ''::text AS table_handle,
    cv.creator_address,
    cv.collection_name,
    tdv.token_name AS name,
    tov.owner_address,
    tov.amount,
    tov.table_type_v1 AS table_type,
    tov.inserted_at,
    tdv.collection_id AS collection_data_id_hash,
    tov.transaction_timestamp
   FROM ((token_ownerships_v2 tov
     JOIN token_datas_v2 tdv ON ((((tov.token_data_id)::text = (tdv.token_data_id)::text) AND (tov.transaction_version = tdv.transaction_version))))
     JOIN collections_v2 cv ON ((((tdv.collection_id)::text = (cv.collection_id)::text) AND (tdv.transaction_version = cv.transaction_version))))
  WHERE ((tov.token_standard)::text = 'v1'::text);
CREATE VIEW legacy_migration_v1.tokens AS
 SELECT tdv.token_data_id AS token_data_id_hash,
    tdv.largest_property_version_v1 AS property_version,
    tdv.transaction_version,
    ccv.creator_address,
    ccv.collection_name,
    tdv.token_name AS name,
    tdv.token_properties,
    tdv.inserted_at,
    tdv.collection_id AS collection_data_id_hash,
    tdv.transaction_timestamp
   FROM (token_datas_v2 tdv
     JOIN current_collections_v2 ccv ON (((tdv.collection_id)::text = (ccv.collection_id)::text)))
  WHERE ((tdv.token_standard)::text = 'v1'::text);

-- nft_metadata_crawler schema

CREATE SCHEMA nft_metadata_crawler;
CREATE TABLE nft_metadata_crawler.ledger_infos (
    chain_id bigint NOT NULL,
    PRIMARY KEY (chain_id)
);
CREATE TABLE nft_metadata_crawler.parsed_asset_uris (
    asset_uri character varying NOT NULL,
    raw_image_uri character varying,
    raw_animation_uri character varying,
    cdn_json_uri character varying,
    cdn_image_uri character varying,
    cdn_animation_uri character varying,
    json_parser_retry_count integer NOT NULL,
    image_optimizer_retry_count integer NOT NULL,
    animation_optimizer_retry_count integer NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (asset_uri)
);
-- CREATE INDEX nft_inserted_at ON nft_metadata_crawler.parsed_asset_uris (inserted_at);
CREATE INDEX nft_raw_animation_uri ON nft_metadata_crawler.parsed_asset_uris (raw_animation_uri);
CREATE INDEX nft_raw_image_uri ON nft_metadata_crawler.parsed_asset_uris (raw_image_uri);

