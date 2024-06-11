CREATE SCHEMA nft_metadata_crawler;
CREATE TABLE nft_metadata_crawler.ledger_infos (
    chain_id bigint NOT NULL
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
    inserted_at timestamp without time zone DEFAULT now() NOT NULL
);
CREATE INDEX nft_raw_animation_uri ON nft_metadata_crawler.parsed_asset_uris (raw_animation_uri);
CREATE INDEX nft_raw_image_uri ON nft_metadata_crawler.parsed_asset_uris (raw_image_uri);

CREATE TABLE account_transactions (
    transaction_version bigint NOT NULL,
    account_address character varying(66) NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL
);
CREATE INDEX at_version_index ON account_transactions (transaction_version DESC);

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
    subdomain_expiration_policy bigint
);
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
    inserted_at timestamp without time zone DEFAULT now() NOT NULL
);

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
    subdomain_expiration_policy bigint
);
CREATE INDEX ans_v2_et_index ON current_ans_lookup_v2 (expiration_timestamp);
CREATE INDEX ans_v2_ra_index ON current_ans_lookup_v2 (registered_address);
CREATE INDEX ans_v2_tn_index ON current_ans_lookup_v2 (token_name, token_standard);

CREATE TABLE current_ans_primary_name_v2 (
    registered_address character varying(66) NOT NULL,
    token_standard character varying(10) NOT NULL,
    domain character varying(64),
    subdomain character varying(64),
    token_name character varying(140),
    is_deleted boolean NOT NULL,
    last_transaction_version bigint NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL
);
CREATE INDEX capn_v2_tn_index ON current_ans_primary_name_v2 (token_name, token_standard);

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
    is_deleted_v2 boolean
);
CREATE INDEX cur_td2_cid_name_index ON current_token_datas_v2 (collection_id, token_name);

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
    non_transferrable_by_owner boolean
);
CREATE INDEX curr_to2_owner_index ON current_token_ownerships_v2 (owner_address);
CREATE INDEX curr_to2_wa_index ON current_token_ownerships_v2 (storage_id);

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
    last_transaction_timestamp timestamp without time zone NOT NULL
);
CREATE INDEX curr_cd_crea_cn_index ON current_collection_datas (creator_address, collection_name);
CREATE INDEX curr_cd_th_index ON current_collection_datas (table_handle);

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
    inserted_at timestamp without time zone DEFAULT now() NOT NULL
);
CREATE INDEX cur_col2_crea_cn_index ON current_collections_v2 (creator_address, collection_name);

CREATE TABLE current_delegated_staking_pool_balances (
    staking_pool_address character varying(66) NOT NULL,
    total_coins numeric NOT NULL,
    total_shares numeric NOT NULL,
    last_transaction_version bigint NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    operator_commission_percentage numeric NOT NULL,
    inactive_table_handle character varying(66) NOT NULL,
    active_table_handle character varying(66) NOT NULL
);
CREATE INDEX cdspb_inactive_index ON current_delegated_staking_pool_balances (inactive_table_handle);

CREATE TABLE current_delegated_voter (
    delegation_pool_address character varying(66) NOT NULL,
    delegator_address character varying(66) NOT NULL,
    table_handle character varying(66),
    voter character varying(66),
    pending_voter character varying(66),
    last_transaction_version bigint NOT NULL,
    last_transaction_timestamp timestamp without time zone NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL
);
CREATE INDEX cdv_da_index ON current_delegated_voter (delegator_address);
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
    parent_table_handle character varying(66) NOT NULL
);
CREATE VIEW delegator_distinct_pool AS
 SELECT current_delegator_balances.delegator_address,
    current_delegator_balances.pool_address
   FROM current_delegator_balances
  WHERE (current_delegator_balances.shares > (0)::numeric)
  GROUP BY current_delegator_balances.delegator_address, current_delegator_balances.pool_address;
CREATE VIEW num_active_delegator_per_pool AS
 SELECT current_delegator_balances.pool_address,
    count(DISTINCT current_delegator_balances.delegator_address) AS num_active_delegator
   FROM current_delegator_balances
  WHERE ((current_delegator_balances.shares > (0)::numeric) AND ((current_delegator_balances.pool_type)::text = 'active_shares'::text))
  GROUP BY current_delegator_balances.pool_address;

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
    inserted_at timestamp without time zone DEFAULT now() NOT NULL
);
CREATE INDEX cfab_owner_at_index ON current_fungible_asset_balances (owner_address, asset_type);

CREATE TABLE current_objects (
    object_address character varying(66) NOT NULL,
    owner_address character varying(66) NOT NULL,
    state_key_hash character varying(66) NOT NULL,
    allow_ungated_transfer boolean NOT NULL,
    last_guid_creation_num numeric NOT NULL,
    last_transaction_version bigint NOT NULL,
    is_deleted boolean NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL
);
CREATE INDEX co_object_skh_idx ON current_objects (object_address, state_key_hash);
CREATE INDEX co_owner_idx ON current_objects (owner_address);
CREATE INDEX co_skh_idx ON current_objects (state_key_hash);

CREATE TABLE current_staking_pool_voter (
    staking_pool_address character varying(66) NOT NULL,
    voter_address character varying(66) NOT NULL,
    last_transaction_version bigint NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    operator_address character varying(66) NOT NULL
);
CREATE INDEX ctpv_va_index ON current_staking_pool_voter (voter_address);

CREATE TABLE current_table_items (
    table_handle character varying(66) NOT NULL,
    key_hash character varying(64) NOT NULL,
    key text NOT NULL,
    decoded_key jsonb NOT NULL,
    decoded_value jsonb,
    is_deleted boolean NOT NULL,
    last_transaction_version bigint NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL
);
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
    collection_id character varying(66) DEFAULT ''::character varying NOT NULL
);
CREATE INDEX ctpc_from_am_index ON current_token_pending_claims (from_address, amount);
CREATE INDEX ctpc_th_index ON current_token_pending_claims (table_handle);
CREATE INDEX ctpc_to_am_index ON current_token_pending_claims (to_address, amount);

CREATE TABLE current_token_royalty_v1 (
    token_data_id character varying(66) NOT NULL,
    payee_address character varying(66) NOT NULL,
    royalty_points_numerator numeric NOT NULL,
    royalty_points_denominator numeric NOT NULL,
    last_transaction_version bigint NOT NULL,
    last_transaction_timestamp timestamp without time zone NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL
);

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
    inserted_at timestamp without time zone DEFAULT now() NOT NULL
);
CREATE INDEX cufab_owner_at_index ON current_unified_fungible_asset_balances (owner_address, asset_type);

CREATE TABLE delegated_staking_activities (
    transaction_version bigint NOT NULL,
    event_index bigint NOT NULL,
    delegator_address character varying(66) NOT NULL,
    pool_address character varying(66) NOT NULL,
    event_type text NOT NULL,
    amount numeric NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL
);
CREATE INDEX dsa_pa_da_index ON delegated_staking_activities (pool_address, delegator_address, transaction_version, event_index);

CREATE TABLE delegated_staking_pool_balances (
    transaction_version bigint NOT NULL,
    staking_pool_address character varying(66) NOT NULL,
    total_coins numeric NOT NULL,
    total_shares numeric NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    operator_commission_percentage numeric NOT NULL,
    inactive_table_handle character varying(66) NOT NULL,
    active_table_handle character varying(66) NOT NULL
);
CREATE TABLE delegated_staking_pools (
    staking_pool_address character varying(66) NOT NULL,
    first_transaction_version bigint NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL
);
CREATE TABLE delegator_balances (
    transaction_version bigint NOT NULL,
    write_set_change_index bigint NOT NULL,
    delegator_address character varying(66) NOT NULL,
    pool_address character varying(66) NOT NULL,
    pool_type character varying(100) NOT NULL,
    table_handle character varying(66) NOT NULL,
    shares numeric NOT NULL,
    parent_table_handle character varying(66) NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL
);
CREATE INDEX db_da_index ON delegator_balances (delegator_address);

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
    indexed_type character varying(300) DEFAULT ''::character varying NOT NULL
);
CREATE INDEX ev_addr_type_index ON events (account_address);
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
    storage_refund_amount numeric DEFAULT 0 NOT NULL
);
CREATE INDEX faa_at_index ON fungible_asset_activities (asset_type);
CREATE INDEX faa_gfpa_index ON fungible_asset_activities (gas_fee_payer_address);
CREATE INDEX faa_owner_type_index ON fungible_asset_activities (owner_address, type);
CREATE INDEX faa_si_index ON fungible_asset_activities (storage_id);

CREATE TABLE fungible_asset_metadata (
    asset_type character varying(1000) NOT NULL,
    creator_address character varying(66) NOT NULL,
    name character varying(32) NOT NULL,
    symbol character varying(10) NOT NULL,
    decimals integer NOT NULL,
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
    maximum_v2 numeric
);
CREATE INDEX fam_creator_index ON fungible_asset_metadata (creator_address);

CREATE TABLE ledger_infos (
    chain_id bigint NOT NULL
);

CREATE TABLE processor_status (
    processor character varying(50) NOT NULL,
    last_success_version bigint NOT NULL,
    last_updated timestamp without time zone DEFAULT now() NOT NULL,
    last_transaction_timestamp timestamp without time zone
);

CREATE TABLE proposal_votes (
    transaction_version bigint NOT NULL,
    proposal_id bigint NOT NULL,
    voter_address character varying(66) NOT NULL,
    staking_pool_address character varying(66) NOT NULL,
    num_votes numeric NOT NULL,
    should_pass boolean NOT NULL,
    transaction_timestamp timestamp without time zone NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL
);
CREATE INDEX pv_pi_va_index ON proposal_votes (proposal_id, voter_address);
CREATE INDEX pv_spa_index ON proposal_votes (staking_pool_address);
CREATE INDEX pv_va_index ON proposal_votes (voter_address);

CREATE TABLE spam_assets (
    asset character varying(1100) NOT NULL,
    is_spam boolean DEFAULT true NOT NULL,
    last_updated timestamp without time zone DEFAULT now() NOT NULL
);

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
    inserted_at timestamp without time zone DEFAULT now() NOT NULL
);
CREATE INDEX ta2_from_type_index ON token_activities_v2 (from_address, type);
CREATE INDEX ta2_owner_type_index ON token_activities_v2 (event_account_address, type);
CREATE INDEX ta2_tid_index ON token_activities_v2 (token_data_id);
CREATE INDEX ta2_to_type_index ON token_activities_v2 (to_address, type);

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
    epoch bigint NOT NULL
);
CREATE INDEX ut_epoch_index ON user_transactions (epoch);
CREATE INDEX ut_sender_seq_index ON user_transactions (sender, sequence_number);

ALTER TABLE ONLY nft_metadata_crawler.ledger_infos
    ADD CONSTRAINT ledger_infos_pkey PRIMARY KEY (chain_id);
ALTER TABLE ONLY nft_metadata_crawler.parsed_asset_uris
    ADD CONSTRAINT parsed_asset_uris_pkey PRIMARY KEY (asset_uri);
ALTER TABLE ONLY account_transactions
    ADD CONSTRAINT account_transactions_pkey PRIMARY KEY (account_address, transaction_version);
ALTER TABLE ONLY ans_lookup_v2
    ADD CONSTRAINT ans_lookup_v2_pkey PRIMARY KEY (transaction_version, write_set_change_index);
ALTER TABLE ONLY block_metadata_transactions
    ADD CONSTRAINT block_metadata_transactions_pkey PRIMARY KEY (version);
ALTER TABLE ONLY current_ans_lookup_v2
    ADD CONSTRAINT current_ans_lookup_v2_pkey PRIMARY KEY (domain, subdomain, token_standard);
ALTER TABLE ONLY current_ans_primary_name_v2
    ADD CONSTRAINT current_ans_primary_name_v2_pkey PRIMARY KEY (registered_address, token_standard);
ALTER TABLE ONLY current_collection_datas
    ADD CONSTRAINT current_collection_datas_pkey PRIMARY KEY (collection_data_id_hash);
ALTER TABLE ONLY current_collections_v2
    ADD CONSTRAINT current_collections_v2_pkey PRIMARY KEY (collection_id);
ALTER TABLE ONLY current_delegated_staking_pool_balances
    ADD CONSTRAINT current_delegated_staking_pool_balances_pkey PRIMARY KEY (staking_pool_address);
ALTER TABLE ONLY current_delegated_voter
    ADD CONSTRAINT current_delegated_voter_pkey PRIMARY KEY (delegation_pool_address, delegator_address);
ALTER TABLE ONLY current_delegator_balances
    ADD CONSTRAINT current_delegator_balances_pkey PRIMARY KEY (delegator_address, pool_address, pool_type, table_handle);
ALTER TABLE ONLY current_fungible_asset_balances
    ADD CONSTRAINT current_fungible_asset_balances_pkey PRIMARY KEY (storage_id);
ALTER TABLE ONLY current_objects
    ADD CONSTRAINT current_objects_pkey PRIMARY KEY (object_address);
ALTER TABLE ONLY current_staking_pool_voter
    ADD CONSTRAINT current_staking_pool_voter_pkey PRIMARY KEY (staking_pool_address);
ALTER TABLE ONLY current_table_items
    ADD CONSTRAINT current_table_items_pkey PRIMARY KEY (table_handle, key_hash);
ALTER TABLE ONLY current_token_datas_v2
    ADD CONSTRAINT current_token_datas_v2_pkey PRIMARY KEY (token_data_id);
ALTER TABLE ONLY current_token_ownerships_v2
    ADD CONSTRAINT current_token_ownerships_v2_pkey PRIMARY KEY (token_data_id, property_version_v1, owner_address, storage_id);
ALTER TABLE ONLY current_token_pending_claims
    ADD CONSTRAINT current_token_pending_claims_pkey PRIMARY KEY (token_data_id_hash, property_version, from_address, to_address);
ALTER TABLE ONLY current_token_royalty_v1
    ADD CONSTRAINT current_token_royalty_v1_pkey PRIMARY KEY (token_data_id);
ALTER TABLE ONLY current_unified_fungible_asset_balances
    ADD CONSTRAINT current_unified_fungible_asset_balances_pkey PRIMARY KEY (storage_id);
ALTER TABLE ONLY delegated_staking_activities
    ADD CONSTRAINT delegated_staking_activities_pkey PRIMARY KEY (transaction_version, event_index);
ALTER TABLE ONLY delegated_staking_pool_balances
    ADD CONSTRAINT delegated_staking_pool_balances_pkey PRIMARY KEY (transaction_version, staking_pool_address);
ALTER TABLE ONLY delegated_staking_pools
    ADD CONSTRAINT delegated_staking_pools_pkey PRIMARY KEY (staking_pool_address);
ALTER TABLE ONLY delegator_balances
    ADD CONSTRAINT delegator_balances_pkey PRIMARY KEY (transaction_version, write_set_change_index);
ALTER TABLE ONLY events
    ADD CONSTRAINT events_pkey PRIMARY KEY (transaction_version, event_index);
ALTER TABLE ONLY fungible_asset_activities
    ADD CONSTRAINT fungible_asset_activities_pkey PRIMARY KEY (transaction_version, event_index);
ALTER TABLE ONLY fungible_asset_metadata
    ADD CONSTRAINT fungible_asset_metadata_pkey PRIMARY KEY (asset_type);
ALTER TABLE ONLY ledger_infos
    ADD CONSTRAINT ledger_infos_pkey PRIMARY KEY (chain_id);
ALTER TABLE ONLY processor_status
    ADD CONSTRAINT processor_status_pkey PRIMARY KEY (processor);
ALTER TABLE ONLY proposal_votes
    ADD CONSTRAINT proposal_votes_pkey PRIMARY KEY (transaction_version, proposal_id, voter_address);
ALTER TABLE ONLY spam_assets
    ADD CONSTRAINT spam_assets_pkey PRIMARY KEY (asset);
ALTER TABLE ONLY token_activities_v2
    ADD CONSTRAINT token_activities_v2_pkey PRIMARY KEY (transaction_version, event_index);
ALTER TABLE ONLY user_transactions
    ADD CONSTRAINT user_transactions_pkey PRIMARY KEY (version);
