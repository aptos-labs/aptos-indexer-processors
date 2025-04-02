-- Your SQL goes here
-- remove unused or duplicate indexes



-- On current_nft_marketplace_listings curr_list_collection_index (collection_id) is covered by curr_list_collection_price_index (collection_id, price)
DROP INDEX CONCURRENTLY IF EXISTS curr_list_collection_index;

-- On current_token_ownerships curr_to_owner_index (owner_address) is covered by curr_to_owner_tt_am_index (owner_address, table_type, amount)
DROP INDEX CONCURRENTLY IF EXISTS curr_to_owner_index; -- saves 1.5GB

-- On current_token_ownerships curr_to_owner_tt_am_index (owner_address, table_type, amount) is covered by curr_to_oa_tt_am_ltv_index (owner_address, table_type, amount, last_transaction_version DESC)
DROP INDEX CONCURRENTLY IF EXISTS curr_to_owner_tt_am_index; -- saves 6.6GB

-- On current_token_ownerships_v2 curr_to2_tdi_index (token_data_id) is covered by current_token_ownerships_v2_pkey (token_data_id, property_version_v1, owner_address, storage_id)
DROP INDEX CONCURRENTLY IF EXISTS curr_to2_tdi_index;

--On token_activities ta_version_index (transaction_version) is covered by token_activities_pkey (transaction_version, event_account_address, event_creation_number, event_sequence_number)
DROP INDEX CONCURRENTLY IF EXISTS ta_version_index; -- saves 1.6GB

--On user_transactions ut_sender_seq_index (sender, sequence_number) is covered by user_transactions_sender_sequence_number_key (sender, sequence_number)
DROP INDEX CONCURRENTLY IF EXISTS ut_sender_seq_index; -- saves 71GB



-- Unused Indexes
DROP INDEX CONCURRENTLY IF EXISTS wsc_addr_type_ver_index;	 -- saves 247 GB
DROP INDEX CONCURRENTLY IF EXISTS cb_ct_a_index;	 -- saves 59.7 GB
DROP INDEX CONCURRENTLY IF EXISTS fab_owner_at_index;	 -- saves 16.9 GB
DROP INDEX CONCURRENTLY IF EXISTS ut_epoch_index;	 -- saves 14.7 GB
DROP INDEX CONCURRENTLY IF EXISTS ca_gfpa_index;	 -- saves 12.2 GB
DROP INDEX CONCURRENTLY IF EXISTS at_insat_index;	 -- saves 10.8 GB
DROP INDEX CONCURRENTLY IF EXISTS faa_gfpa_index;	 -- saves 10.8 GB
DROP INDEX CONCURRENTLY IF EXISTS fab_insat_index;	 -- saves 8.78 GB
DROP INDEX CONCURRENTLY IF EXISTS txn_epoch_index;	 -- saves 7.29 GB
DROP INDEX CONCURRENTLY IF EXISTS curr_to_owner_tt_am_index;	 -- saves 6.6 GB
DROP INDEX CONCURRENTLY IF EXISTS cs_epoch_index;	 -- saves 3.6 GB
DROP INDEX CONCURRENTLY IF EXISTS col2_crea_cn_index;	 -- saves 1.99 GB
DROP INDEX CONCURRENTLY IF EXISTS col2_id_index;	 -- saves 1.65 GB
DROP INDEX CONCURRENTLY IF EXISTS to2_owner_index;	 -- saves 1.63 GB
DROP INDEX CONCURRENTLY IF EXISTS o_skh_idx;	 -- saves 552 MB
DROP INDEX CONCURRENTLY IF EXISTS co_skh_idx;	 -- saves 488 MB
DROP INDEX CONCURRENTLY IF EXISTS to2_insat_index;	 -- saves 311 MB
DROP INDEX CONCURRENTLY IF EXISTS cur_col2_insat_index;	 -- saves 302 MB
DROP INDEX CONCURRENTLY IF EXISTS curr_cd_insat_index;	 -- saves 300 MB
DROP INDEX CONCURRENTLY IF EXISTS col2_insat_index;	 -- saves 297 MB
DROP INDEX CONCURRENTLY IF EXISTS cd_insat_index;	 -- saves 271 MB
DROP INDEX CONCURRENTLY IF EXISTS o_owner_idx;	 -- saves 270 MB
DROP INDEX CONCURRENTLY IF EXISTS to_insat_index;	 -- saves 248 MB
DROP INDEX CONCURRENTLY IF EXISTS token_insat_index;	 -- saves 230 MB
DROP INDEX CONCURRENTLY IF EXISTS tm_insat_index;	 -- saves 228 MB
DROP INDEX CONCURRENTLY IF EXISTS o_insat_idx;	 -- saves 91.3 MB
DROP INDEX CONCURRENTLY IF EXISTS ctpc_insat_index;	 -- saves 70.4 MB
DROP INDEX CONCURRENTLY IF EXISTS ctpc_th_index;	 -- saves 54.5 MB
DROP INDEX CONCURRENTLY IF EXISTS co_insat_idx;	 -- saves 43.8 MB
DROP INDEX CONCURRENTLY IF EXISTS al_v2_ra_index;	 -- saves 4.46 MB
DROP INDEX CONCURRENTLY IF EXISTS db_da_index;	 -- saves 4.02 MB
DROP INDEX CONCURRENTLY IF EXISTS db_insat_index;	 -- saves 4.02 MB
DROP INDEX CONCURRENTLY IF EXISTS dspb_insat_index;	 -- saves 3.52 MB
DROP INDEX CONCURRENTLY IF EXISTS fam_insat_index;	 -- saves 1.57 MB
DROP INDEX CONCURRENTLY IF EXISTS ans_insat_index;	 -- saves 1.13 MB
DROP INDEX CONCURRENTLY IF EXISTS cdb_insat_index;	 -- saves 504 KB
DROP INDEX CONCURRENTLY IF EXISTS cdv_pv_index;	 -- saves 360 KB
DROP INDEX CONCURRENTLY IF EXISTS cdv_v_index;	 -- saves 360 KB
DROP INDEX CONCURRENTLY IF EXISTS cdspb_insat_index;	 -- saves 344 KB
DROP INDEX CONCURRENTLY IF EXISTS cdspb_inactive_index;	 -- saves 152 KB
DROP INDEX CONCURRENTLY IF EXISTS cdv_insat_index;	 -- saves 136 KB
DROP INDEX CONCURRENTLY IF EXISTS apn_v2_name_index;	 -- saves 88 KB
DROP INDEX CONCURRENTLY IF EXISTS apn_v2_ra_index;	 -- saves 88 KB
DROP INDEX CONCURRENTLY IF EXISTS cdv_th_index;	 -- saves 64 KB
DROP INDEX CONCURRENTLY IF EXISTS apn_v2_insat_index;	 -- saves 56 KB
DROP INDEX CONCURRENTLY IF EXISTS apn_insat_index;	 -- saves 40 KB
DROP INDEX CONCURRENTLY IF EXISTS apn_tn_index;	 -- saves 40 KB
DROP INDEX CONCURRENTLY IF EXISTS capn_insat_index;	 -- saves 40 KB
DROP INDEX CONCURRENTLY IF EXISTS curr_auc_bidder_index;	 -- saves 40 KB
DROP INDEX CONCURRENTLY IF EXISTS curr_auc_collection_index;	 -- saves 40 KB
DROP INDEX CONCURRENTLY IF EXISTS curr_auc_seller_index;	 -- saves 40 KB
DROP INDEX CONCURRENTLY IF EXISTS dsp_insat_index;	 -- saves 40 KB
DROP INDEX CONCURRENTLY IF EXISTS ev_collection_id_index;	 -- saves 40 KB
DROP INDEX CONCURRENTLY IF EXISTS ev_offer_or_listing_index;	 -- saves 40 KB
DROP INDEX CONCURRENTLY IF EXISTS ev_token_data_id_index;	 -- saves 40 KB
DROP INDEX CONCURRENTLY IF EXISTS np_insat_idx;	 -- saves 40 KB
DROP INDEX CONCURRENTLY IF EXISTS np_oa_idx;	 -- saves 40 KB
DROP INDEX CONCURRENTLY IF EXISTS np_tt_oa_idx;	 -- saves 40 KB
DROP INDEX CONCURRENTLY IF EXISTS pv_ia_index;	 -- saves 40 KB
DROP INDEX CONCURRENTLY IF EXISTS pv_spa_index;	 -- saves 40 KB
DROP INDEX CONCURRENTLY IF EXISTS pv_va_index;	 -- saves 40 KB
DROP INDEX CONCURRENTLY IF EXISTS curr_coll_off_buyer_index;	 -- saves 8 KB
DROP INDEX CONCURRENTLY IF EXISTS curr_coll_off_fee_schedule_index;	 -- saves 8 KB
DROP INDEX CONCURRENTLY IF EXISTS curr_coll_off_price_index;	 -- saves 8 KB
DROP INDEX CONCURRENTLY IF EXISTS curr_tok_offer_buyer_index;	 -- saves 8 KB
DROP INDEX CONCURRENTLY IF EXISTS curr_tok_offer_collection_index;	 -- saves 8 KB
DROP INDEX CONCURRENTLY IF EXISTS curr_tok_offer_fee_schedule_index;	 -- saves 8 KB