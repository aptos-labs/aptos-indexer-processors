-- This file should undo anything in `up.sql`
DROP VIEW IF EXISTS current_collection_ownership_v2_view;
create or replace view current_collection_ownership_v2_view as
select owner_address,
  b.collection_id,
  MAX(a.last_transaction_version) as last_transaction_version,
  COUNT(distinct a.token_data_id) as distinct_tokens
from current_token_ownerships_v2 a
  join current_token_datas_v2 b on a.token_data_id = b.token_data_id
where a.amount > 0
group by 1,
  2;
DROP INDEX IF EXISTS faa_owner_type_index;
DROP INDEX IF EXISTS faa_si_index;
DROP INDEX IF EXISTS faa_at_index;
DROP INDEX IF EXISTS faa_gfpa_index;
DROP INDEX IF EXISTS faa_insat_idx;
DROP TABLE IF EXISTS fungible_asset_activities;
DROP INDEX IF EXISTS cfab_owner_at_index;
DROP INDEX IF EXISTS cfab_insat_index;
DROP TABLE IF EXISTS current_fungible_asset_balances;
DROP INDEX IF EXISTS fab_owner_at_index;
DROP INDEX IF EXISTS fab_insat_index;
DROP TABLE IF EXISTS fungible_asset_balances;
DROP INDEX IF EXISTS fam_creator_index;
DROP INDEX IF EXISTS fam_insat_index;
DROP TABLE IF EXISTS fungible_asset_metadata;
DROP INDEX IF EXISTS ca_gfpa_index;
ALTER TABLE coin_activities DROP COLUMN IF EXISTS gas_fee_payer_address;