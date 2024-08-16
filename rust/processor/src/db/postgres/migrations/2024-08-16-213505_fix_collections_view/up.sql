-- Your SQL goes here
CREATE OR REPLACE VIEW current_collection_ownership_v2_view as
select owner_address,
  creator_address,
  collection_name,
  b.collection_id,
  max(a.last_transaction_version) as last_transaction_version,
  count(distinct a.token_data_id) as distinct_tokens,
  min(c.uri) as collection_uri,
  min(token_uri) as single_token_uri
from current_token_ownerships_v2 a
  join current_token_datas_v2 b on a.token_data_id = b.token_data_id
  join current_collections_v2 c on b.collection_id = c.collection_id
where amount > 0
group by 1,
  2,
  3,
  4;