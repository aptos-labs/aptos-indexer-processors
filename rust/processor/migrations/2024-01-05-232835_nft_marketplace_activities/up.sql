-- Your SQL goes here
-- NFT marketplace activities
CREATE TABLE IF NOT EXISTS nft_marketplace_activities (
  transaction_version BIGINT NOT NULL,
  event_index BIGINT NOT NULL,
  offer_or_listing_id VARCHAR(66) NOT NULL,
  fee_schedule_id VARCHAR(66) NOT NULL,
  collection_id VARCHAR(66) NOT NULL,
  token_data_id VARCHAR(66),
  creator_address VARCHAR(66) NOT NULL,
  collection_name VARCHAR(100) NOT NULL,
  token_name VARCHAR(100),
  property_version NUMERIC,
  price NUMERIC NOT NULL,
  token_amount NUMERIC NOT NULL,
  token_standard VARCHAR(10) NOT NULL,
  seller VARCHAR(66),
  buyer VARCHAR(66),
  coin_type VARCHAR(1000),
  marketplace VARCHAR(100) NOT NULL,
  contract_address VARCHAR(66) NOT NULL,
  entry_function_id_str VARCHAR(512) NOT NULL,
  event_type VARCHAR(32) NOT NULL,
  transaction_timestamp TIMESTAMP NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  -- constraints
  PRIMARY KEY (transaction_version, event_index)
);

-- Create indexes
CREATE INDEX IF NOT EXISTS ev_offer_or_listing_index ON nft_marketplace_activities (offer_or_listing_id);
CREATE INDEX IF NOT EXISTS ev_token_data_id_index ON nft_marketplace_activities (token_data_id);
CREATE INDEX IF NOT EXISTS ev_collection_id_index ON nft_marketplace_activities (collection_id);

-- Current marketplace listings
CREATE TABLE IF NOT EXISTS current_nft_marketplace_listings (
  listing_id VARCHAR(66) NOT NULL,
  token_data_id VARCHAR(66) NOT NULL,
  collection_id VARCHAR(66) NOT NULL,
  fee_schedule_id VARCHAR(66) NOT NULL,
  price NUMERIC NOT NULL,
  token_amount NUMERIC NOT NULL,
  token_standard VARCHAR(10) NOT NULL,
  seller VARCHAR(66),
  is_deleted BOOLEAN NOT NULL,
  coin_type VARCHAR(1000),
  marketplace VARCHAR(100) NOT NULL,
  contract_address VARCHAR(66) NOT NULL,
  entry_function_id_str VARCHAR(512) NOT NULL,
  last_transaction_version BIGINT NOT NULL,
  last_transaction_timestamp TIMESTAMP NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  -- constraints
  PRIMARY KEY (listing_id, token_data_id)
);

-- Create indexes
CREATE INDEX IF NOT EXISTS curr_list_collection_index ON current_nft_marketplace_listings (collection_id);
CREATE INDEX IF NOT EXISTS curr_list_fee_schedule_index ON current_nft_marketplace_listings (fee_schedule_id);
CREATE INDEX IF NOT EXISTS curr_list_collection_price_index ON current_nft_marketplace_listings (collection_id, price);
CREATE INDEX IF NOT EXISTS curr_list_seller_index ON current_nft_marketplace_listings (seller);

-- Current nft marketplace token offers
CREATE TABLE IF NOT EXISTS current_nft_marketplace_token_offers (
  offer_id VARCHAR(66) NOT NULL,
  token_data_id VARCHAR(66) NOT NULL,
  collection_id VARCHAR(66) NOT NULL,
  fee_schedule_id VARCHAR(66) NOT NULL,
  buyer VARCHAR(66) NOT NULL,
  price NUMERIC NOT NULL,
  token_amount NUMERIC NOT NULL,
  expiration_time NUMERIC NOT NULL,
  is_deleted BOOLEAN NOT NULL,
  token_standard VARCHAR(10) NOT NULL,
  coin_type VARCHAR(1000),
  marketplace VARCHAR(100) NOT NULL,
  contract_address VARCHAR(66) NOT NULL,
  entry_function_id_str VARCHAR(512) NOT NULL,
  last_transaction_version BIGINT NOT NULL,
  last_transaction_timestamp TIMESTAMP NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  -- constraints
  PRIMARY KEY (offer_id, token_data_id)
);

-- Create indexes
CREATE INDEX IF NOT EXISTS curr_tok_offer_collection_index ON current_nft_marketplace_token_offers (collection_id);
CREATE INDEX IF NOT EXISTS curr_tok_offer_fee_schedule_index ON current_nft_marketplace_token_offers (fee_schedule_id);
CREATE INDEX IF NOT EXISTS curr_tok_offer_buyer_index ON current_nft_marketplace_token_offers (buyer);

-- Current NFT marketplace collection offers
CREATE TABLE IF NOT EXISTS current_nft_marketplace_collection_offers (
  collection_offer_id VARCHAR(66) NOT NULL,
  collection_id VARCHAR(66) NOT NULL,
  fee_schedule_id VARCHAR(66) NOT NULL,
  buyer VARCHAR(66) NOT NULL,
  item_price NUMERIC NOT NULL,
  remaining_token_amount NUMERIC NOT NULL,
  expiration_time NUMERIC NOT NULL,
  is_deleted BOOLEAN NOT NULL,
  token_standard VARCHAR(10) NOT NULL,
  coin_type VARCHAR(1000),
  marketplace VARCHAR(100) NOT NULL,
  contract_address VARCHAR(66) NOT NULL,
  entry_function_id_str VARCHAR(512) NOT NULL,
  last_transaction_version BIGINT NOT NULL,
  transaction_timestamp TIMESTAMP NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  -- constraints
  PRIMARY KEY (collection_offer_id, collection_id)
);

-- Create indexes
CREATE INDEX IF NOT EXISTS curr_coll_off_fee_schedule_index ON current_nft_marketplace_collection_offers (fee_schedule_id);
CREATE INDEX IF NOT EXISTS curr_coll_off_buyer_index ON current_nft_marketplace_collection_offers (buyer);

-- Current NFT marketplace Auctions
CREATE TABLE IF NOT EXISTS current_nft_marketplace_auctions (
  listing_id VARCHAR(66) NOT NULL,
  token_data_id VARCHAR(66) NOT NULL,
  collection_id VARCHAR(66) NOT NULL,
  fee_schedule_id VARCHAR(66) NOT NULL,
  seller VARCHAR(66) NOT NULL,
  current_bid_price NUMERIC,
  current_bidder VARCHAR(66),
  starting_bid_price NUMERIC NOT NULL,
  buy_it_now_price NUMERIC,
  token_amount NUMERIC NOT NULL,
  expiration_time NUMERIC NOT NULL,
  is_deleted BOOLEAN NOT NULL,
  token_standard VARCHAR(10) NOT NULL,
  coin_type VARCHAR(1000),
  marketplace VARCHAR(100) NOT NULL,
  contract_address VARCHAR(66) NOT NULL,
  entry_function_id_str VARCHAR(512) NOT NULL,
  last_transaction_version BIGINT NOT NULL,
  last_transaction_timestamp TIMESTAMP NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  -- constraints
  PRIMARY KEY (listing_id, token_data_id)
);

-- Create indexes
CREATE INDEX IF NOT EXISTS curr_auc_collection_index ON current_nft_marketplace_auctions (collection_id);
CREATE INDEX IF NOT EXISTS curr_auc_fee_schedule_index ON current_nft_marketplace_auctions (fee_schedule_id);
CREATE INDEX IF NOT EXISTS curr_auc_seller_index ON current_nft_marketplace_auctions (seller);
CREATE INDEX IF NOT EXISTS curr_auc_bidder_index ON current_nft_marketplace_auctions (current_bidder);
