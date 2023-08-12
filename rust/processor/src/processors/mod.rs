// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

pub mod coin_processor;
pub mod default_processor;
pub mod fungible_asset_processor;
pub mod nft_metadata_processor;
pub mod processor_trait;
pub mod stake_processor;
pub mod token_processor;
pub mod token_v2_processor;

use self::{
    coin_processor::NAME as COIN_PROCESSOR_NAME, default_processor::NAME as DEFAULT_PROCESSOR_NAME,
    fungible_asset_processor::NAME as FUNGIBLE_ASSET_PROCESSOR_NAME,
    nft_metadata_processor::NAME as NFT_METADATA_PROCESSOR_NAME,
    stake_processor::NAME as STAKE_PROCESSOR_NAME, token_processor::NAME as TOKEN_PROCESSOR_NAME,
    token_v2_processor::NAME as TOKEN_V2_PROCESSOR_NAME,
};

pub enum Processor {
    CoinProcessor,
    DefaultProcessor,
    FungibleAssetProcessor,
    StakeProcessor,
    TokenProcessor,
    TokenV2Processor,
    NFTMetadataProcessor,
}

impl Processor {
    pub fn from_string(input_str: &String) -> Self {
        match input_str.as_str() {
            DEFAULT_PROCESSOR_NAME => Self::DefaultProcessor,
            COIN_PROCESSOR_NAME => Self::CoinProcessor,
            FUNGIBLE_ASSET_PROCESSOR_NAME => Self::FungibleAssetProcessor,
            STAKE_PROCESSOR_NAME => Self::StakeProcessor,
            TOKEN_PROCESSOR_NAME => Self::TokenProcessor,
            TOKEN_V2_PROCESSOR_NAME => Self::TokenV2Processor,
            NFT_METADATA_PROCESSOR_NAME => Self::NFTMetadataProcessor,
            _ => panic!("Processor unsupported {}", input_str),
        }
    }
}
