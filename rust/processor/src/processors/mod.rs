// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

pub mod ans_processor;
pub mod coin_processor;
pub mod default_processor;
pub mod events_processor;
pub mod fungible_asset_processor;
pub mod nft_metadata_processor;
pub mod processor_trait;
pub mod stake_processor;
pub mod token_processor;
pub mod token_v2_processor;
pub mod user_transaction_processor;

use self::{
    ans_processor::NAME as ANS_PROCESSOR_NAME, coin_processor::NAME as COIN_PROCESSOR_NAME,
    default_processor::NAME as DEFAULT_PROCESSOR_NAME, events_processor::NAME as EVENTS_PROCESSOR,
    fungible_asset_processor::NAME as FUNGIBLE_ASSET_PROCESSOR_NAME,
    nft_metadata_processor::NAME as NFT_METADATA_PROCESSOR_NAME,
    stake_processor::NAME as STAKE_PROCESSOR_NAME, token_processor::NAME as TOKEN_PROCESSOR_NAME,
    token_v2_processor::NAME as TOKEN_V2_PROCESSOR_NAME,
    user_transaction_processor::NAME as USER_TRANSACTION_PROCESSOR,
};

pub enum Processor {
    CoinProcessor,
    DefaultProcessor,
    EventsProcessor,
    FungibleAssetProcessor,
    StakeProcessor,
    TokenProcessor,
    TokenV2Processor,
    NFTMetadataProcessor,
    AnsProcessor,
    UserTransactionProcessor,
}

impl Processor {
    pub fn from_string(input_str: &String) -> Self {
        match input_str.as_str() {
            DEFAULT_PROCESSOR_NAME => Self::DefaultProcessor,
            COIN_PROCESSOR_NAME => Self::CoinProcessor,
            EVENTS_PROCESSOR => Self::EventsProcessor,
            FUNGIBLE_ASSET_PROCESSOR_NAME => Self::FungibleAssetProcessor,
            STAKE_PROCESSOR_NAME => Self::StakeProcessor,
            TOKEN_PROCESSOR_NAME => Self::TokenProcessor,
            TOKEN_V2_PROCESSOR_NAME => Self::TokenV2Processor,
            NFT_METADATA_PROCESSOR_NAME => Self::NFTMetadataProcessor,
            ANS_PROCESSOR_NAME => Self::AnsProcessor,
            USER_TRANSACTION_PROCESSOR => Self::UserTransactionProcessor,
            _ => panic!("Processor unsupported {}", input_str),
        }
    }
}
