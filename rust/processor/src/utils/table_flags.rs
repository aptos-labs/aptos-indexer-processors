use bitflags::bitflags;
use std::collections::HashSet;

bitflags! {
    #[derive(Debug, Clone, Copy)]
    pub struct TableFlags: u128 {
       // Default Processor: 1-10
        const TRANSACTIONS = 1 << 1;
        const WRITE_SET_CHANGES = 1 << 2;
        const MOVE_RESOURCES = 1 << 3;
        const TABLE_ITEMS = 1 << 4;
        const TABLE_METADATAS = 1 << 5;
        const MOVE_MODULES = 1 << 6;
        const CURRENT_TABLE_ITEMS = 1 << 7;
        const BLOCK_METADATA_TRANSACTIONS = 1 << 8;

        // Fungible Asset Processor: 11-20
        const FUNGIBLE_ASSET_BALANCES = 1 << 11;
        const CURRENT_FUNGIBLE_ASSET_BALANCES = 1 << 12;
        const FUNGIBLE_ASSET_ACTIVITIES = 1 << 13;
        const FUNGIBLE_ASSET_METADATA = 1 << 14;
        const CURRENT_UNIFIED_FUNGIBLE_ASSET_BALANCES = 1 << 15;
        const CURRENT_FUNGIBLE_ASSET_BALANCES_LEGACY = 1 << 16;

        // Objects Processor: 21-30
        const OBJECTS = 1 << 21;
        const CURRENT_OBJECTS = 1 << 22;

        // Ans Processor: 31-40
        const CURRENT_ANS_LOOKUP_V2 = 1 << 31;
        const CURRENT_ANS_PRIMARY_NAME_V2 = 1 << 32;
        const ANS_LOOKUP_V2 = 1 << 33;

        // Stake Processor: 41-50
        const DELEGATED_STAKING_ACTIVITIES = 1 << 41;
        const DELEGATED_STAKING_POOLS = 1 << 42;
        const DELEGATED_STAKING_POOL_BALANCES = 1 << 43;
        const CURRENT_DELEGATED_STAKING_POOL_BALANCES = 1 << 44;
        const DELEGATOR_BALANCES = 1 << 45;
        const CURRENT_DELEGATOR_BALANCES = 1 << 46;
        const CURRENT_DELEGATED_VOTER = 1 << 47;
        const CURRENT_STAKING_POOL_VOTER = 1 << 48;
        const PROPOSAL_VOTES = 1 << 49;

        // Token V2 Processor: 51-60
        const TOKEN_ACTIVITIES_V2 = 1 << 51;
        const CURRENT_TOKEN_OWNERSHIPS_V2 = 1 << 52;
        const CURRENT_TOKEN_DATAS_V2 = 1 << 53;
        const CURRENT_TOKEN_PENDING_CLAIMS = 1 << 54;
        const CURRENT_COLLECTIONS_V2 = 1 << 55;
        const CURRENT_TOKEN_V2_METADATA = 1 << 56;
        const COLLECTIONS_V2 = 1 << 57;
        const TOKEN_OWNERSHIPS_V2 = 1 << 58;
        const TOKEN_DATAS_V2 = 1 << 59;
        const CURRENT_TOKEN_ROYALTY_V1 = 1 << 60;

        // User Transactions and Signatures: 61-70
        const USER_TRANSACTIONS = 1 << 61;
        const SIGNATURES = 1 << 62;

        // Account Transaction Processor: 71-80
        const ACCOUNT_TRANSACTIONS = 1 << 71;

        // Events 81-90
        const EVENTS = 1 << 81;

        // transaction metadata 91-100
        const WRITE_SET_SIZE = 1 << 91;

        // Deprecated Tables 101-110
        const COIN_SUPPLY = 1 << 101;
        const CURRENT_ANS_LOOKUP = 1 << 102;
        const CURRENT_ANS_PRIMARY_NAME = 1 << 103;
        const ANS_PRIMARY_NAME_V2 = 1 << 104;
        const ANS_LOOKUP = 1 << 105;
        const ANS_PRIMARY_NAME = 1 << 106;
    }
}

impl TableFlags {
    pub fn from_set(set: &HashSet<String>) -> Self {
        let mut flags = TableFlags::empty();
        for table in set {
            if let Some(flag) = TableFlags::from_name(table) {
                flags |= flag;
            }
        }
        flags
    }
}
