script {
    use std::signer;
    use std::string::utf8;

    use aptos_framework::fungible_asset::{Metadata};
    use aptos_framework::object::{Self};
    use aptos_token_objects::token;
    use test_addr::managed_fungible_asset::{Self};

    const FT: vector<u8> = b"FT2";

    fun transfer_ft(deployer: &signer) {
        let deployer_address = signer::address_of(deployer);
        let token_address = token::create_token_address(&deployer_address, &utf8(FT), &utf8(FT));
        let metadata = object::address_to_object<Metadata>(token_address);
        managed_fungible_asset::transfer_between_primary_stores(
            deployer, 
            metadata, 
            vector[deployer_address],
            vector[@0xdead],
            vector[100000000],
        );
    }
}