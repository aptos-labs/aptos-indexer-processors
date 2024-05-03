script {
    use std::signer;
    use std::string::utf8;
    use std::option;

    use aptos_framework::fungible_asset::{Metadata};
    use aptos_framework::object::object_from_constructor_ref;
    use aptos_token_objects::collection;
    use aptos_token_objects::token;
    use test_addr::managed_fungible_asset::{Self};

    const FT: vector<u8> = b"FT2";

    fun create_ft(deployer: &signer) {
        // Create token part
        collection::create_unlimited_collection(
            deployer,
            utf8(FT),
            utf8(FT),
            option::none(),
            utf8(FT),
        );
        let constructor_ref = &token::create_named_token(
            deployer,
            utf8(FT),
            utf8(FT),
            utf8(FT),
            option::none(),
            utf8(FT),
        );

        // Create fungible asset part
        managed_fungible_asset::initialize(
            constructor_ref,
            0, /* maximum_supply */
            utf8(FT),
            utf8(FT),
            8, /* decimals */
            utf8(b"http://example.com/favicon.ico"), /* icon */
            utf8(b"http://example.com"), /* project */
            vector[true, true, true], /* ref_flags */
        );
        let metadata = object_from_constructor_ref<Metadata>(constructor_ref);
        let deployer_addr = signer::address_of(deployer);
        managed_fungible_asset::mint_to_primary_stores(
            deployer,
            metadata,
            vector[deployer_addr, @0xcafe],
            vector[200000000, 100000000],
        );
    }
}