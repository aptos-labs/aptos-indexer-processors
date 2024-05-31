script {
    use std::signer;
    use std::string::{Self, utf8};
    use aptos_std::string_utils;
    use std::vector;
    use std::option;

    use aptos_framework::coin::{Self, Coin};
    use aptos_framework::object::{Self, ConstructorRef, Object};
    use aptos_token_objects::collection::{Self, Collection};
    use aptos_token_objects::token;
    use aptos_token_objects::property_map;

    const S: vector<u8> = b"TEST2";

    fun main(deployer: &signer) {
        collection::create_unlimited_collection(
            deployer,
            utf8(S),
            utf8(S),
            option::none(),
            utf8(S),
        );
        let constructor_ref = token::create_named_token(
            deployer,
            utf8(S),
            utf8(S),
            utf8(S),
            option::none(),
            utf8(S),
        );
        let transfer_ref = object::generate_transfer_ref(&constructor_ref);
        let linear_transfer_ref = object::generate_linear_transfer_ref(&transfer_ref);
        object::transfer_with_ref(linear_transfer_ref, @0xcafe);
        let linear_transfer_ref = object::generate_linear_transfer_ref(&transfer_ref);
        object::transfer_with_ref(linear_transfer_ref, @0xcafe2);
        // Disabling transfer ref after transferring
        object::disable_ungated_transfer(&transfer_ref);
        let burn_ref = token::generate_burn_ref(&constructor_ref);
        token::burn(burn_ref);
    }
}
