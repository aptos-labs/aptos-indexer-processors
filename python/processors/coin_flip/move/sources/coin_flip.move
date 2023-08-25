module coin_flip::coin_flip {
    use aptos_framework::object::{Self, Object, DeleteRef};
    use aptos_framework::timestamp;
    use aptos_framework::account;
    use aptos_framework::transaction_context;
    use std::event::{Self, EventHandle};
    use std::signer;
    use std::error;
    use std::vector;
    use coin_flip::package_manager;

    /// The contract should never be able to reach this state.
    const EINVALID_CONTRACT_STATE: u64 = 0;

    #[resource_group_member(group = aptos_framework::object::ObjectGroup)]
    /// A resource stored at an object that tracks a user's coin flip stats
    /// It also handles the event emission of each flip.
    struct CoinFlipStats has key {
        wins: u64,
        losses: u64,
        event_handle: EventHandle<CoinFlipEvent>,
        delete_ref: DeleteRef,
    }

    struct CoinFlipEvent has copy, drop, store {
        prediction: bool,     // true = heads, false = tails
        result: bool,
        wins: u64,
        losses: u64,
    }

    /// Private entry to disallow gaming the results by aborting in a Move script
    entry fun flip_many(
        user: &signer,
        prediction: vector<bool>,
    ) acquires CoinFlipStats {
        vector::enumerate_ref(&prediction, |i, pred| {
            flip_coin(user, *pred, i);
        });
    }

    /// Private entry to disallow gaming the results by aborting in a Move script
    entry fun flip_once(
        user: &signer,
        prediction: bool, 
    ) acquires CoinFlipStats {
        flip_coin(user, prediction, 0);
    }

    fun flip_coin(
        user: &signer,
        prediction: bool,
        nonce: u64,
    ) acquires CoinFlipStats {
        let user_address = signer::address_of(user);
        let stats_object = get_user_stats(user_address);
        let coin_flip_stats = borrow_global_mut<CoinFlipStats>(object::object_address(&stats_object));

        let (heads, correct_prediction) = flip(prediction, nonce);

        if (correct_prediction) {
            coin_flip_stats.wins = coin_flip_stats.wins + 1;
        } else {
            coin_flip_stats.losses = coin_flip_stats.losses + 1;
        };

        event::emit_event<CoinFlipEvent>(
            &mut coin_flip_stats.event_handle,
            CoinFlipEvent {
                prediction: prediction,
                result: heads,
                wins: coin_flip_stats.wins,
                losses: coin_flip_stats.losses,
            }
        );
    }

    inline fun flip(
        prediction: bool,
        nonce: u64,
    ): (bool, bool) {
        let txn_hash = transaction_context::get_transaction_hash();
        let now = timestamp::now_microseconds();

        // Pseudo-random number calculated by current microseconds + the first byte of the transaction hash + the nonce to allow for multiple flips in a single tx.
        let prng = now + nonce + (*vector::borrow(&txn_hash, 0) as u64);

        // Even = heads, odd = tails
        let result = prng % 2 == 0;

        (result, prediction == result)
    }

    #[view]
    public fun get_user_stats(user_address: address): Object<CoinFlipStats> {
        // look up the user's address and see if a corresponding object address exists already
        let named_address = package_manager::address_to_string(user_address);

        // if it does, return the object
        if (package_manager::named_address_exists(named_address)) {
            let obj_address = package_manager::get_named_address(named_address);
            assert!(exists<CoinFlipStats>(obj_address), error::invalid_state(EINVALID_CONTRACT_STATE));
            object::address_to_object<CoinFlipStats>(obj_address)
        } else {
            // otherwise create it and return it
            let stats_object = create_user_stats(user_address);
            package_manager::add_named_address(named_address, object::object_address(&stats_object));
            stats_object
            
        }
    }

    fun create_user_stats(user_address: address): Object<CoinFlipStats> {
        let constructor_ref = object::create_object(user_address);
        let delete_ref = object::generate_delete_ref(&constructor_ref);
        let obj_signer = object::generate_signer(&constructor_ref);

        // object needs to have an Account resource so it can create an event handle successfully
        aptos_framework::aptos_account::create_account(signer::address_of(&obj_signer));
        move_to(
            &obj_signer,
            CoinFlipStats {
                wins: 0,
                losses: 0,
                event_handle: account::new_event_handle<CoinFlipEvent>(&obj_signer),
                delete_ref: delete_ref,
            }
        );

        let stats_object = object::object_from_constructor_ref<CoinFlipStats>(&constructor_ref);
        stats_object
    }

    #[test(deployer = @deployer, resource_signer = @coin_flip, aptos_framework = @0x1)]
    fun test(
        deployer: &signer,
        resource_signer: &signer,
        aptos_framework: &signer,
    ) acquires CoinFlipStats {
        timestamp::set_time_has_started_for_testing(aptos_framework);
        package_manager::enable_auids_for_test(aptos_framework);
        package_manager::init_for_test(deployer, resource_signer);

        flip_many(deployer, vector<bool> [true, false, true, false, true, false, true, false, true, false]);
        flip_many(deployer, vector<bool> [true, false, true, false, true, false, true, false, true, false]);
        flip_many(deployer, vector<bool> [true, false, true, false, true, false, true, false, true, false]);
        flip_many(deployer, vector<bool> [true, false, true, false, true, false, true, false, true, false]);
        std::debug::print(&get_user_stats(signer::address_of(deployer)));
        std::debug::print(borrow_global<CoinFlipStats>(object::object_address<CoinFlipStats>(&get_user_stats(signer::address_of(deployer)))));
    }
}