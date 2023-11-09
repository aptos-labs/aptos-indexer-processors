// Copyright © Aptos Foundation
// @generated automatically by Diesel CLI.

pub mod aptos_tournament {
    diesel::table! {
        aptos_tournament.players (address, tournament_address, round) {
            address -> Varchar,
            tournament_address -> Varchar,
            round -> Int4,
            alive -> Bool,
            submitted -> Bool,
        }
    }

    diesel::table! {
        aptos_tournament.rooms (tournament_address, round) {
            tournament_address -> Varchar,
            round -> Int4,
            address -> Varchar,
            players_per_room -> Nullable<Int4>,
        }
    }

    diesel::table! {
        aptos_tournament.tournaments (address) {
            address -> Varchar,
            tournament_name -> Varchar,
            current_round -> Int4,
            max_players -> Int4,
            num_winners -> Int4,
            has_started -> Bool,
            is_joinable -> Bool,
        }
    }

    diesel::joinable!(players -> tournaments (tournament_address));
    diesel::joinable!(rooms -> tournaments (tournament_address));

    diesel::allow_tables_to_appear_in_same_query!(players, rooms, tournaments,);
}
