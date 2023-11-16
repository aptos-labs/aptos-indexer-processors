// @generated automatically by Diesel CLI.

pub mod aptos_tournament {
    diesel::table! {
        aptos_tournament.players (address, tournament_address) {
            address -> Varchar,
            tournament_address -> Varchar,
            room_address -> Nullable<Varchar>,
            token_address -> Varchar,
            alive -> Bool,
            submitted -> Bool,
        }
    }

    diesel::table! {
        aptos_tournament.rooms (round_address, address) {
            round_address -> Varchar,
            address -> Varchar,
            players_per_room -> Nullable<Int4>,
        }
    }

    diesel::table! {
        aptos_tournament.rounds (tournament_address, number) {
            tournament_address -> Varchar,
            number -> Int4,
            address -> Varchar,
            game_module -> Varchar,
            matchmaking_ended -> Bool,
            play_started -> Bool,
            play_ended -> Bool,
            paused -> Bool,
            matchmaker_address -> Varchar,
        }
    }

    diesel::table! {
        aptos_tournament.tournaments (address) {
            address -> Varchar,
            tournament_name -> Varchar,
            max_players -> Varchar,
            max_num_winners -> Int4,
            players_joined -> Int4,
            secondary_admin_address -> Nullable<Varchar>,
            is_joinable -> Bool,
            has_ended -> Bool,
        }
    }

    diesel::allow_tables_to_appear_in_same_query!(players, rooms, rounds, tournaments,);
}
