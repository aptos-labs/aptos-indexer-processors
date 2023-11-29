CREATE TABLE IF NOT EXISTS tournaments (
    address VARCHAR(66) PRIMARY KEY NOT NULL,
    tournament_name VARCHAR NOT NULL,
    max_players BIGINT NOT NULL,
    max_num_winners BIGINT NOT NULL,
    players_joined BIGINT NOT NULL,
    is_joinable BOOLEAN NOT NULL,
    has_ended BOOLEAN NOT NULL,
    current_round_address VARCHAR(66),
    current_round_number BIGINT NOT NULL,
    current_game_module VARCHAR,
    last_transaction_version BIGINT NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS tournament_is_joinable ON tournaments (is_joinable);
CREATE INDEX IF NOT EXISTS tournament_has_ended ON tournaments (has_ended);
CREATE INDEX IF NOT EXISTS tournament_current_round_number ON tournaments (current_round_number);
CREATE TABLE IF NOT EXISTS tournament_rounds (
    address VARCHAR(66) PRIMARY KEY NOT NULL,
    number BIGINT NOT NULL,
    play_started BOOLEAN NOT NULL,
    play_ended BOOLEAN NOT NULL,
    paused BOOLEAN NOT NULL,
    last_transaction_version BIGINT NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS tournament_round_address ON tournament_rounds (address);
CREATE TABLE IF NOT EXISTS tournament_players (
    token_address VARCHAR(66) PRIMARY KEY NOT NULL,
    user_address VARCHAR(66) NOT NULL,
    tournament_address VARCHAR(66) NOT NULL,
    room_address VARCHAR(66),
    player_name VARCHAR NOT NULL,
    alive BOOLEAN NOT NULL,
    token_uri VARCHAR NOT NULL,
    last_transaction_version BIGINT NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS tournament_player_room_address ON tournament_players (room_address);
-- when user lost (we have delete resource)
-- token_address: delete resource w/ token address. find address from checking the db
-- tournament_address: lk.tournament addres
-- room_address: none
-- alive: false,
-- submitted: lk.submitted
-- when user won (we have the user address)
-- user_address
-- token_address:
-- tournament_address: lk.tournament address
-- room_address: none
-- alive: true,
-- submitted: true
-- when submitted happens
-- token_address:
-- tournament_address: lk.tournament address
-- room_address: none
-- alive: true,
-- submitted: true