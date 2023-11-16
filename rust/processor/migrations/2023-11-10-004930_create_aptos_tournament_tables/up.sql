-- Create tournaments table
CREATE SCHEMA IF NOT EXISTS aptos_tournament;
CREATE TABLE IF NOT EXISTS aptos_tournament.tournaments (
    address VARCHAR PRIMARY KEY NOT NULL,
    tournament_name VARCHAR NOT NULL,
    max_players VARCHAR NOT NULL,
    max_num_winners INTEGER NOT NULL,
    players_joined INTEGER NOT NULL,
    secondary_admin_address VARCHAR,
    is_joinable BOOLEAN NOT NULL,
    has_ended BOOLEAN NOT NULL
);

-- Create rounds table
CREATE TABLE IF NOT EXISTS aptos_tournament.rounds (
    tournament_address VARCHAR NOT NULL,
    number INTEGER NOT NULL,
    address VARCHAR NOT NULL,
    game_module VARCHAR NOT NULL,
    matchmaking_ended BOOLEAN NOT NULL,
    play_started BOOLEAN NOT NULL,
    play_ended BOOLEAN NOT NULL,
    paused BOOLEAN NOT NULL,
    matchmaker_address VARCHAR NOT NULL,
    PRIMARY KEY (tournament_address, number)
);

-- Create rooms table
CREATE TABLE IF NOT EXISTS aptos_tournament.rooms (
    round_address VARCHAR NOT NULL,
    address VARCHAR NOT NULL,
    players_per_room INTEGER,
    PRIMARY KEY (round_address, address)
);

-- Create players table
CREATE TABLE IF NOT EXISTS aptos_tournament.players (
    address VARCHAR NOT NULL,
    tournament_address VARCHAR NOT NULL,
    room_address VARCHAR,
    token_address VARCHAR NOT NULL,
    alive BOOLEAN NOT NULL,
    submitted BOOLEAN NOT NULL,
    PRIMARY KEY (address, tournament_address)
);
