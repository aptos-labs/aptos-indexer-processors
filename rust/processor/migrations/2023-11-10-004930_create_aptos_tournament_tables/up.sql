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
    current_game_module VARCHAR
);
CREATE INDEX IF NOT EXISTS tournament_is_joinable ON tournaments (is_joinable);
CREATE INDEX IF NOT EXISTS tournament_has_ended ON tournaments (has_ended);
CREATE INDEX IF NOT EXISTS tournament_current_round_number ON tournaments (current_round_number);
CREATE TABLE IF NOT EXISTS tournament_rounds (
    address VARCHAR(66) PRIMARY KEY NOT NULL,
    matchmaking_ended BOOLEAN NOT NULL,
    play_started BOOLEAN NOT NULL,
    play_ended BOOLEAN NOT NULL,
    paused BOOLEAN NOT NULL,
    matchmaker_address VARCHAR(66) NOT NULL
);
CREATE INDEX IF NOT EXISTS tournament_round_address ON tournament_rounds (address);
CREATE TABLE IF NOT EXISTS tournament_rooms (
    round_address VARCHAR(66) NOT NULL,
    address VARCHAR(66) NOT NULL,
    players_per_room BIGINT,
    PRIMARY KEY (round_address, address)
);
CREATE TABLE IF NOT EXISTS tournament_players (
    address VARCHAR(66) NOT NULL,
    tournament_address VARCHAR(66) NOT NULL,
    room_address VARCHAR(66),
    token_address VARCHAR(66) NOT NULL,
    alive BOOLEAN NOT NULL,
    submitted BOOLEAN NOT NULL,
    PRIMARY KEY (address, tournament_address)
);
