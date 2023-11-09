CREATE SCHEMA IF NOT EXISTS aptos_tournament;
CREATE TABLE IF NOT EXISTS aptos_tournament.tournaments (
    address VARCHAR PRIMARY KEY NOT NULL,
    tournament_name VARCHAR NOT NULL,
    current_round INTEGER NOT NULL,
    max_players INTEGER NOT NULL,
    num_winners INTEGER NOT NULL,
    has_started BOOLEAN NOT NULL,
    is_joinable BOOLEAN NOT NULL
);
CREATE TABLE IF NOT EXISTS aptos_tournament.rooms (
    tournament_address VARCHAR NOT NULL,
    round INTEGER NOT NULL,
    address VARCHAR NOT NULL,
    players_per_room INTEGER,
    PRIMARY KEY (tournament_address, round),
    FOREIGN KEY (tournament_address) REFERENCES aptos_tournament.tournaments(address)
);
CREATE TABLE IF NOT EXISTS aptos_tournament.players (
    address VARCHAR NOT NULL,
    tournament_address VARCHAR NOT NULL,
    round INTEGER NOT NULL,
    alive BOOLEAN NOT NULL,
    submitted BOOLEAN NOT NULL,
    PRIMARY KEY (address, tournament_address, round),
    FOREIGN KEY (tournament_address) REFERENCES aptos_tournament.tournaments(address),
    FOREIGN KEY (tournament_address, round) REFERENCES aptos_tournament.rooms(tournament_address, round)
);
