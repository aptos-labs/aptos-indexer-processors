CREATE SCHEMA IF NOT EXISTS tournament;
CREATE TABLE IF NOT EXISTS tournament.tournaments (
    address VARCHAR PRIMARY KEY NOT NULL,
    tournament_name VARCHAR NOT NULL,
    current_round INTEGER NOT NULL,
    max_players INTEGER NOT NULL,
    num_winners INTEGER NOT NULL,
    has_started BOOLEAN NOT NULL,
    is_joinable BOOLEAN NOT NULL
);
CREATE TABLE IF NOT EXISTS tournament.rooms (
    tournament_address VARCHAR NOT NULL,
    round INTEGER NOT NULL,
    address VARCHAR NOT NULL,
    players_per_room INTEGER,
    PRIMARY KEY (tournament_address, round),
    FOREIGN KEY (tournament_address) REFERENCES tournament.tournaments(address)
);
CREATE TABLE IF NOT EXISTS tournament.players (
    address VARCHAR NOT NULL,
    tournament_address VARCHAR NOT NULL,
    round INTEGER NOT NULL,
    alive BOOLEAN NOT NULL,
    submitted BOOLEAN NOT NULL,
    PRIMARY KEY (address, tournament_address, round),
    FOREIGN KEY (tournament_address) REFERENCES tournament.tournaments(address),
    FOREIGN KEY (tournament_address, round) REFERENCES tournament.rooms(tournament_address, round)
);
