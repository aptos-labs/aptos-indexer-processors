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
    round INTEGER PRIMARY KEY NOT NULL,
    address VARCHAR NOT NULL,
    players_per_room INTEGER,
    FOREIGN KEY (tournament_address) REFERENCES tournament.tournaments(address)
);
CREATE TABLE IF NOT EXISTS tournament.players (
    address VARCHAR PRIMARY KEY NOT NULL,
    tournament_address VARCHAR NOT NULL,
    round INTEGER NOT NULL,
    alive BOOLEAN NOT NULL,
    submitted BOOLEAN NOT NULL,
    FOREIGN KEY (tournament_address) REFERENCES tournament.tournaments(address),
    FOREIGN KEY (round) REFERENCES tournament.rooms(round)
);
