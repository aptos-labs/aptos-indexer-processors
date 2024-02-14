CREATE TABLE IF NOT EXISTS tournaments_debug (
    transaction_version BIGINT,
    index BIGINT,
    address VARCHAR(66),
    tournament_name VARCHAR,
    max_players BIGINT NOT NULL,
    max_num_winners BIGINT NOT NULL,
    players_joined BIGINT NOT NULL,
    is_joinable BOOLEAN NOT NULL,
    current_round_address VARCHAR(66),
    current_round_number BIGINT NOT NULL,
    current_game_module VARCHAR,
    tournament_ended_at TIMESTAMP,
    tournament_start_timestamp TIMESTAMP DEFAULT TIMESTAMP '1970-01-01 00:00:00',
    inserted_at TIMESTAMP DEFAULT NOW() NOT NULL,
    PRIMARY KEY (transaction_version, index)
);
CREATE TABLE IF NOT EXISTS tournament_players_debug (
    transaction_version BIGINT NOT NULL,
    index BIGINT NOT NULL,
    token_address VARCHAR(66) NOT NULL,
    user_address VARCHAR(66),
    tournament_address VARCHAR(66),
    room_address VARCHAR(66),
    player_name VARCHAR,
    alive BOOLEAN DEFAULT TRUE,
    token_uri VARCHAR,
    coin_reward_claimed_type VARCHAR,
    coin_reward_claimed_amount BIGINT,
    token_reward_claimed TEXT[],
    inserted_at TIMESTAMP DEFAULT NOW() NOT NULL,
    PRIMARY KEY (transaction_version, index)
);
CREATE INDEX IF NOT EXISTS tournaments_debug_address ON tournaments_debug (address);
CREATE INDEX IF NOT EXISTS tournament_players_debug_token_address ON tournament_players_debug (token_address);
CREATE INDEX IF NOT EXISTS tournament_players_debug_user_address ON tournament_players_debug (user_address);
CREATE INDEX IF NOT EXISTS tournament_players_debug_tournament_address ON tournament_players_debug (tournament_address);
