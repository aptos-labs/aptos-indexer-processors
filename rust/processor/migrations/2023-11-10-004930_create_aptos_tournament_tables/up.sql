CREATE TABLE IF NOT EXISTS tournaments (
    address VARCHAR(66) PRIMARY KEY NOT NULL,
    tournament_name VARCHAR NOT NULL,
    max_players BIGINT NOT NULL,
    max_num_winners BIGINT NOT NULL,
    players_joined BIGINT NOT NULL,
    is_joinable BOOLEAN NOT NULL,
    current_round_address VARCHAR(66),
    current_round_number BIGINT NOT NULL,
    current_game_module VARCHAR,
    last_transaction_version BIGINT NOT NULL,
    tournament_ended_at TIMESTAMP,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS tournament_is_joinable ON tournaments (is_joinable);
CREATE INDEX IF NOT EXISTS tournament_current_round_number ON tournaments (current_round_number);
CREATE TABLE IF NOT EXISTS tournament_coin_rewards (
    tournament_address VARCHAR(66) NOT NULL,
    coin_type VARCHAR(66) NOT NULL,
    coins NUMERIC NOT NULL,
    coin_reward_amount BIGINT NOT NULL,
    last_transaction_version BIGINT NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tournament_address, coin_type)
);
CREATE INDEX IF NOT EXISTS tournament_coin_rewards_coins ON tournament_coin_rewards (coins);
CREATE INDEX IF NOT EXISTS tournament_coin_rewards_coin_reward_amount ON tournament_coin_rewards (coin_reward_amount);
CREATE TABLE IF NOT EXISTS tournament_token_rewards (
    tournament_address VARCHAR(66) PRIMARY KEY NOT NULL,
    tokens TEXT[] NOT NULL,
    last_transaction_version BIGINT NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS tournament_token_rewards_tokens ON tournament_token_rewards (tokens);
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
CREATE TABLE IF NOT EXISTS tournament_rooms (
    address VARCHAR(66) PRIMARY KEY NOT NULL,
    tournament_address VARCHAR(66) NOT NULL,
    round_address VARCHAR(66) NOT NULL,
    in_progress BOOLEAN NOT NULL,
    last_transaction_version BIGINT NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS tournament_room_round_address ON tournament_rooms (round_address);
CREATE INDEX IF NOT EXISTS tournament_room_in_progress ON tournament_rooms (in_progress);
CREATE TABLE IF NOT EXISTS tournament_players (
    token_address VARCHAR(66) PRIMARY KEY NOT NULL,
    user_address VARCHAR(66) NOT NULL,
    tournament_address VARCHAR(66) NOT NULL,
    room_address VARCHAR(66),
    player_name VARCHAR NOT NULL,
    alive BOOLEAN NOT NULL,
    token_uri VARCHAR NOT NULL,
    coin_reward_claimed_type VARCHAR,
    coin_reward_claimed_amount BIGINT,
    token_reward_claimed TEXT[] NOT NULL,
    last_transaction_version BIGINT NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS tournament_player_user_address ON tournament_players (user_address);
CREATE INDEX IF NOT EXISTS tournament_player_tournament_address ON tournament_players (tournament_address);
CREATE INDEX IF NOT EXISTS tournament_player_room_address ON tournament_players (room_address);
-- MANUAL GAME INDEXING
CREATE TABLE IF NOT EXISTS rock_paper_scissors_games (
    room_address VARCHAR(66) PRIMARY KEY NOT NULL,
    player1_token_address VARCHAR(66) NOT NULL,
    player2_token_address VARCHAR(66) NOT NULL,
    last_transaction_version BIGINT NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS rock_paper_scissors_game_player1_token_address ON rock_paper_scissors_game (player1_token_address);
CREATE INDEX IF NOT EXISTS rock_paper_scissors_game_player2_token_address ON rock_paper_scissors_game (player2_token_address);
CREATE TABLE IF NOT EXISTS rock_paper_scissors_players (
    token_address VARCHAR(66) NOT NULL,
    room_address VARCHAR(66) NOT NULL,
    user_address VARCHAR(66) NOT NULL,
    committed_action TEXT[],
    verified_action TEXT[],
    last_transaction_version BIGINT NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (room_address, token_address)
);
CREATE TABLE IF NOT EXISTS trivia_questions (
    round_address VARCHAR(66) PRIMARY KEY NOT NULL,
    question VARCHAR NOT NULL,
    possible_answers TEXT[] NOT NULL,
    revealed_answer_index BIGINT NOT NULL,
    last_transaction_version BIGINT NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS trivia_question_question ON trivia_question (question);
CREATE INDEX IF NOT EXISTS trivia_question_revealed_answer_index ON trivia_question (revealed_answer_index);
CREATE TABLE IF NOT EXISTS trivia_answers (
    token_address VARCHAR(66) NOT NULL,
    round_address VARCHAR(66) NOT NULL,
    answer_index BIGINT NOT NULL,
    last_transaction_version BIGINT NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (token_address, round_address)
);
CREATE INDEX IF NOT EXISTS trivia_answer_answer_index ON trivia_answer (answer_index);