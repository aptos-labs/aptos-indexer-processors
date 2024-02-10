ALTER TABLE IF EXISTS tournament_coin_rewards ALTER COLUMN coin_type TYPE VARCHAR(66);
DROP VIEW IF EXISTS trivia_answer_counts;
