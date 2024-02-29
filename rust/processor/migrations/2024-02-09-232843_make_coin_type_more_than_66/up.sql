ALTER TABLE IF EXISTS tournament_coin_rewards ALTER COLUMN coin_type TYPE VARCHAR;
CREATE OR REPLACE VIEW trivia_answer_counts AS
    SELECT 
        trivia_answers.round_address,
        trivia_answers.answer_index,
        count(1) AS count
    FROM trivia_answers
    GROUP BY trivia_answers.round_address, trivia_answers.answer_index;
