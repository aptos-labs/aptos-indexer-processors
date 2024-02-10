ALTER TABLE IF EXISTS tournaments DROP COLUMN tournament_start_timestamp;
ALTER TABLE IF EXISTS tournament_rounds DROP COLUMN round_end_timestamp;
DROP TABLE IF EXISTS main_page_tournament;
