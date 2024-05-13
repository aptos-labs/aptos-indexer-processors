-- To avoid having two entries for the same relationship, we erase the notion of "from"
-- and "to" addresses, instead directly tracking whose turn it is next to poke.
CREATE TABLE IF NOT EXISTS current_pokes (
    -- The address of the person who initiated the poke relationship.
    initial_poker_address VARCHAR(66) NOT NULL,
    -- The address of the person who received the poke from the initial poker.
    recipient_poker_address VARCHAR(66) NOT NULL,
    -- This is true if the initial poker is the person who needs to poke next.
    initial_poker_is_next_poker BOOLEAN NOT NULL,
    times_poked BIGINT NOT NULL,
    last_transaction_version BIGINT NOT NULL,
    last_poke_at TIMESTAMP NOT NULL DEFAULT NOW(),
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (
        initial_poker_address,
        recipient_poker_address
    )
);

-- Index for queries checking if the initial poker is next
CREATE INDEX IF NOT EXISTS idx_initial_poker_next ON current_pokes
  (initial_poker_address, initial_poker_is_next_poker);

-- Index for queries checking if the recipient poker is next
CREATE INDEX IF NOT EXISTS idx_recipient_poker_next ON current_pokes
  (recipient_poker_address, initial_poker_is_next_poker);
