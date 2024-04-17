-- This file should undo anything in `up.sql`
ALTER TABLE signatures ALTER COLUMN public_key TYPE VARCHAR(66);