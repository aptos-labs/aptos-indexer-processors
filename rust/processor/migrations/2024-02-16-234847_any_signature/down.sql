-- This file should undo anything in `up.sql`
ALTER TABLE signatures
ALTER COLUMN signature TYPE VARCHAR(200);