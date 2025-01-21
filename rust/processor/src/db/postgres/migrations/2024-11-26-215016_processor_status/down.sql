-- This file should undo anything in `up.sql`
-- Revert changes to the processor column
ALTER TABLE processor_status DROP CONSTRAINT IF EXISTS processor_status_pkey;

-- Reset the processor column to its original type
ALTER TABLE processor_status
ALTER COLUMN processor TYPE TEXT, -- Replace TEXT with the original type
ALTER COLUMN processor DROP NOT NULL;

-- Remove primary key constraint
ALTER TABLE processor_status DROP CONSTRAINT IF EXISTS processor_status_pkey;
