-- Your SQL goes here
-- Drop existing constraints if needed
ALTER TABLE processor_status DROP CONSTRAINT IF EXISTS processor_status_pkey;

-- Modify the processor column
ALTER TABLE processor_status
ALTER COLUMN processor TYPE VARCHAR(100),
ALTER COLUMN processor SET NOT NULL;

-- Add unique constraint and primary key
ALTER TABLE processor_status ADD CONSTRAINT processor_status_pkey UNIQUE (processor);
ALTER TABLE processor_status ADD PRIMARY KEY (processor);
