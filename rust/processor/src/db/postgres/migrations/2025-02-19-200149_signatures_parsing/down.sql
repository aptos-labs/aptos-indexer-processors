-- This file should undo anything in `up.sql`
ALTER TABLE signatures DROP COLUMN IF EXISTS any_signature_type,
  DROP COLUMN IF EXISTS public_key_type;