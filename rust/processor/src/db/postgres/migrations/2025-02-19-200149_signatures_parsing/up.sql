-- Your SQL goes here
ALTER TABLE signatures
ADD COLUMN IF NOT EXISTS any_signature_type VARCHAR,
  ADD COLUMN IF NOT EXISTS public_key_type VARCHAR;