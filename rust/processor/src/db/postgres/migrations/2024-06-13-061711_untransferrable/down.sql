-- This file should undo anything in `up.sql`
ALTER TABLE public.current_objects DROP COLUMN IF EXISTS untransferrable;
ALTER TABLE public.objects DROP COLUMN IF EXISTS untransferrable;