ALTER TABLE current_collections_v2
DROP COLUMN IF EXISTS collection_properties;

ALTER TABLE collections_v2
DROP COLUMN IF EXISTS collection_properties;
