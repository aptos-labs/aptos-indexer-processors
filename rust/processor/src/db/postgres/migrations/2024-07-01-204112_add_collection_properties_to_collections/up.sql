ALTER TABLE current_collections_v2
ADD COLUMN collection_properties JSONB;

ALTER TABLE collections_v2
ADD COLUMN collection_properties JSONB;
