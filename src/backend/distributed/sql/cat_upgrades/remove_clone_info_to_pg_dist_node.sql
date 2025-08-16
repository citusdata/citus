-- Remove clone information columns to pg_dist_node
ALTER TABLE pg_catalog.pg_dist_node DROP COLUMN IF EXISTS nodeisclone;
ALTER TABLE pg_catalog.pg_dist_node DROP COLUMN IF EXISTS nodeprimarynodeid;
