-- Add replica information columns to pg_dist_node
ALTER TABLE pg_catalog.pg_dist_node ADD COLUMN nodeisclone BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE pg_catalog.pg_dist_node ADD COLUMN nodeprimarynodeid INT4 NOT NULL DEFAULT 0;

-- Add a comment to the table and columns for clarity in \d output
COMMENT ON COLUMN pg_catalog.pg_dist_node.nodeisclone IS 'Indicates if this node is a replica of another node.';
COMMENT ON COLUMN pg_catalog.pg_dist_node.nodeprimarynodeid IS 'If nodeisclone is true, this stores the nodeid of its primary node.';
