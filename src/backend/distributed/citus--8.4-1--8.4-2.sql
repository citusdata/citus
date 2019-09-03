/* citus--8.3-1--8.4-1 */

ALTER TABLE pg_catalog.pg_dist_node ADD COLUMN metadatasynced BOOLEAN DEFAULT FALSE;
COMMENT ON COLUMN pg_catalog.pg_dist_node.metadatasynced IS
    'indicates whether the node has the most recent metadata';
