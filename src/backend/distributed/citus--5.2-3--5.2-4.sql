/* citus--5.2-3--5.2-4.sql */

ALTER TABLE pg_dist_partition ADD COLUMN colocationid BIGINT DEFAULT 0 NOT NULL;

CREATE INDEX pg_dist_partition_colocationid_index
ON pg_dist_partition using btree(colocationid);

