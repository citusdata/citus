/* citus--5.2-4--6.0-1.sql */

/* change logicalrelid type to regclass to allow implicit casts to text */
ALTER TABLE pg_catalog.pg_dist_partition ALTER COLUMN logicalrelid TYPE regclass;
ALTER TABLE pg_catalog.pg_dist_shard ALTER COLUMN logicalrelid TYPE regclass;
