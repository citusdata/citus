/* citus--6.0-2--6.0-3.sql */

ALTER TABLE pg_catalog.pg_dist_partition
ADD COLUMN repmodel "char" DEFAULT 'c' NOT NULL;
