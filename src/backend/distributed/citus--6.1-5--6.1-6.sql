/* citus--6.1-5--6.1-6.sql */

SET search_path = 'pg_catalog';

-- we don't need this constraint any more since reference tables 
-- wouldn't have partition columns, which we represent as NULL
ALTER TABLE pg_dist_partition ALTER COLUMN partkey DROP NOT NULL;

RESET search_path;
