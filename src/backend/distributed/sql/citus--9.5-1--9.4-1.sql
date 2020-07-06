-- citus--9.5-1--9.4-1
-- this is a downgrade path that will revert the changes made in citus--9.4-1--9.5-1.sql

DROP FUNCTION pg_catalog.create_citus_local_table(table_name regclass);
