-- citus--12.1-1--12.0-1
-- this is an empty downgrade path since citus--12.0-1--12.1-1.sql is empty for now


DROP SCHEMA citus_catalog CASCADE;
DROP FUNCTION pg_catalog.execute_command_on_all_nodes(text);
DROP FUNCTION pg_catalog.execute_command_on_other_nodes(text);
DROP FUNCTION pg_catalog.citus_internal_database_command(text);
DROP FUNCTION pg_catalog.citus_internal_add_database_shard(text,int);
DROP FUNCTION pg_catalog.citus_internal_start_migration_monitor(text,text);
DROP FUNCTION pg_catalog.database_move(text,int);
DROP FUNCTION pg_catalog.pgcopydb_move(text,text,text);
