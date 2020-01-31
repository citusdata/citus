/* cstore_fdw/cstore_fdw--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION UPDATE
\echo Use "ALTER EXTENSION cstore_fdw UPDATE TO '1.1'" to load this file. \quit

CREATE FUNCTION cstore_ddl_event_end_trigger()
RETURNS event_trigger
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE EVENT TRIGGER cstore_ddl_event_end
ON ddl_command_end
EXECUTE PROCEDURE cstore_ddl_event_end_trigger();

CREATE FUNCTION cstore_table_size(relation regclass)
RETURNS bigint
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

-- cstore_fdw creates directories to store files for tables with automatically
-- determined filename during the CREATE SERVER statement. Since this feature
-- was newly added in v1.1, servers created with v1.0 did not create them. So,
-- we create a server with v1.1 to ensure that the required directories are
-- created to allow users to create automatically managed tables with old servers.
CREATE SERVER cstore_server_for_updating_1_0_to_1_1 FOREIGN DATA WRAPPER cstore_fdw;
DROP SERVER cstore_server_for_updating_1_0_to_1_1;
