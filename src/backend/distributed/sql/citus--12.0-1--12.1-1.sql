-- citus--12.0-1--12.1-1

-- bump version to 12.1-1


/*
 * execute_command_on_all_nodes runs a command on all nodes
 * in a 2PC.
 */
CREATE OR REPLACE FUNCTION pg_catalog.execute_command_on_all_nodes(
    command text)
 RETURNS void
 LANGUAGE C
 STRICT
AS 'MODULE_PATHNAME', $$execute_command_on_all_nodes$$;
COMMENT ON FUNCTION pg_catalog.execute_command_on_all_nodes(text) IS
 'run a command on all other nodes in a 2PC';

/*
 * execute_command_on_other_nodes runs a command on all other nodes
 * in a 2PC.
 */
CREATE OR REPLACE FUNCTION pg_catalog.execute_command_on_other_nodes(
    command text)
 RETURNS void
 LANGUAGE C
 STRICT
AS 'MODULE_PATHNAME', $$execute_command_on_other_nodes$$;
COMMENT ON FUNCTION pg_catalog.execute_command_on_other_nodes(text) IS
 'run a command on all other nodes in a 2PC';

/*
 * database_shard_assign assigns a database to a specific shard.
 */
CREATE OR REPLACE FUNCTION pg_catalog.database_shard_assign(database_name text)
 RETURNS int
 LANGUAGE C
 STRICT
AS 'MODULE_PATHNAME', $$database_shard_assign$$;
COMMENT ON FUNCTION pg_catalog.database_shard_assign(text) IS
 'run a command on all other nodes in a 2PC';

/*
 * citus_internal_database_command creates a database according to the given command.
 */
CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_database_command(command text)
 RETURNS void
 LANGUAGE C
 STRICT
AS 'MODULE_PATHNAME', $$citus_internal_database_command$$;
COMMENT ON FUNCTION pg_catalog.citus_internal_database_command(text) IS
 'run a database command without transaction block restrictions';

/*
 * citus_internal_add_database_shard inserts a database shard
 * into the database shards metadata.
 */
CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_add_database_shard(database_name text, node_group_id int)
 RETURNS void
 LANGUAGE C
 STRICT
AS 'MODULE_PATHNAME', $$citus_internal_add_database_shard$$;
COMMENT ON FUNCTION pg_catalog.citus_internal_add_database_shard(text,int) IS
 'add a database shard to the metadata';

/*
 * citus_internal_delete_database_shard deletes a database shard
 * from the metadata
 */
CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_delete_database_shard(database_name text)
 RETURNS void
 LANGUAGE C
 STRICT
AS 'MODULE_PATHNAME', $$citus_internal_delete_database_shard$$;
COMMENT ON FUNCTION pg_catalog.citus_internal_delete_database_shard(text) IS
 'delete a database shard from the metadata';


CREATE FUNCTION pg_catalog.database_move(
	database_name text,
	target_node_group_id int)
RETURNS void
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$pgcopydb_database_move$$;
COMMENT ON FUNCTION pg_catalog.database_move(text, int)
IS 'move a database shard';

CREATE FUNCTION pg_catalog.pgcopydb_clone(
	source_url text,
	destination_url text,
    follow bool DEFAULT false,
    migration_name text DEFAULT 'pgcopydb')
RETURNS void
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$pgcopydb_clone$$;
COMMENT ON FUNCTION pg_catalog.pgcopydb_clone(text, text, bool, text)
IS 'clone a database using pgcopydb';

CREATE FUNCTION pg_catalog.pgcopydb_list_progress(
	source_url text,
    migration_name text DEFAULT 'pgcopydb')
RETURNS json
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$pgcopydb_list_progress$$;
COMMENT ON FUNCTION pg_catalog.pgcopydb_list_progress(text, text)
IS 'list progress of a database clone using pgcopydb';

CREATE FUNCTION pg_catalog.pgcopydb_end_follow(
	source_url text,
    migration_name text DEFAULT 'pgcopydb')
RETURNS void
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$pgcopydb_end_follow$$;
COMMENT ON FUNCTION pg_catalog.pgcopydb_end_follow(text, text)
IS 'set the end position of a migration on the source URL';

#include "udfs/citus_shard_cost_by_disk_size/12.1-1.sql"

UPDATE pg_catalog.pg_dist_rebalance_strategy 
SET shard_cost_function = 'pg_catalog.citus_shard_cost_by_disk_size(bigint,char)'::regprocedure
WHERE shard_cost_function = 'pg_catalog.citus_shard_cost_by_disk_size(bigint)'::regprocedure;

DROP FUNCTION pg_catalog.citus_shard_cost_by_disk_size(bigint);

#include "udfs/citus_database_size/12.1-1.sql"

CREATE FUNCTION pg_catalog.citus_database_lock(database_name name)
RETURNS void
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$citus_database_lock_by_name$$;
COMMENT ON FUNCTION pg_catalog.citus_database_lock(name)
IS 'lock a database for new connections';
