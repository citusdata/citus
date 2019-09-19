/* citus--8.3-1--9.0-1 */

-- We swapped the groupid and nodeid sequences when creating pg_dist_node
ALTER TABLE pg_dist_node ALTER COLUMN groupid SET DEFAULT nextval ('pg_dist_groupid_seq');
ALTER TABLE pg_dist_node ALTER COLUMN nodeid SET DEFAULT nextval('pg_dist_node_nodeid_seq');

CREATE SCHEMA IF NOT EXISTS citus_internal;

-- move citus internal functions to citus_internal to make space in the citus schema for
-- our public interface
ALTER FUNCTION citus.find_groupid_for_node SET SCHEMA citus_internal;
ALTER FUNCTION citus.pg_dist_node_trigger_func SET SCHEMA citus_internal;
ALTER FUNCTION citus.pg_dist_shard_placement_trigger_func SET SCHEMA citus_internal;
ALTER FUNCTION citus.refresh_isolation_tester_prepared_statement SET SCHEMA citus_internal;
ALTER FUNCTION citus.replace_isolation_tester_func SET SCHEMA citus_internal;
ALTER FUNCTION citus.restore_isolation_tester_func SET SCHEMA citus_internal;

#include "udfs/pg_dist_shard_placement_trigger_func/9.0-1.sql"

CREATE OR REPLACE FUNCTION pg_catalog.worker_create_or_replace_object(statement text)
  RETURNS bool
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$worker_create_or_replace_object$$;
COMMENT ON FUNCTION pg_catalog.worker_create_or_replace_object(statement text)
    IS 'takes a sql CREATE statement, before executing the create it will check if an object with that name already exists and safely replaces that named object with the new object';

CREATE OR REPLACE FUNCTION pg_catalog.master_unmark_object_distributed(classid oid, objid oid, objsubid int)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$master_unmark_object_distributed$$;
COMMENT ON FUNCTION pg_catalog.master_unmark_object_distributed(classid oid, objid oid, objsubid int)
    IS 'remove an object address from citus.pg_dist_object once the object has been deleted';

CREATE TABLE citus.pg_dist_object (
	-- fields used for composite primary key
    classid oid NOT NULL,
    objid oid NOT NULL,
    objsubid integer NOT NULL,

    -- fields used for upgrades
    type text DEFAULT NULL,
    object_names text[] DEFAULT NULL,
    object_args text[] DEFAULT NULL,

    -- fields that are only valid for distributed
    -- functions/procedures
    distribution_argument_index int,
    colocationid int,

    CONSTRAINT pg_dist_object_pkey PRIMARY KEY (classid, objid, objsubid)
);

CREATE FUNCTION master_dist_object_cache_invalidate()
    RETURNS trigger
    LANGUAGE C
    AS 'MODULE_PATHNAME', $$master_dist_object_cache_invalidate$$;
COMMENT ON FUNCTION master_dist_object_cache_invalidate()
    IS 'register relcache invalidation for changed rows';
CREATE TRIGGER dist_object_cache_invalidate
    AFTER INSERT OR UPDATE OR DELETE
    ON citus.pg_dist_object
    FOR EACH ROW EXECUTE PROCEDURE master_dist_object_cache_invalidate();

#include "udfs/create_distributed_function/9.0-1.sql"

#include "udfs/citus_drop_trigger/9.0-1.sql"
#include "udfs/citus_prepare_pg_upgrade/9.0-1.sql"
#include "udfs/citus_finish_pg_upgrade/9.0-1.sql"

/*
 * We truncate pg_dist_node during metadata syncing, but we do not want
 * this to cascade to pg_dist_poolinfo, which is generally maintained
 * by the operator.
 */
ALTER TABLE pg_dist_poolinfo DROP CONSTRAINT pg_dist_poolinfo_nodeid_fkey;

#include "udfs/get_rebalance_table_shards_plan/9.0-1.sql"
#include "udfs/replicate_table_shards/9.0-1.sql"
#include "udfs/rebalance_table_shards/9.0-1.sql"

SET search_path = 'pg_catalog';

DROP EXTENSION IF EXISTS shard_rebalancer;

-- get_rebalance_progress returns the list of shard placement move operations along with
-- their progressions for ongoing rebalance operations.
--
CREATE OR REPLACE FUNCTION get_rebalance_progress()
  RETURNS TABLE(sessionid integer,
                table_name regclass,
                shardid bigint,
                shard_size bigint,
                sourcename text,
                sourceport int,
                targetname text,
                targetport int,
                progress bigint)
  AS 'MODULE_PATHNAME'
  LANGUAGE C STRICT;
COMMENT ON FUNCTION get_rebalance_progress()
    IS 'provides progress information about the ongoing rebalance operations';

DROP FUNCTION master_add_node(text, integer, integer, noderole, name);
CREATE FUNCTION master_add_node(nodename text,
                                nodeport integer,
                                groupid integer default 0,
                                noderole noderole default 'primary',
                                nodecluster name default 'default')
  RETURNS INTEGER
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$master_add_node$$;
COMMENT ON FUNCTION master_add_node(nodename text, nodeport integer,
                                    groupid integer, noderole noderole, nodecluster name)
  IS 'add node to the cluster';

DROP FUNCTION master_add_inactive_node(text, integer, integer, noderole, name);
CREATE FUNCTION master_add_inactive_node(nodename text,
                                         nodeport integer,
                                         groupid integer default 0,
                                         noderole noderole default 'primary',
                                         nodecluster name default 'default')
  RETURNS INTEGER
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME',$$master_add_inactive_node$$;
COMMENT ON FUNCTION master_add_inactive_node(nodename text,nodeport integer,
                                             groupid integer, noderole noderole,
                                             nodecluster name)
  IS 'prepare node by adding it to pg_dist_node';

SET search_path = 'pg_catalog';

DROP FUNCTION master_activate_node(text, integer);
CREATE FUNCTION master_activate_node(nodename text,
                                     nodeport integer)
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME',$$master_activate_node$$;
COMMENT ON FUNCTION master_activate_node(nodename text, nodeport integer)
    IS 'activate a node which is in the cluster';

DROP FUNCTION master_add_secondary_node(text, integer, text, integer, name);
CREATE FUNCTION master_add_secondary_node(nodename text,
                                          nodeport integer,
                                          primaryname text,
                                          primaryport integer,
                                          nodecluster name default 'default')
  RETURNS INTEGER
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$master_add_secondary_node$$;
COMMENT ON FUNCTION master_add_secondary_node(nodename text, nodeport integer,
                                              primaryname text, primaryport integer,
                                              nodecluster name)
  IS 'add a secondary node to the cluster';


REVOKE ALL ON FUNCTION master_activate_node(text,int) FROM PUBLIC;
REVOKE ALL ON FUNCTION master_add_inactive_node(text,int,int,noderole,name) FROM PUBLIC;
REVOKE ALL ON FUNCTION master_add_node(text,int,int,noderole,name) FROM PUBLIC;
REVOKE ALL ON FUNCTION master_add_secondary_node(text,int,text,int,name) FROM PUBLIC;

ALTER TABLE pg_dist_node ADD COLUMN metadatasynced BOOLEAN DEFAULT FALSE;
COMMENT ON COLUMN pg_dist_node.metadatasynced IS
    'indicates whether the node has the most recent metadata';

CREATE FUNCTION worker_apply_sequence_command(create_sequence_command text,
                                              sequence_type_id regtype DEFAULT 'bigint'::regtype)
    RETURNS VOID
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_apply_sequence_command$$;
COMMENT ON FUNCTION worker_apply_sequence_command(text,regtype)
    IS 'create a sequence which produces globally unique values';

RESET search_path;
