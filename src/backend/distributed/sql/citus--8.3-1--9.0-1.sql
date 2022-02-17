--  citus--8.3-1--9.0-1

SET search_path = 'pg_catalog';

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

-- we can now safely grant usage on the citus schema to use types
GRANT USAGE ON SCHEMA citus TO public;

#include "udfs/pg_dist_shard_placement_trigger_func/9.0-1.sql"
#include "udfs/worker_create_or_replace_object/9.0-1.sql"

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

-- We truncate pg_dist_node during metadata syncing, but we do not want
-- this to cascade to pg_dist_poolinfo, which is generally maintained
-- by the operator.
ALTER TABLE pg_dist_poolinfo DROP CONSTRAINT pg_dist_poolinfo_nodeid_fkey;

--  if the rebalancer extension is still around, drop it before creating Citus functions
DROP EXTENSION IF EXISTS shard_rebalancer;

#include "udfs/get_rebalance_table_shards_plan/9.0-1.sql"
#include "udfs/replicate_table_shards/9.0-1.sql"
#include "udfs/rebalance_table_shards/9.0-1.sql"
#include "udfs/get_rebalance_progress/9.0-1.sql"

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

#include "udfs/citus_isolation_test_session_is_blocked/9.0-1.sql"


CREATE FUNCTION ensure_truncate_trigger_is_after()
    RETURNS void
    LANGUAGE plpgsql
    SET search_path = pg_catalog
    AS $$
DECLARE
    table_name regclass;
    command text;
    trigger_name text;
BEGIN
    --
    -- register triggers
    --
    FOR table_name, trigger_name IN SELECT tgrelid::regclass, tgname
      FROM pg_dist_partition
      JOIN pg_trigger ON tgrelid=logicalrelid
      JOIN pg_class ON pg_class.oid=logicalrelid
      WHERE
        tgname LIKE 'truncate_trigger_%' AND tgfoid = 'citus_truncate_trigger'::regproc
    LOOP
        command := 'drop trigger ' || trigger_name || ' on ' || table_name;
        EXECUTE command;
        command := 'create trigger ' || trigger_name || ' after truncate on ' || table_name || ' execute procedure pg_catalog.citus_truncate_trigger()';
        EXECUTE command;
        command := 'update pg_trigger set tgisinternal = true where tgname = ' || quote_literal(trigger_name);
        EXECUTE command;
    END LOOP;
END;
$$;

SELECT ensure_truncate_trigger_is_after();
DROP FUNCTION ensure_truncate_trigger_is_after;

-- This sequence is unused
DROP SEQUENCE pg_catalog.pg_dist_jobid_seq;

RESET search_path;
