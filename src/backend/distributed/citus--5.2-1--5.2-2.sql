
SET search_path = 'pg_catalog';

/* add pg_dist_node */
CREATE TABLE citus.pg_dist_node(
	nodeid int NOT NULL PRIMARY KEY,
	groupid int NOT NULL,
	nodename text NOT NULL,
	nodeport int NOT NULL,
	UNIQUE (nodename, nodeport)
);

CREATE SEQUENCE citus.pg_dist_groupid_seq
	MINVALUE 1
	MAXVALUE 4294967296;

CREATE SEQUENCE citus.pg_dist_node_nodeid_seq
	MINVALUE 1
	MAXVALUE 4294967296;

ALTER TABLE citus.pg_dist_node SET SCHEMA pg_catalog;
ALTER SEQUENCE citus.pg_dist_groupid_seq SET SCHEMA pg_catalog;
ALTER SEQUENCE citus.pg_dist_node_nodeid_seq SET SCHEMA pg_catalog;

CREATE FUNCTION master_dist_node_cache_invalidate()
	RETURNS trigger
	LANGUAGE C
	AS 'MODULE_PATHNAME', $$master_dist_node_cache_invalidate$$;
COMMENT ON FUNCTION master_dist_node_cache_invalidate()
	IS 'invalidate internal cache of nodes when pg_dist_nodes changes';
CREATE TRIGGER dist_node_cache_invalidate
    AFTER INSERT OR UPDATE OR DELETE
    ON pg_catalog.pg_dist_node
    FOR EACH ROW EXECUTE PROCEDURE master_dist_node_cache_invalidate();

CREATE FUNCTION cluster_add_node(nodename text,
								 nodeport integer,
								 groupid integer DEFAULT 0)
	RETURNS record
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$cluster_add_node$$;
COMMENT ON FUNCTION cluster_add_node(nodename text,
									 nodeport integer,
									 groupid integer)
	IS 'add node to the cluster';

CREATE FUNCTION cluster_remove_node(nodename text, nodeport integer)
	RETURNS void
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$cluster_remove_node$$;
COMMENT ON FUNCTION cluster_remove_node(nodename text, nodeport integer)
	IS 'remove node from the cluster';

/* this only needs to run once, now. */
CREATE FUNCTION cluster_initialize_node_metadata()
    RETURNS BOOL
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$cluster_initialize_node_metadata$$;

SELECT cluster_initialize_node_metadata();

RESET search_path;
