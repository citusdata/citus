SET search_path = 'pg_catalog';

CREATE SEQUENCE citus.pg_dist_groupid_seq
	MINVALUE 1
	MAXVALUE 4294967296;

CREATE SEQUENCE citus.pg_dist_node_nodeid_seq
	MINVALUE 1
	MAXVALUE 4294967296;

ALTER SEQUENCE citus.pg_dist_groupid_seq SET SCHEMA pg_catalog;
ALTER SEQUENCE citus.pg_dist_node_nodeid_seq SET SCHEMA pg_catalog;

/* add pg_dist_node */
CREATE TABLE citus.pg_dist_node(
	nodeid int NOT NULL DEFAULT nextval('pg_dist_groupid_seq') PRIMARY KEY,
	groupid int NOT NULL DEFAULT nextval('pg_dist_node_nodeid_seq'),
	nodename text NOT NULL,
	nodeport int NOT NULL DEFAULT 5432,
	noderack text NOT NULL DEFAULT 'default',
	UNIQUE (nodename, nodeport)
);

ALTER TABLE citus.pg_dist_node SET SCHEMA pg_catalog;

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

CREATE FUNCTION master_add_node(nodename text,
								nodeport integer)
	RETURNS record
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$master_add_node$$;
COMMENT ON FUNCTION master_add_node(nodename text,
									nodeport integer)
	IS 'add node to the cluster';

CREATE FUNCTION master_remove_node(nodename text, nodeport integer)
	RETURNS void
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$master_remove_node$$;
COMMENT ON FUNCTION master_remove_node(nodename text, nodeport integer)
	IS 'remove node from the cluster';

/* this only needs to run once, now. */
CREATE FUNCTION master_initialize_node_metadata()
    RETURNS BOOL
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$master_initialize_node_metadata$$;

SELECT master_initialize_node_metadata();

RESET search_path;
