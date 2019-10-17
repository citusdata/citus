SET search_path = 'pg_catalog';

CREATE TYPE isdatanode AS ENUM('true', 'marked for draining', 'false');
COMMENT ON TYPE isdatanode IS
    'enum that contains the different possible states of pg_dist_node.isdatanode';

ALTER TABLE pg_dist_node ADD isdatanode isdatanode NOT NULL DEFAULT 'true';
COMMENT ON COLUMN pg_dist_node.isdatanode IS
    'indicates whether the node is eligible to contain data from distributed tables';

RESET search_path;

CREATE FUNCTION pg_catalog.master_make_nodata_node(nodename text,
                                                   nodeport integer)
  RETURNS VOID
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$master_make_nodata_node$$;
COMMENT ON FUNCTION pg_catalog.master_make_nodata_node(nodename text,
                                                       nodeport integer)
  IS 'make node a node that doesn''t store any distributed data';

REVOKE ALL ON FUNCTION pg_catalog.master_make_nodata_node(text,int) FROM PUBLIC;

CREATE FUNCTION pg_catalog.master_make_data_node(nodename text,
                                                 nodeport integer)
  RETURNS VOID
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$master_make_data_node$$;
COMMENT ON FUNCTION pg_catalog.master_make_data_node(nodename text,
                                                     nodeport integer)
  IS 'make node a node that stores distributed data';

REVOKE ALL ON FUNCTION pg_catalog.master_make_data_node(text,int) FROM PUBLIC;

CREATE FUNCTION pg_catalog.master_mark_node_for_draining(nodename text,
                                                         nodeport integer)
  RETURNS VOID
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$master_mark_node_for_draining$$;
COMMENT ON FUNCTION pg_catalog.master_mark_node_for_draining(nodename text,
                                                             nodeport integer)
  IS 'mark a node to be drained of data';

REVOKE ALL ON FUNCTION pg_catalog.master_mark_node_for_draining(text,int) FROM PUBLIC;


#include "udfs/master_drain_node/9.1-1.sql"
