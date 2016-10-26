/* citus--6.0-16--6.0-17.sql */

SET search_path = 'pg_catalog';

DROP FUNCTION pg_catalog.master_copy_shard_placement(bigint, text, integer, text, integer);

CREATE FUNCTION pg_catalog.master_copy_shard_placement(shard_id bigint,
                                                                                                           source_node_name text,
                                                                                                           source_node_port integer,
                                                                                                           target_node_name text,
                                                                                                           target_node_port integer,
                                                                                                           do_repair bool DEFAULT true)
    RETURNS void
    LANGUAGE C STRICT
    AS 'citus', $$master_copy_shard_placement$$;
COMMENT ON FUNCTION pg_catalog.master_copy_shard_placement(shard_id bigint,
                                                                                                           source_node_name text,
                                                                                                           source_node_port integer,
                                                                                                           target_node_name text,
                                                                                                           target_node_port integer,
                                                                                                           do_repair bool)
    IS 'copy shard from remote node';

RESET search_path;
