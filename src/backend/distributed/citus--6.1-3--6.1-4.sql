/* citus--6.1-3--6.1-4.sql */

SET search_path = 'pg_catalog';

ALTER TABLE pg_dist_colocation ADD COLUMN defaultgroup BOOLEAN;

UPDATE pg_dist_colocation SET defaultgroup = TRUE;

DROP INDEX pg_dist_colocation_configuration_index;

CREATE INDEX pg_dist_colocation_configuration_index
ON pg_dist_colocation USING btree(shardcount, replicationfactor, distributioncolumntype, defaultgroup);

DROP FUNCTION create_distributed_table(regclass, text, citus.distribution_type);

CREATE FUNCTION create_distributed_table(table_name regclass,
										 distribution_column text,
										 distribution_type citus.distribution_type DEFAULT 'hash',
										 colocate_with text DEFAULT 'default')
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$create_distributed_table$$;
COMMENT ON FUNCTION create_distributed_table(table_name regclass,
											 distribution_column text,
											 distribution_type citus.distribution_type,
											 colocate_with text)
    IS 'creates a distributed table';

CREATE FUNCTION mark_colocation_group_default(colocation_id integer)
	RETURNS void
	LANGUAGE plpgsql
	SET search_path = pg_catalog
	AS $$
DECLARE
	shard_count integer;
	replication_factor integer;
	distribution_column_type oid;
BEGIN
	-- get colocation group configuration
	SELECT
		shardcount,
		replicationfactor,
		distributioncolumntype
	INTO
		shard_count,
		replication_factor,
		distribution_column_type
	FROM
		pg_dist_colocation
	WHERE
		colocationid = colocation_id;

	-- set all defaults to false
	UPDATE
		pg_dist_colocation
	SET
		defaultgroup = false
	WHERE
		shardcount = shard_count AND
		replicationfactor = replication_factor AND
		distributioncolumntype = distribution_column_type;

	-- set new default colocation group
	UPDATE
		pg_dist_colocation
	SET
		defaultgroup = true
	WHERE
		colocationid = colocation_id;
END;
$$;

COMMENT ON FUNCTION mark_colocation_group_default(colocation_id integer)
    IS 'marks given colocation group as default';

RESET search_path;
