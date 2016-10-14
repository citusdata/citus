/*
 * citus_tools.sql
 * Contains definitions of citus_tools UDFs
 * - citus_run_on_all_workers
 * - citus_run_on_all_placements
 * - citus_run_on_all_colocated_placements
 * - citus_run_on_all_shards
 *
 * These functions depends on presence of UDF master_run_on_worker
 */

CREATE OR REPLACE FUNCTION master_run_on_worker(worker_name text[], port integer[],
												command text[], parallel boolean,
												OUT node_name text, OUT node_port integer,
												OUT success boolean, OUT result text )
	RETURNS SETOF record
	LANGUAGE C STABLE STRICT
	AS 'citus.so', $$master_run_on_worker$$;


--
-- tables_colocated returns true if given tables are co-located, false otherwise.
-- The function only checks shard definitions, it does not check placement matching.
--
CREATE OR REPLACE FUNCTION citus_tables_colocated(table1 regclass, table2 regclass)
    RETURNS bool
    LANGUAGE plpgsql
    AS $function$
DECLARE
	colocated_shard_count int;
	table1_shard_count int;
	table2_shard_count int;
	table1_placement_count int;
	table2_placement_count int;
BEGIN
	SELECT count(*),
	    (SELECT count(*) FROM pg_dist_shard a WHERE a.logicalrelid = table1),
	    (SELECT count(*) FROM pg_dist_shard b WHERE b.logicalrelid = table2)
	INTO colocated_shard_count, table1_shard_count, table2_shard_count
	FROM pg_dist_shard tba JOIN pg_dist_shard tbb USING(shardminvalue, shardmaxvalue)
	WHERE tba.logicalrelid = table1 AND tbb.logicalrelid = table2;

	IF (table1_shard_count != table2_shard_count OR
		table1_shard_count != colocated_shard_count)
	THEN
		RETURN false;
	END IF;

	SELECT count(*) 
	INTO table2_placement_count
	FROM pg_dist_shard ds JOIN pg_dist_shard_placement sp USING (shardid)
	WHERE ds.logicalrelid = table1 AND sp.shardstate = 1;
	
	SELECT count(*)
	INTO table1_placement_count
	FROM pg_dist_shard ds JOIN pg_dist_shard_placement sp USING (shardid)
	WHERE ds.logicalrelid = table2 AND sp.shardstate = 1;
	
	IF (table1_placement_count != table2_placement_count) THEN
		RETURN false;
	END IF;
	
	RETURN true;
END;
$function$;

CREATE OR REPLACE FUNCTION citus_run_on_all_workers(command text,
													parallel bool default true,
													OUT nodename text,
													OUT nodeport int,
													OUT success bool,
													OUT result text)
	RETURNS SETOF record
	LANGUAGE plpgsql
	AS $function$
DECLARE
	workers text[];
	ports int[];
	commands text[];
BEGIN
	WITH citus_workers AS (
		SELECT * FROM master_get_active_worker_nodes() ORDER BY node_name, node_port)
	SELECT array_agg(node_name), array_agg(node_port), array_agg(command)
	INTO workers, ports, commands
	FROM citus_workers;

	RETURN QUERY SELECT * FROM master_run_on_worker(workers, ports, commands, parallel);
END;
$function$;


CREATE OR REPLACE FUNCTION citus_run_on_all_placements(table_name regclass, command text,
													   parallel bool default true,
													   OUT nodename text,
													   OUT nodeport int,
													   OUT shardid bigint,
													   OUT success bool, OUT result text)
	RETURNS SETOF record
	LANGUAGE plpgsql
	AS $function$
DECLARE
	workers text[];
	ports int[];
	shards bigint[];
	commands text[];
BEGIN
	WITH citus_placements AS (
		SELECT
			ds.logicalrelid::regclass AS tablename,
			ds.shardid AS shardid,
			shard_name(ds.logicalrelid, ds.shardid) AS shardname,
			dsp.nodename AS nodename, dsp.nodeport::int AS nodeport
		FROM pg_dist_shard ds JOIN pg_dist_shard_placement dsp USING (shardid)
		WHERE dsp.shardstate = 1 and ds.logicalrelid::regclass = table_name
		ORDER BY ds.logicalrelid, ds.shardid, dsp.nodename, dsp.nodeport)
	SELECT
		array_agg(cp.nodename), array_agg(cp.nodeport), array_agg(cp.shardid),
		array_agg(format(command, cp.shardname))
	INTO workers, ports, shards, commands
	FROM citus_placements cp;

	RETURN QUERY
		SELECT r.node_name, r.node_port, shards[ordinality],
			r.success, r.result
		FROM master_run_on_worker(workers, ports, commands, parallel) WITH ORDINALITY r;
END;
$function$;


CREATE OR REPLACE FUNCTION citus_run_on_all_colocated_placements(table_name1 regclass,
																 table_name2 regclass,
																 command text,
																 parallel bool default true,
																 OUT nodename text,
																 OUT nodeport int,
																 OUT shardid1 bigint,
																 OUT shardid2 bigint,
																 OUT success bool,
																 OUT result text)
	RETURNS SETOF record
	LANGUAGE plpgsql
	AS $function$
DECLARE
	workers text[];
	ports int[];
	shards1 bigint[];
	shards2 bigint[];
	commands text[];
BEGIN
	IF NOT (SELECT citus_tables_colocated(table_name1, table_name2)) THEN
		RAISE EXCEPTION 'tables % and % are not co-located', table_name1, table_name2;
	END IF;

	WITH active_shard_placements AS (
		SELECT
			ds.logicalrelid,
			ds.shardid AS shardid,
			shard_name(ds.logicalrelid, ds.shardid) AS shardname,
			ds.shardminvalue AS shardminvalue,
			ds.shardmaxvalue AS shardmaxvalue,
			dsp.nodename AS nodename,
			dsp.nodeport::int AS nodeport
		FROM pg_dist_shard ds JOIN pg_dist_shard_placement dsp USING (shardid)
		WHERE dsp.shardstate = 1 and (ds.logicalrelid::regclass = table_name1 or
			ds.logicalrelid::regclass = table_name2)
		ORDER BY ds.logicalrelid, ds.shardid, dsp.nodename, dsp.nodeport),
	citus_colocated_placements AS (
		SELECT
			a.logicalrelid::regclass AS tablename1,
			a.shardid AS shardid1,
			shard_name(a.logicalrelid, a.shardid) AS shardname1,
			b.logicalrelid::regclass AS tablename2,
			b.shardid AS shardid2,
			shard_name(b.logicalrelid, b.shardid) AS shardname2,
			a.nodename AS nodename,
			a.nodeport::int AS nodeport
		FROM
			active_shard_placements a, active_shard_placements b
		WHERE
			a.shardminvalue = b.shardminvalue AND
			a.shardmaxvalue = b.shardmaxvalue AND
			a.logicalrelid != b.logicalrelid AND
			a.nodename = b.nodename AND
			a.nodeport = b.nodeport AND
			a.logicalrelid::regclass = table_name1 AND
			b.logicalrelid::regclass = table_name2
		ORDER BY a.logicalrelid, a.shardid, nodename, nodeport)
	SELECT
		array_agg(cp.nodename), array_agg(cp.nodeport), array_agg(cp.shardid1),
		array_agg(cp.shardid2), array_agg(format(command, cp.shardname1, cp.shardname2))
	INTO workers, ports, shards1, shards2, commands
  	FROM citus_colocated_placements cp;

	RETURN QUERY SELECT r.node_name, r.node_port, shards1[ordinality],
		shards2[ordinality], r.success, r.result
	FROM master_run_on_worker(workers, ports, commands, parallel) WITH ORDINALITY r;
END;
$function$;


CREATE OR REPLACE FUNCTION citus_run_on_all_shards(table_name regclass, command text,
												   parallel bool default true,
												   OUT shardid bigint,
												   OUT success bool,
												   OUT result text)
	RETURNS SETOF record
	LANGUAGE plpgsql
	AS $function$
DECLARE
	workers text[];
	ports int[];
	shards bigint[];
	commands text[];
	shard_count int;
BEGIN
	SELECT COUNT(*) INTO shard_count FROM pg_dist_shard
	WHERE logicalrelid = table_name;

	WITH citus_shards AS (
		SELECT ds.logicalrelid::regclass AS tablename,
			ds.shardid AS shardid,
			shard_name(ds.logicalrelid, ds.shardid) AS shardname,
			array_agg(dsp.nodename) AS nodenames,
			array_agg(dsp.nodeport) AS nodeports
		FROM pg_dist_shard ds LEFT JOIN pg_dist_shard_placement dsp USING (shardid)
		WHERE dsp.shardstate = 1 and ds.logicalrelid::regclass = table_name
		GROUP BY ds.logicalrelid, ds.shardid
		ORDER BY ds.logicalrelid, ds.shardid)
	SELECT
		array_agg(cs.nodenames[1]), array_agg(cs.nodeports[1]), array_agg(cs.shardid),
		array_agg(format(command, cs.shardname))
	INTO workers, ports, shards, commands
	FROM citus_shards cs;

	IF (shard_count != array_length(workers, 1)) THEN
		RAISE NOTICE 'some shards do  not have active placements';
	END IF;

	RETURN QUERY
		SELECT shards[ordinality], r.success, r.result
		FROM master_run_on_worker(workers, ports, commands, parallel) WITH ORDINALITY r;
END;
$function$;
