--
-- MULTI_REPARTITIONED_SUBQUERY_UDF
--


SET citus.next_shard_id TO 830000;


-- Create UDF in master and workers
\c - - :master_host :master_port
DROP FUNCTION IF EXISTS median(double precision[]);

CREATE FUNCTION median(double precision[]) RETURNS double precision
LANGUAGE sql IMMUTABLE AS $_$
	SELECT AVG(val) FROM
	  (SELECT val FROM unnest($1) val
	   ORDER BY 1 LIMIT  2 - MOD(array_upper($1, 1), 2)
	   OFFSET CEIL(array_upper($1, 1) / 2.0) - 1) sub;
$_$;

\c - - :public_worker_1_host :worker_1_port
DROP FUNCTION IF EXISTS median(double precision[]);

CREATE FUNCTION median(double precision[]) RETURNS double precision
LANGUAGE sql IMMUTABLE AS $_$
	SELECT AVG(val) FROM
	  (SELECT val FROM unnest($1) val
	   ORDER BY 1 LIMIT  2 - MOD(array_upper($1, 1), 2)
	   OFFSET CEIL(array_upper($1, 1) / 2.0) - 1) sub;
$_$;

\c - - :public_worker_2_host :worker_2_port
DROP FUNCTION IF EXISTS median(double precision[]);

CREATE FUNCTION median(double precision[]) RETURNS double precision
LANGUAGE sql IMMUTABLE AS $_$
	SELECT AVG(val) FROM
	  (SELECT val FROM unnest($1) val
	   ORDER BY 1 LIMIT  2 - MOD(array_upper($1, 1), 2)
	   OFFSET CEIL(array_upper($1, 1) / 2.0) - 1) sub;
$_$;

-- Run query on master
\c - - :master_host :master_port

SET citus.task_executor_type TO 'task-tracker';

SELECT * FROM (SELECT median(ARRAY[1,2,sum(l_suppkey)]) as median, count(*)
    	  FROM lineitem GROUP BY l_partkey) AS a
  WHERE median > 2;
