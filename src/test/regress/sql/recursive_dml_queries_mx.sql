CREATE SCHEMA recursive_dml_queries_mx;
SET search_path TO recursive_dml_queries_mx, public;

SET citus.shard_replication_factor TO 1;
SET citus.replication_model TO streaming;

CREATE TABLE recursive_dml_queries_mx.distributed_table (tenant_id text, dept int, info jsonb);
SELECT create_distributed_table('distributed_table', 'tenant_id');

CREATE TABLE recursive_dml_queries_mx.second_distributed_table (tenant_id text, dept int, info jsonb);
SELECT create_distributed_table('second_distributed_table', 'tenant_id');

CREATE TABLE recursive_dml_queries_mx.reference_table (id text, name text);
SELECT create_reference_table('reference_table');

INSERT INTO distributed_table SELECT i::text, i % 10, row_to_json(row(i, i*i)) FROM generate_series (0, 100) i;
INSERT INTO second_distributed_table SELECT i::text, i % 10, row_to_json(row(i, i*i)) FROM generate_series (0, 100) i;
INSERT INTO reference_table SELECT i::text, 'user_' || i FROM generate_series (0, 100) i;

SET client_min_messages TO DEBUG1;

-- the subquery foo is recursively planned
UPDATE
	reference_table
SET
	name = 'new_' || name
FROM
(
	SELECT
		avg(second_distributed_table.tenant_id::int) as avg_tenant_id
	FROM
		second_distributed_table
) as foo
WHERE
	foo.avg_tenant_id::int::text = reference_table.id;

-- the subquery foo is recursively planned
-- but note that the subquery foo itself is pushdownable
UPDATE
	second_distributed_table
SET
	dept = foo.max_dept * 2
FROM
(
	SELECT DISTINCT ON (tenant_id) tenant_id, max(dept) as max_dept FROM
	(
		SELECT
			second_distributed_table.dept, second_distributed_table.tenant_id
		FROM
			second_distributed_table, distributed_table
		WHERE
			distributed_table.tenant_id = second_distributed_table.tenant_id
	) foo_inner
	GROUP BY
		tenant_id
	ORDER BY 1 DESC
) as foo
WHERE
	foo.tenant_id != second_distributed_table.tenant_id
	AND second_distributed_table.dept IN (2);

-- run some queries from worker nodes
\c - - :public_worker_1_host :worker_1_port
SET search_path TO recursive_dml_queries_mx, public;

-- the subquery foo is recursively planned
-- and foo itself is a non colocated subquery and recursively planned
UPDATE
	second_distributed_table
SET
	dept = foo.tenant_id::int / 4
FROM
(
	SELECT DISTINCT foo_inner_1.tenant_id FROM
	(
		SELECT
			second_distributed_table.dept, second_distributed_table.tenant_id
		FROM
			second_distributed_table, distributed_table
		WHERE
			distributed_table.tenant_id = second_distributed_table.tenant_id
		AND
			second_distributed_table.dept IN (3,4)
	) foo_inner_1,
	(
		SELECT
			second_distributed_table.tenant_id
		FROM
			second_distributed_table, distributed_table
		WHERE
			distributed_table.tenant_id = second_distributed_table.tenant_id
		AND
			second_distributed_table.dept IN (4,5)
	)foo_inner_2
	WHERE foo_inner_1.tenant_id != foo_inner_2.tenant_id
) as foo
WHERE
	foo.tenant_id != second_distributed_table.tenant_id
	AND second_distributed_table.dept IN (3);

-- use the second worker
\c - - :public_worker_2_host :worker_2_port
SET search_path TO recursive_dml_queries_mx, public;

CREATE TABLE recursive_dml_queries_mx.local_table (id text, name text);
INSERT INTO local_table SELECT i::text, 'user_' || i FROM generate_series (0, 100) i;

CREATE VIEW tenant_ids AS
	SELECT
		tenant_id, name
	FROM
		distributed_table, reference_table
	WHERE
		distributed_table.dept::text = reference_table.id
	ORDER BY 2 DESC, 1 DESC;

-- we currently do not allow local tables in modification queries
UPDATE
	distributed_table
SET
	dept = avg_tenant_id::int
FROM
(
	SELECT
		avg(local_table.id::int) as avg_tenant_id
	FROM
		local_table
) as foo
WHERE
	foo.avg_tenant_id::int::text = distributed_table.tenant_id
RETURNING
	distributed_table.*;

-- we currently do not allow views in modification queries
UPDATE
	distributed_table
SET
	dept = avg_tenant_id::int
FROM
(
	SELECT
		avg(tenant_id::int) as avg_tenant_id
	FROM
		tenant_ids
) as foo
WHERE
	foo.avg_tenant_id::int::text = distributed_table.tenant_id
RETURNING
	distributed_table.*;

DROP TABLE local_table;

\c - - :master_host :master_port
SET search_path TO recursive_dml_queries_mx, public;

RESET client_min_messages;
DROP SCHEMA recursive_dml_queries_mx CASCADE;
RESET citus.shard_replication_factor;
RESET citus.replication_model;
