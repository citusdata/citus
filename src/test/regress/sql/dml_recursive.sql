CREATE SCHEMA recursive_dml_queries;
SET search_path TO recursive_dml_queries, public;
SET citus.next_shard_id TO 2370000;

CREATE TABLE recursive_dml_queries.distributed_table (tenant_id text, dept int, info jsonb);
SELECT create_distributed_table('distributed_table', 'tenant_id');

CREATE TABLE recursive_dml_queries.second_distributed_table (tenant_id text, dept int, info jsonb);
SELECT create_distributed_table('second_distributed_table', 'tenant_id');

CREATE TABLE recursive_dml_queries.reference_table (id text, name text);
SELECT create_reference_table('reference_table');

CREATE TABLE recursive_dml_queries.local_table (id text, name text);

INSERT INTO distributed_table SELECT i::text, i % 10, row_to_json(row(i, i*i)) FROM generate_series (0, 100) i;
INSERT INTO second_distributed_table SELECT i::text, i % 10, row_to_json(row(i, i*i)) FROM generate_series (0, 100) i;
INSERT INTO reference_table SELECT i::text, 'user_' || i FROM generate_series (0, 100) i;
INSERT INTO local_table SELECT i::text, 'user_' || i FROM generate_series (0, 100) i;

CREATE VIEW tenant_ids AS
	SELECT
		tenant_id, name
	FROM
		distributed_table, reference_table
	WHERE
		distributed_table.dept::text = reference_table.id
	ORDER BY 2 DESC, 1 DESC;

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
	foo.avg_tenant_id::int::text = reference_table.id
RETURNING
	reference_table.name;

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
	AND second_distributed_table.dept IN (2)
RETURNING
	second_distributed_table.tenant_id, second_distributed_table.dept;

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

-- there is a lateral join (e.g., corrolated subquery) thus the subqueries cannot be
-- recursively planned, however it can be planned using the repartition planner
SET citus.enable_repartition_joins to on;
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
)
foo_inner_1 JOIN LATERAL
(
    SELECT
        second_distributed_table.tenant_id
    FROM
        second_distributed_table, distributed_table
    WHERE
        distributed_table.tenant_id = second_distributed_table.tenant_id
        AND foo_inner_1.dept = second_distributed_table.dept
    AND
        second_distributed_table.dept IN (4,5)
) foo_inner_2
ON (foo_inner_2.tenant_id != foo_inner_1.tenant_id)
ORDER BY foo_inner_1.tenant_id;
RESET citus.enable_repartition_joins;


-- there is a lateral join (e.g., corrolated subquery) thus the subqueries cannot be
-- recursively planned, this one can not be planned by the repartion planner
-- because of the IN query on a non unique column
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
			second_distributed_table.dept IN (select dept from second_distributed_table))
	foo_inner_1 JOIN LATERAL
	(
		SELECT
			second_distributed_table.tenant_id
		FROM
			second_distributed_table, distributed_table
		WHERE
			distributed_table.tenant_id = second_distributed_table.tenant_id
			AND foo_inner_1.dept = second_distributed_table.dept
		AND
			second_distributed_table.dept IN (4,5)
	) foo_inner_2
	ON (foo_inner_2.tenant_id != foo_inner_1.tenant_id)
	) as foo
RETURNING *;


-- again a corrolated subquery
-- this time distribution key eq. exists
-- however recursive planning is prevented due to correlated subqueries
UPDATE
	second_distributed_table
SET
	dept = foo.tenant_id::int / 4
FROM
(
	SELECT baz.tenant_id FROM
	(
		SELECT
			second_distributed_table.dept, second_distributed_table.tenant_id
		FROM
			second_distributed_table, distributed_table as d1
		WHERE
			d1.tenant_id = second_distributed_table.tenant_id
		AND
			second_distributed_table.dept IN (3,4)
			AND
			second_distributed_table.tenant_id IN
			(
					SELECT s2.tenant_id
					FROM second_distributed_table as s2
					GROUP BY d1.tenant_id, s2.tenant_id
			)
	) as baz
	) as foo WHERE second_distributed_table.tenant_id = foo.tenant_id
RETURNING *;

-- we don't support subqueries/CTEs inside VALUES
INSERT INTO
	second_distributed_table (tenant_id, dept)
VALUES ('3', (WITH  vals AS (SELECT 3) select * from vals));

INSERT INTO
	second_distributed_table (tenant_id, dept)
VALUES ('3', (SELECT 3));

-- DML with an unreferenced SELECT CTE
WITH cte_1 AS (
    WITH cte_2 AS (
        SELECT tenant_id as cte2_id
        FROM second_distributed_table
        WHERE dept >= 2
    )

    UPDATE distributed_table
    SET dept = 10
    RETURNING *
)
UPDATE distributed_table
SET dept = 5
FROM cte_1
WHERE distributed_table.tenant_id < cte_1.tenant_id;

WITH cte_1 AS (
    WITH cte_2 AS (
        SELECT tenant_id as cte2_id
        FROM second_distributed_table
        WHERE dept >= 2
    )

    UPDATE distributed_table
    SET dept = 10
    RETURNING *
)
UPDATE distributed_table
SET dept = 5
FROM cte_1
WHERE distributed_table.tenant_id < cte_1.tenant_id;

-- we don't support updating local table with a join with
-- distributed tables
UPDATE
	local_table
SET
	id = 'citus_test'
FROM
	distributed_table
WHERE
	distributed_table.tenant_id = local_table.id;

RESET client_min_messages;
DROP SCHEMA recursive_dml_queries CASCADE;
