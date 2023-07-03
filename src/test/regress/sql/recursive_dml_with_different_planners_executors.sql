CREATE SCHEMA recursive_dml_with_different_planner_executors;
SET search_path TO recursive_dml_with_different_planner_executors, public;

CREATE TABLE distributed_table (tenant_id text, dept int, info jsonb);
SELECT create_distributed_table('distributed_table', 'tenant_id');

CREATE TABLE second_distributed_table (tenant_id text, dept int, info jsonb);
SELECT create_distributed_table('second_distributed_table', 'tenant_id');

CREATE TABLE reference_table (id text, name text);
SELECT create_reference_table('reference_table');

INSERT INTO distributed_table SELECT i::text, i % 10, row_to_json(row(i, i*i)) FROM generate_series (0, 100) i;
INSERT INTO second_distributed_table SELECT i::text, i % 10, row_to_json(row(i, i*i)) FROM generate_series (0, 100) i;

SET client_min_messages TO DEBUG1;

-- subquery with router planner
-- joined with a real-time query
UPDATE
	distributed_table
SET dept = foo.dept FROM
	(SELECT tenant_id, dept FROM second_distributed_table WHERE dept = 1 ) as foo,
	(SELECT tenant_id FROM second_distributed_table WHERE dept IN (1, 2, 3, 4) OFFSET 0) as bar
	WHERE foo.tenant_id = bar.tenant_id
	AND distributed_table.tenant_id = bar.tenant_id;

-- a non colocated subquery inside the UPDATE
UPDATE distributed_table SET dept = foo.max_dept FROM
(
	SELECT
		max(dept) as max_dept
	FROM
		(SELECT DISTINCT tenant_id, dept FROM distributed_table) as distributed_table
	WHERE tenant_id NOT IN
				(SELECT tenant_id FROM second_distributed_table WHERE dept IN (1, 2, 3, 4))
) as  foo WHERE foo.max_dept > dept * 3;


-- subquery with repartition query
SET citus.enable_repartition_joins to ON;

UPDATE distributed_table SET dept = foo.some_tenants::int FROM
(
	SELECT
	 	DISTINCT second_distributed_table.tenant_id as some_tenants
	 FROM second_distributed_table, distributed_table WHERE second_distributed_table.dept = distributed_table.dept
) as foo;

SET citus.enable_repartition_joins to OFF;

-- final query is router
UPDATE distributed_table SET dept = foo.max_dept FROM
(
	SELECT
		max(dept) as max_dept
	FROM
		(SELECT DISTINCT tenant_id, dept FROM distributed_table) as distributed_table
	WHERE tenant_id IN
				(SELECT tenant_id FROM second_distributed_table WHERE dept IN (1, 2, 3, 4))
) as  foo WHERE foo.max_dept >= dept and tenant_id = '8';

SET client_min_messages TO WARNING;
DROP SCHEMA recursive_dml_with_different_planner_executors CASCADE;
SET search_path TO public;
