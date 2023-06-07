CREATE SCHEMA with_dml;
SET search_path TO 	with_dml, public;

CREATE TABLE with_dml.distributed_table (tenant_id text PRIMARY KEY, dept int);
SELECT create_distributed_table('distributed_table', 'tenant_id');


CREATE TABLE with_dml.second_distributed_table (tenant_id text, dept int);
SELECT create_distributed_table('second_distributed_table', 'tenant_id');

CREATE TABLE with_dml.reference_table (id text, name text);
SELECT create_reference_table('reference_table');

INSERT INTO distributed_table SELECT i::text, i % 10 FROM generate_series (0, 100) i;
INSERT INTO second_distributed_table SELECT i::text, i % 10 FROM generate_series (0, 100) i;
INSERT INTO reference_table SELECT i::text, 'user_' || i FROM generate_series (0, 100) i;

SET client_min_messages TO DEBUG1;

-- delete all tenants from the reference table whose dept is 1
WITH ids_to_delete AS (
  SELECT tenant_id FROM distributed_table WHERE dept = 1
)
DELETE FROM reference_table WHERE id IN (SELECT tenant_id FROM ids_to_delete);

-- update the name of the users whose dept is 2
WITH ids_to_update AS (
  SELECT tenant_id FROM distributed_table WHERE dept = 2
)
UPDATE reference_table SET name = 'new_' || name WHERE id IN (SELECT tenant_id FROM ids_to_update);

-- now the CTE is also modifying
WITH ids_deleted_3 AS
(
	DELETE FROM distributed_table WHERE dept = 3 RETURNING tenant_id
),
ids_deleted_4 AS
(
	DELETE FROM distributed_table WHERE dept = 4 RETURNING tenant_id
)
DELETE FROM reference_table WHERE id IN (SELECT * FROM ids_deleted_3 UNION SELECT * FROM ids_deleted_4);

-- now the final UPDATE command is pushdownable
WITH ids_to_delete AS
(
	SELECT tenant_id FROM distributed_table WHERE dept = 5
)
UPDATE
	distributed_table
SET
	dept = dept + 1
FROM
	ids_to_delete, (SELECT tenant_id FROM distributed_table WHERE tenant_id::int < 60) as some_tenants
WHERE
	some_tenants.tenant_id = ids_to_delete.tenant_id
	AND distributed_table.tenant_id = some_tenants.tenant_id
	AND EXISTS (SELECT * FROM ids_to_delete);

SET client_min_messages TO WARNING;

-- this query falls back repartitioned insert/select since we've some hard
-- errors in the INSERT ... SELECT pushdown which prevents to fallback to
-- recursive planning
SELECT * FROM
coordinator_plan($Q$
EXPLAIN (costs off)
WITH ids_to_upsert AS
(
	SELECT tenant_id FROM distributed_table WHERE dept > 7
)
INSERT INTO distributed_table
       SELECT distributed_table.tenant_id FROM ids_to_upsert, distributed_table
       		WHERE  distributed_table.tenant_id = ids_to_upsert.tenant_id
       	ON CONFLICT (tenant_id) DO UPDATE SET dept = 8;
$Q$) s
WHERE s LIKE '%INSERT/SELECT method%';

SET client_min_messages TO DEBUG1;

-- the following query is very similar to the above one
-- but this time the query is pulled to coordinator since
-- we return before hitting any hard errors
WITH ids_to_insert AS
(
	SELECT (tenant_id::int * 100)::text as tenant_id FROM distributed_table WHERE dept > 7
)
INSERT INTO distributed_table
       SELECT DISTINCT ids_to_insert.tenant_id FROM ids_to_insert, distributed_table
       		WHERE  distributed_table.tenant_id < ids_to_insert.tenant_id;


-- not a very meaningful query
-- but has two modifying CTEs along with another
-- modify statement

-- We need to force 1 connection per placement
-- otherwise the coordinator insert select fails
-- since COPY cannot be executed
SET citus.force_max_query_parallelization TO on;
-- We are reducing the log level here to avoid alternative test output
-- in PG15 because of the change in the display of SQL-standard
-- function's arguments in INSERT/SELECT in PG15.
-- The log level changes can be reverted when we drop support for PG14
SET client_min_messages TO LOG;
WITH copy_to_other_table AS (
    INSERT INTO distributed_table
        SELECT *
            FROM second_distributed_table
        WHERE dept = 3
        ON CONFLICT (tenant_id) DO UPDATE SET dept = 4
        RETURNING *
),
main_table_deleted AS (
    DELETE
    FROM distributed_table
    WHERE dept < 10
      AND NOT EXISTS (SELECT 1 FROM second_distributed_table
                      WHERE second_distributed_table.dept = 1
                        AND second_distributed_table.tenant_id = distributed_table.tenant_id)
                        RETURNING *
)
INSERT INTO second_distributed_table
        SELECT *
            FROM main_table_deleted
        EXCEPT
        SELECT *
            FROM copy_to_other_table;

SET citus.force_max_query_parallelization TO off;
SET client_min_messages TO DEBUG1;

-- CTE inside the UPDATE statement
UPDATE
	second_distributed_table
SET dept =
		(WITH  vals AS (
		SELECT DISTINCT tenant_id::int FROM distributed_table
		) select * from vals where tenant_id = 8 )
		WHERE dept = 8;

-- Subquery inside the UPDATE statement
UPDATE
	second_distributed_table
SET dept =

		(SELECT DISTINCT tenant_id::int FROM distributed_table WHERE tenant_id = '9')
		WHERE dept = 8;

-- delete all remaining tenants
WITH ids_to_delete AS (
  SELECT tenant_id FROM distributed_table
)
DELETE FROM distributed_table WHERE tenant_id = ANY(SELECT tenant_id FROM ids_to_delete);

WITH ids_to_delete AS (
  SELECT id FROM reference_table
)
DELETE FROM reference_table WHERE id = ANY(SELECT id FROM ids_to_delete);

SET client_min_messages TO WARNING;
DROP SCHEMA with_dml CASCADE;
