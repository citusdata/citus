-- ===================================================================
-- test recursive planning functionality on prepared statements
-- ===================================================================
CREATE SCHEMA subquery_prepared_statements;
SELECT run_command_on_workers('CREATE SCHEMA subquery_prepared_statements;');

SET search_path TO subquery_prepared_statements, public;

CREATE TYPE subquery_prepared_statements.xy AS (x int, y int);

SET client_min_messages TO DEBUG1;

PREPARE subquery_prepare_without_param AS 
SELECT
   DISTINCT values_of_subquery
FROM
    (SELECT 
      DISTINCT (users_table.user_id, events_table.event_type)::xy  as values_of_subquery
     FROM 
      users_table, events_table 
     WHERE 
      users_table.user_id = events_table.user_id AND 
     event_type IN (1,2,3,4)
     ORDER BY 1 DESC LIMIT 5
     ) as foo
    ORDER BY 1 DESC;

PREPARE subquery_prepare_param_on_partkey(int) AS 
SELECT
   DISTINCT values_of_subquery
FROM
    (SELECT 
      DISTINCT (users_table.user_id, events_table.event_type)::xy  as values_of_subquery
     FROM 
      users_table, events_table 
     WHERE 
      users_table.user_id = events_table.user_id AND
      (users_table.user_id = $1 OR users_table.user_id = 2) AND
     event_type IN (1,2,3,4)
     ORDER BY 1 DESC LIMIT 5
     ) as foo
    ORDER BY 1 DESC;

PREPARE subquery_prepare_param_non_partkey(int) AS 
SELECT
   DISTINCT values_of_subquery
FROM
    (SELECT 
      DISTINCT (users_table.user_id, events_table.event_type)::xy  as values_of_subquery
     FROM 
      users_table, events_table 
     WHERE 
      users_table.user_id = events_table.user_id AND 
     event_type = $1
     ORDER BY 1 DESC LIMIT 5
     ) as foo
    ORDER BY 1 DESC;

-- execute each test with 6 times

EXECUTE subquery_prepare_without_param;
EXECUTE subquery_prepare_without_param;
EXECUTE subquery_prepare_without_param;
EXECUTE subquery_prepare_without_param;
EXECUTE subquery_prepare_without_param;
EXECUTE subquery_prepare_without_param;
EXECUTE subquery_prepare_without_param;


EXECUTE subquery_prepare_param_on_partkey(1);
EXECUTE subquery_prepare_param_on_partkey(1);
EXECUTE subquery_prepare_param_on_partkey(1);
EXECUTE subquery_prepare_param_on_partkey(1);
EXECUTE subquery_prepare_param_on_partkey(1);
EXECUTE subquery_prepare_param_on_partkey(1);

EXECUTE subquery_prepare_param_non_partkey(1);
EXECUTE subquery_prepare_param_non_partkey(1);
EXECUTE subquery_prepare_param_non_partkey(1);
EXECUTE subquery_prepare_param_non_partkey(1);
EXECUTE subquery_prepare_param_non_partkey(1);
EXECUTE subquery_prepare_param_non_partkey(1);


SET client_min_messages TO DEFAULT;

DROP SCHEMA subquery_prepared_statements CASCADE;
SET search_path TO public;
