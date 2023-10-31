CREATE ROLE role1;
CREATE ROLE role2;

GRANT CREATE ON SCHEMA public TO role1;

SET ROLE role1;
CREATE TABLE public.test_table (col1 int);
RESET ROLE;
select create_distributed_table('test_table', 'col1');

SELECT result from run_command_on_all_nodes(
  $$
  SELECT jsonb_agg(to_jsonb(q2.*)) FROM (
   SELECT
    schemaname,
    tablename,
    tableowner
FROM
    pg_tables
WHERE
    tablename = 'test_table'
  ) q2
  $$
) ORDER BY result;

set citus.log_remote_commands to on;
set citus.grep_remote_commands = '%REASSIGN OWNED BY%';
REASSIGN OWNED BY role1 TO role2;


SET citus.log_remote_commands = false;
SELECT result from run_command_on_all_nodes(
  $$
  SELECT jsonb_agg(to_jsonb(q2.*)) FROM (
   SELECT
    schemaname,
    tablename,
    tableowner
FROM
    pg_tables
WHERE
    tablename = 'test_table'
  ) q2
  $$
) ORDER BY result;



SET citus.log_remote_commands = true;
DROP OWNED BY role1, role2;

SET citus.log_remote_commands = false;
SELECT result from run_command_on_all_nodes(
  $$
  SELECT jsonb_agg(to_jsonb(q2.*)) FROM (
   SELECT
    schemaname,
    tablename,
    tableowner
FROM
    pg_tables
WHERE
    tablename = 'test_table'
  ) q2
  $$
) ORDER BY result;

SET citus.log_remote_commands = true;
set citus.grep_remote_commands = '%DROP ROLE%';
drop role role1, role2 ;
