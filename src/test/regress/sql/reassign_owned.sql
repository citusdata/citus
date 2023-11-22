CREATE ROLE role1;

create ROLE role2;
CREATE ROLE role3;

GRANT CREATE ON SCHEMA public TO role1,role2;

SET ROLE role1;
CREATE TABLE public.test_table (col1 int);

set role role2;
CREATE TABLE public.test_table2 (col2 int);
RESET ROLE;
select create_distributed_table('test_table', 'col1');
select create_distributed_table('test_table2', 'col2');


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
    tablename in ('test_table', 'test_table2')
  ) q2
  $$
) ORDER BY result;

set citus.log_remote_commands to on;
set citus.grep_remote_commands = '%REASSIGN OWNED BY%';
REASSIGN OWNED BY role1,role2 TO role3;


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
    tablename in ('test_table', 'test_table2')
  ) q2
  $$
) ORDER BY result;


--clear resources
SET citus.log_remote_commands = true;
DROP OWNED BY role1, role2,role3;

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
    tablename in ('test_table', 'test_table2')
  ) q2
  $$
) ORDER BY result;



SET citus.log_remote_commands = true;
set citus.grep_remote_commands = '%DROP ROLE%';
drop role role1, role2,role3 ;
