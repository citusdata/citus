CREATE ROLE distributed_source_role1;
create ROLE "distributed_source_role-\!";

CREATE ROLE "distributed_target_role1-\!";

set citus.enable_create_role_propagation to off;
create ROLE local_target_role1;


\c - - - :worker_1_port
set citus.enable_create_role_propagation to off;
CREATE ROLE local_target_role1;

\c - - - :master_port
set citus.enable_create_role_propagation to off;
create role local_source_role1;
reset citus.enable_create_role_propagation;

GRANT CREATE ON SCHEMA public TO distributed_source_role1,"distributed_source_role-\!";

SET ROLE distributed_source_role1;
CREATE TABLE public.test_table (col1 int);

set role "distributed_source_role-\!";
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
   ORDER BY tablename
  ) q2
  $$
) ORDER BY result;

--tests for reassing owned by with multiple distributed roles and a local role to a distributed role
--local role should be ignored
set citus.log_remote_commands to on;
set citus.grep_remote_commands = '%REASSIGN OWNED BY%';
REASSIGN OWNED BY distributed_source_role1,"distributed_source_role-\!",local_source_role1  TO "distributed_target_role1-\!";
reset citus.grep_remote_commands;
reset citus.log_remote_commands;

--check if the owner changed to "distributed_target_role1-\!"

RESET citus.log_remote_commands;
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
   ORDER BY tablename
  ) q2
  $$
) ORDER BY result;

--tests for reassing owned by with multiple distributed roles and a local role to a local role
--local role should be ignored
SET ROLE distributed_source_role1;
CREATE TABLE public.test_table3 (col1 int);

set role "distributed_source_role-\!";
CREATE TABLE public.test_table4 (col2 int);
RESET ROLE;
select create_distributed_table('test_table3', 'col1');
select create_distributed_table('test_table4', 'col2');

set citus.log_remote_commands to on;
set citus.grep_remote_commands = '%REASSIGN OWNED BY%';
set citus.enable_create_role_propagation to off;
set citus.enable_alter_role_propagation to off;
set citus.enable_alter_role_set_propagation to off;
REASSIGN OWNED BY distributed_source_role1,"distributed_source_role-\!",local_source_role1 TO local_target_role1;

show citus.enable_create_role_propagation;
show citus.enable_alter_role_propagation;
show citus.enable_alter_role_set_propagation;

reset citus.grep_remote_commands;
reset citus.log_remote_commands;
reset citus.enable_create_role_propagation;
reset citus.enable_alter_role_propagation;
reset citus.enable_alter_role_set_propagation;


--check if the owner changed to local_target_role1
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
        tablename in ('test_table3', 'test_table4')
   ORDER BY tablename
  ) q2
  $$
) ORDER BY result;

--clear resources
DROP OWNED BY distributed_source_role1, "distributed_source_role-\!","distributed_target_role1-\!",local_target_role1;

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
    tablename in ('test_table', 'test_table2', 'test_table3', 'test_table4')
  ) q2
  $$
) ORDER BY result;


set client_min_messages to warning;
drop role distributed_source_role1, "distributed_source_role-\!","distributed_target_role1-\!",local_target_role1,local_source_role1;
