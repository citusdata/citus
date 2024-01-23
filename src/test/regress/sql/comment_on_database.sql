set citus.log_remote_commands to on;

set citus.enable_create_database_propagation to on;
set citus.grep_remote_commands to 'COMMENT ON DATABASE';

create database "test1-\!escape";

comment on DATABASE "test1-\!escape" is 'test-comment';

SELECT result FROM run_command_on_all_nodes(
  $$
    SELECT ds.description AS database_comment
    FROM pg_database d
    LEFT JOIN pg_shdescription ds ON d.oid = ds.objoid
    WHERE d.datname = 'test1-\!escape';
  $$
);

comment on DATABASE "test1-\!escape" is 'comment-needs\!escape';

SELECT result FROM run_command_on_all_nodes(
  $$
    SELECT ds.description AS database_comment
    FROM pg_database d
    LEFT JOIN pg_shdescription ds ON d.oid = ds.objoid
    WHERE d.datname = 'test1-\!escape';
  $$
);

comment on DATABASE "test1-\!escape" is null;

SELECT result FROM run_command_on_all_nodes(
  $$
    SELECT ds.description AS database_comment
    FROM pg_database d
    LEFT JOIN pg_shdescription ds ON d.oid = ds.objoid
    WHERE d.datname = 'test1-\!escape';
  $$
);

drop DATABASE "test1-\!escape";

--test metadata sync
select 1 from citus_remove_node('localhost', :worker_2_port);
create database "test1-\!escape";
comment on DATABASE "test1-\!escape" is 'test-comment';

SELECT result FROM run_command_on_all_nodes(
  $$
    SELECT ds.description AS database_comment
    FROM pg_database d
    LEFT JOIN pg_shdescription ds ON d.oid = ds.objoid
    WHERE d.datname = 'test1-\!escape';
  $$
);

select 1 from citus_add_node('localhost', :worker_2_port);

SELECT result FROM run_command_on_all_nodes(
  $$
    SELECT ds.description AS database_comment
    FROM pg_database d
    LEFT JOIN pg_shdescription ds ON d.oid = ds.objoid
    WHERE d.datname = 'test1-\!escape';
  $$
);

drop DATABASE "test1-\!escape";


reset citus.enable_create_database_propagation;
reset citus.grep_remote_commands;
reset citus.log_remote_commands;
