set citus.log_remote_commands to on;

set citus.grep_remote_commands to 'COMMENT ON ROLE';

create role "role1-\!escape";

comment on ROLE "role1-\!escape" is 'test-comment';

SELECT result FROM run_command_on_all_nodes(
  $$
    SELECT ds.description AS role_comment
    FROM pg_roles r
    LEFT JOIN pg_shdescription ds ON r.oid = ds.objoid
    WHERE r.rolname = 'role1-\!escape';
  $$
);

comment on role "role1-\!escape" is 'comment-needs\!escape';

SELECT result FROM run_command_on_all_nodes(
  $$
    SELECT ds.description AS role_comment
    FROM pg_roles r
    LEFT JOIN pg_shdescription ds ON r.oid = ds.objoid
    WHERE r.rolname = 'role1-\!escape';
  $$
);

comment on role "role1-\!escape" is NULL;

SELECT result FROM run_command_on_all_nodes(
  $$
    SELECT ds.description AS role_comment
    FROM pg_roles r
    LEFT JOIN pg_shdescription ds ON r.oid = ds.objoid
    WHERE r.rolname = 'role1-\!escape';
  $$
);

drop role "role1-\!escape";


--test metadata sync

select 1 from citus_remove_node('localhost', :worker_2_port);
create role "role1-\!escape";
comment on ROLE "role1-\!escape" is 'test-comment';

SELECT result FROM run_command_on_all_nodes(
  $$
    SELECT ds.description AS role_comment
    FROM pg_roles r
    LEFT JOIN pg_shdescription ds ON r.oid = ds.objoid
    WHERE r.rolname = 'role1-\!escape';
  $$
);

select 1 from citus_add_node('localhost', :worker_2_port);

SELECT result FROM run_command_on_all_nodes(
  $$
    SELECT ds.description AS role_comment
    FROM pg_roles r
    LEFT JOIN pg_shdescription ds ON r.oid = ds.objoid
    WHERE r.rolname = 'role1-\!escape';
  $$
);

drop role "role1-\!escape";

reset citus.grep_remote_commands;
reset citus.log_remote_commands;
