-- Active: 1700033167033@@localhost@9700@gurkanindibay@public
--In below tests, complex role hierarchy is created and then granted by support is tested.

select 1 from citus_remove_node ('localhost',:worker_2_port);

create role role1;

create role role2;

create role role3;

create role role4;

create role "role5'_test";

grant role2 to role1 with admin option;

grant role2 to role3 with admin option granted by role1;

grant role3 to role4 with admin option;

grant role3 to "role5'_test" granted by role4;

grant role2 to "role5'_test" granted by role3;

grant role4 to "role5'_test" with admin option;

grant role4 to role1 with admin option GRANTED BY "role5'_test";

SELECT roleid::regrole::text AS role, member::regrole::text, grantor::regrole::text, admin_option, inherit_option,set_option FROM pg_auth_members pa;

grant role4 to role3 with admin option GRANTED BY role1;

grant role3 to role1 with admin option GRANTED BY role4;

grant "role5'_test" to role1 with admin option;

grant "role5'_test" to role3 with admin option GRANTED BY role1;


SELECT roleid::regrole::text AS role, member::regrole::text, grantor::regrole::text, admin_option, inherit_option,set_option FROM pg_auth_members pa;


select result FROM run_command_on_all_nodes(
    $$
    SELECT array_to_json(array_agg(row_to_json(t)))
    FROM (
        SELECT member::regrole, roleid::regrole as role, grantor::regrole, admin_option
        FROM pg_auth_members
        WHERE member::regrole::text in
            ('role1','role2','role3','role4','"role5''_test"')
        order by member::regrole::text, roleid::regrole::text
    ) t
    $$
);



set citus.log_remote_commands to on;

set citus.grep_remote_commands to '%GRANT%';

select 1 from citus_add_node ('localhost',:worker_2_port);

set citus.log_remote_commands to off;
reset citus.grep_remote_commands;

--clean all resources

drop role role1,role2,role3,role4,"role5'_test";

select result FROM run_command_on_all_nodes(
    $$
    SELECT array_to_json(array_agg(row_to_json(t)))
    FROM (
        SELECT member::regrole, roleid::regrole as role, grantor::regrole, admin_option
        FROM pg_auth_members
        WHERE member::regrole::text in
            ('role1','role2','role3','role4','"role5''_test"')
        order by member::regrole::text, roleid::regrole::text
    ) t
    $$
);
