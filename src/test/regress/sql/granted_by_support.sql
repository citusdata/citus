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
grant role4 to role3 with admin option GRANTED BY role1;
grant role3 to role1 with admin option GRANTED BY role4;
grant "role5'_test" to role1 with admin option;
grant "role5'_test" to role3 with admin option GRANTED BY role1;

SELECT member::regrole, roleid::regrole as role, grantor::regrole, admin_option
        FROM pg_auth_members
        WHERE member::regrole::text in
            ('role1','role2','role3','role4','"role5''_test"')
        order by member::regrole::text, roleid::regrole::text;


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

set citus.log_remote_commands = on;
--set citus.grep_remote_commands = '%GRANT%';

select 1 from citus_add_node ('localhost',:worker_2_port);

reset citus.log_remote_commands;
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


GRANT role2 TO  role3 WITH INHERIT TRUE, ADMIN OPTION GRANTED BY role1;;
GRANT role2 TO  role3 WITH INHERIT TRUE, ADMIN OPTION GRANTED BY role1;;
GRANT role3 TO  role4 WITH INHERIT TRUE, ADMIN OPTION GRANTED BY postgres;; x
GRANT role3 TO  role4 WITH INHERIT TRUE, ADMIN OPTION GRANTED BY postgres;;
GRANT role2 TO  role3 WITH INHERIT TRUE, ADMIN OPTION GRANTED BY role1;; x
GRANT role4 TO  "role5'_test" WITH INHERIT TRUE, ADMIN OPTION GRANTED BY postgres;; x
GRANT role2 TO  "role5'_test" WITH INHERIT TRUE GRANTED BY role3;; x
GRANT role3 TO  "role5'_test" WITH INHERIT TRUE GRANTED BY role4 ; x


 "role5'_test" | role2         | role3         | f x
 "role5'_test" | role3         | role4         | f x
 "role5'_test" | role4         | postgres      | t x
 role1         | "role5'_test" | postgres      | t
 role1         | role2         | postgres      | t
 role1         | role3         | role4         | t
 role1         | role4         | "role5'_test" | t
 role3         | role2         | role1         | t x
 role4         | role3         | postgres      | t x
