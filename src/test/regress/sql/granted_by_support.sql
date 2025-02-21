-- Active: 1700033167033@@localhost@9700@gurkanindibay@public
--In below tests, complex role hierarchy is created and then granted by support is tested.

--- Test 1: Tests from main database
select 1 from citus_remove_node ('localhost',:worker_2_port);
set citus.enable_create_role_propagation to off;
create role non_dist_role1;
reset citus.enable_create_role_propagation;
SELECT objid::regrole FROM pg_catalog.pg_dist_object WHERE classid='pg_authid'::regclass::oid AND objid::regrole::text= 'non_dist_role1' ORDER BY 1;

create role dist_role1;
create role dist_role2;
create role dist_role3;
create role dist_role4;
create role "dist_role5'_test";

grant dist_role2 to dist_role1 with admin option;
grant dist_role2 to dist_role3 with admin option granted by dist_role1;
grant dist_role3 to dist_role4 with admin option;

-- With enable_create_role_propagation on, all grantees are propagated.
-- To test non-distributed grantor, set this option off for some roles.
set citus.enable_create_role_propagation to off;
grant non_dist_role1 to dist_role1 with admin option;
reset citus.enable_create_role_propagation;

SELECT objid::regrole FROM pg_catalog.pg_dist_object WHERE classid='pg_authid'::regclass::oid AND objid::regrole::text= 'non_dist_role1' ORDER BY 1;



grant dist_role2 to non_dist_role1 with admin option;

grant dist_role3 to "dist_role5'_test" granted by dist_role4;
grant dist_role2 to "dist_role5'_test" granted by dist_role3;
grant dist_role2 to dist_role4 granted by non_dist_role1 ;--will not be propagated since grantor is non-distributed


grant dist_role4 to "dist_role5'_test" with admin option;

--below command propagates the non_dist_role1 since non_dist_role1 is already granted to dist_role1
--and citus sees granted roles as a dependency and citus propagates the dependent roles
grant dist_role4 to dist_role1 with admin option GRANTED BY "dist_role5'_test";

SELECT objid::regrole FROM pg_catalog.pg_dist_object WHERE classid='pg_authid'::regclass::oid AND objid::regrole::text= 'non_dist_role1' ORDER BY 1;

grant dist_role4 to dist_role3 with admin option GRANTED BY dist_role1; --fails since already dist_role3 granted to dist_role4

--Below command will not be successful since non_dist_role1 is propagated with the dependency resolution above
--however, ADMIN OPTION is not propagated for non_dist_role1 to worker 1 because the citus.enable_create_role_propagation is off
grant non_dist_role1 to dist_role4 granted by dist_role1;

grant dist_role3 to dist_role1 with admin option GRANTED BY dist_role4;
grant "dist_role5'_test" to dist_role1 with admin option;
grant "dist_role5'_test" to dist_role3 with admin option GRANTED BY dist_role1;--fails since already dist_role3 granted to "dist_role5'_test"


select result FROM run_command_on_all_nodes(
    $$
    SELECT array_to_json(array_agg(row_to_json(t)))
    FROM (
        SELECT member::regrole, roleid::regrole as role, grantor::regrole, admin_option
        FROM pg_auth_members
        WHERE member::regrole::text in
            ('dist_role1','dist_role2','dist_role3','dist_role4','"role5''_test"')
        order by member::regrole::text, roleid::regrole::text
    ) t
    $$
);

select 1 from citus_add_node ('localhost',:worker_2_port);

select result FROM run_command_on_all_nodes(
    $$
    SELECT array_to_json(array_agg(row_to_json(t)))
    FROM (
        SELECT member::regrole, roleid::regrole as role, grantor::regrole, admin_option
        FROM pg_auth_members
        WHERE member::regrole::text in
            ('dist_role1','dist_role2','dist_role3','dist_role4','"role5''_test"')
        order by member::regrole::text, roleid::regrole::text
    ) t
    $$
);

--clean all resources
drop role dist_role1,dist_role2,dist_role3,dist_role4,"dist_role5'_test";
drop role non_dist_role1;

SELECT objid::regrole FROM pg_catalog.pg_dist_object WHERE classid='pg_authid'::regclass::oid AND objid::regrole::text= 'non_dist_role1' ORDER BY 1;
reset citus.enable_create_role_propagation;

select result FROM run_command_on_all_nodes(
    $$
    SELECT array_to_json(array_agg(row_to_json(t)))
    FROM (
        SELECT member::regrole, roleid::regrole as role, grantor::regrole, admin_option
        FROM pg_auth_members
        WHERE member::regrole::text in
            ('dist_role1','dist_role2','dist_role3','dist_role4','"role5''_test"')
        order by member::regrole::text, roleid::regrole::text
    ) t
    $$
);

--- Test 2: Tests from non-main database
set citus.enable_create_database_propagation to on;
create database test_granted_by_support;

select 1 from citus_remove_node ('localhost',:worker_2_port);

SELECT objid::regrole FROM pg_catalog.pg_dist_object WHERE classid='pg_authid'::regclass::oid AND objid::regrole::text= 'non_dist_role1' ORDER BY 1;

\c test_granted_by_support
--here in below block since 'citus.enable_create_role_propagation to off ' is not effective,
--non_dist_role1 is being propagated to dist_role1 unlike main db scenario
--non_dist_role1 will be used for the test scenarios in this section
set citus.enable_create_role_propagation to off;
create role non_dist_role1;
reset citus.enable_create_role_propagation;

--dropping since it isn't non-distributed as intended
drop role non_dist_role1;

--creating non_dist_role1 again in main database
--This is actually non-distributed role
\c regression
set citus.enable_create_role_propagation to off;
create role non_dist_role1;
reset citus.enable_create_role_propagation;

\c test_granted_by_support
create role dist_role1;
create role dist_role1;
create role dist_role2;
create role dist_role3;
create role dist_role4;
create role "dist_role5'_test";
\c regression - - :master_port
SELECT objid::regrole FROM pg_catalog.pg_dist_object WHERE classid='pg_authid'::regclass::oid AND objid::regrole::text= 'non_dist_role1' ORDER BY 1;


\c test_granted_by_support
grant dist_role2 to dist_role1 with admin option;
grant dist_role2 to dist_role3 with admin option granted by dist_role1;
grant dist_role3 to dist_role4 with admin option;

-- With enable_create_role_propagation on, all grantees are propagated.
-- To test non-distributed grantor, set this option off for some roles.

\c regression
set citus.enable_create_role_propagation to off;
grant non_dist_role1 to dist_role1 with admin option;
reset citus.enable_create_role_propagation;

\c test_granted_by_support
grant dist_role2 to non_dist_role1 with admin option;

\c test_granted_by_support - - :worker_1_port
grant dist_role3 to "dist_role5'_test" granted by dist_role4;
grant dist_role2 to "dist_role5'_test" granted by dist_role3;
grant dist_role2 to dist_role4 granted by non_dist_role1 ;--will not be propagated since grantor is non-distributed
\c regression - - :master_port
SELECT objid::regrole FROM pg_catalog.pg_dist_object WHERE classid='pg_authid'::regclass::oid AND objid::regrole::text= 'non_dist_role1' ORDER BY 1;
\c test_granted_by_support - - :worker_1_port
grant dist_role4 to "dist_role5'_test" with admin option;

\c regression - - :master_port
SELECT objid::regrole FROM pg_catalog.pg_dist_object WHERE classid='pg_authid'::regclass::oid AND objid::regrole::text= 'non_dist_role1' ORDER BY 1;

\c test_granted_by_support

-- Unlike maindb scenario, non-maindb scenario doesn't propagate 'create non_dist_role1' to
--workers as it doesn't create dependency objects for non-distributed roles.
grant dist_role4 to dist_role1 with admin option GRANTED BY "dist_role5'_test";

\c regression - - :master_port
SELECT objid::regrole FROM pg_catalog.pg_dist_object WHERE classid='pg_authid'::regclass::oid AND objid::regrole::text= 'non_dist_role1' ORDER BY 1;


\c test_granted_by_support - - :worker_1_port
grant dist_role4 to dist_role3 with admin option GRANTED BY dist_role1; --fails since already dist_role3 granted to dist_role4
grant non_dist_role1 to dist_role4 granted by dist_role1;
grant dist_role3 to dist_role1 with admin option GRANTED BY dist_role4;
grant "dist_role5'_test" to dist_role1 with admin option;
grant "dist_role5'_test" to dist_role3 with admin option GRANTED BY dist_role1;--fails since already dist_role3 granted to "dist_role5'_test"

\c regression - - :master_port

select result FROM run_command_on_all_nodes(
    $$
    SELECT array_to_json(array_agg(row_to_json(t)))
    FROM (
        SELECT member::regrole, roleid::regrole as role, grantor::regrole, admin_option
        FROM pg_auth_members
        WHERE member::regrole::text in
            ('dist_role1','dist_role2','dist_role3','dist_role4','"role5''_test"')
        order by member::regrole::text, roleid::regrole::text
    ) t
    $$
);
select 1 from citus_add_node ('localhost',:worker_2_port);

select result FROM run_command_on_all_nodes(
    $$
    SELECT array_to_json(array_agg(row_to_json(t)))
    FROM (
        SELECT member::regrole, roleid::regrole as role, grantor::regrole, admin_option
        FROM pg_auth_members
        WHERE member::regrole::text in
            ('dist_role1','dist_role2','dist_role3','dist_role4','"role5''_test"')
        order by member::regrole::text, roleid::regrole::text
    ) t
    $$
);

--clean all resources

set citus.enable_create_database_propagation to on;
drop database test_granted_by_support;
drop role dist_role1,dist_role2,dist_role3,dist_role4,"dist_role5'_test";
drop role non_dist_role1;

drop role if exists non_dist_role1;


select result FROM run_command_on_all_nodes(
    $$
    SELECT array_to_json(array_agg(row_to_json(t)))
    FROM (
        SELECT member::regrole, roleid::regrole as role, grantor::regrole, admin_option
        FROM pg_auth_members
        WHERE member::regrole::text in
            ('dist_role1','dist_role2','dist_role3','dist_role4','"role5''_test"')
        order by member::regrole::text, roleid::regrole::text
    ) t
    $$
);
reset citus.enable_create_database_propagation;
