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
grant dist_role2 to non_dist_role1 with admin option;
grant dist_role2 to dist_role4 granted by non_dist_role1 ;
reset citus.enable_create_role_propagation;

grant dist_role2 to "dist_role5'_test" granted by non_dist_role1;--will fail since non_dist_role1 does not exist on worker_1


\c - - - :master_port
grant dist_role3 to "dist_role5'_test" granted by dist_role4;
grant dist_role2 to "dist_role5'_test" granted by dist_role3;


--will fail since non_dist_role2 does not exist in worker_1
grant dist_role2 to non_dist_role2 with admin option;
grant dist_role2 to dist_role4 granted by non_dist_role2 ;
grant non_dist_role2 to "dist_role5'_test";


\c - - - :worker_1_port
create role non_dist_role2;

\c - - - :master_port
--will be successful since non_dist_role has been created on worker_1
grant dist_role2 to non_dist_role2 with admin option;
grant dist_role2 to dist_role4 granted by non_dist_role2 ;
grant non_dist_role2 to "dist_role5'_test";


grant dist_role4 to "dist_role5'_test" with admin option;

select result FROM run_command_on_all_nodes(
    $$
    SELECT array_to_json(array_agg(row_to_json(t)))
    FROM (
        SELECT member::regrole, roleid::regrole as role, grantor::regrole, admin_option
        FROM pg_auth_members
        WHERE member::regrole::text in
            ('dist_role1','non_dist_role1')
        order by member::regrole::text, roleid::regrole::text
    ) t
    $$
);

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




set citus.enable_create_role_propagation to off;
create role non_dist_role_for_mds;

grant dist_role3 to non_dist_role_for_mds with admin option;
grant non_dist_role_for_mds to dist_role1 with admin option;

grant dist_role3 to dist_role4 with admin option GRANTED BY non_dist_role_for_mds;
reset citus.enable_create_role_propagation;

SELECT objid::regrole FROM pg_catalog.pg_dist_object WHERE classid='pg_authid'::regclass::oid AND objid::regrole::text= 'non_dist_role_for_mds' ORDER BY 1;


select result FROM run_command_on_all_nodes(
    $$
    SELECT array_to_json(array_agg(row_to_json(t)))
    FROM (
        SELECT member::regrole, roleid::regrole as role, grantor::regrole, admin_option
        FROM pg_auth_members
        WHERE member::regrole::text in
            ('dist_role1','dist_role2','dist_role3','dist_role4','"role5''_test"', 'non_dist_role_for_mds','non_dist_role1','non_dist_role2')
        order by member::regrole::text, roleid::regrole::text
    ) t
    $$
);


set citus.enable_create_role_propagation to off;
create role non_dist_role_mds_fail;

grant dist_role2 to non_dist_role_mds_fail with admin option;
grant dist_role2 to non_dist_role_for_mds GRANTED BY non_dist_role_mds_fail;


reset citus.enable_create_role_propagation;

--will fail since non_dist_role_for_mds is not in dependency resolution
select 1 from citus_add_node ('localhost',:worker_2_port);

--this grant statement will add non_dist_role_mds_fail to dist_role3 dependencies
grant non_dist_role_mds_fail to dist_role3;

--will be successful since non_dist_role_mds_fail is in dependency resolution of dist_role3
-- and will be created in metadata sync phase
select 1 from citus_add_node ('localhost',:worker_2_port);

select result FROM run_command_on_all_nodes(
    $$
    SELECT array_to_json(array_agg(row_to_json(t)))
    FROM (
        SELECT member::regrole, roleid::regrole as role, grantor::regrole, admin_option
        FROM pg_auth_members
        WHERE member::regrole::text in
            ('dist_role1','dist_role2','dist_role3','dist_role4','"role5''_test"','non_dist_role_for_mds','non_dist_role1','non_dist_role2')
        order by member::regrole::text, roleid::regrole::text
    ) t
    $$
);


--clean all resources
drop role dist_role1,dist_role2,dist_role3,dist_role4,"dist_role5'_test";
drop role non_dist_role1,non_dist_role2,non_dist_role_for_mds,non_dist_role_mds_fail;

SELECT objid::regrole FROM pg_catalog.pg_dist_object WHERE classid='pg_authid'::regclass::oid AND objid::regrole::text= 'non_dist_role1' ORDER BY 1;
reset citus.enable_create_role_propagation;

select result FROM run_command_on_all_nodes(
    $$
    SELECT array_to_json(array_agg(row_to_json(t)))
    FROM (
        SELECT member::regrole, roleid::regrole as role, grantor::regrole, admin_option
        FROM pg_auth_members
        WHERE member::regrole::text in
            ('dist_role1','dist_role2','dist_role3','dist_role4','"role5''_test"','non_dist_role_for_mds','non_dist_role1','non_dist_role2')
        order by member::regrole::text, roleid::regrole::text
    ) t
    $$
);



