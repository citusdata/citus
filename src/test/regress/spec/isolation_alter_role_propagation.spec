setup
{
    CREATE ROLE alter_role_1 WITH LOGIN;
    SELECT run_command_on_workers($$CREATE ROLE alter_role_1 WITH LOGIN;$$);

    CREATE ROLE alter_role_2 WITH LOGIN;
    SELECT run_command_on_workers($$CREATE ROLE alter_role_2 WITH LOGIN;$$);
}

teardown
{
    DROP ROLE IF EXISTS alter_role_1;
    SELECT run_command_on_workers($$DROP ROLE IF EXISTS alter_role_1;$$);

    DROP ROLE IF EXISTS alter_role_2;
    SELECT run_command_on_workers($$DROP ROLE IF EXISTS alter_role_2;$$);
}

session "s1"

step "s1-enable-propagation"
{
    SET citus.enable_alter_role_propagation to ON;
}

step "s1-begin"
{
    BEGIN;
}

step "s1-alter-role-1"
{
    ALTER ROLE alter_role_1 NOSUPERUSER;
}

step "s1-add-node"
{
    SELECT 1 FROM master_add_node('localhost', 57637);
}

step "s1-commit"
{
    COMMIT;
}


session "s2"

step "s2-enable-propagation"
{
    SET citus.enable_alter_role_propagation to ON;
}

step "s2-alter-role-1"
{
    ALTER ROLE alter_role_1 NOSUPERUSER;
}

step "s2-alter-role-2"
{
    ALTER ROLE alter_role_2 NOSUPERUSER;
}

step "s2-add-node"
{
    SELECT 1 FROM master_add_node('localhost', 57637);
}

permutation "s1-enable-propagation" "s2-enable-propagation" "s1-begin" "s1-alter-role-1" "s2-add-node" "s1-commit"
permutation "s1-enable-propagation" "s2-enable-propagation" "s1-begin" "s1-add-node" "s2-alter-role-1" "s1-commit"

permutation "s1-enable-propagation" "s2-enable-propagation" "s1-begin" "s1-alter-role-1" "s2-alter-role-1" "s1-commit"
permutation "s1-enable-propagation" "s2-enable-propagation" "s1-begin" "s1-alter-role-1" "s2-alter-role-2" "s1-commit"


