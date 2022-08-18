// reference tables _do_ lock on the first reference table in the shardgroup
// due to the lack of shardgroups in the system. When we run the tests we want
// to make sure the tables we are testing against cannot be the first reference
// table. For this purpose we create a reference table that we will _not_
// interact with during the tests. However, when a VACUUM happens on
// pg_dist_partition in the middle of the test, to table that we don't use on
// purpose might not actually be the first in the catalogs. To avoid VACUUM
// running we run VACUUM FULL on pg_dist_partition before we run this test.
// This VACUUM is done in a separate setup block because it cannot run in a
// transaction.
setup
{
	VACUUM FULL pg_dist_partition;
}
setup
{
 	CREATE TABLE first_reference_table(a int);
 	SELECT create_reference_table('first_reference_table');
}

teardown
{
    DROP TABLE first_reference_table;
    DROP TABLE IF EXISTS reference_table_s1;
    DROP TABLE IF EXISTS reference_table_s2;
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-create"
{
    CREATE TABLE reference_table_s1(a int);
 	SELECT create_reference_table('reference_table_s1');
}

step "s1-drop"
{
    DROP TABLE reference_table_s1;
}

step "s1-commit"
{
    COMMIT;
}

session "s2"

step "s2-begin"
{
    BEGIN;
}

step "s2-create"
{
    CREATE TABLE reference_table_s2(a int);
 	SELECT create_reference_table('reference_table_s2');
}

step "s2-drop"
{
    DROP TABLE reference_table_s2;
}

step "s2-commit"
{
    COMMIT;
}

// creates don't block each other
permutation "s1-begin" "s1-create" "s2-create" "s1-commit"

// drops don't block each other
permutation "s1-create" "s2-create" "s1-begin" "s1-drop" "s2-drop" "s1-commit"

// create and drop don't block each other
permutation "s1-create" "s2-begin" "s2-create" "s1-drop" "s2-commit"
permutation "s2-create" "s2-begin" "s2-drop" "s1-create" "s2-commit"
