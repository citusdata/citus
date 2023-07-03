setup
{
    SET citus.enable_schema_based_sharding TO ON;
    SET citus.shard_replication_factor TO 1;
    CREATE SCHEMA tenant_1;
    CREATE SCHEMA tenant_2;
    CREATE SCHEMA tenant_3;

    CREATE SCHEMA tenant_4;
    CREATE TABLE tenant_4.first_table (a int);
}

teardown
{
    DROP SCHEMA tenant_1, tenant_2, tenant_3, tenant_4 CASCADE;
}

session "s1"

step "s1-begin" { BEGIN; SET citus.shard_replication_factor TO 1;}
step "s1-tenant-1-create-table-1" { CREATE TABLE tenant_1.tbl_1 (a int); }
step "s1-tenant-4-create-table-1" { CREATE TABLE tenant_4.tbl_1 (a int); }
step "s1-tenant-2-create-table-1" { CREATE TABLE tenant_2.tbl_1 (a int); }
step "s1-commit" { COMMIT; }

session "s2"

step "s2-begin" { BEGIN; SET citus.shard_replication_factor TO 1;}
step "s2-tenant-1-create-table-2" { CREATE TABLE tenant_1.tbl_2 (a int); }
step "s2-tenant-4-create-table-2" { CREATE TABLE tenant_4.tbl_2 (a int); }
step "s2-tenant-1-verify-colocation" { SELECT COUNT(DISTINCT(colocationid))=1 FROM pg_dist_partition WHERE logicalrelid::text LIKE 'tenant_1.%'; }
step "s2-tenant-4-verify-colocation" { SELECT COUNT(DISTINCT(colocationid))=1 FROM pg_dist_partition WHERE logicalrelid::text LIKE 'tenant_4.%'; }
step "s2-tenant-3-create-table-1" { CREATE TABLE tenant_3.tbl_1 (a int); }
step "s2-commit" { COMMIT; }

// two sessions competing with each other to create the first table in the same schema
permutation "s1-begin" "s2-begin" "s1-tenant-1-create-table-1" "s2-tenant-1-create-table-2" "s1-commit" "s2-tenant-1-verify-colocation" "s2-commit"

// two sessions creating further tenant tables in the same schema
permutation "s1-begin" "s2-begin" "s1-tenant-4-create-table-1" "s2-tenant-4-create-table-2" "s1-commit" "s2-tenant-4-verify-colocation" "s2-commit"

// two sessions creating tenant tables in different schemas
permutation "s1-begin" "s2-begin" "s1-tenant-2-create-table-1" "s2-tenant-3-create-table-1" "s1-commit" "s2-commit"
