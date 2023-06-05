setup
{
    SELECT citus_set_coordinator_host('localhost', 57636);

    CREATE SCHEMA tenant1;
    CREATE TABLE tenant1.table1(id int PRIMARY KEY, name text, col bigint);
    INSERT INTO tenant1.table1 SELECT i, 'asd', i*1000 FROM generate_series(11, 20) i;

    CREATE TABLE tenant1.table2(id int PRIMARY KEY, name text, col bigint);

    CREATE TABLE public.ref(id int PRIMARY KEY);
    SELECT  create_reference_table('public.ref');

    CREATE TABLE tenant1.table3(id int PRIMARY KEY, name text, col1 int, col int REFERENCES public.ref(id));
    SELECT citus_add_local_table_to_metadata('tenant1.table3');
}

teardown
{
    DROP TABLE public.ref CASCADE;
    DROP SCHEMA tenant1 CASCADE;
    SELECT citus_remove_node('localhost', 57636);
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-schema-distribute"
{
    SELECT citus_schema_distribute('tenant1');
}

step "s1-schema-undistribute"
{
    SELECT citus_schema_undistribute('tenant1');
}

step "s1-commit"
{
    COMMIT;
}

session "s2"

step "s2-add-table"
{
    CREATE TABLE tenant1.table4(id int PRIMARY KEY, name text, col bigint);
}

step "s2-drop-table"
{
    DROP TABLE tenant1.table3;
}

step "s2-alter-col-type"
{
    ALTER TABLE tenant1.table3 ALTER COLUMN col1 TYPE text;
}

step "s2-add-foreign-key"
{
    ALTER TABLE tenant1.table3 ADD CONSTRAINT table3_fk1 FOREIGN KEY (id) REFERENCES tenant1.table2 (id);
}

step "s2-drop-foreign-key"
{
    ALTER TABLE tenant1.table3 DROP CONSTRAINT table3_col_fkey;
}

step "s2-create-unique-index"
{
    CREATE UNIQUE INDEX idx_2 ON tenant1.table3 (col);
}

step "s2-create-unique-index-concurrently"
{
    CREATE UNIQUE INDEX CONCURRENTLY idx_3 ON tenant1.table3 (col);
}

step "s2-reindex-unique-concurrently"
{
    REINDEX INDEX CONCURRENTLY tenant1.idx_2;
}

step "s2-insert"
{
    // we insert into public.ref table as well to prevent fkey violation
    INSERT INTO public.ref SELECT i FROM generate_series(11, 20) i;
    INSERT INTO tenant1.table3 SELECT i, 'asd', i*1000 FROM generate_series(11, 20) i;
}

step "s2-delete"
{
    DELETE FROM tenant1.table3;
}

step "s2-update"
{
    UPDATE tenant1.table3 SET col = 11 WHERE col = 11;
}

// CREATE TABLE
permutation "s1-begin" "s1-schema-distribute" "s2-add-table" "s1-commit"
permutation "s1-schema-distribute" "s1-begin" "s1-schema-undistribute" "s2-add-table" "s1-commit"

// DROP TABLE
permutation "s1-begin" "s1-schema-distribute" "s2-drop-table" "s1-commit"
permutation "s1-schema-distribute" "s1-begin" "s1-schema-undistribute" "s2-drop-table" "s1-commit"

// ALTER TABLE ALTER COLUMN TYPE
permutation "s1-begin" "s1-schema-distribute" "s2-alter-col-type" "s1-commit"
permutation "s1-schema-distribute" "s1-begin" "s1-schema-undistribute" "s2-alter-col-type" "s1-commit"

// ADD FOREIGN KEY
permutation "s1-begin" "s1-schema-distribute" "s2-add-foreign-key" "s1-commit"
permutation "s1-schema-distribute" "s1-begin" "s1-schema-undistribute" "s2-add-foreign-key" "s1-commit"

// DROP FOREIGN KEY
permutation "s1-begin" "s1-schema-distribute" "s2-drop-foreign-key" "s1-commit"
permutation "s1-schema-distribute" "s1-begin" "s1-schema-undistribute" "s2-drop-foreign-key" "s1-commit"

// CREATE UNIQUE INDEX
permutation "s1-begin" "s1-schema-distribute" "s2-create-unique-index" "s1-commit"
permutation "s1-schema-distribute" "s1-begin" "s1-schema-undistribute" "s2-create-unique-index" "s1-commit"

// CREATE UNIQUE INDEX CONCURRENTLY
permutation "s1-begin" "s1-schema-distribute" "s2-create-unique-index-concurrently" "s1-commit"
permutation "s1-schema-distribute" "s1-begin" "s1-schema-undistribute" "s2-create-unique-index-concurrently" "s1-commit"

// REINDEX CONCURRENTLY
permutation "s2-create-unique-index" "s1-begin" "s1-schema-distribute" "s2-reindex-unique-concurrently" "s1-commit"
permutation "s2-create-unique-index" "s1-schema-distribute" "s1-begin" "s1-schema-undistribute" "s2-reindex-unique-concurrently" "s1-commit"

// INSERT
permutation "s1-begin" "s1-schema-distribute" "s2-insert" "s1-commit"
permutation "s1-schema-distribute" "s1-begin" "s1-schema-undistribute" "s2-insert" "s1-commit"

// UPDATE
permutation "s2-insert" "s1-begin" "s1-schema-distribute" "s2-update" "s1-commit"
permutation "s2-insert" "s1-schema-distribute" "s1-begin" "s1-schema-undistribute" "s2-update" "s1-commit"

// DELETE
permutation "s2-insert" "s1-begin" "s1-schema-distribute" "s2-delete" "s1-commit"
permutation "s2-insert" "s1-schema-distribute" "s1-begin" "s1-schema-undistribute" "s2-delete" "s1-commit"
