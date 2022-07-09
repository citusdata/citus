setup
{
    CREATE TABLE ref_table_1(id int PRIMARY KEY, value int);
	SELECT create_reference_table('ref_table_1');

    CREATE TABLE ref_table_2(id int PRIMARY KEY, value int REFERENCES ref_table_1(id) ON DELETE CASCADE ON UPDATE CASCADE);
	SELECT create_reference_table('ref_table_2');

    CREATE TABLE ref_table_3(id int PRIMARY KEY, value int REFERENCES ref_table_2(id) ON DELETE CASCADE ON UPDATE CASCADE);
	SELECT create_reference_table('ref_table_3');

    INSERT INTO ref_table_1 VALUES (1, 1), (3, 3), (5, 5);
    INSERT INTO ref_table_2 SELECT * FROM ref_table_1;
    INSERT INTO ref_table_3 SELECT * FROM ref_table_2;
}

teardown
{
	DROP TABLE ref_table_1, ref_table_2, ref_table_3;
}

session "s1"

step "s1-begin"
{
	BEGIN;
}

step "s1-delete-table-2"
{
    DELETE FROM ref_table_2 WHERE value = 2;
}

step "s1-insert-table-2"
{
    INSERT INTO ref_table_2 VALUES (7, 2);
}

step "s1-update-table-2"
{
    UPDATE ref_table_2 SET id = 0 WHERE value = 2;
}

step "s1-delete-table-3"
{
    DELETE FROM ref_table_3 WHERE value = 1 RETURNING id;
}

step "s1-insert-table-3"
{
    INSERT INTO ref_table_3 VALUES (7, 1);
}

step "s1-update-table-3"
{
    UPDATE ref_table_3 SET id = 2 WHERE value = 1 RETURNING id;
}

step "s1-select-table-1"
{
    SELECT * FROM ref_table_1 ORDER BY id, value;
}

step "s1-select-table-2"
{
    SELECT * FROM ref_table_2 ORDER BY id, value;
}

step "s1-select-table-3"
{
    SELECT * FROM ref_table_3 ORDER BY id, value;
}

step "s1-view-locks"
{
    SELECT mode, count(*)
    FROM pg_locks
    WHERE locktype='advisory'
    GROUP BY mode;
}

step "s1-rollback"
{
    ROLLBACK;
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

step "s2-insert-table-1"
{
    INSERT INTO ref_table_1 VALUES (7, 7);
}

step "s2-update-table-1"
{
    UPDATE ref_table_1 SET id = 2 WHERE id = 1;
}

step "s2-delete-table-1"
{
    DELETE FROM ref_table_1 WHERE id = 1;
}

step "s2-insert-table-2"
{
    INSERT INTO ref_table_2 VALUES (7, 5);
}

step "s2-update-table-2"
{
    UPDATE ref_table_2 SET id = 2 WHERE id = 1;
}

step "s2-delete-table-2"
{
    DELETE FROM ref_table_2 WHERE id = 1;
}

step "s2-insert-table-3"
{
    INSERT INTO ref_table_3 VALUES (7, 5);
}

step "s2-update-table-3"
{
    UPDATE ref_table_3 SET id = 2 WHERE id = 1;
}

step "s2-delete-table-3"
{
    DELETE FROM ref_table_3 WHERE id = 1;
}

step "s2-rollback"
{
    ROLLBACK;
}

step "s2-commit"
{
    COMMIT;
}

// Check that we get necessary resource locks

// Case 1. UPDATE/DELETE ref_table_1 should only lock its own shard in Exclusive mode.
permutation "s2-begin" "s2-update-table-1" "s1-begin" "s1-view-locks" "s1-rollback" "s2-rollback" "s1-view-locks"
permutation "s2-begin" "s2-delete-table-1" "s1-view-locks" "s2-rollback" "s1-view-locks"
// Case 2. Modifying ref_table_2 should also lock ref_table_1 shard in Exclusive mode.
permutation "s2-begin" "s2-update-table-2" "s1-view-locks" "s2-rollback" "s1-view-locks"
permutation "s2-begin" "s2-delete-table-2" "s1-view-locks" "s2-rollback" "s1-view-locks"
// Case 3. Modifying ref_table_3 should also lock ref_table_1 and ref_table_2 shards in Exclusive mode.
permutation "s2-begin" "s2-update-table-3" "s1-begin" "s1-view-locks" "s1-rollback" "s2-rollback" "s1-view-locks"
permutation "s2-begin" "s2-delete-table-3" "s1-begin" "s1-view-locks" "s1-rollback" "s2-rollback" "s1-view-locks"
// Case 4. Inserting into ref_table_1 should only lock its own shard in RowExclusive mode.
permutation "s2-begin" "s2-insert-table-1" "s1-view-locks" "s2-rollback" "s1-view-locks"
// Case 5. Modifying ref_table_2 should also lock ref_table_1 in RowExclusive mode.
permutation "s2-begin" "s2-insert-table-2" "s1-view-locks" "s2-rollback" "s1-view-locks"
// Case 6. Modifying ref_table_2 should also lock ref_table_1 in RowExclusive mode.
permutation "s2-begin" "s2-insert-table-3" "s1-view-locks" "s2-rollback" "s1-view-locks"

// Now some concurrent operations

// Updates/Deletes from ref_table_1 cascade to ref_table_2, so DML on ref_table_2 should block
// Case 1. UPDATE -> DELETE
permutation "s1-begin" "s2-begin" "s2-update-table-1" "s1-delete-table-2" "s2-commit" "s1-commit" "s1-select-table-2"
// Case 2. UPDATE -> INSERT
permutation "s1-begin" "s2-begin" "s2-update-table-1" "s1-insert-table-2" "s2-commit" "s1-commit" "s1-select-table-2"
// Case 3. UPDATE -> UPDATE
permutation "s1-begin" "s2-begin" "s2-update-table-1" "s1-update-table-2" "s2-commit" "s1-commit" "s1-select-table-2"
// Case 4. DELETE -> DELETE
permutation "s1-begin" "s2-begin" "s2-delete-table-1" "s1-delete-table-2" "s2-commit" "s1-commit" "s1-select-table-2"
// Case 5. DELETE -> INSERT
permutation "s1-begin" "s2-begin" "s2-delete-table-1" "s1-insert-table-2" "s2-commit" "s1-commit" "s1-select-table-2"
// Case 6. DELETE -> UPDATE
permutation "s1-begin" "s2-begin" "s2-delete-table-1" "s1-update-table-2" "s2-commit" "s1-commit" "s1-select-table-2"

// Deletes from ref_table_1 can transitively cascade to ref_table_3, so DML on ref_table_3 should block
// Case 1. DELETE -> DELETE
permutation "s1-begin" "s2-begin" "s2-delete-table-1" "s1-delete-table-3" "s2-commit" "s1-commit" "s1-select-table-3"
// Case 2. DELETE -> INSERT, should error out
permutation "s1-begin" "s2-begin" "s2-delete-table-1" "s1-insert-table-3" "s2-commit" "s1-commit" "s1-select-table-3"
// Case 3. DELETE -> UPDATE
permutation "s1-begin" "s2-begin" "s2-delete-table-1" "s1-update-table-3" "s2-commit" "s1-commit" "s1-select-table-3"

// Any DML on any of ref_table_{1,2,3} should block others from DML in the foreign constraint graph ...
permutation "s1-begin" "s2-begin" "s2-insert-table-1" "s1-update-table-3" "s2-commit" "s1-commit" "s1-select-table-3"
permutation "s1-begin" "s2-begin" "s1-update-table-3" "s2-insert-table-1" "s1-commit" "s2-commit" "s1-select-table-3"
permutation "s1-begin" "s2-begin" "s2-insert-table-1" "s1-update-table-2" "s2-commit" "s1-commit" "s1-select-table-3"
permutation "s1-begin" "s2-begin" "s1-update-table-2" "s2-insert-table-1" "s1-commit" "s2-commit" "s1-select-table-3"
permutation "s1-begin" "s2-begin" "s2-insert-table-2" "s1-update-table-3" "s2-commit" "s1-commit" "s1-select-table-3"
permutation "s1-begin" "s2-begin" "s1-update-table-3" "s2-insert-table-2" "s1-commit" "s2-commit" "s1-select-table-3"

// DMLs shouldn't block select on tables in the same foreign constraint graph
permutation "s1-begin" "s2-begin" "s2-insert-table-1" "s1-select-table-1" "s2-commit" "s1-commit"
permutation "s1-begin" "s2-begin" "s2-insert-table-1" "s1-select-table-2" "s2-commit" "s1-commit"
permutation "s1-begin" "s2-begin" "s2-insert-table-1" "s1-select-table-3" "s2-commit" "s1-commit"
permutation "s1-begin" "s2-begin" "s2-delete-table-2" "s1-select-table-1" "s2-commit" "s1-commit"
permutation "s1-begin" "s2-begin" "s2-delete-table-2" "s1-select-table-2" "s2-commit" "s1-commit"
permutation "s1-begin" "s2-begin" "s2-delete-table-2" "s1-select-table-3" "s2-commit" "s1-commit"
permutation "s1-begin" "s2-begin" "s2-update-table-3" "s1-select-table-1" "s2-commit" "s1-commit"
permutation "s1-begin" "s2-begin" "s2-update-table-3" "s1-select-table-2" "s2-commit" "s1-commit"
permutation "s1-begin" "s2-begin" "s2-update-table-3" "s1-select-table-3" "s2-commit" "s1-commit"
