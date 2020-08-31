setup
{
    CREATE TABLE dist_table(a INT, b INT);
    SELECT create_distributed_table('dist_table', 'a');
    INSERT INTO dist_table VALUES (1, 2), (3, 4), (5, 6);
}

teardown
{
    DROP TABLE IF EXISTS dist_table;
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-undistribute"
{
    SELECT undistribute_table('dist_table');
}

step "s1-commit"
{
    COMMIT;
}

session "s2"

step "s2-undistribute"
{
    SELECT undistribute_table('dist_table');
}

step "s2-insert"
{
    INSERT INTO dist_table VALUES (7, 8), (9, 10);
}

step "s2-select"
{
    SELECT * FROM dist_table ORDER BY 1, 2;
}

step "s2-insert-select"
{
    INSERT INTO dist_table SELECT * FROM dist_table;
}

step "s2-delete"
{
    DELETE FROM dist_table WHERE a = 3;
}

step "s2-copy"
{
    COPY dist_table FROM PROGRAM 'echo 11, 12 && echo 13, 14' WITH CSV;
}

step "s2-drop"
{
    DROP TABLE dist_table;
}

step "s2-truncate"
{
    TRUNCATE dist_table;
}

step "s2-select-for-update"
{
    SELECT * FROM dist_table WHERE a = 5 FOR UPDATE;
}

step "s2-create-index-concurrently"
{
    CREATE INDEX CONCURRENTLY idx ON dist_table (a);
}


permutation "s1-begin" "s1-undistribute" "s2-undistribute" "s1-commit"

permutation "s1-begin" "s1-undistribute" "s2-select" "s1-commit"
permutation "s1-begin" "s1-undistribute" "s2-insert" "s1-commit" "s2-select"
permutation "s1-begin" "s1-undistribute" "s2-insert-select" "s1-commit" "s2-select"
permutation "s1-begin" "s1-undistribute" "s2-delete" "s1-commit" "s2-select"
permutation "s1-begin" "s1-undistribute" "s2-copy" "s1-commit" "s2-select"
permutation "s1-begin" "s1-undistribute" "s2-drop" "s1-commit" "s2-select"
permutation "s1-begin" "s1-undistribute" "s2-truncate" "s1-commit" "s2-select"
permutation "s1-begin" "s1-undistribute" "s2-select-for-update" "s1-commit"
permutation "s1-begin" "s1-undistribute" "s2-create-index-concurrently" "s1-commit"
