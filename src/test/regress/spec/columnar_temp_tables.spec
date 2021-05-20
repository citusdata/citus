session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-create-temp"
{
    CREATE TEMPORARY TABLE columnar_temp (a int, b text, c int) USING columnar;
}

step "s1-insert"
{
    INSERT INTO columnar_temp VALUES (1, '1', 1);
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

step "s2-create-temp"
{
    CREATE TEMPORARY TABLE columnar_temp (a int, b text, c int) USING columnar;
}

step "s2-insert"
{
    INSERT INTO columnar_temp VALUES (1, '1', 1);
}

step "s2-commit"
{
    COMMIT;
}

// make sure that we allow creating same-named temporary columnar tables in different sessions
// also make sure that they don't block each other
permutation "s1-begin" "s2-begin" "s1-create-temp" "s1-insert" "s2-create-temp" "s2-insert" "s1-commit" "s2-commit"
