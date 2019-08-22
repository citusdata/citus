--
-- SQL_PROCEDURE
--
-- Tests basic PROCEDURE functionality with SQL and PLPGSQL procedures.
--
-- print whether we're using version > 10 to make version-specific tests clear

SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int > 10 AS version_above_ten;

SET citus.next_shard_id TO 100500;

CREATE SCHEMA procedure_schema;
SET SEARCH_PATH = procedure_schema;

CREATE TABLE test_table(id integer , org_id integer);
CREATE UNIQUE INDEX idx_table ON test_table(id, org_id);
SELECT create_distributed_table('test_table','id');

INSERT INTO test_table VALUES(1, 1);


-- test CREATE PROCEDURE
CREATE PROCEDURE test_procedure_delete_insert(id int, org_id int) LANGUAGE SQL AS $$
	DELETE FROM test_table;
	INSERT INTO test_table VALUES(id, org_id);
$$;

CALL test_procedure_delete_insert(2,3);
SELECT * FROM test_table ORDER BY 1, 2;

-- commit/rollback is not allowed in procedures in SQL
-- following calls should fail
CREATE PROCEDURE test_procedure_commit(tt_id int, tt_org_id int) LANGUAGE SQL AS $$
	DELETE FROM test_table;
	COMMIT;
	INSERT INTO test_table VALUES(tt_id, -1);
	UPDATE test_table SET org_id = tt_org_id WHERE id = tt_id;
	COMMIT;
$$;

CALL test_procedure_commit(2,5);
SELECT * FROM test_table ORDER BY 1, 2;

CREATE PROCEDURE test_procedure_rollback(tt_id int, tt_org_id int) LANGUAGE SQL AS $$
	DELETE FROM test_table;
	ROLLBACK;
	UPDATE test_table SET org_id = tt_org_id WHERE id = tt_id;
    COMMIT;
$$;

CALL test_procedure_rollback(2,15);
SELECT * FROM test_table ORDER BY 1, 2;

DROP PROCEDURE test_procedure_delete_insert(int, int);
DROP PROCEDURE test_procedure_commit(int, int);
DROP PROCEDURE test_procedure_rollback(int, int);

-- same tests with plpgsql

-- test CREATE PROCEDURE
CREATE PROCEDURE test_procedure_delete_insert(id int, org_id int) LANGUAGE PLPGSQL AS $$
BEGIN
	DELETE FROM test_table;
	INSERT INTO test_table VALUES(id, org_id);
END;
$$;

CALL test_procedure_delete_insert(2,3);
SELECT * FROM test_table ORDER BY 1, 2;

-- notice that the update succeed and committed
CREATE PROCEDURE test_procedure_modify_insert(tt_id int, tt_org_id int) LANGUAGE PLPGSQL AS $$
BEGIN
	UPDATE test_table SET org_id = tt_org_id WHERE id = tt_id;
	COMMIT;
	INSERT INTO test_table VALUES (tt_id, tt_org_id);
	ROLLBACK;
END;
$$;

CALL test_procedure_modify_insert(2,12);
SELECT * FROM test_table ORDER BY 1, 2;


CREATE PROCEDURE test_procedure_modify_insert_commit(tt_id int, tt_org_id int) LANGUAGE PLPGSQL AS $$
BEGIN
	UPDATE test_table SET org_id = tt_org_id WHERE id = tt_id;
	COMMIT;
	INSERT INTO test_table VALUES (tt_id, tt_org_id);
	COMMIT;
END;
$$;

CALL test_procedure_modify_insert_commit(2,30);
SELECT * FROM test_table ORDER BY 1, 2;

-- delete is commited but insert is rolled back
CREATE PROCEDURE test_procedure_rollback(tt_id int, tt_org_id int) LANGUAGE PLPGSQL AS $$
BEGIN
	DELETE FROM test_table;
	COMMIT;
	INSERT INTO test_table VALUES (tt_id, tt_org_id);
	ROLLBACK;
END;
$$;

CALL test_procedure_rollback(2,5);
SELECT * FROM test_table ORDER BY 1, 2;

-- rollback is successfull when insert is on multiple rows
CREATE PROCEDURE test_procedure_rollback_2(tt_id int, tt_org_id int) LANGUAGE PLPGSQL AS $$
BEGIN
	DELETE FROM test_table;
	COMMIT;
	INSERT INTO test_table VALUES (tt_id, tt_org_id), (tt_id+1, tt_org_id+1);
	ROLLBACK;
END;
$$;

CALL test_procedure_rollback_2(12, 15);
SELECT * FROM test_table ORDER BY 1, 2;

-- delete is rolled back, update is committed
CREATE PROCEDURE test_procedure_rollback_3(tt_id int, tt_org_id int) LANGUAGE PLPGSQL AS $$
BEGIN
	DELETE FROM test_table;
	ROLLBACK;
	UPDATE test_table SET org_id = tt_org_id WHERE id = tt_id;
    COMMIT;
END;
$$;

INSERT INTO test_table VALUES (1, 1), (2, 2);
CALL test_procedure_rollback_3(2,15);
SELECT * FROM test_table ORDER BY 1, 2;

TRUNCATE test_table;

-- nested procedure calls should roll back normally
CREATE OR REPLACE PROCEDURE test_procedure_rollback(tt_id int, tt_org_id int) LANGUAGE PLPGSQL AS $$
BEGIN
    INSERT INTO test_table VALUES (tt_id+12, tt_org_id+12);
    ROLLBACK;
END;
$$;

CREATE OR REPLACE PROCEDURE test_procedure_rollback_2(tt_id int, tt_org_id int) LANGUAGE PLPGSQL AS $$
BEGIN
    INSERT INTO test_table VALUES (tt_id+2, tt_org_id+1);
    ROLLBACK;
END;
$$;

CREATE OR REPLACE PROCEDURE test_procedure(tt_id int, tt_org_id int) LANGUAGE PLPGSQL AS $$
BEGIN
    CALL test_procedure_rollback(tt_id, tt_org_id);
    CALL test_procedure_rollback_2(tt_id, tt_org_id);
    INSERT INTO test_table VALUES (tt_id+100, tt_org_id+100);
    ROLLBACK;
END;
$$;

SELECT * from test_table;
call test_procedure(1,1);
call test_procedure(20, 20);
SELECT * from test_table;

\set VERBOSITY terse
DROP SCHEMA procedure_schema CASCADE;
\set VERBOSITY default

RESET SEARCH_PATH;
