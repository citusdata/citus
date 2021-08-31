CREATE SCHEMA modification_correctness;
SET search_path to 'modification_correctness';

CREATE TABLE test(k int, a int, b int, c int unique, d int, e int);
ALTER TABLE test DROP column k;
SELECT create_distributed_table('test', 'c');
ALTER TABLE test DROP column b;

UPDATE test SET a = 5, c = 5;
UPDATE test SET a = 5, c = d, d =3;
UPDATE test SET c = d, d =3;
UPDATE test SET d=c, c = d;
UPDATE test SET e = c, d = 3;
UPDATE test SET c = c;
UPDATE test SET c = c, c = 3;
UPDATE test SET c = c, d = c;
UPDATE test SET c = c, d = 5, e = 3;



INSERT INTO test (c,d) VALUES(3,4) ON CONFLICT(c) DO UPDATE SET c=7;
INSERT INTO test (c,d) VALUES(3,4) ON CONFLICT(c) DO UPDATE SET d=7;
INSERT INTO test (c,d) VALUES(3,4) ON CONFLICT(c) DO UPDATE SET a=7;
INSERT INTO test (d,c) VALUES(3,4) ON CONFLICT(c) DO UPDATE SET c=7;
INSERT INTO test (d,c) VALUES(3,4) ON CONFLICT(c) DO UPDATE SET d=7;
INSERT INTO test (a,c) VALUES(3,4) ON CONFLICT(c) DO UPDATE SET d=7;
INSERT INTO test (c) VALUES(3) ON CONFLICT(c) DO UPDATE SET d=7;
INSERT INTO test (c,a) VALUES(3,4) ON CONFLICT(c) DO UPDATE SET d=7;
INSERT INTO test (c,a) VALUES(3,4) ON CONFLICT(c) DO UPDATE SET c=EXCLUDED.c, d = EXCLUDED.c;
INSERT INTO test (c,a) VALUES(3,4) ON CONFLICT(c) DO UPDATE SET c=EXCLUDED.c, d = EXCLUDED.c, e = EXCLUDED.c;
INSERT INTO test (c,a) VALUES(3,4) ON CONFLICT(c) DO UPDATE SET c=EXCLUDED.c, d = EXCLUDED.c, e = 7;

-- make sure that without fast path planner, we don't get any unexpected errors.
SET citus.enable_fast_path_router_planner to false;
INSERT INTO test (c,a) VALUES(3,4) ON CONFLICT(c) DO UPDATE SET d=7;
INSERT INTO test (c,a) VALUES(3,4) ON CONFLICT(c) DO UPDATE SET c=EXCLUDED.c, d = EXCLUDED.c;
INSERT INTO test (c,a) VALUES(3,4) ON CONFLICT(c) DO UPDATE SET c=EXCLUDED.c, d = EXCLUDED.c, e = EXCLUDED.c;
INSERT INTO test (c,a) VALUES(3,4) ON CONFLICT(c) DO UPDATE SET c=EXCLUDED.c, d = EXCLUDED.c, e = 7;
UPDATE test SET e = c, d = 3;
UPDATE test SET c = c;
UPDATE test SET c = c, c = 3;
UPDATE test SET c = c, d = c;
UPDATE test SET c = c, d = 5, e = 3;
RESET citus.enable_fast_path_router_planner;


PREPARE foo(int,int) AS INSERT INTO test (c,a) VALUES($1,$2) ON CONFLICT(c) DO UPDATE SET c=EXCLUDED.c, d = EXCLUDED.c, e = $1;
EXECUTE foo(1,2);
EXECUTE foo(1,2);
EXECUTE foo(1,2);
EXECUTE foo(1,2);
EXECUTE foo(1,2);
EXECUTE foo(1,2);
EXECUTE foo(1,2);

PREPARE foo1(int, int) AS UPDATE test SET c = c, d = $1, e = $2;
EXECUTE foo1(1,2);
EXECUTE foo1(1,2);
EXECUTE foo1(1,2);
EXECUTE foo1(1,2);
EXECUTE foo1(1,2);
EXECUTE foo1(1,2);
EXECUTE foo1(1,2);

-- we don't get an error because this is something like c = c
UPDATE test SET a = 5,d  = 2, c = 5 FROM (SELECT * FROM test LIMIT 10) t2 WHERE t2.d = test.c  and test.c = 5;
-- we should get an error because c gets 6 -> 5
UPDATE test SET a = 5,d  = 2, c = 5 FROM (SELECT * FROM test LIMIT 10) t2 WHERE t2.d = test.c  and test.c = 6;


DROP SCHEMA modification_correctness CASCADE;
