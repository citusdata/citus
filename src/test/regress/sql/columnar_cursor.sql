--
-- Testing cursors on columnar tables.
--

CREATE TABLE test_cursor (a int, b int) USING columnar;
INSERT INTO test_cursor SELECT i, j FROM generate_series(0, 100)i, generate_series(100, 200)j;

-- A case where the WHERE clause might filter out some chunks
EXPLAIN (analyze on, costs off, timing off, summary off) SELECT * FROM test_cursor WHERE a = 25;
BEGIN;
DECLARE a_25 SCROLL CURSOR
FOR SELECT * FROM test_cursor WHERE a = 25 ORDER BY 2;
FETCH 3 FROM a_25;
FETCH PRIOR FROM a_25;
FETCH NEXT FROM a_25;
FETCH NEXT FROM a_25;
FETCH RELATIVE -2 FROM a_25;
FETCH LAST FROM a_25;
FETCH RELATIVE -25 FROM a_25;
MOVE a_25;
FETCH a_25;
MOVE LAST FROM a_25;
FETCH a_25;
MOVE RELATIVE -3 FROM a_25;
FETCH a_25;
MOVE RELATIVE -3 FROM a_25;
FETCH a_25;
MOVE FORWARD 2 FROM a_25;
FETCH a_25;
MOVE RELATIVE -3 FROM a_25;
FETCH a_25;
UPDATE test_cursor SET a = 8000 WHERE CURRENT OF a_25;
COMMIT;

-- A case where the WHERE clause doesn't filter out any chunks
EXPLAIN (analyze on, costs off, timing off, summary off) SELECT * FROM test_cursor WHERE a > 25;
BEGIN;
DECLARE a_25 SCROLL CURSOR
FOR SELECT * FROM test_cursor WHERE a > 25 ORDER BY 1, 2;
FETCH 3 FROM a_25;
FETCH PRIOR FROM a_25;
FETCH NEXT FROM a_25;
FETCH NEXT FROM a_25;
FETCH RELATIVE -2 FROM a_25;
FETCH LAST FROM a_25;
FETCH RELATIVE -25 FROM a_25;
MOVE a_25;
FETCH a_25;
MOVE LAST FROM a_25;
FETCH a_25;
MOVE RELATIVE -3 FROM a_25;
FETCH a_25;
MOVE RELATIVE -3 FROM a_25;
FETCH a_25;
MOVE FORWARD 2 FROM a_25;
FETCH a_25;
MOVE RELATIVE -3 FROM a_25;
FETCH a_25;
UPDATE test_cursor SET a = 8000 WHERE CURRENT OF a_25;
COMMIT;

DROP TABLE test_cursor CASCADE;
