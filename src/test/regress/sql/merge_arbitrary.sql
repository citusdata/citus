SET search_path TO merge_arbitrary_schema;
INSERT INTO target_cj VALUES (1, 'target', 0);
INSERT INTO target_cj VALUES (2, 'target', 0);
INSERT INTO target_cj VALUES (2, 'target', 0);
INSERT INTO target_cj VALUES (3, 'target', 0);

INSERT INTO source_cj1 VALUES (2, 'source-1', 10);
INSERT INTO source_cj2 VALUES (2, 'source-2', 20);

BEGIN;
MERGE INTO target_cj t
USING source_cj1 s1 INNER JOIN source_cj2 s2 ON sid1 = sid2
ON t.tid = sid1 AND t.tid = 2
WHEN MATCHED THEN
        UPDATE SET src = src2
WHEN NOT MATCHED THEN
        DO NOTHING;
SELECT * FROM target_cj ORDER BY 1;
ROLLBACK;

BEGIN;
-- try accessing columns from either side of the source join
MERGE INTO target_cj t
USING source_cj1 s2
        INNER JOIN source_cj2 s1 ON sid1 = sid2 AND val1 = 10
ON t.tid = sid1 AND t.tid = 2
WHEN MATCHED THEN
        UPDATE SET src = src1, val = val2
WHEN NOT MATCHED THEN
        DO NOTHING;
SELECT * FROM target_cj ORDER BY 1;
ROLLBACK;

-- Test PREPARE
PREPARE insert(int, int, int) AS
MERGE INTO prept
USING (SELECT $2, s1, s2 FROM preps WHERE s2 > $3) as foo
ON prept.t1 = foo.s1
WHEN MATCHED THEN
        UPDATE SET t2 = t2 + $1
WHEN NOT MATCHED THEN
        INSERT VALUES(s1, s2);

PREPARE delete(int) AS
MERGE INTO prept
USING preps
ON prept.t1 = preps.s1
WHEN MATCHED AND prept.t2 = $1 THEN
        DELETE
WHEN MATCHED THEN
        UPDATE SET t2 = t2 + 1;

INSERT INTO prept VALUES(100, 0);

INSERT INTO preps VALUES(100, 0);
INSERT INTO preps VALUES(200, 0);

EXECUTE insert(1, 1, -1); EXECUTE delete(0);
EXECUTE insert(1, 1, -1); EXECUTE delete(0);
EXECUTE insert(1, 1, -1); EXECUTE delete(0);
EXECUTE insert(1, 1, -1); EXECUTE delete(0);
EXECUTE insert(1, 1, -1); EXECUTE delete(0);

-- sixth time
EXECUTE insert(1, 1, -1); EXECUTE delete(0);
EXECUTE insert(1, 1, -1); EXECUTE delete(0);

-- Should have the counter as 14 (7 * 2)
SELECT * FROM prept;

-- Test local tables
INSERT INTO s1 VALUES(1, 0); -- Matches DELETE clause
INSERT INTO s1 VALUES(2, 1); -- Matches UPDATE clause
INSERT INTO s1 VALUES(3, 1); -- No Match INSERT clause
INSERT INTO s1 VALUES(4, 1); -- No Match INSERT clause
INSERT INTO s1 VALUES(6, 1); -- No Match INSERT clause

INSERT INTO t1 VALUES(1, 0); -- Will be deleted
INSERT INTO t1 VALUES(2, 0); -- Will be updated
INSERT INTO t1 VALUES(5, 0); -- Will be intact

PREPARE local(int, int) AS
WITH s1_res AS (
        SELECT * FROM s1
)
MERGE INTO t1
        USING s1_res ON (s1_res.id = t1.id)

        WHEN MATCHED AND s1_res.val = $1 THEN
                DELETE
        WHEN MATCHED THEN
                UPDATE SET val = t1.val + $2
        WHEN NOT MATCHED THEN
                INSERT (id, val) VALUES (s1_res.id, s1_res.val);

BEGIN;
EXECUTE local(0, 1);
SELECT * FROM t1 order by id;
ROLLBACK;

BEGIN;
EXECUTE local(0, 1);
ROLLBACK;

BEGIN;
EXECUTE local(0, 1);
ROLLBACK;

BEGIN;
EXECUTE local(0, 1);
ROLLBACK;

BEGIN;
EXECUTE local(0, 1);
ROLLBACK;

-- sixth time
BEGIN;
EXECUTE local(0, 1);
ROLLBACK;

BEGIN;
EXECUTE local(0, 1);
SELECT * FROM t1 order by id;
ROLLBACK;

-- Test prepared statements with repartition
PREPARE merge_repartition_pg(int,int,int,int) as
        MERGE INTO pg_target target
        USING (SELECT id+1+$1 as key, val FROM (SELECT * FROM pg_source UNION SELECT * FROM pg_source WHERE id = $2) as foo) as source
        ON (source.key = target.id AND $3 < 10000)
        WHEN MATCHED THEN UPDATE SET val = (source.key::int+$4)
        WHEN NOT MATCHED THEN INSERT VALUES (source.key, source.val);

PREPARE merge_repartition_citus(int,int,int,int) as
        MERGE INTO citus_target target
        USING (SELECT id+1+$1 as key, val FROM (SELECT * FROM citus_source UNION SELECT * FROM citus_source WHERE id = $2) as foo) as source
        ON (source.key = target.id AND $3 < 10000)
        WHEN MATCHED THEN UPDATE SET val = (source.key::int+$4)
        WHEN NOT MATCHED THEN INSERT VALUES (source.key, source.val);

EXECUTE merge_repartition_pg(1,1,1,1);
EXECUTE merge_repartition_citus(1,1,1,1);

SET client_min_messages = NOTICE;
SELECT compare_data();
RESET client_min_messages;

EXECUTE merge_repartition_pg(1,100,1,1);
EXECUTE merge_repartition_citus(1,100,1,1);

EXECUTE merge_repartition_pg(2,200,1,1);
EXECUTE merge_repartition_citus(2,200,1,1);

EXECUTE merge_repartition_pg(3,300,1,1);
EXECUTE merge_repartition_citus(3,300,1,1);

EXECUTE merge_repartition_pg(4,400,1,1);
EXECUTE merge_repartition_citus(4,400,1,1);

EXECUTE merge_repartition_pg(5,500,1,1);
EXECUTE merge_repartition_citus(5,500,1,1);

-- Sixth time
EXECUTE merge_repartition_pg(6,600,1,6);
EXECUTE merge_repartition_citus(6,600,1,6);

SET client_min_messages = NOTICE;
SELECT compare_data();
RESET client_min_messages;
