--
-- Testing we materialized views properly
--

SET columnar.compression TO 'none';

CREATE TABLE t(a int, b int) USING columnar;

INSERT INTO t SELECT floor(i / 4), 2 * i FROM generate_series(1, 10) i;

CREATE MATERIALIZED VIEW t_view(a, bsum, cnt) USING columnar AS
   SELECT a, sum(b), count(*) FROM t GROUP BY a;

SELECT * FROM t_view a ORDER BY a;

INSERT INTO t SELECT floor(i / 4), 2 * i FROM generate_series(11, 20) i;

SELECT * FROM t_view a ORDER BY a;

-- show columnar options for materialized view
SELECT * FROM columnar.options
WHERE regclass = 't_view'::regclass;

-- show we can set options on a materialized view
SELECT alter_columnar_table_set('t_view', compression => 'pglz');
SELECT * FROM columnar.options
WHERE regclass = 't_view'::regclass;

REFRESH MATERIALIZED VIEW t_view;

-- verify options have not been changed
SELECT * FROM columnar.options
WHERE regclass = 't_view'::regclass;

SELECT * FROM t_view a ORDER BY a;

-- verify that we have created metadata entries for the materialized view
SELECT columnar_relation_storageid(oid) AS storageid
FROM pg_class WHERE relname='t_view' \gset

SELECT count(*) FROM columnar.columnar_stripes WHERE storageid=:storageid;
SELECT count(*) FROM columnar.columnar_skipnodes WHERE storageid=:storageid;

DROP TABLE t CASCADE;

-- dropping must remove metadata
SELECT count(*) FROM columnar.columnar_stripes WHERE storageid=:storageid;
SELECT count(*) FROM columnar.columnar_skipnodes WHERE storageid=:storageid;
