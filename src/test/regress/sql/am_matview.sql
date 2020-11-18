--
-- Testing we materialized views properly
--

CREATE TABLE t(a int, b int) USING cstore_tableam;

INSERT INTO t SELECT floor(i / 4), 2 * i FROM generate_series(1, 10) i;

CREATE MATERIALIZED VIEW t_view(a, bsum, cnt) USING cstore_tableam AS
   SELECT a, sum(b), count(*) FROM t GROUP BY a;

SELECT * FROM t_view a ORDER BY a;

INSERT INTO t SELECT floor(i / 4), 2 * i FROM generate_series(11, 20) i;

SELECT * FROM t_view a ORDER BY a;

REFRESH MATERIALIZED VIEW t_view;

SELECT * FROM t_view a ORDER BY a;

-- verify that we have created metadata entries for the materialized view
SELECT relfilenode FROM pg_class WHERE relname='t_view' \gset

SELECT count(*) FROM cstore.cstore_data_files WHERE relfilenode=:relfilenode;
SELECT count(*) FROM cstore.cstore_stripes WHERE relfilenode=:relfilenode;
SELECT count(*) FROM cstore.cstore_skipnodes WHERE relfilenode=:relfilenode;

DROP TABLE t CASCADE;

-- dropping must remove metadata
SELECT count(*) FROM cstore.cstore_data_files WHERE relfilenode=:relfilenode;
SELECT count(*) FROM cstore.cstore_stripes WHERE relfilenode=:relfilenode;
SELECT count(*) FROM cstore.cstore_skipnodes WHERE relfilenode=:relfilenode;
