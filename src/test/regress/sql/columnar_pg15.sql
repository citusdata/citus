SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int >= 15 AS server_version_ge_15
\gset
\if :server_version_ge_15
\else
\q
\endif

CREATE TABLE alter_am(i int);

INSERT INTO alter_am SELECT generate_series(1,1000000);

SELECT * FROM columnar.options WHERE regclass = 'alter_am'::regclass;
SELECT SUM(i) FROM alter_am;

ALTER TABLE alter_am
  SET ACCESS METHOD columnar,
  SET (columnar.compression = pglz, fillfactor = 20);

SELECT * FROM columnar.options WHERE regclass = 'alter_am'::regclass;
SELECT SUM(i) FROM alter_am;

ALTER TABLE alter_am SET ACCESS METHOD heap;

-- columnar options should be gone
SELECT * FROM columnar.options WHERE regclass = 'alter_am'::regclass;
SELECT SUM(i) FROM alter_am;

-- error: setting columnar options must happen after converting to columnar
ALTER TABLE alter_am
  SET (columnar.stripe_row_limit = 1111),
  SET ACCESS METHOD columnar;

DROP TABLE alter_am;
