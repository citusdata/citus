SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int >= 12 AS have_table_am
\gset

CREATE TEMPORARY TABLE output (line text);

CREATE SCHEMA dumper;
SET search_path TO 'dumper';

SET citus.next_shard_id TO 2900000;
SET citus.shard_replication_factor TO 1;

CREATE TABLE data (
	key int,
	value text
);
SELECT create_distributed_table('data', 'key');

COPY data FROM STDIN WITH (format csv, delimiter '|', escape '\');
1|{"this":"is","json":1}
2|{"$\"":9}
3|{"{}":"	"}
4|{}
\.

-- duplicate the data using pg_dump
\COPY output FROM PROGRAM 'pg_dump --quote-all-identifiers -h localhost -p 57636 -U postgres -d regression -t dumper.data --data-only | psql -tAX -h localhost -p 57636 -U postgres -d regression'

-- data should now appear twice
COPY data TO STDOUT;

\if :have_table_am
CREATE TABLE simple_columnar(i INT, t TEXT) USING columnar;
\else
CREATE TABLE simple_columnar(i INT, t TEXT);
\endif

INSERT INTO simple_columnar VALUES (1, 'one'), (2, 'two');

\if :have_table_am
CREATE TABLE dist_columnar(i INT, t TEXT) USING columnar;
\else
CREATE TABLE dist_columnar(i INT, t TEXT);
\endif

SELECT create_distributed_table('dist_columnar', 'i');

INSERT INTO dist_columnar VALUES (1000, 'one thousand'), (2000, 'two thousand');

-- go crazy with names
CREATE TABLE "weird.table" (
	"key," int primary key,
	"data.jsonb" jsonb,
	"?empty(" text default ''
);
SELECT create_distributed_table('"weird.table"', 'key,');
CREATE INDEX "weird.json_idx" ON "weird.table" USING GIN ("data.jsonb" jsonb_path_ops);

COPY "weird.table" ("key,", "data.jsonb") FROM STDIN WITH (format 'text');
1	{"weird":{"table":"{:"}}
2	{"?\\\"":[]}
\.

-- fast table dump with many options
COPY dumper."weird.table" ("data.jsonb", "?empty(")TO STDOUT WITH (format csv, force_quote ("?empty("), null 'null', delimiter '?', quote '_', header 1);

-- do a full pg_dump of the schema, use some weird quote/escape/delimiter characters to capture the full line
\COPY output FROM PROGRAM 'pg_dump -f results/pg_dump.tmp -h localhost -p 57636 -U postgres -d regression -n dumper --quote-all-identifiers' WITH (format csv, delimiter '|', escape '^', quote '^')

-- drop the schema
DROP SCHEMA dumper CASCADE;

-- recreate the schema
\COPY (SELECT line FROM output WHERE line IS NOT NULL) TO PROGRAM 'psql -qtAX -h localhost -p 57636 -U postgres -d regression -f results/pg_dump.tmp' WITH (format csv, delimiter '|', escape '^', quote '^')

-- redistribute the schema
SELECT create_distributed_table('data', 'key');
SELECT create_distributed_table('"weird.table"', 'key,');
SELECT create_distributed_table('dist_columnar', 'i');

-- check the table contents
COPY data (value) TO STDOUT WITH (format csv, force_quote *);
COPY dumper."weird.table" ("data.jsonb", "?empty(") TO STDOUT WITH (format csv, force_quote ("?empty("), null 'null', header true);

-- If server supports table access methods, check to be sure that the
-- recreated table is still columnar. Otherwise, just return true.
\if :have_table_am
\set is_columnar '(SELECT amname=''columnar'' from pg_am where relam=oid)'
\else
\set is_columnar TRUE
\endif

SELECT :is_columnar AS check_columnar FROM pg_class WHERE oid='simple_columnar'::regclass;

COPY simple_columnar TO STDOUT;

SELECT :is_columnar AS check_columnar FROM pg_class WHERE oid='dist_columnar'::regclass;

COPY dist_columnar TO STDOUT;

SELECT indexname FROM pg_indexes WHERE tablename = 'weird.table' ORDER BY indexname;

DROP SCHEMA dumper CASCADE;
