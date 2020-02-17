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

-- check the table contents
COPY data (value) TO STDOUT WITH (format csv, force_quote *);
COPY dumper."weird.table" ("data.jsonb", "?empty(") TO STDOUT WITH (format csv, force_quote ("?empty("), null 'null', header true);

SELECT indexname FROM pg_indexes WHERE tablename = 'weird.table' ORDER BY indexname;

DROP SCHEMA dumper CASCADE;
