CREATE SCHEMA subquery_append;
SET search_path TO subquery_append;

CREATE TABLE append_table (key text, value int, extra int default 0);
CREATE INDEX ON append_table (key);

SELECT create_distributed_table('append_table', 'key', 'append');
SELECT master_create_empty_shard('append_table') AS shardid1 \gset
SELECT master_create_empty_shard('append_table') AS shardid2 \gset
SELECT master_create_empty_shard('append_table') AS shardid3 \gset

CREATE TABLE ref_table (value int);
CREATE INDEX ON ref_table (value);
SELECT create_reference_table('ref_table');

COPY append_table (key,value) FROM STDIN WITH (format 'csv', append_to_shard :shardid1);
abc,234
bcd,123
bcd,234
cde,345
def,456
efg,234
\.

COPY append_table (key,value) FROM STDIN WITH (format 'csv', append_to_shard :shardid2);
abc,123
efg,123
hij,123
hij,234
ijk,1
jkl,0
\.

COPY ref_table FROM STDIN WITH CSV;
123
234
345
\.

-- exercise some optimizer pushdown features with subqueries
SELECT count(*) FROM (SELECT random() FROM append_table) u;

SELECT * FROM (SELECT DISTINCT key FROM append_table) sub ORDER BY 1 LIMIT 3;
SELECT DISTINCT key FROM (SELECT key FROM append_table) sub ORDER BY 1 LIMIT 3;

SELECT key, max(v) FROM (SELECT key, value + 1 AS v FROM append_table) sub GROUP BY key ORDER BY 1,2 LIMIT 3;
SELECT v, max(key) FROM (SELECT key, value + 1 AS v FROM append_table) sub GROUP BY v ORDER BY 1,2 LIMIT 3;

SELECT key, row_number() OVER (ORDER BY key, value) FROM (SELECT key, value, random() FROM append_table) sub ORDER BY 1,2 LIMIT 3;
SELECT key, row_number() OVER (PARTITION BY key ORDER BY key,value) FROM (SELECT key, value, random() FROM append_table) sub ORDER BY 1,2 LIMIT 3;

SELECT key, row_number() OVER (ORDER BY key, value) FROM (SELECT key, value, random() FROM append_table) sub ORDER BY 1,2 LIMIT 3;
SELECT key, row_number() OVER (PARTITION BY key ORDER BY key,value) FROM (SELECT key, value, random() FROM append_table) sub ORDER BY 1,2 LIMIT 3;

-- try some joins in subqueries
SELECT key, count(*) FROM (SELECT *, random() FROM append_table a JOIN append_table b USING (key)) u GROUP BY key ORDER BY 1,2 LIMIT 3;
SELECT key, count(*) FROM (SELECT *, random() FROM append_table a JOIN ref_table b USING (value)) u GROUP BY key ORDER BY 1,2 LIMIT 3;
SELECT key, value FROM append_table a WHERE value IN (SELECT * FROM ref_table) ORDER BY 1,2;
SELECT key, value FROM append_table a WHERE key IN (SELECT key FROM append_table WHERE value > 100) ORDER BY 1,2;
SELECT key, value FROM append_table a WHERE value = (SELECT max(value) FROM ref_table) ORDER BY 1,2;
SELECT key, value FROM append_table a WHERE value = (SELECT max(value) FROM ref_table r WHERE a.value = r.value) ORDER BY 1,2;
SELECT key, (SELECT max(value) FROM ref_table r WHERE r.value = a.value) FROM append_table a ORDER BY 1,2;

-- Test delete
BEGIN;
DELETE FROM append_table a WHERE a.value IN (SELECT s FROM generate_series(1,100) s);
SELECT count(*) FROM append_table;
DELETE FROM append_table a USING ref_table r WHERE a.value = r.value;
SELECT count(*) FROM append_table;
DELETE FROM append_table WHERE value < 2;
SELECT count(*) FROM append_table;
DELETE FROM append_table;
SELECT count(*) FROM append_table;
DELETE FROM append_table a USING append_table b WHERE a.key = b.key;
END;

-- Test update
BEGIN;
UPDATE append_table a SET extra = 1 WHERE a.value IN (SELECT s FROM generate_series(1,100) s);
SELECT count(*) FROM append_table WHERE extra = 1;
UPDATE append_table a SET extra = 1 FROM ref_table r WHERE a.value = r.value;
SELECT count(*) FROM append_table WHERE extra = 1;
UPDATE append_table SET extra = 1 WHERE value < 2;
SELECT count(*) FROM append_table WHERE extra = 1;
UPDATE append_table SET extra = 1;
SELECT count(*) FROM append_table WHERE extra = 1;
UPDATE append_table a sET extra = 1 FROM append_table b WHERE a.key = b.key;
END;

SET client_min_messages TO WARNING;
DROP SCHEMA subquery_append CASCADE;
