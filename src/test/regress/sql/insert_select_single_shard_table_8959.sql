-- Regression for #8959: INSERT ... SELECT into a single-shard target used to
-- hit AddPartitionKeyNotNullFilterToSelect with no partition-key Var, which
-- segfaulted on PG18 with GROUP BY and deparsed to "( IS NOT NULL)" without.
-- Kept out of multi_1_schedule because N-1 runs against older coordinator
-- libraries would still crash on these statements.
CREATE SCHEMA insert_select_single_shard_table_8959;
SET search_path TO insert_select_single_shard_table_8959;
SET citus.shard_replication_factor TO 1;

CREATE TABLE src_8959(k text);
CREATE TABLE dst_8959(k text, created_at timestamptz DEFAULT now());
SELECT create_distributed_table('src_8959', null, colocate_with=>'none');
SELECT create_distributed_table('dst_8959', null, colocate_with=>'src_8959');

INSERT INTO src_8959 VALUES ('a'), ('a'), ('b');

-- GROUP BY variants (crashed)
INSERT INTO dst_8959 (k) SELECT k FROM src_8959 GROUP BY k;
INSERT INTO dst_8959 (k, created_at) SELECT k, now() FROM src_8959 GROUP BY k;
INSERT INTO dst_8959 (k) SELECT k FROM src_8959 WHERE now() > '2020-01-01' GROUP BY k;
INSERT INTO dst_8959 (k) SELECT k FROM src_8959 GROUP BY k HAVING count(*) > 0;

-- No-GROUP-BY variants (deparsed as "( IS NOT NULL)")
INSERT INTO dst_8959 (k) SELECT 'x' FROM src_8959 HAVING count(*) > 0;
INSERT INTO dst_8959 (k) SELECT k FROM src_8959;

SELECT count(*) FROM dst_8959;

SET client_min_messages TO WARNING;
DROP SCHEMA insert_select_single_shard_table_8959 CASCADE;
