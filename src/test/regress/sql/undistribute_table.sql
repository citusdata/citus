CREATE SCHEMA undistribute_table;
SET search_path TO undistribute_table;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;

CREATE TABLE dist_table (id INT, a INT, b TEXT);
SELECT create_distributed_table('dist_table', 'id');
INSERT INTO dist_table VALUES (1, 2, 'abc'), (2, 3, 'abcd'), (1, 3, 'abc');

SELECT logicalrelid FROM pg_dist_partition WHERE logicalrelid = 'dist_table'::regclass;
SELECT run_command_on_workers($$SELECT COUNT(*) FROM pg_catalog.pg_class WHERE relname LIKE 'dist\_table\_%'$$);
SELECT * FROM dist_table ORDER BY 1, 2, 3;

-- we cannot immediately convert in the same statement, because
-- the name->OID conversion happens at parse time.
SELECT undistribute_table('dist_table'), create_distributed_table('dist_table', 'a');

SELECT undistribute_table('dist_table');

SELECT logicalrelid FROM pg_dist_partition WHERE logicalrelid = 'dist_table'::regclass;
SELECT run_command_on_workers($$SELECT COUNT(*) FROM pg_catalog.pg_class WHERE relname LIKE 'dist\_table\_%'$$);
SELECT * FROM dist_table ORDER BY 1, 2, 3;

DROP TABLE dist_table;

-- test indexes
CREATE TABLE dist_table (id INT, a INT, b TEXT);
SELECT create_distributed_table('dist_table', 'id');
INSERT INTO dist_table VALUES (1, 2, 'abc'), (2, 3, 'abcd'), (1, 3, 'abc');

CREATE INDEX index1 ON dist_table (id);
SELECT * FROM pg_indexes WHERE tablename = 'dist_table';

SELECT undistribute_table('dist_table');

SELECT * FROM pg_indexes WHERE tablename = 'dist_table';

DROP TABLE dist_table;

-- test tables with references
-- we expect errors
CREATE TABLE referenced_table (id INT PRIMARY KEY, a INT, b TEXT);
SELECT create_distributed_table('referenced_table', 'id');
INSERT INTO referenced_table VALUES (1, 2, 'abc'), (2, 3, 'abcd'), (4, 3, 'abc');

CREATE TABLE referencing_table (id INT REFERENCES referenced_table (id), a INT, b TEXT);
SELECT create_distributed_table('referencing_table', 'id');
INSERT INTO referencing_table VALUES (4, 6, 'cba'), (1, 1, 'dcba'), (2, 3, 'aaa');

SELECT undistribute_table('referenced_table');
SELECT undistribute_table('referencing_table');

DROP TABLE referenced_table, referencing_table;

-- test partitioned tables
CREATE TABLE partitioned_table (id INT, a INT) PARTITION BY RANGE (id);
CREATE TABLE partitioned_table_1_5 PARTITION OF partitioned_table FOR VALUES FROM (1) TO (5);
CREATE TABLE partitioned_table_6_10 PARTITION OF partitioned_table FOR VALUES FROM (6) TO (10);
SELECT create_distributed_table('partitioned_table', 'id');
INSERT INTO partitioned_table VALUES (2, 12), (7, 2);

SELECT logicalrelid FROM pg_dist_partition WHERE logicalrelid::regclass::text LIKE 'partitioned\_table%' ORDER BY 1;
SELECT run_command_on_workers($$SELECT COUNT(*) FROM pg_catalog.pg_class WHERE relname SIMILAR TO 'partitioned\_table%\d{3,}'$$);
SELECT inhrelid::regclass FROM pg_catalog.pg_inherits WHERE inhparent = 'partitioned_table'::regclass ORDER BY 1;
SELECT * FROM partitioned_table ORDER BY 1, 2;
SELECT * FROM partitioned_table_1_5 ORDER BY 1, 2;
SELECT * FROM partitioned_table_6_10 ORDER BY 1, 2;

-- undistributing partitions are not supported
SELECT undistribute_table('partitioned_table_1_5');
-- we can undistribute partitioned parent tables
SELECT undistribute_table('partitioned_table');

SELECT logicalrelid FROM pg_dist_partition WHERE logicalrelid::regclass::text LIKE 'partitioned\_table%'  ORDER BY 1;
SELECT run_command_on_workers($$SELECT COUNT(*) FROM pg_catalog.pg_class WHERE relname SIMILAR TO 'partitioned\_table%\d{3,}'$$);
SELECT inhrelid::regclass FROM pg_catalog.pg_inherits WHERE inhparent = 'partitioned_table'::regclass ORDER BY 1;
SELECT * FROM partitioned_table ORDER BY 1, 2;
SELECT * FROM partitioned_table_1_5 ORDER BY 1, 2;
SELECT * FROM partitioned_table_6_10 ORDER BY 1, 2;

DROP TABLE partitioned_table;


-- test tables with sequences
CREATE TABLE seq_table (id INT, a bigserial);
SELECT create_distributed_table('seq_table', 'id');

SELECT objid::regclass AS "Sequence Name" FROM pg_depend WHERE refobjid = 'seq_table'::regclass::oid AND classid = 'pg_class'::regclass::oid;
INSERT INTO seq_table (id) VALUES (5), (9), (3);
SELECT * FROM seq_table ORDER BY a;

SELECT undistribute_table('seq_table');

SELECT objid::regclass AS "Sequence Name" FROM pg_depend WHERE refobjid = 'seq_table'::regclass::oid AND classid = 'pg_class'::regclass::oid;
INSERT INTO seq_table (id) VALUES (7), (1), (8);
SELECT * FROM seq_table ORDER BY a;

DROP TABLE seq_table;


--test tables with views
CREATE TABLE view_table (a int, b int, c int);
SELECT create_distributed_table('view_table', 'a');
INSERT INTO view_table VALUES (1, 2, 3), (2, 4, 6), (3, 6, 9);

CREATE SCHEMA another_schema;

CREATE VIEW undis_view1 AS SELECT a, b FROM view_table table_name_for_view;
CREATE VIEW undis_view2 AS SELECT a, c FROM view_table table_name_for_view;
CREATE VIEW another_schema.undis_view3 AS SELECT b, c FROM undis_view1 JOIN undis_view2 ON undis_view1.a = undis_view2.a;

SELECT schemaname, viewname, viewowner, definition FROM pg_views WHERE viewname LIKE 'undis\_view%' ORDER BY viewname;
SELECT * FROM another_schema.undis_view3 ORDER BY 1, 2;

SELECT undistribute_table('view_table');

SELECT schemaname, viewname, viewowner, definition FROM pg_views WHERE viewname LIKE 'undis\_view%' ORDER BY viewname;
SELECT * FROM another_schema.undis_view3 ORDER BY 1, 2;

-- test the drop in undistribute_table cannot cascade to an extension
CREATE TABLE extension_table (a INT);
SELECT create_distributed_table('extension_table', 'a');
CREATE VIEW extension_view AS SELECT * FROM extension_table;
ALTER EXTENSION plpgsql ADD VIEW extension_view;

SELECT undistribute_table ('extension_table');

-- test the drop that doesn't cascade to an extension
CREATE TABLE dist_type_table (a INT, b citus.distribution_type);
SELECT create_distributed_table('dist_type_table', 'a');

SELECT undistribute_table('dist_type_table');

-- test CREATE RULE without ON SELECT
CREATE TABLE rule_table_3 (a INT);
CREATE TABLE rule_table_4 (a INT);
SELECT create_distributed_table('rule_table_4', 'a');

CREATE RULE "rule_1" AS ON INSERT TO rule_table_3 DO INSTEAD SELECT * FROM rule_table_4;

ALTER EXTENSION plpgsql ADD TABLE rule_table_3;

SELECT undistribute_table('rule_table_4');

ALTER EXTENSION plpgsql DROP VIEW extension_view;
ALTER EXTENSION plpgsql DROP TABLE rule_table_3;

DROP TABLE view_table CASCADE;

DROP SCHEMA undistribute_table, another_schema CASCADE;
