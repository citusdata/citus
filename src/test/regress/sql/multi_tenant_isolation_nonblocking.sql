--
-- MULTI_TENANT_ISOLATION
--
-- Tests tenant isolation feature
--
ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1230000;

SELECT nextval('pg_catalog.pg_dist_placement_placementid_seq') AS last_placement_id
\gset
ALTER SEQUENCE pg_catalog.pg_dist_placement_placementid_seq RESTART 100000;


CREATE SCHEMA "Tenant Isolation";
SET search_path to "Tenant Isolation";

CREATE ROLE mx_isolation_role_ent WITH LOGIN;
GRANT ALL ON SCHEMA "Tenant Isolation", public TO mx_isolation_role_ent;

-- connect with this new role
\c - mx_isolation_role_ent - :master_port
SET search_path to "Tenant Isolation";

SET citus.shard_replication_factor TO 1;
SET citus.shard_count to 2;

CREATE TABLE lineitem_streaming (
	l_orderkey bigint not null,
	l_partkey integer not null,
	l_suppkey integer not null,
	l_linenumber integer not null,
	l_quantity decimal(15, 2) not null,
	l_extendedprice decimal(15, 2) not null,
	l_discount decimal(15, 2) not null,
	l_tax decimal(15, 2) not null,
	l_returnflag char(1) not null,
	l_linestatus char(1) not null,
	l_shipdate date not null,
	l_commitdate date not null,
	l_receiptdate date not null,
	l_shipinstruct char(25) not null,
	l_shipmode char(10) not null,
	l_comment varchar(44) not null);
SELECT create_distributed_table('lineitem_streaming', 'l_orderkey');

CREATE TABLE orders_streaming (
	o_orderkey bigint not null primary key,
	o_custkey integer not null,
	o_orderstatus char(1) not null,
	o_totalprice decimal(15,2) not null,
	o_orderdate date not null,
	o_orderpriority char(15) not null,
	o_clerk char(15) not null,
	o_shippriority integer not null,
	o_comment varchar(79) not null);
SELECT create_distributed_table('orders_streaming', 'o_orderkey');

\COPY lineitem_streaming FROM STDIN WITH DELIMITER '|'
99|87114|4639|1|10|11011.10|0.02|0.01|A|F|1994-05-18|1994-06-03|1994-05-23|COLLECT COD|RAIL|kages. requ
99|123766|3767|2|5|8948.80|0.02|0.07|R|F|1994-05-06|1994-05-28|1994-05-20|TAKE BACK RETURN|RAIL|ests cajole fluffily waters. blithe
99|134082|1622|3|42|46875.36|0.02|0.02|A|F|1994-04-19|1994-05-18|1994-04-20|NONE|RAIL|kages are fluffily furiously ir
99|108338|849|4|36|48467.88|0.09|0.02|A|F|1994-07-04|1994-04-17|1994-07-30|DELIVER IN PERSON|AIR|slyly. slyly e
100|62029|2030|1|28|27748.56|0.04|0.05|N|O|1998-05-08|1998-05-13|1998-06-07|COLLECT COD|TRUCK|sts haggle. slowl
100|115979|8491|2|22|43889.34|0.00|0.07|N|O|1998-06-24|1998-04-12|1998-06-29|DELIVER IN PERSON|SHIP|nto beans alongside of the fi
100|46150|8655|3|46|50422.90|0.03|0.04|N|O|1998-05-02|1998-04-10|1998-05-22|TAKE BACK RETURN|SHIP|ular accounts. even
100|38024|3031|4|14|13468.28|0.06|0.03|N|O|1998-05-22|1998-05-01|1998-06-03|COLLECT COD|MAIL|y. furiously ironic ideas gr
100|53439|955|5|37|51519.91|0.05|0.00|N|O|1998-03-06|1998-04-16|1998-03-31|TAKE BACK RETURN|TRUCK|nd the quickly s
101|118282|5816|1|49|63713.72|0.10|0.00|N|O|1996-06-21|1996-05-27|1996-06-29|DELIVER IN PERSON|REG AIR|ts
101|163334|883|2|36|50303.88|0.00|0.01|N|O|1996-05-19|1996-05-01|1996-06-04|DELIVER IN PERSON|AIR|tes. blithely pending dolphins x-ray f
101|138418|5958|3|12|17476.92|0.06|0.02|N|O|1996-03-29|1996-04-20|1996-04-12|COLLECT COD|MAIL|. quickly regular
102|88914|3931|1|37|70407.67|0.06|0.00|N|O|1997-07-24|1997-08-02|1997-08-07|TAKE BACK RETURN|SHIP|ully across the ideas. final deposit
102|169238|6787|2|34|44445.82|0.03|0.08|N|O|1997-08-09|1997-07-28|1997-08-26|TAKE BACK RETURN|SHIP|eposits cajole across
102|182321|4840|3|25|35083.00|0.01|0.01|N|O|1997-07-31|1997-07-24|1997-08-17|NONE|RAIL|bits. ironic accoun
102|61158|8677|4|15|16787.25|0.07|0.07|N|O|1997-06-02|1997-07-13|1997-06-04|DELIVER IN PERSON|SHIP|final packages. carefully even excu
103|194658|2216|1|6|10515.90|0.03|0.05|N|O|1996-10-11|1996-07-25|1996-10-28|NONE|FOB|cajole. carefully ex
103|10426|2928|2|37|49447.54|0.02|0.07|N|O|1996-09-17|1996-07-27|1996-09-20|TAKE BACK RETURN|MAIL|ies. quickly ironic requests use blithely
103|28431|8432|3|23|31266.89|0.01|0.04|N|O|1996-09-11|1996-09-18|1996-09-26|NONE|FOB|ironic accou
103|29022|4027|4|32|30432.64|0.01|0.07|N|O|1996-07-30|1996-08-06|1996-08-04|NONE|RAIL|kages doze. special, regular deposit
-1995148554|112942|2943|1|9|17594.46|0.04|0.04|N|O|1996-08-03|1996-05-31|1996-08-04|DELIVER IN PERSON|TRUCK|c realms print carefully car
-1686493264|15110|113|5|2|2050.22|0.03|0.08|R|F|1994-04-26|1994-03-15|1994-05-15|TAKE BACK RETURN|MAIL|e final, regular requests. carefully
\.

\COPY orders_streaming FROM STDIN WITH DELIMITER '|'
99|890|F|108594.87|1994-03-13|4-NOT SPECIFIED|Clerk#000000973|0|e carefully ironic packages. pending
100|1471|O|198978.27|1998-02-28|4-NOT SPECIFIED|Clerk#000000577|0|heodolites detect slyly alongside of the ent
101|280|O|118448.39|1996-03-17|3-MEDIUM|Clerk#000000419|0|ding accounts above the slyly final asymptote
102|8|O|184806.58|1997-05-09|2-HIGH|Clerk#000000596|0| slyly according to the asymptotes. carefully final packages integrate furious
103|292|O|118745.16|1996-06-20|4-NOT SPECIFIED|Clerk#000000090|0|ges. carefully unusual instructions haggle quickly regular f
-1995148554|142|O|3553.15|1995-05-08|3-MEDIUM|Clerk#000000378|0|nts hinder fluffily ironic instructions. express, express excuses
-1686493264|878|O|177809.13|1997-09-05|3-MEDIUM|Clerk#000000379|0|y final packages. final foxes since the quickly even
\.

ALTER TABLE lineitem_streaming ADD CONSTRAINT test_constraint
	FOREIGN KEY(l_orderkey) REFERENCES orders_streaming(o_orderkey);

-- test failing foreign constraints
\COPY lineitem_streaming FROM STDIN WITH DELIMITER '|'
128|106828|9339|1|38|69723.16|0.06|0.01|A|F|1992-09-01|1992-08-27|1992-10-01|TAKE BACK RETURN|FOB| cajole careful
\.

-- tests for cluster health
SELECT count(*) FROM lineitem_streaming;
SELECT count(*) FROM orders_streaming;

SELECT
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate
FROM
	orders_streaming,
	lineitem_streaming
WHERE
	l_orderkey = o_orderkey
GROUP BY
	l_orderkey,
	o_orderdate
ORDER BY
	revenue DESC,
	o_orderdate;

-- Checks to see if metadata and data are isolated properly. If there are problems in
-- metadata and/or data on workers, these queries should return different results below
-- after tenant isolation operations are applied.
SELECT count(*) FROM lineitem_streaming WHERE l_orderkey = 99;
SELECT count(*) FROM lineitem_streaming WHERE l_orderkey = 100;
SELECT count(*) FROM lineitem_streaming WHERE l_orderkey = 101;
SELECT count(*) FROM lineitem_streaming WHERE l_orderkey = 102;
SELECT count(*) FROM lineitem_streaming WHERE l_orderkey = 103;
SELECT count(*) FROM orders_streaming WHERE o_orderkey = 99;
SELECT count(*) FROM orders_streaming WHERE o_orderkey = 100;
SELECT count(*) FROM orders_streaming WHERE o_orderkey = 101;
SELECT count(*) FROM orders_streaming WHERE o_orderkey = 102;
SELECT count(*) FROM orders_streaming WHERE o_orderkey = 103;

SELECT * FROM pg_dist_shard
	WHERE logicalrelid = 'lineitem_streaming'::regclass OR logicalrelid = 'orders_streaming'::regclass
	ORDER BY shardminvalue::BIGINT, logicalrelid;

-- check without cascade option
SELECT isolate_tenant_to_new_shard('lineitem_streaming', 100, shard_transfer_mode => 'force_logical');

-- check with an input not castable to bigint
SELECT isolate_tenant_to_new_shard('lineitem_streaming', 'abc', 'CASCADE', shard_transfer_mode => 'force_logical');

SELECT isolate_tenant_to_new_shard('lineitem_streaming', 100, 'CASCADE', shard_transfer_mode => 'force_logical');
SELECT isolate_tenant_to_new_shard('lineitem_streaming', 101, 'CASCADE', shard_transfer_mode => 'force_logical');

-- add an explain check to see if we hit the new isolated shard
EXPLAIN (COSTS false) SELECT count(*) FROM lineitem_streaming WHERE l_orderkey = 101;

-- create an MX node
\c - postgres - :master_port
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

\c - mx_isolation_role_ent - :master_port
SET search_path to "Tenant Isolation";

-- test a failing transaction block
BEGIN;
SELECT isolate_tenant_to_new_shard('orders_streaming', 102, 'CASCADE', shard_transfer_mode => 'force_logical');
SELECT isolate_tenant_to_new_shard('lineitem_streaming', 102, 'CASCADE', shard_transfer_mode => 'force_logical');
COMMIT;

-- test a rollback transaction block
BEGIN;
SELECT isolate_tenant_to_new_shard('orders_streaming', 102, 'CASCADE', shard_transfer_mode => 'force_logical');
SELECT isolate_tenant_to_new_shard('orders_streaming', 103, 'CASCADE', shard_transfer_mode => 'force_logical');
ROLLBACK;

-- test a succesfull transaction block
BEGIN;
SELECT isolate_tenant_to_new_shard('orders_streaming', 102, 'CASCADE', shard_transfer_mode => 'force_logical');
COMMIT;

SELECT isolate_tenant_to_new_shard('orders_streaming', 103, 'CASCADE', shard_transfer_mode => 'force_logical');
SELECT isolate_tenant_to_new_shard('lineitem_streaming', 100, 'CASCADE', shard_transfer_mode => 'force_logical');
SELECT isolate_tenant_to_new_shard('orders_streaming', 101, 'CASCADE', shard_transfer_mode => 'force_logical');

SELECT public.wait_for_resource_cleanup();

-- test corner cases: hash(-1995148554) = -2147483648 and hash(-1686493264) = 2147483647
SELECT isolate_tenant_to_new_shard('lineitem_streaming', -1995148554, 'CASCADE', shard_transfer_mode => 'force_logical');
SELECT isolate_tenant_to_new_shard('orders_streaming', -1686493264, 'CASCADE', shard_transfer_mode => 'force_logical');

SELECT public.wait_for_resource_cleanup();

SELECT count(*) FROM orders_streaming WHERE o_orderkey = -1995148554;
SELECT count(*) FROM orders_streaming WHERE o_orderkey = -1686493264;

-- tests for cluster health
SELECT count(*) FROM lineitem_streaming;
SELECT count(*) FROM orders_streaming;

SELECT
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate
FROM
	orders_streaming,
	lineitem_streaming
WHERE
	l_orderkey = o_orderkey
GROUP BY
	l_orderkey,
	o_orderdate
ORDER BY
	revenue DESC,
	o_orderdate;

SELECT count(*) FROM lineitem_streaming WHERE l_orderkey = 99;
SELECT count(*) FROM lineitem_streaming WHERE l_orderkey = 100;
SELECT count(*) FROM lineitem_streaming WHERE l_orderkey = 101;
SELECT count(*) FROM lineitem_streaming WHERE l_orderkey = 102;
SELECT count(*) FROM lineitem_streaming WHERE l_orderkey = 103;
SELECT count(*) FROM orders_streaming WHERE o_orderkey = 99;
SELECT count(*) FROM orders_streaming WHERE o_orderkey = 100;
SELECT count(*) FROM orders_streaming WHERE o_orderkey = 101;
SELECT count(*) FROM orders_streaming WHERE o_orderkey = 102;
SELECT count(*) FROM orders_streaming WHERE o_orderkey = 103;

SELECT * FROM pg_dist_shard
	WHERE logicalrelid = 'lineitem_streaming'::regclass OR logicalrelid = 'orders_streaming'::regclass
	ORDER BY shardminvalue::BIGINT, logicalrelid;

SELECT * FROM pg_dist_shard_placement WHERE shardid BETWEEN 1230000 AND 1399999 ORDER BY nodeport, shardid;

-- test failing foreign constraints after multiple tenant isolation
\COPY lineitem_streaming FROM STDIN WITH DELIMITER '|'
128|106828|9339|1|38|69723.16|0.06|0.01|A|F|1992-09-01|1992-08-27|1992-10-01|TAKE BACK RETURN|FOB| cajole careful
\.

\c - postgres - :master_port
SELECT public.wait_for_resource_cleanup();

-- connect to the worker node with metadata
\c - mx_isolation_role_ent - :worker_1_port
SET search_path to "Tenant Isolation";

-- check mx tables
SELECT count(*) FROM lineitem_streaming;
SELECT count(*) FROM orders_streaming;

SELECT
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate
FROM
	orders_streaming,
	lineitem_streaming
WHERE
	l_orderkey = o_orderkey
GROUP BY
	l_orderkey,
	o_orderdate
ORDER BY
	revenue DESC,
	o_orderdate;

-- check shards
SET citus.override_table_visibility TO false;
\d

\c - postgres - :worker_1_port
SET search_path to "Tenant Isolation";
SELECT "Column", "Type", "Modifiers" FROM public.table_desc WHERE relid='orders_streaming_1230039'::regclass;

\c - mx_isolation_role_ent - :worker_1_port
SET search_path to "Tenant Isolation";

-- check MX metadata
SELECT * FROM pg_dist_shard
	WHERE logicalrelid = 'lineitem_streaming'::regclass OR logicalrelid = 'orders_streaming'::regclass
	ORDER BY shardminvalue::BIGINT, logicalrelid;

-- return to master node
\c - mx_isolation_role_ent - :master_port

-- test a distribution type which does not have a sql hash function
SET search_path to "Tenant Isolation";

SET citus.shard_replication_factor TO 2;
SET citus.shard_count to 2;

CREATE TABLE lineitem_date (
	l_orderkey bigint not null,
	l_partkey integer not null,
	l_suppkey integer not null,
	l_linenumber integer not null,
	l_quantity decimal(15, 2) not null,
	l_extendedprice decimal(15, 2) not null,
	l_discount decimal(15, 2) not null,
	l_tax decimal(15, 2) not null,
	l_returnflag char(1) not null,
	l_linestatus char(1) not null,
	l_shipdate date not null,
	l_commitdate date not null,
	l_receiptdate date not null,
	l_shipinstruct char(25) not null,
	l_shipmode char(10) not null,
	l_comment varchar(44) not null);
SELECT create_distributed_table('lineitem_date', 'l_shipdate');

\COPY lineitem_date FROM STDIN WITH DELIMITER '|'
390|106523|9034|1|10|15295.20|0.02|0.05|N|O|1998-05-26|1998-07-06|1998-06-23|TAKE BACK RETURN|SHIP| requests. final accounts x-ray beside the
1347|112077|4589|4|28|30493.96|0.01|0.00|N|O|1997-07-30|1997-07-22|1997-08-18|TAKE BACK RETURN|FOB|foxes after the blithely special i
1794|116434|1457|5|47|68170.21|0.10|0.06|N|O|1998-01-15|1997-11-30|1998-02-14|DELIVER IN PERSON|TRUCK| haggle slyly. furiously express orbit
1859|74969|4970|1|18|34991.28|0.10|0.00|N|O|1997-08-08|1997-06-30|1997-08-26|TAKE BACK RETURN|SHIP|e carefully a
\.

SELECT count(*) FROM lineitem_date;
SELECT count(*) FROM lineitem_date WHERE l_shipdate = '1998-05-26';
SELECT count(*) FROM lineitem_date WHERE l_shipdate = '1997-07-30';
SELECT count(*) FROM lineitem_date WHERE l_shipdate = '1998-01-15';
SELECT count(*) FROM lineitem_date WHERE l_shipdate = '1997-08-08';

SELECT isolate_tenant_to_new_shard('lineitem_date', '1998-05-26', shard_transfer_mode => 'force_logical');
SELECT isolate_tenant_to_new_shard('lineitem_date', '1997-07-30', shard_transfer_mode => 'force_logical');
SELECT isolate_tenant_to_new_shard('lineitem_date', '1998-01-15', shard_transfer_mode => 'force_logical');

SELECT count(*) FROM lineitem_date;
SELECT count(*) FROM lineitem_date WHERE l_shipdate = '1998-05-26';
SELECT count(*) FROM lineitem_date WHERE l_shipdate = '1997-07-30';
SELECT count(*) FROM lineitem_date WHERE l_shipdate = '1998-01-15';
SELECT count(*) FROM lineitem_date WHERE l_shipdate = '1997-08-08';

-- test with text distribution column (because of collations)
SET citus.shard_replication_factor TO 1;
CREATE TABLE text_column (tenant_id text, value jsonb);
INSERT INTO text_column VALUES ('hello','{}');
SELECT create_distributed_table('text_column','tenant_id');
SELECT isolate_tenant_to_new_shard('text_column', 'hello', shard_transfer_mode => 'force_logical');
SELECT * FROM text_column;
SELECT public.wait_for_resource_cleanup();

\c - mx_isolation_role_ent - :master_port
SET search_path to "Tenant Isolation";
DROP TABLE lineitem_date;

-- test on append distributed table
CREATE TABLE test_append (
	tenant_id integer
);

SELECT create_distributed_table('test_append', 'tenant_id', 'append');
SELECT isolate_tenant_to_new_shard('test_append', 100, shard_transfer_mode => 'force_logical');

-- check metadata for comparison
SELECT * FROM pg_dist_shard
	WHERE logicalrelid = 'lineitem_streaming'::regclass OR logicalrelid = 'orders_streaming'::regclass
	ORDER BY shardminvalue::BIGINT, logicalrelid;

\c - postgres - :master_port
SELECT public.wait_for_resource_cleanup();

-- test failure scenarios with triggers on workers
\c - postgres - :worker_1_port
SET search_path to "Tenant Isolation";

SET citus.enable_metadata_sync TO OFF;
CREATE OR REPLACE FUNCTION abort_any_command()
  RETURNS event_trigger
 LANGUAGE plpgsql
  AS $$
BEGIN
  RAISE EXCEPTION 'command % is disabled', tg_tag;
END;
$$;
RESET citus.enable_metadata_sync;

CREATE EVENT TRIGGER abort_ddl ON ddl_command_end
   EXECUTE PROCEDURE abort_any_command();

SET citus.override_table_visibility TO false;
\d

\c - mx_isolation_role_ent - :master_port
SET search_path to "Tenant Isolation";

\set VERBOSITY terse
SELECT isolate_tenant_to_new_shard('orders_streaming', 104, 'CASCADE', shard_transfer_mode => 'force_logical');

\set VERBOSITY default

\c - postgres - :worker_1_port
SET search_path to "Tenant Isolation";

SET citus.override_table_visibility TO false;
\d

DROP EVENT TRIGGER abort_ddl;

\c - mx_isolation_role_ent - :master_port
SET search_path to "Tenant Isolation";

-- tests for cluster health
SELECT count(*) FROM lineitem_streaming;
SELECT count(*) FROM orders_streaming;

SELECT
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate
FROM
	orders_streaming,
	lineitem_streaming
WHERE
	l_orderkey = o_orderkey
GROUP BY
	l_orderkey,
	o_orderdate
ORDER BY
	revenue DESC,
	o_orderdate;

SELECT count(*) FROM lineitem_streaming WHERE l_orderkey = 99;
SELECT count(*) FROM lineitem_streaming WHERE l_orderkey = 100;
SELECT count(*) FROM lineitem_streaming WHERE l_orderkey = 101;
SELECT count(*) FROM lineitem_streaming WHERE l_orderkey = 102;
SELECT count(*) FROM lineitem_streaming WHERE l_orderkey = 103;
SELECT count(*) FROM orders_streaming WHERE o_orderkey = 99;
SELECT count(*) FROM orders_streaming WHERE o_orderkey = 100;
SELECT count(*) FROM orders_streaming WHERE o_orderkey = 101;
SELECT count(*) FROM orders_streaming WHERE o_orderkey = 102;
SELECT count(*) FROM orders_streaming WHERE o_orderkey = 103;

-- test composite types with tenant isolation
set search_path to default;

\c - postgres - :worker_1_port
SET search_path to "Tenant Isolation", public, pg_catalog;

-- ... create a test HASH function. Though it is a poor hash function,
-- it is acceptable for our tests
SET citus.enable_metadata_sync TO OFF;
CREATE FUNCTION test_composite_type_hash(test_composite_type) RETURNS int
AS 'SELECT hashtext( ($1.i + $1.i2)::text);'
LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT;
RESET citus.enable_metadata_sync;

CREATE OPERATOR CLASS cats_op_fam_class
DEFAULT FOR TYPE test_composite_type USING HASH AS
OPERATOR 1 = (test_composite_type, test_composite_type),
FUNCTION 1 test_composite_type_hash(test_composite_type);

\c - - - :worker_2_port
SET search_path to "Tenant Isolation", public, pg_catalog;

-- ... create a test HASH function. Though it is a poor hash function,
-- it is acceptable for our tests
SET citus.enable_metadata_sync TO OFF;
CREATE FUNCTION test_composite_type_hash(test_composite_type) RETURNS int
AS 'SELECT hashtext( ($1.i + $1.i2)::text);'
LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT;
RESET citus.enable_metadata_sync;

CREATE OPERATOR CLASS cats_op_fam_class
DEFAULT FOR TYPE test_composite_type USING HASH AS
OPERATOR 1 = (test_composite_type, test_composite_type),
FUNCTION 1 test_composite_type_hash(test_composite_type);

\c - mx_isolation_role_ent - :master_port
SET search_path to "Tenant Isolation", public, pg_catalog;

CREATE TABLE composite_table (
	composite_key test_composite_type);

SELECT create_distributed_table('composite_table', 'composite_key');

INSERT INTO composite_table VALUES  ('(1, 2)'::test_composite_type);
INSERT INTO composite_table VALUES  ('(1, 3)'::test_composite_type);
INSERT INTO composite_table VALUES  ('(1, 4)'::test_composite_type);

SELECT isolate_tenant_to_new_shard('composite_table', '(1, 3)', shard_transfer_mode => 'force_logical');

SELECT count(*) FROM composite_table WHERE composite_key = '(1, 2)'::test_composite_type;
SELECT count(*) FROM composite_table WHERE composite_key = '(1, 3)'::test_composite_type;
SELECT count(*) FROM composite_table WHERE composite_key = '(1, 4)'::test_composite_type;

DROP TABLE composite_table;

-- create foreign keys from a reference and distributed table
-- to another distributed table
SET search_path to "Tenant Isolation", public, pg_catalog;
SET citus.shard_replication_factor TO 1;
SET citus.shard_count to 8;
CREATE TABLE test_reference_table_fkey(id int PRIMARY KEY);
SELECT create_reference_table('test_reference_table_fkey');

CREATE TABLE test_colocated_table_1(id int PRIMARY KEY, value_1 int, FOREIGN KEY(id) REFERENCES test_colocated_table_1(id));
SELECT create_distributed_table('test_colocated_table_1', 'id', colocate_with => 'NONE');

CREATE TABLE test_colocated_table_2(id int PRIMARY KEY, value_1 int, FOREIGN KEY(value_1) REFERENCES test_reference_table_fkey(id), FOREIGN KEY(id) REFERENCES test_colocated_table_1(id));
SELECT create_distributed_table('test_colocated_table_2', 'id', colocate_with => 'test_colocated_table_1');

CREATE TABLE test_colocated_table_3(id int PRIMARY KEY, value_1 int, FOREIGN KEY(value_1) REFERENCES test_reference_table_fkey(id), FOREIGN KEY(id) REFERENCES test_colocated_table_1(id), FOREIGN KEY(id) REFERENCES test_colocated_table_2(id));
SELECT create_distributed_table('test_colocated_table_3', 'id', colocate_with => 'test_colocated_table_1');

CREATE TABLE test_colocated_table_no_rep_identity(id int, value_1 int, FOREIGN KEY(value_1) REFERENCES test_reference_table_fkey(id), FOREIGN KEY(id) REFERENCES test_colocated_table_1(id), FOREIGN KEY(id) REFERENCES test_colocated_table_2(id));
SELECT create_distributed_table('test_colocated_table_no_rep_identity', 'id', colocate_with => 'test_colocated_table_1');

INSERT INTO test_reference_table_fkey SELECT i FROM generate_series (0, 100) i;
INSERT INTO test_colocated_table_1 SELECT i, i FROM generate_series (0, 100) i;
INSERT INTO test_colocated_table_2 SELECT i, i FROM generate_series (0, 100) i;
INSERT INTO test_colocated_table_3 SELECT i, i FROM generate_series (0, 100) i;
INSERT INTO test_colocated_table_no_rep_identity SELECT i, i FROM generate_series (0, 100) i;

-- show that we donot support tenant isolation if the table has a colocated table with no replica identity and shard_transfer_mode=auto
SELECT isolate_tenant_to_new_shard('test_colocated_table_2', 1, 'CASCADE', shard_transfer_mode => 'auto');

-- show that we can isolate it after removing the colocated table with no replica identity
DROP TABLE test_colocated_table_no_rep_identity;
SELECT isolate_tenant_to_new_shard('test_colocated_table_2', 1, 'CASCADE', shard_transfer_mode => 'auto');

SELECT count(*) FROM test_colocated_table_2;

\c - postgres - :master_port
SELECT public.wait_for_resource_cleanup();

\c - postgres - :worker_1_port

-- show the foreign keys of the main table & its colocated shard on other tables
SELECT tbl.relname, fk."Constraint", fk."Definition"
FROM pg_catalog.pg_class tbl
JOIN public.table_fkeys fk on tbl.oid = fk.relid
WHERE tbl.relname like 'test_colocated_table_%'
ORDER BY 1, 2;

\c - mx_isolation_role_ent - :master_port

SET search_path to "Tenant Isolation";

--
-- Make sure that isolate_tenant_to_new_shard() replicats reference tables
--


CREATE TABLE ref_table(a int);
SELECT create_reference_table('ref_table');

\c - postgres - :master_port
SET search_path to "Tenant Isolation";

-- partitioning tests
-- create partitioned table
CREATE TABLE partitioning_test(id int, time date) PARTITION BY RANGE (time);

-- create a regular partition
CREATE TABLE partitioning_test_2009 PARTITION OF partitioning_test FOR VALUES FROM ('2009-01-01') TO ('2010-01-01');
-- create a columnar partition
CREATE TABLE partitioning_test_2010 PARTITION OF partitioning_test FOR VALUES FROM ('2010-01-01') TO ('2011-01-01') USING columnar;

-- load some data and distribute tables
INSERT INTO partitioning_test VALUES (1, '2009-06-06');
INSERT INTO partitioning_test VALUES (2, '2010-07-07');

INSERT INTO partitioning_test_2009 VALUES (3, '2009-09-09');
INSERT INTO partitioning_test_2010 VALUES (4, '2010-03-03');

-- distribute partitioned table
SET citus.shard_replication_factor TO 1;
SELECT create_distributed_table('partitioning_test', 'id');

SELECT count(*) FROM pg_dist_shard WHERE logicalrelid = 'partitioning_test'::regclass;
SELECT count(*) FROM partitioning_test;

-- isolate a value into its own shard
SELECT 1 FROM isolate_tenant_to_new_shard('partitioning_test', 2, 'CASCADE', shard_transfer_mode => 'force_logical');

SELECT count(*) FROM pg_dist_shard WHERE logicalrelid = 'partitioning_test'::regclass;
SELECT count(*) FROM partitioning_test;


SET client_min_messages TO WARNING;

SELECT 1 FROM master_add_node('localhost', :master_port, groupId=>0);

SELECT count(*) FROM pg_dist_shard NATURAL JOIN pg_dist_shard_placement WHERE logicalrelid = 'ref_table'::regclass;

\c - mx_isolation_role_ent - :master_port
SET search_path to "Tenant Isolation";

SELECT 1 FROM isolate_tenant_to_new_shard('test_colocated_table_2', 2, 'CASCADE', shard_transfer_mode => 'force_logical');

SELECT count(*) FROM pg_dist_shard NATURAL JOIN pg_dist_shard_placement WHERE logicalrelid = 'ref_table'::regclass;

\c - postgres - :master_port
SELECT 1 FROM master_remove_node('localhost', :master_port);
SET client_min_messages TO WARNING;
DROP SCHEMA "Tenant Isolation" CASCADE;
REVOKE ALL ON SCHEMA public FROM mx_isolation_role_ent;
DROP ROLE mx_isolation_role_ent;

-- stop  & resync and stop syncing metadata
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);

-- restart metadata sync for rest of the tests
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

-- make sure there are no tables with non-zero colocationid
SELECT count(*) FROM pg_catalog.pg_dist_partition WHERE colocationid > 0;
TRUNCATE TABLE pg_catalog.pg_dist_colocation;
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART 100;

ALTER SEQUENCE pg_catalog.pg_dist_placement_placementid_seq RESTART :last_placement_id;

SELECT 	citus_set_coordinator_host('localhost');

