--
-- MULTI_MX_REPARTITION_UDT_PREPARE
--

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 535000;

-- START type creation

CREATE TYPE test_udt AS (i integer, i2 integer);

-- ... as well as a function to use as its comparator...
CREATE FUNCTION equal_test_udt_function(test_udt, test_udt) RETURNS boolean
AS 'select $1.i = $2.i AND $1.i2 = $2.i2;'
LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT;

-- ... use that function to create a custom equality operator...
CREATE OPERATOR = (
    LEFTARG = test_udt,
    RIGHTARG = test_udt,
    PROCEDURE = equal_test_udt_function,
    COMMUTATOR = =,
    HASHES
);

-- ... and create a custom operator family for hash indexes...
CREATE OPERATOR FAMILY tudt_op_fam USING hash;

-- ... create a test HASH function. Though it is a poor hash function,
-- it is acceptable for our tests
CREATE FUNCTION test_udt_hash(test_udt) RETURNS int
AS 'SELECT hashtext( ($1.i + $1.i2)::text);'
LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT;


-- We need to define two different operator classes for the composite types
-- One uses BTREE the other uses HASH
CREATE OPERATOR CLASS tudt_op_fam_clas3
DEFAULT FOR TYPE test_udt USING BTREE AS
OPERATOR 3 = (test_udt, test_udt);

CREATE OPERATOR CLASS tudt_op_fam_class
DEFAULT FOR TYPE test_udt USING HASH AS
OPERATOR 1 = (test_udt, test_udt),
FUNCTION 1 test_udt_hash(test_udt);

-- END type creation

CREATE TABLE repartition_udt (
    pk integer not null,
    udtcol test_udt,
    txtcol text
);

CREATE TABLE repartition_udt_other (
    pk integer not null,
    udtcol test_udt,
    txtcol text
);

-- Connect directly to a worker, create and drop the type, then
-- proceed with type creation as above; thus the OIDs will be different.
-- so that the OID is off.

\c - - - :worker_1_port

-- START type creation
-- ... as well as a function to use as its comparator...
CREATE FUNCTION equal_test_udt_function(test_udt, test_udt) RETURNS boolean
AS 'select $1.i = $2.i AND $1.i2 = $2.i2;'
LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT;

-- ... use that function to create a custom equality operator...
CREATE OPERATOR = (
    LEFTARG = test_udt,
    RIGHTARG = test_udt,
    PROCEDURE = equal_test_udt_function,
    COMMUTATOR = =,
    HASHES
);

-- ... and create a custom operator family for hash indexes...
CREATE OPERATOR FAMILY tudt_op_fam USING hash;

-- ... create a test HASH function. Though it is a poor hash function,
-- it is acceptable for our tests
CREATE FUNCTION test_udt_hash(test_udt) RETURNS int
AS 'SELECT hashtext( ($1.i + $1.i2)::text);'
LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT;


-- We need to define two different operator classes for the composite types
-- One uses BTREE the other uses HASH
CREATE OPERATOR CLASS tudt_op_fam_clas3
DEFAULT FOR TYPE test_udt USING BTREE AS
OPERATOR 3 = (test_udt, test_udt);

CREATE OPERATOR CLASS tudt_op_fam_class
DEFAULT FOR TYPE test_udt USING HASH AS
OPERATOR 1 = (test_udt, test_udt),
FUNCTION 1 test_udt_hash(test_udt);

-- END type creation

\c - - - :worker_2_port

-- START type creation
-- ... as well as a function to use as its comparator...
CREATE FUNCTION equal_test_udt_function(test_udt, test_udt) RETURNS boolean
AS 'select $1.i = $2.i AND $1.i2 = $2.i2;'
LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT;

-- ... use that function to create a custom equality operator...
CREATE OPERATOR = (
    LEFTARG = test_udt,
    RIGHTARG = test_udt,
    PROCEDURE = equal_test_udt_function,
    COMMUTATOR = =,
    HASHES
);

-- ... and create a custom operator family for hash indexes...
CREATE OPERATOR FAMILY tudt_op_fam USING hash;

-- ... create a test HASH function. Though it is a poor hash function,
-- it is acceptable for our tests
CREATE FUNCTION test_udt_hash(test_udt) RETURNS int
AS 'SELECT hashtext( ($1.i + $1.i2)::text);'
LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT;


-- We need to define two different operator classes for the composite types
-- One uses BTREE the other uses HASH
CREATE OPERATOR CLASS tudt_op_fam_clas3
DEFAULT FOR TYPE test_udt USING BTREE AS
OPERATOR 3 = (test_udt, test_udt);

CREATE OPERATOR CLASS tudt_op_fam_class
DEFAULT FOR TYPE test_udt USING HASH AS
OPERATOR 1 = (test_udt, test_udt),
FUNCTION 1 test_udt_hash(test_udt);

-- END type creation

-- Connect to master

\c - - - :master_port

-- Distribute and populate the two tables.
SET citus.shard_replication_factor TO 1;
SET citus.replication_model TO streaming;

SET citus.shard_count TO 3;
SELECT create_distributed_table('repartition_udt', 'pk');

SET citus.shard_count TO 5;
SELECT create_distributed_table('repartition_udt_other', 'pk');

INSERT INTO repartition_udt values (1, '(1,1)'::test_udt, 'foo');
INSERT INTO repartition_udt values (2, '(1,2)'::test_udt, 'foo');
INSERT INTO repartition_udt values (3, '(1,3)'::test_udt, 'foo');
INSERT INTO repartition_udt values (4, '(2,1)'::test_udt, 'foo');
INSERT INTO repartition_udt values (5, '(2,2)'::test_udt, 'foo');
INSERT INTO repartition_udt values (6, '(2,3)'::test_udt, 'foo');

INSERT INTO repartition_udt_other values (7, '(1,1)'::test_udt, 'foo');
INSERT INTO repartition_udt_other values (8, '(1,2)'::test_udt, 'foo');
INSERT INTO repartition_udt_other values (9, '(1,3)'::test_udt, 'foo');
INSERT INTO repartition_udt_other values (10, '(2,1)'::test_udt, 'foo');
INSERT INTO repartition_udt_other values (11, '(2,2)'::test_udt, 'foo');
INSERT INTO repartition_udt_other values (12, '(2,3)'::test_udt, 'foo');

SET client_min_messages = LOG;
SET citus.task_executor_type = 'task-tracker';

-- Query that should result in a repartition
-- join on int column, and be empty.
SELECT * FROM repartition_udt JOIN repartition_udt_other
    ON repartition_udt.pk = repartition_udt_other.pk;

-- Query that should result in a repartition join on UDT column.
SET citus.log_multi_join_order = true;

EXPLAIN SELECT * FROM repartition_udt JOIN repartition_udt_other
    ON repartition_udt.udtcol = repartition_udt_other.udtcol
    WHERE repartition_udt.pk > 1;

SELECT * FROM repartition_udt JOIN repartition_udt_other
    ON repartition_udt.udtcol = repartition_udt_other.udtcol
    WHERE repartition_udt.pk > 1
    ORDER BY repartition_udt.pk;

\c - - - :worker_1_port
\c - - - :worker_2_port
