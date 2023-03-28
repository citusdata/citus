-- ===================================================================
-- test composite type, varchar and enum types
-- create, distribute, INSERT, SELECT and UPDATE
-- ===================================================================


SET citus.next_shard_id TO 530000;

-- Given that other test files depend on the existence of types created in this file,
-- we cannot drop them at the end. Instead, we drop them at the beginning of the test
-- to make this file runnable multiple times via run_test.py.
BEGIN;
  SET LOCAL client_min_messages TO WARNING;
  DROP TYPE IF EXISTS test_composite_type, other_composite_type, bug_status CASCADE;
  DROP OPERATOR FAMILY IF EXISTS cats_op_fam USING hash;
COMMIT;

-- create a custom type...
CREATE TYPE test_composite_type AS (
    i integer,
    i2 integer
);

-- ... as well as a function to use as its comparator...
SELECT run_command_on_coordinator_and_workers($cf$
    CREATE FUNCTION equal_test_composite_type_function(test_composite_type, test_composite_type) RETURNS boolean
    LANGUAGE 'internal'
    AS 'record_eq'
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
$cf$);

SELECT run_command_on_coordinator_and_workers($cf$
    CREATE FUNCTION cmp_test_composite_type_function(test_composite_type, test_composite_type) RETURNS int
    LANGUAGE 'internal'
    AS 'btrecordcmp'
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
$cf$);

-- ... use that function to create a custom equality operator...
SELECT run_command_on_coordinator_and_workers($co$
    CREATE OPERATOR = (
        LEFTARG = test_composite_type,
        RIGHTARG = test_composite_type,
        PROCEDURE = equal_test_composite_type_function,
        HASHES
    );
$co$);

-- ... and create a custom operator family for hash indexes...
CREATE OPERATOR FAMILY cats_op_fam USING hash;

-- ... create a test HASH function. Though it is a poor hash function,
-- it is acceptable for our tests
CREATE FUNCTION test_composite_type_hash(test_composite_type) RETURNS int
AS 'SELECT hashtext( ($1.i + $1.i2)::text);'
LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT;


-- We need to define two different operator classes for the composite types
-- One uses BTREE the other uses HASH
CREATE OPERATOR CLASS cats_op_fam_clas3
DEFAULT FOR TYPE test_composite_type USING BTREE AS
OPERATOR 3 = (test_composite_type, test_composite_type),
FUNCTION 1 cmp_test_composite_type_function(test_composite_type, test_composite_type);

CREATE OPERATOR CLASS cats_op_fam_class
DEFAULT FOR TYPE test_composite_type USING HASH AS
OPERATOR 1 = (test_composite_type, test_composite_type),
FUNCTION 1 test_composite_type_hash(test_composite_type);

-- create and distribute a table on composite type column
CREATE TABLE composite_type_partitioned_table
(
	id integer,
	col test_composite_type
);

SET citus.shard_replication_factor TO 1;
SELECT create_distributed_table('composite_type_partitioned_table', 'col', 'hash');

-- execute INSERT, SELECT and UPDATE queries on composite_type_partitioned_table
INSERT INTO composite_type_partitioned_table VALUES  (1, '(1, 2)'::test_composite_type);
INSERT INTO composite_type_partitioned_table VALUES  (2, '(3, 4)'::test_composite_type);
INSERT INTO composite_type_partitioned_table VALUES  (3, '(5, 6)'::test_composite_type);
INSERT INTO composite_type_partitioned_table VALUES  (4, '(7, 8)'::test_composite_type);
INSERT INTO composite_type_partitioned_table VALUES  (5, '(9, 10)'::test_composite_type);

PREPARE do_insert(int,test_composite_type) AS INSERT INTO composite_type_partitioned_table VALUES ($1,$2);
EXECUTE do_insert(5, '(9,10)');
EXECUTE do_insert(5, '(9,10)');
EXECUTE do_insert(5, '(9,10)');
EXECUTE do_insert(5, '(9,10)');
EXECUTE do_insert(5, '(9,10)');
EXECUTE do_insert(5, '(9,10)');

PREPARE get_id(test_composite_type) AS SELECT min(id) FROM composite_type_partitioned_table WHERE col = $1;
EXECUTE get_id('(9,10)');
EXECUTE get_id('(9,10)');
EXECUTE get_id('(9,10)');
EXECUTE get_id('(9,10)');
EXECUTE get_id('(9,10)');
EXECUTE get_id('(9,10)');


SELECT * FROM composite_type_partitioned_table WHERE col =  '(7, 8)'::test_composite_type;

UPDATE composite_type_partitioned_table SET id = 6 WHERE col =  '(7, 8)'::test_composite_type;

SELECT * FROM composite_type_partitioned_table WHERE col =  '(7, 8)'::test_composite_type;

CREATE TYPE other_composite_type AS (
    i integer,
    i2 integer
);

-- Check that casts are correctly done on partition columns
SELECT run_command_on_coordinator_and_workers($cf$
    CREATE CAST (other_composite_type AS test_composite_type) WITH INOUT AS IMPLICIT;
$cf$);

INSERT INTO composite_type_partitioned_table VALUES (123, '(123, 456)'::other_composite_type);
SELECT * FROM composite_type_partitioned_table WHERE id = 123;

EXPLAIN (ANALYZE TRUE, COSTS FALSE, VERBOSE FALSE, TIMING FALSE, SUMMARY FALSE)
INSERT INTO composite_type_partitioned_table VALUES (123, '(123, 456)'::other_composite_type);

SELECT run_command_on_coordinator_and_workers($cf$
    DROP CAST (other_composite_type as test_composite_type);
$cf$);
SELECT run_command_on_coordinator_and_workers($cf$
    CREATE FUNCTION to_test_composite_type(arg other_composite_type) RETURNS test_composite_type
        AS 'select arg::text::test_composite_type;'
        LANGUAGE SQL
        IMMUTABLE
        RETURNS NULL ON NULL INPUT;
$cf$);

SELECT run_command_on_coordinator_and_workers($cf$
    CREATE CAST (other_composite_type AS test_composite_type) WITH FUNCTION to_test_composite_type(other_composite_type) AS IMPLICIT;
$cf$);
INSERT INTO composite_type_partitioned_table VALUES (456, '(456, 678)'::other_composite_type);

EXPLAIN (ANALYZE TRUE, COSTS FALSE, VERBOSE FALSE, TIMING FALSE, SUMMARY FALSE)
INSERT INTO composite_type_partitioned_table VALUES (123, '(456, 678)'::other_composite_type);



-- create and distribute a table on enum type column
CREATE TYPE bug_status AS ENUM ('new', 'open', 'closed');

CREATE TABLE bugs (
    id integer,
    status bug_status
);

SELECT create_distributed_table('bugs', 'status', 'hash');

-- execute INSERT, SELECT and UPDATE queries on composite_type_partitioned_table
INSERT INTO bugs VALUES  (1, 'new');
INSERT INTO bugs VALUES  (2, 'open');
INSERT INTO bugs VALUES  (3, 'closed');
INSERT INTO bugs VALUES  (4, 'closed');
INSERT INTO bugs VALUES  (5, 'open');

SELECT * FROM bugs WHERE status =  'closed'::bug_status;

UPDATE bugs SET status = 'closed'::bug_status WHERE id = 2;

SELECT * FROM bugs WHERE status = 'open'::bug_status;

-- create and distribute a table on varchar column
CREATE TABLE varchar_hash_partitioned_table
(
	id int,
    name varchar
);

SELECT create_distributed_table('varchar_hash_partitioned_table', 'name', 'hash');

-- execute INSERT, SELECT and UPDATE queries on composite_type_partitioned_table
INSERT INTO varchar_hash_partitioned_table VALUES  (1, 'Jason');
INSERT INTO varchar_hash_partitioned_table VALUES  (2, 'Ozgun');
INSERT INTO varchar_hash_partitioned_table VALUES  (3, 'Onder');
INSERT INTO varchar_hash_partitioned_table VALUES  (4, 'Sumedh');
INSERT INTO varchar_hash_partitioned_table VALUES  (5, 'Marco');

SELECT * FROM varchar_hash_partitioned_table WHERE id = 1;

UPDATE varchar_hash_partitioned_table SET id = 6 WHERE name = 'Jason';

SELECT * FROM varchar_hash_partitioned_table WHERE id = 6;
DROP TABLE composite_type_partitioned_table;
DROP TABLE bugs;
DROP TABLE varchar_hash_partitioned_table;
