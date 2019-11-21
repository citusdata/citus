-- ===================================================================
-- test composite type, varchar and enum types
-- create, distribute, INSERT, SELECT and UPDATE
-- ===================================================================


SET citus.next_shard_id TO 530000;

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

SELECT * FROM composite_type_partitioned_table WHERE col =  '(7, 8)'::test_composite_type;

UPDATE composite_type_partitioned_table SET id = 6 WHERE col =  '(7, 8)'::test_composite_type;

SELECT * FROM composite_type_partitioned_table WHERE col =  '(7, 8)'::test_composite_type;


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
