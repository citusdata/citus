CREATE SCHEMA distributed_domain;
SET search_path TO distributed_domain;
SET citus.next_shard_id TO 93631000;

-- verify domain is not already present on workers
SELECT * FROM run_command_on_workers($$
    SELECT 'distributed_domain.age'::regtype;
$$) ORDER BY 1,2;
CREATE DOMAIN age AS int CHECK( VALUE >= 0 );
-- check domain exists on workers to proof the domain got propagated
SELECT * FROM run_command_on_workers($$
    SELECT 'distributed_domain.age'::regtype;
$$) ORDER BY 1,2;

-- verify the constraint triggers when operations that conflict are pushed to the shards
CREATE TABLE foo (a int);
SELECT create_distributed_table('foo', 'a', shard_count => 2);
CREATE TABLE bar (a age);
SELECT create_distributed_table('bar', 'a', shard_count => 2);
INSERT INTO foo (a) VALUES (-1); -- foo can have negative values
INSERT INTO bar (a) SELECT a FROM foo; -- bar cannot

DROP TABLE bar; -- need to drop this one directly, there is currently a bug in drop schema cascade when it drops both a table and the type of the distribution column at the same time

-- create a domain that is not propagated
SET citus.enable_ddl_propagation TO off;
CREATE DOMAIN us_postal_code AS TEXT
    CHECK(
        VALUE ~ '^\d{5}$'
    OR VALUE ~ '^\d{5}-\d{4}$'
    );
RESET citus.enable_ddl_propagation;

-- and use in distributed table to trigger domain propagation
CREATE TABLE us_snail_addy (
   address_id SERIAL PRIMARY KEY,
   street1 TEXT NOT NULL,
   street2 TEXT,
   street3 TEXT,
   city TEXT NOT NULL,
   postal us_postal_code NOT NULL
);
SELECT create_distributed_table('us_snail_addy', 'address_id');

-- defaults are marked as a constraint on the statement, which makes it interesting for deparsing, so some extensive
-- test coverage to make sure it works in all strange cases.
CREATE SCHEMA distributed_domain_constraints;
SET search_path TO distributed_domain_constraints;
CREATE DOMAIN with_default AS int DEFAULT 0;
CREATE DOMAIN age_with_default AS int DEFAULT 0 CHECK (value > 0);
CREATE DOMAIN age_with_default2 AS int CHECK (value > 0) DEFAULT 0;
CREATE DOMAIN age_with_default3 AS int CHECK (value > 0) DEFAULT NULL;
CREATE DOMAIN age_with_default4 AS int NOT NULL CHECK (value > 0) DEFAULT NULL;

-- test casting with worker queries
-- should simply work, has no check constraints
SELECT * FROM run_command_on_workers($$
    SELECT NULL::distributed_domain_constraints.with_default;
$$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$
    SELECT 0::distributed_domain_constraints.with_default;
$$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$
    SELECT 1::distributed_domain_constraints.with_default;
$$) ORDER BY 1,2;

-- has a constraint where the number needs to be greater than 0
SELECT * FROM run_command_on_workers($$
    SELECT NULL::distributed_domain_constraints.age_with_default;
$$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$
    SELECT 0::distributed_domain_constraints.age_with_default;
$$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$
    SELECT 1::distributed_domain_constraints.age_with_default;
$$) ORDER BY 1,2;

-- has a constraint where the number needs to be greater than 0
SELECT * FROM run_command_on_workers($$
    SELECT NULL::distributed_domain_constraints.age_with_default2;
$$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$
    SELECT 0::distributed_domain_constraints.age_with_default2;
$$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$
    SELECT 1::distributed_domain_constraints.age_with_default2;
$$) ORDER BY 1,2;

-- has a constraint where the number needs to be greater than 0
SELECT * FROM run_command_on_workers($$
    SELECT NULL::distributed_domain_constraints.age_with_default3;
$$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$
    SELECT 0::distributed_domain_constraints.age_with_default3;
$$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$
    SELECT 1::distributed_domain_constraints.age_with_default3;
$$) ORDER BY 1,2;

-- has a constraint where the number needs to be greater than 0 and not null
SELECT * FROM run_command_on_workers($$
    SELECT NULL::distributed_domain_constraints.age_with_default4;
$$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$
    SELECT 0::distributed_domain_constraints.age_with_default4;
$$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$
    SELECT 1::distributed_domain_constraints.age_with_default4;
$$) ORDER BY 1,2;

-- test usage in distributed tables
-- we use all domains defined earlier and insert all possible values, NULL, default, non-violation, violation. Some of
-- the default values will violate a constraint.
-- Based on the constraints on the domain we see different errors and will end up with a final set of data stored.
CREATE TABLE use_default (a int, b with_default);
SELECT create_distributed_table('use_default', 'a', shard_count => 2);
INSERT INTO use_default (a, b) VALUES (0, NULL);
INSERT INTO use_default (a) VALUES (1);
INSERT INTO use_default (a, b) VALUES (2, 1);
INSERT INTO use_default (a, b) VALUES (3, -1);
SELECT * FROM use_default ORDER BY a;

CREATE TABLE use_age_default (a int, b age_with_default);
SELECT create_distributed_table('use_age_default', 'a', shard_count => 2);
INSERT INTO use_age_default (a, b) VALUES (0, NULL);
INSERT INTO use_age_default (a) VALUES (1);
INSERT INTO use_age_default (a, b) VALUES (2, 1);
INSERT INTO use_age_default (a, b) VALUES (3, -1);
-- also load some data with copy to verify coercions.
\COPY use_age_default FROM STDIN DELIMITER AS ',';
4, -1
\.
\COPY use_age_default FROM STDIN DELIMITER AS ',';
5, 1
\.
SELECT * FROM use_age_default ORDER BY a;

CREATE TABLE use_age_default2 (a int, b age_with_default2);
SELECT create_distributed_table('use_age_default2', 'a', shard_count => 2);
INSERT INTO use_age_default2 (a, b) VALUES (0, NULL);
INSERT INTO use_age_default2 (a) VALUES (1);
INSERT INTO use_age_default2 (a, b) VALUES (2, 1);
INSERT INTO use_age_default2 (a, b) VALUES (3, -1);
SELECT * FROM use_age_default2 ORDER BY a;

CREATE TABLE use_age_default3 (a int, b age_with_default3);
SELECT create_distributed_table('use_age_default3', 'a', shard_count => 2);
INSERT INTO use_age_default3 (a, b) VALUES (0, NULL);
INSERT INTO use_age_default3 (a) VALUES (1);
INSERT INTO use_age_default3 (a, b) VALUES (2, 1);
INSERT INTO use_age_default3 (a, b) VALUES (3, -1);
SELECT * FROM use_age_default3 ORDER BY a;

CREATE TABLE use_age_default4 (a int, b age_with_default4);
SELECT create_distributed_table('use_age_default4', 'a', shard_count => 2);
INSERT INTO use_age_default4 (a, b) VALUES (0, NULL);
INSERT INTO use_age_default4 (a) VALUES (1);
INSERT INTO use_age_default4 (a, b) VALUES (2, 1);
INSERT INTO use_age_default4 (a, b) VALUES (3, -1);
SELECT * FROM use_age_default4 ORDER BY a;

SET client_min_messages TO warning;
DROP SCHEMA distributed_domain_constraints CASCADE;
RESET client_min_messages;

-- same tests as above, with with just in time propagation of domains
CREATE SCHEMA distributed_domain_constraints;
SET citus.enable_ddl_propagation TO off;
CREATE DOMAIN with_default AS int DEFAULT 0;
CREATE DOMAIN age_with_default AS int DEFAULT 0 CHECK (value > 0);
CREATE DOMAIN age_with_default2 AS int CHECK (value > 0) DEFAULT 0;
CREATE DOMAIN age_with_default3 AS int CHECK (value > 0) DEFAULT NULL;
CREATE DOMAIN age_with_default4 AS int NOT NULL CHECK (value > 0) DEFAULT NULL;
RESET citus.enable_ddl_propagation;

-- use all domains in tables to get them propagated
CREATE TABLE use_default (a int, b with_default);
SELECT create_distributed_table('use_default', 'a', shard_count => 2);
INSERT INTO use_default (a, b) VALUES (0, NULL);
INSERT INTO use_default (a) VALUES (1);
INSERT INTO use_default (a, b) VALUES (2, 1);
INSERT INTO use_default (a, b) VALUES (3, -1);
SELECT * FROM use_default ORDER BY a;

CREATE TABLE use_age_default (a int, b age_with_default);
SELECT create_distributed_table('use_age_default', 'a', shard_count => 2);
INSERT INTO use_age_default (a, b) VALUES (0, NULL);
INSERT INTO use_age_default (a) VALUES (1);
INSERT INTO use_age_default (a, b) VALUES (2, 1);
INSERT INTO use_age_default (a, b) VALUES (3, -1);
SELECT * FROM use_age_default ORDER BY a;

CREATE TABLE use_age_default2 (a int, b age_with_default2);
SELECT create_distributed_table('use_age_default2', 'a', shard_count => 2);
INSERT INTO use_age_default2 (a, b) VALUES (0, NULL);
INSERT INTO use_age_default2 (a) VALUES (1);
INSERT INTO use_age_default2 (a, b) VALUES (2, 1);
INSERT INTO use_age_default2 (a, b) VALUES (3, -1);
SELECT * FROM use_age_default2 ORDER BY a;

CREATE TABLE use_age_default3 (a int, b age_with_default3);
SELECT create_distributed_table('use_age_default3', 'a', shard_count => 2);
INSERT INTO use_age_default3 (a, b) VALUES (0, NULL);
INSERT INTO use_age_default3 (a) VALUES (1);
INSERT INTO use_age_default3 (a, b) VALUES (2, 1);
INSERT INTO use_age_default3 (a, b) VALUES (3, -1);
SELECT * FROM use_age_default3 ORDER BY a;

CREATE TABLE use_age_default4 (a int, b age_with_default4);
SELECT create_distributed_table('use_age_default4', 'a', shard_count => 2);
INSERT INTO use_age_default4 (a, b) VALUES (0, NULL);
INSERT INTO use_age_default4 (a) VALUES (1);
INSERT INTO use_age_default4 (a, b) VALUES (2, 1);
INSERT INTO use_age_default4 (a, b) VALUES (3, -1);
SELECT * FROM use_age_default4 ORDER BY a;

-- clean up
SET client_min_messages TO warning;
DROP SCHEMA distributed_domain_constraints CASCADE;
RESET client_min_messages;

CREATE SCHEMA postgres_domain_examples;
SET search_path TO postgres_domain_examples;

-- make sure the function gets automatically propagated when we propagate the domain
SET citus.enable_ddl_propagation TO off;
create function sql_is_distinct_from(anyelement, anyelement)
    returns boolean language sql
    as 'select $1 is distinct from $2 limit 1';
RESET citus.enable_ddl_propagation;

CREATE DOMAIN inotnull int
    CHECK (sql_is_distinct_from(value, null));

SELECT * FROM run_command_on_workers($$ SELECT 1::postgres_domain_examples.inotnull; $$);
SELECT * FROM run_command_on_workers($$ SELECT null::postgres_domain_examples.inotnull; $$);

-- create a domain with sql function as a default value
SET citus.enable_ddl_propagation TO off;
create function random_between(min int, max int)
    returns int language sql
    as 'SELECT round(random()*($2-$1))+$1;';
RESET citus.enable_ddl_propagation;

-- this verifies the function in the default expression is found and distributed, otherwise the creation of the domain would fail on the workers.
CREATE DOMAIN with_random_default int DEFAULT random_between(100, 200);

SET client_min_messages TO warning;
DROP SCHEMA postgres_domain_examples CASCADE;
RESET client_min_messages;

SET search_path TO distributed_domain;

-- verify drops are propagated
CREATE DOMAIN will_drop AS text DEFAULT 'foo';
DROP DOMAIN will_drop;
-- verify domain is dropped from workers
SELECT * FROM run_command_on_workers($$ SELECT 'dropped?'::distributed_domain.will_drop; $$);

-- verify the type modifiers are deparsed correctly, both for direct propagation as well as on demand propagation
CREATE DOMAIN varcharmod AS varchar(3);
SELECT * FROM run_command_on_workers($$ SELECT '12345'::distributed_domain.varcharmod; $$) ORDER BY 1,2;

SET citus.enable_ddl_propagation TO off;
CREATE DOMAIN varcharmod_ondemand AS varchar(3);
RESET citus.enable_ddl_propagation;
CREATE TABLE use_varcharmod_ondemand (a int, b varcharmod_ondemand);
SELECT create_distributed_table('use_varcharmod_ondemand', 'a');
-- epxected error due to value being too long for varchar(3)
INSERT INTO use_varcharmod_ondemand VALUES (1,'12345');
SELECT * FROM run_command_on_workers($$ SELECT '12345'::distributed_domain.varcharmod_ondemand; $$) ORDER BY 1,2;

-- section testing default altering
CREATE DOMAIN alter_default AS text DEFAULT 'foo';
CREATE TABLE use_alter_default (a int, b alter_default);
SELECT create_distributed_table('use_alter_default', 'a', shard_count => 4);
INSERT INTO use_alter_default (a) VALUES (1);
ALTER DOMAIN alter_default SET DEFAULT 'bar';
INSERT INTO use_alter_default (a) VALUES (2);
ALTER DOMAIN alter_default DROP DEFAULT;
INSERT INTO use_alter_default (a) VALUES (3);
SELECT * FROM use_alter_default ORDER BY 1,2;

-- add new dependency while adding default
CREATE DOMAIN add_default_with_function AS int;
SET citus.enable_ddl_propagation TO off;
create function random_between(min int, max int)
    returns int language sql
    as 'SELECT round(random()*($2-$1))+$1;';
RESET citus.enable_ddl_propagation;
ALTER DOMAIN add_default_with_function SET DEFAULT random_between(100, 200);
CREATE TABLE use_add_default_with_function (a int, b add_default_with_function);
SELECT create_distributed_table('use_add_default_with_function', 'a', shard_count => 4);
INSERT INTO use_add_default_with_function (a) VALUES (1);

-- altering NULL/NOT NULL
CREATE DOMAIN alter_null AS int;
CREATE TABLE use_alter_null (a int, b alter_null);
SELECT create_distributed_table('use_alter_null', 'a');
INSERT INTO use_alter_null (a) VALUES (1);
ALTER DOMAIN alter_null SET NOT NULL;
TRUNCATE use_alter_null;
ALTER DOMAIN alter_null SET NOT NULL;
INSERT INTO use_alter_null (a) VALUES (2);
ALTER DOMAIN alter_null DROP NOT NULL;
INSERT INTO use_alter_null (a) VALUES (3);
SELECT * FROM use_alter_null ORDER BY 1;

-- testing adding/dropping constraints
SET citus.enable_ddl_propagation TO off;
create function sql_is_distinct_from(anyelement, anyelement)
    returns boolean language sql
    as 'select $1 is distinct from $2 limit 1';
RESET citus.enable_ddl_propagation;

CREATE DOMAIN alter_add_constraint int;
ALTER DOMAIN alter_add_constraint ADD CONSTRAINT check_distinct CHECK (sql_is_distinct_from(value, null));
SELECT * FROM run_command_on_workers($$ SELECT 1::distributed_domain.alter_add_constraint; $$);
SELECT * FROM run_command_on_workers($$ SELECT null::distributed_domain.alter_add_constraint; $$);

ALTER DOMAIN alter_add_constraint DROP CONSTRAINT check_distinct;
SELECT * FROM run_command_on_workers($$ SELECT 1::distributed_domain.alter_add_constraint; $$);
SELECT * FROM run_command_on_workers($$ SELECT null::distributed_domain.alter_add_constraint; $$);
ALTER DOMAIN alter_add_constraint DROP CONSTRAINT IF EXISTS check_distinct;

ALTER DOMAIN alter_add_constraint ADD CONSTRAINT check_distinct CHECK (sql_is_distinct_from(value, null));
ALTER DOMAIN alter_add_constraint RENAME CONSTRAINT check_distinct TO check_distinct_renamed;
ALTER DOMAIN alter_add_constraint DROP CONSTRAINT check_distinct_renamed;

-- test validating invalid constraints
CREATE DOMAIN age_invalid AS int NOT NULL DEFAULT 0;
CREATE TABLE use_age_invalid (a int, b age_invalid);
SELECT create_distributed_table('use_age_invalid', 'a', shard_count => 4);
INSERT INTO use_age_invalid VALUES (1,1), (2, 2), (3, 0), (4, -1);
ALTER DOMAIN age_invalid ADD CONSTRAINT check_age_positive CHECK (value>=0) NOT VALID;
-- should fail, even though constraint is not valid
INSERT INTO use_age_invalid VALUES (5,-1);
-- reading violating data of an non-valid constraint errors in citus
SELECT * FROM use_age_invalid ORDER BY 1;
-- should fail since there is data in the table that violates the check
ALTER DOMAIN age_invalid VALIDATE CONSTRAINT check_age_positive;
DELETE FROM use_age_invalid WHERE b < 0;
-- should succeed now since the violating data has been removed
ALTER DOMAIN age_invalid VALIDATE CONSTRAINT check_age_positive;
-- still fails for constraint
INSERT INTO use_age_invalid VALUES (5,-1);
SELECT * FROM use_age_invalid ORDER BY 1;
-- verify we can validate a constraint that is already validated, can happen when we add a node while a domain constraint was not validated
ALTER DOMAIN age_invalid VALIDATE CONSTRAINT check_age_positive;

-- test changing the owner of a domain
SET client_min_messages TO error;
SELECT 1 FROM run_command_on_workers($$ CREATE ROLE domain_owner; $$);
CREATE ROLE domain_owner;
RESET client_min_messages;

CREATE DOMAIN alter_domain_owner AS int;
ALTER DOMAIN alter_domain_owner OWNER TO domain_owner;

SELECT u.rolname
FROM pg_type t
         JOIN pg_roles u
              ON (t.typowner = u.oid)
WHERE t.oid = 'distributed_domain.alter_domain_owner'::regtype;

SELECT * FROM run_command_on_workers($$
    SELECT u.rolname
      FROM pg_type t
      JOIN pg_roles u
        ON (t.typowner = u.oid)
     WHERE t.oid = 'distributed_domain.alter_domain_owner'::regtype;
$$) ORDER BY 1,2;

DROP DOMAIN alter_domain_owner;

SET citus.enable_ddl_propagation TO off;
CREATE DOMAIN alter_domain_owner AS int;
ALTER DOMAIN alter_domain_owner OWNER TO domain_owner;
RESET citus.enable_ddl_propagation;

CREATE TABLE use_alter_domain_owner (a int, b alter_domain_owner);
SELECT create_distributed_table('use_alter_domain_owner', 'a', shard_count => 4);

SELECT u.rolname
FROM pg_type t
         JOIN pg_roles u
              ON (t.typowner = u.oid)
WHERE t.oid = 'distributed_domain.alter_domain_owner'::regtype;

SELECT * FROM run_command_on_workers($$
    SELECT u.rolname
      FROM pg_type t
      JOIN pg_roles u
        ON (t.typowner = u.oid)
     WHERE t.oid = 'distributed_domain.alter_domain_owner'::regtype;
$$) ORDER BY 1,2;

-- rename the domain
ALTER DOMAIN alter_domain_owner RENAME TO renamed_domain;
SELECT * FROM run_command_on_workers($$ SELECT NULL::distributed_domain.renamed_domain; $$) ORDER BY 1,2;

-- move schema
SET citus.enable_ddl_propagation TO off;
CREATE SCHEMA distributed_domain_moved;
RESET citus.enable_ddl_propagation;
ALTER DOMAIN renamed_domain SET SCHEMA distributed_domain_moved;

-- test collation
CREATE COLLATION german_phonebook (provider = icu, locale = 'de-u-co-phonebk');

CREATE DOMAIN with_collation AS text COLLATE german_phonebook NOT NULL;
SELECT run_command_on_workers($$ SELECT typcollation::regcollation FROM pg_type WHERE oid = 'distributed_domain.with_collation'::regtype; $$);
DROP DOMAIN with_collation;

SET citus.enable_ddl_propagation TO off;
CREATE DOMAIN with_collation AS text COLLATE german_phonebook NOT NULL;
RESET citus.enable_ddl_propagation;

CREATE TABLE use_with_collation (a int, b with_collation);
SELECT create_reference_table('use_with_collation');
SELECT run_command_on_workers($$ SELECT typcollation::regcollation FROM pg_type WHERE oid = 'distributed_domain.with_collation'::regtype; $$);

INSERT INTO use_with_collation VALUES (1, U&'\00E4sop'), (2, 'Vossr');
SELECT * FROM use_with_collation WHERE b < 'b';

-- test domain backed by array
CREATE DOMAIN domain_array AS int[] NOT NULL CHECK (array_length(value,1) >= 2);
SELECT * FROM run_command_on_workers($$ SELECT NULL::distributed_domain.domain_array; $$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$ SELECT ARRAY[1]::distributed_domain.domain_array; $$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$ SELECT ARRAY[1,2]::distributed_domain.domain_array; $$) ORDER BY 1,2;

DROP DOMAIN domain_array;

SET citus.enable_ddl_propagation TO off;
CREATE DOMAIN domain_array AS int[] NOT NULL CHECK (array_length(value,1) >= 2);
RESET citus.enable_ddl_propagation;

CREATE TABLE use_domain_array (a int, b domain_array);
SELECT create_distributed_table('use_domain_array', 'a');
INSERT INTO use_domain_array VALUES (1, NULL);
INSERT INTO use_domain_array VALUES (2, ARRAY[1]);
INSERT INTO use_domain_array VALUES (3, ARRAY[1,2]);
SELECT * FROM use_domain_array ORDER BY 1;

-- add nameless constraint
CREATE DOMAIN nameless_constraint AS int;
ALTER DOMAIN nameless_constraint ADD CHECK (value > 0);
SELECT * FROM run_command_on_workers($$ SELECT NULL::distributed_domain.nameless_constraint; $$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$ SELECT 1::distributed_domain.nameless_constraint; $$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$ SELECT (-1)::distributed_domain.nameless_constraint; $$) ORDER BY 1,2;

-- Test domains over domains
create domain vchar4 varchar(4);
create domain dinter vchar4 check (substring(VALUE, 1, 1) = 'x');
create domain dtop dinter check (substring(VALUE, 2, 1) = '1');
create table dtest(f1 dtop, id bigserial);
SELECT create_distributed_table('dtest', 'id');

insert into dtest values('x123');
insert into dtest values('123');
insert into dtest values('x234');

DROP TABLE dtest;

DROP DOMAIN IF EXISTS dtop;
DROP DOMAIN vchar4;
DROP DOMAIN vchar4 CASCADE;

-- drop multiple domains at once, for which one is not distributed
CREATE DOMAIN domain1 AS int;
CREATE DOMAIN domain2 AS text;
SET citus.enable_ddl_propagation TO off;
CREATE DOMAIN domain3 AS text;
RESET citus.enable_ddl_propagation;

SELECT * FROM run_command_on_workers($$ SELECT 1::distributed_domain.domain1; $$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$ SELECT '1'::distributed_domain.domain2; $$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$ SELECT '1'::distributed_domain.domain3; $$) ORDER BY 1,2;

DROP DOMAIN domain1, domain2, domain3;

SELECT * FROM run_command_on_workers($$ SELECT 1::distributed_domain.domain1; $$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$ SELECT '1'::distributed_domain.domain2; $$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$ SELECT '1'::distributed_domain.domain3; $$) ORDER BY 1,2;

DROP DOMAIN IF EXISTS domain_does_not_exist;

SET client_min_messages TO warning;
DROP SCHEMA distributed_domain, distributed_domain_moved CASCADE;
DROP ROLE domain_owner;
