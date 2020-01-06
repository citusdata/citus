--
--  VALIDATE_CONSTRAINT
--

-- PostgreSQL 11 Docs Excerpt:

-- The queries that can have 'NOT VALID' option are of the form:
-- ALTER TABLE [ IF EXISTS ] [ ONLY ] name [ * ] ADD table_constraint NOT VALID [, ...]
--
-- where table_constraint is:
--   [ CONSTRAINT constraint_name ]
--   { CHECK ( expression ) [ NO INHERIT ] |
--     UNIQUE ( column_name [, ... ] ) index_parameters |
--     PRIMARY KEY ( column_name [, ... ] ) index_parameters |
--     EXCLUDE [ USING index_method ] ( exclude_element WITH operator [, ... ] ) index_parameters [ WHERE ( predicate ) ] |
--     FOREIGN KEY ( column_name [, ... ] ) REFERENCES reftable [ ( refcolumn [, ... ] ) ]
--       [ MATCH FULL | MATCH PARTIAL | MATCH SIMPLE ] [ ON DELETE action ] [ ON UPDATE action ] }
--   [ DEFERRABLE | NOT DEFERRABLE ] [ INITIALLY DEFERRED | INITIALLY IMMEDIATE ]

-- This form adds a new constraint to a table using the same syntax as CREATE TABLE, plus the option NOT VALID, which is
-- currently only allowed for foreign key and CHECK constraints. If the constraint is marked NOT VALID, the
-- potentially-lengthy initial check to verify that all rows in the table satisfy the constraint is skipped.

-- The constraint will still be enforced against subsequent inserts or updates (that is, they'll fail unless there is a
-- matching row in the referenced table, in the case of foreign keys; and they'll fail unless the new row matches the
-- specified check constraints). But the database will not assume that the constraint holds for all rows in the table,
-- until it is validated by using the VALIDATE CONSTRAINT option.

CREATE SCHEMA validate_constraint;
SET search_path TO 'validate_constraint';
SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 8;
SET citus.next_shard_id TO 8000000;
SET citus.next_placement_id TO 8000000;

CREATE TYPE constraint_validity AS (name text, validated bool);

CREATE VIEW constraint_validations_in_workers AS
  SELECT (json_populate_record(NULL :: constraint_validity,
                               json_array_elements_text((run_command_on_workers($$
    SELECT
      COALESCE(json_agg(row_to_json(d)), '[]'::json)
    FROM
      (
        SELECT conname as name,
        convalidated as validated
        FROM pg_catalog.pg_constraint AS con
        JOIN pg_catalog.pg_namespace AS ns
        ON ns.oid = con.connamespace
        WHERE nspname = 'validate_constraint'
        AND contype = 'c'
        ORDER BY 1,2
      )
      d $$)).RESULT :: json) :: json)).*
    ORDER BY 1,2;

CREATE VIEW constraint_validations AS
  SELECT conname as "Constraint",
         convalidated as "Validated?"
  FROM pg_catalog.pg_constraint AS con
         JOIN pg_catalog.pg_namespace AS ns ON ns.oid = con.connamespace
  WHERE nspname = 'validate_constraint'
    AND contype = 'c';

CREATE TABLE referenced_table (id int UNIQUE, test_column int);
SELECT create_reference_table('referenced_table');

CREATE TABLE referencing_table (id int, ref_id int);
SELECT create_distributed_table('referencing_table', 'ref_id');

CREATE TABLE constrained_table (id int, constrained_column int);
SELECT create_distributed_table('constrained_table', 'constrained_column');

-- The two constraint types that are allowed to be NOT VALID
BEGIN;
ALTER TABLE constrained_table
  ADD CONSTRAINT check_constraint CHECK (constrained_column > 100) NOT VALID;
ALTER TABLE constrained_table
  VALIDATE CONSTRAINT check_constraint;
ROLLBACK;
BEGIN;
ALTER TABLE referencing_table
  ADD CONSTRAINT fk_constraint FOREIGN KEY (ref_id) REFERENCES referenced_table (id) NOT VALID;
ALTER TABLE referencing_table
  VALIDATE CONSTRAINT fk_constraint;
ROLLBACK;

-- It is possible that some other table_constraint commands will support NOT VALID option in the future
-- These should fail as Postgres does not yet allow these constraints to be NOT VALID
-- If one of these queries are not failing on a future Postgres version, we will need to test corresponding VALIDATE CONSTRAINT queries
ALTER TABLE constrained_table
  ADD CONSTRAINT unique_constraint UNIQUE (constrained_column) NOT VALID;

ALTER TABLE constrained_table
  ADD CONSTRAINT pk_constraint PRIMARY KEY (id) NOT VALID;

ALTER TABLE constrained_table
  ADD CONSTRAINT exclude_constraint EXCLUDE USING gist (id WITH =, constrained_column WITH <>) NOT VALID;

INSERT INTO constrained_table
SELECT x, x
from generate_series(1, 1000) as f (x);

-- a constraint that can be validated
ALTER TABLE constrained_table
  ADD CONSTRAINT validatable_constraint CHECK (constrained_column < 10000) NOT VALID;
ALTER TABLE constrained_table
  VALIDATE CONSTRAINT validatable_constraint;

-- Check which constraints are validated
SELECT *
FROM constraint_validations
ORDER BY 1, 2;

SELECT *
FROM constraint_validations_in_workers
ORDER BY 1, 2;

DROP TABLE constrained_table;
DROP TABLE referenced_table CASCADE;
DROP TABLE referencing_table;

DROP SCHEMA validate_constraint CASCADE;
SET search_path TO DEFAULT;
