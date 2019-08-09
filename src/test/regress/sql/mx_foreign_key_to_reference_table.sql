CREATE SCHEMA fkey_reference_table;
SET search_path TO 'fkey_reference_table';
SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 8;
SET citus.next_shard_id TO 7000000;
SET citus.next_placement_id TO 7000000;
SET citus.replication_model TO streaming;

-- Setup the view so that we can check if the foreign keys are created properly
CREATE TYPE foreign_details AS (name text, relid text, refd_relid text);

CREATE VIEW table_fkeys_in_workers AS 
SELECT
(json_populate_record(NULL::foreign_details, 
  json_array_elements_text((run_command_on_workers( $$ 
    SELECT
      COALESCE(json_agg(row_to_json(d)), '[]'::json) 
    FROM
      (
        SELECT
          distinct name,
          relid::regclass::text,
          refd_relid::regclass::text
        FROM
          table_fkey_cols 
      )
      d $$ )).RESULT::json )::json )).* ;

-- Check if MX can create foreign keys properly on foreign keys from distributed to reference tables
CREATE TABLE referenced_table(test_column int, test_column2 int, PRIMARY KEY(test_column));
CREATE TABLE referenced_table2(test_column int, test_column2 int, PRIMARY KEY(test_column2));
CREATE TABLE referencing_table(id int, ref_id int);
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY (id) REFERENCES referenced_table(test_column) ON DELETE CASCADE;
ALTER TABLE referencing_table ADD CONSTRAINT foreign_key_2 FOREIGN KEY (id) REFERENCES referenced_table2(test_column2) ON DELETE CASCADE;

SELECT create_reference_table('referenced_table');
SELECT create_reference_table('referenced_table2');
SELECT create_distributed_table('referencing_table', 'id');

SET search_path TO 'fkey_reference_table';
SELECT * FROM table_fkeys_in_workers WHERE relid LIKE 'fkey_reference_table.%' AND refd_relid LIKE 'fkey_reference_table.%' ORDER BY 1, 2;

DROP SCHEMA fkey_reference_table CASCADE;
SET search_path TO DEFAULT;
