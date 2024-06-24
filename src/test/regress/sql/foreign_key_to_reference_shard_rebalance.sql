--
-- FOREIGN_KEY_TO_REFERENCE_SHARD_REBALANCE
--

SET citus.next_shard_id TO 15000000;
CREATE SCHEMA fkey_to_reference_shard_rebalance;
SET search_path to fkey_to_reference_shard_rebalance;
SET citus.shard_replication_factor TO 1;
SET citus.shard_count to 8;


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

-- check if master_move_shard_placement with logical replication creates the
-- foreign constraints properly after moving the shard
CREATE TABLE referenced_table(test_column int, test_column2 int UNIQUE, PRIMARY KEY(test_column));
CREATE TABLE referencing_table(id int PRIMARY KEY, ref_id int, FOREIGN KEY (id) REFERENCES referenced_table(test_column) ON DELETE CASCADE);
CREATE TABLE referencing_table2(id int, ref_id int, FOREIGN KEY (ref_id) REFERENCES referenced_table(test_column2) ON DELETE CASCADE, FOREIGN KEY (id) REFERENCES referencing_table(id) ON DELETE CASCADE);
SELECT create_reference_table('referenced_table');
SELECT create_distributed_table('referencing_table', 'id');
SELECT create_distributed_table('referencing_table2', 'id');

INSERT INTO referenced_table SELECT i,i FROM generate_series (0, 100) i;
INSERT INTO referencing_table SELECT i,i FROM generate_series (0, 100) i;
INSERT INTO referencing_table2 SELECT i,i FROM generate_series (0, 100) i;

SELECT master_move_shard_placement(15000009, 'localhost', :worker_1_port, 'localhost', :worker_2_port, 'force_logical');

SELECT count(*) FROM referencing_table2;

SELECT public.wait_for_resource_cleanup();
SELECT * FROM table_fkeys_in_workers WHERE relid LIKE 'fkey_to_reference_shard_rebalance.%' AND refd_relid LIKE 'fkey_to_reference_shard_rebalance.%' ORDER BY 1,2,3;

SELECT master_move_shard_placement(15000009, 'localhost', :worker_2_port, 'localhost', :worker_1_port, 'block_writes');

SELECT count(*) FROM referencing_table2;

SELECT public.wait_for_resource_cleanup();
SELECT * FROM table_fkeys_in_workers WHERE relid LIKE 'fkey_to_reference_shard_rebalance.%' AND refd_relid LIKE 'fkey_to_reference_shard_rebalance.%' ORDER BY 1,2,3;

CREATE TABLE reference_table_commands (id int UNIQUE);
CREATE TABLE referenceing_dist_table (id int, col1 int, col2 int, col3 int);
SELECT create_reference_table('reference_table_commands');
SELECT create_distributed_table('referenceing_dist_table', 'id');
ALTER TABLE referenceing_dist_table ADD CONSTRAINT c1 FOREIGN KEY (col1) REFERENCES reference_table_commands(id) ON UPDATE CASCADE;
ALTER TABLE referenceing_dist_table ADD CONSTRAINT c2 FOREIGN KEY (col2) REFERENCES reference_table_commands(id) ON UPDATE CASCADE NOT VALID;
ALTER TABLE referenceing_dist_table ADD CONSTRAINT very_very_very_very_very_very_very_very_very_very_very_very_very_long FOREIGN KEY (col3) REFERENCES reference_table_commands(id) ON UPDATE CASCADE;

-- and show that rebalancer works fine
SELECT master_move_shard_placement(15000018, 'localhost', :worker_1_port, 'localhost', :worker_2_port, 'force_logical');

\c - - - :worker_2_port

SELECT conname, contype, convalidated FROM pg_constraint WHERE conrelid = 'fkey_to_reference_shard_rebalance.referenceing_dist_table_15000018'::regclass ORDER BY 1;

\c - - - :master_port

SET search_path TO fkey_to_reference_shard_rebalance;
SET citus.shard_replication_factor to 1;

-- test moving a shard with foreign key
create table ref_table_with_fkey (id int primary key);
select create_reference_table('ref_table_with_fkey');
insert into ref_table_with_fkey select s from generate_series(0,9) s;

create table partitioned_tbl_with_fkey (x int, y int, t timestamptz default now()) partition by range (t);
select create_distributed_table('partitioned_tbl_with_fkey','x');
create table partition_1_with_fkey partition of partitioned_tbl_with_fkey for values from ('2022-01-01') to ('2022-12-31');
create table partition_2_with_fkey partition of partitioned_tbl_with_fkey for values from ('2023-01-01') to ('2023-12-31');
create table partition_3_with_fkey partition of partitioned_tbl_with_fkey DEFAULT;
insert into partitioned_tbl_with_fkey (x,y) select s,s%10 from generate_series(1,100) s;

ALTER TABLE partitioned_tbl_with_fkey ADD CONSTRAINT fkey_to_ref_tbl FOREIGN KEY (y) REFERENCES ref_table_with_fkey(id);

WITH shardid AS (SELECT shardid FROM pg_dist_shard where logicalrelid = 'partitioned_tbl_with_fkey'::regclass ORDER BY shardid LIMIT 1)
SELECT citus_move_shard_placement(shardid.shardid, 'localhost', 57637, 'localhost', 57638, shard_transfer_mode := 'force_logical') FROM shardid;

SET client_min_messages TO WARNING;
DROP SCHEMA fkey_to_reference_shard_rebalance CASCADE;
