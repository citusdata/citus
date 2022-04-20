
-- test cases for #3970
SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;

CREATE SCHEMA post_11_upgrade;
SET search_path = post_11_upgrade;

--1. create a partitioned table, and a vanilla table that will be colocated with this table
CREATE TABLE part_table (
    work_ymdt timestamp without time zone NOT NULL,
    seq bigint NOT NULL,
    my_seq bigint NOT NULL,
    work_memo character varying(150),
    CONSTRAINT work_memo_check CHECK ((octet_length((work_memo)::text) <= 150)),
    PRIMARY KEY(seq, work_ymdt)
)
PARTITION BY RANGE (work_ymdt);

CREATE TABLE dist(seq bigint UNIQUE);

--2. perform create_distributed_table
SELECT create_distributed_table('part_table', 'seq');
SELECT create_distributed_table('dist','seq');

--3. add a partitions
CREATE TABLE part_table_p202008 PARTITION OF part_table FOR VALUES FROM ('2020-08-01 00:00:00') TO ('2020-09-01 00:00:00');
CREATE TABLE part_table_p202009 PARTITION OF part_table FOR VALUES FROM ('2020-09-01 00:00:00') TO ('2020-10-01 00:00:00');

--3. create indexes
CREATE INDEX i_part_1 ON part_table(seq);
CREATE INDEX i_part_2 ON part_table(my_seq, seq);
CREATE INDEX i_part_3 ON part_table(work_memo, seq);


CREATE TABLE sensors(
measureid         integer,
eventdatetime     date,
measure_data      jsonb,
PRIMARY KEY (measureid, eventdatetime, measure_data))
PARTITION BY RANGE(eventdatetime);

CREATE TABLE sensors_old PARTITION OF sensors FOR VALUES FROM ('2000-01-01') TO ('2020-01-01');
CREATE TABLE sensors_2020_01_01 PARTITION OF sensors FOR VALUES FROM ('2020-01-01') TO ('2020-02-01');
CREATE TABLE sensors_news PARTITION OF sensors FOR VALUES FROM ('2020-05-01') TO ('2025-01-01');

CREATE INDEX index_on_parent ON sensors(lower(measureid::text));
CREATE INDEX index_on_child ON sensors_2020_01_01(lower(measure_data::text));
CREATE INDEX hash_index ON sensors USING HASH((measure_data->'IsFailed'));
CREATE INDEX index_with_include ON sensors ((measure_data->'IsFailed')) INCLUDE (measure_data, eventdatetime);

CREATE STATISTICS s1 (dependencies) ON measureid, eventdatetime FROM sensors;
CREATE STATISTICS s2 (dependencies) ON measureid, eventdatetime FROM sensors_2020_01_01;

ALTER INDEX index_on_parent ALTER COLUMN 1 SET STATISTICS 1000;
ALTER INDEX index_on_child ALTER COLUMN 1 SET STATISTICS 1000;

CLUSTER sensors_2020_01_01 USING index_on_child;
SELECT create_distributed_table('sensors', 'measureid');


-- create a colocated distributed tables and create foreign keys FROM/TO
-- the partitions
CREATE TABLE colocated_dist_table (measureid integer PRIMARY KEY);
SELECT create_distributed_table('colocated_dist_table', 'measureid');

CLUSTER colocated_dist_table USING colocated_dist_table_pkey;

CREATE TABLE colocated_partitioned_table(
  measureid         integer,
  eventdatetime     date,
  PRIMARY KEY (measureid, eventdatetime))
PARTITION BY RANGE(eventdatetime);

CREATE TABLE colocated_partitioned_table_2020_01_01 PARTITION OF colocated_partitioned_table FOR VALUES FROM ('2020-01-01') TO ('2020-02-01');
SELECT create_distributed_table('colocated_partitioned_table', 'measureid');

CLUSTER colocated_partitioned_table_2020_01_01 USING colocated_partitioned_table_2020_01_01_pkey;

CREATE TABLE reference_table (measureid integer PRIMARY KEY);
SELECT create_reference_table('reference_table');

-- this table is used to make sure that index backed
-- replica identites can have clustered indexes
-- and no index statistics
CREATE TABLE index_backed_rep_identity(key int NOT NULL);
CREATE UNIQUE INDEX uqx ON index_backed_rep_identity(key);
ALTER TABLE index_backed_rep_identity REPLICA IDENTITY USING INDEX uqx;
CLUSTER index_backed_rep_identity USING uqx;
SELECT create_distributed_table('index_backed_rep_identity', 'key');

-- from parent to regular dist
ALTER TABLE sensors ADD CONSTRAINT fkey_from_parent_to_dist FOREIGN KEY (measureid) REFERENCES colocated_dist_table(measureid);

-- from parent to parent
ALTER TABLE sensors ADD CONSTRAINT fkey_from_parent_to_parent FOREIGN KEY (measureid, eventdatetime) REFERENCES colocated_partitioned_table(measureid, eventdatetime);

-- from parent to child
ALTER TABLE sensors ADD CONSTRAINT fkey_from_parent_to_child FOREIGN KEY (measureid, eventdatetime) REFERENCES colocated_partitioned_table_2020_01_01(measureid, eventdatetime);

-- load some data
INSERT INTO reference_table SELECT i FROM generate_series(0,1000)i;
INSERT INTO colocated_dist_table SELECT i FROM generate_series(0,1000)i;
INSERT INTO colocated_partitioned_table SELECT i, '2020-01-05' FROM generate_series(0,1000)i;
INSERT INTO sensors SELECT i, '2020-01-05', '{}' FROM generate_series(0,1000)i;


SET citus.enable_ddl_propagation TO off;
CREATE TEXT SEARCH CONFIGURATION post_11_upgrade.partial_index_test_config ( parser = default );
SELECT 1 FROM run_command_on_workers($$CREATE TEXT SEARCH CONFIGURATION post_11_upgrade.partial_index_test_config ( parser = default );$$);

CREATE OR REPLACE FUNCTION post_11_upgrade.func_in_transaction_def()
RETURNS int
LANGUAGE plpgsql AS
$$
BEGIN
    return 1;
END;
$$;

SELECT run_command_on_workers('SET citus.enable_ddl_propagation TO off;
CREATE OR REPLACE FUNCTION post_11_upgrade.func_in_transaction_def()
RETURNS int
LANGUAGE plpgsql AS
$$
BEGIN
    return 1;
END;
$$;');

CREATE TYPE post_11_upgrade.my_type AS (a int);

CREATE VIEW post_11_upgrade.view_for_upgrade_test AS SELECT * FROM sensors;

SELECT run_command_on_workers('SET citus.enable_ddl_propagation TO off;
CREATE VIEW post_11_upgrade.view_for_upgrade_test AS SELECT * FROM sensors;');

RESET citus.enable_ddl_propagation;

CREATE TABLE sensors_parser(
    measureid integer,
    eventdatetime date,
    measure_data jsonb,
    name text,
    col_with_def int DEFAULT post_11_upgrade.func_in_transaction_def(),
    col_with_type post_11_upgrade.my_type,
    PRIMARY KEY (measureid, eventdatetime, measure_data)
) PARTITION BY RANGE(eventdatetime);
CREATE TABLE sensors_parser_a_partition PARTITION OF sensors_parser FOR VALUES FROM ('2000-01-01') TO ('2020-01-01');
CREATE INDEX sensors_parser_search_name ON sensors_parser USING gin (to_tsvector('partial_index_test_config'::regconfig, (COALESCE(name, ''::character varying))::text));
SELECT create_distributed_table('sensors_parser', 'measureid');


SET citus.enable_ddl_propagation TO off;
CREATE COLLATION post_11_upgrade.german_phonebook_unpropagated (provider = icu, locale = 'de-u-co-phonebk');
SELECT 1 FROM run_command_on_workers($$CREATE COLLATION post_11_upgrade.german_phonebook_unpropagated (provider = icu, locale = 'de-u-co-phonebk');$$);
SET citus.enable_ddl_propagation TO on;

CREATE TABLE test_propagate_collate(id int, t2 text COLLATE german_phonebook_unpropagated);
SELECT create_distributed_table('test_propagate_collate', 'id');
