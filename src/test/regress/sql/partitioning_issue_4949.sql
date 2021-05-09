CREATE SCHEMA shard_move_fkeys_indexes;
SET search_path TO shard_move_fkeys_indexes;
SET citus.next_shard_id TO 8970000;
SET citus.next_placement_id TO 8770000;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;

CREATE TABLE sensors(
measureid         integer,
eventdatetime     date,
measure_data      jsonb,
PRIMARY KEY (measureid, eventdatetime, measure_data)) 
PARTITION BY RANGE(eventdatetime);

CREATE TABLE sensors_old PARTITION OF sensors FOR VALUES FROM ('2000-01-01') TO ('2020-01-01');
CREATE TABLE sensors_2020_01_01 PARTITION OF sensors FOR VALUES FROM ('2020-01-01') TO ('2020-02-01');
CREATE TABLE sensors_news PARTITION OF sensors FOR VALUES FROM ('2020-05-01') TO ('2025-01-01');

CREATE INDEX index_on_parent ON sensors(measureid);
CREATE INDEX index_on_child ON sensors_2020_01_01(measureid);

SELECT create_distributed_table('sensors', 'measureid');


CREATE TABLE colocated_partitioned_table(
    measureid         integer,
    eventdatetime     date,
    PRIMARY KEY (measureid, eventdatetime))
PARTITION BY RANGE(eventdatetime);

CREATE TABLE colocated_partitioned_table_2020_01_01 PARTITION OF colocated_partitioned_table FOR VALUES FROM ('2020-01-01') TO ('2020-02-01');
SELECT create_distributed_table('colocated_partitioned_table', 'measureid');



-- from child to parent
ALTER TABLE sensors_2020_01_01 ADD CONSTRAINT fkey_from_child_to_parent FOREIGN KEY (measureid,eventdatetime) REFERENCES colocated_partitioned_table(measureid,eventdatetime);

-- load some data
INSERT INTO colocated_partitioned_table SELECT i, '2020-01-05' FROM generate_series(0,1000)i;
INSERT INTO sensors SELECT i, '2020-01-05', '{}' FROM generate_series(0,1000)i;


SELECT citus_move_shard_placement(8970000, 'localhost', :worker_1_port, 'localhost', :worker_2_port, shard_transfer_mode:='block_writes');