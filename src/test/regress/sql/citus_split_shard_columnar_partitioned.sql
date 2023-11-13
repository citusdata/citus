CREATE SCHEMA "citus_split_test_schema_columnar_partitioned";
SET search_path TO "citus_split_test_schema_columnar_partitioned";
SET citus.next_shard_id TO 8970000;
SET citus.next_placement_id TO 8770000;
SET citus.shard_count TO 1;
SET citus.shard_replication_factor TO 1;

-- Disable Deferred drop auto cleanup to avoid flaky tests.
ALTER SYSTEM SET citus.defer_shard_delete_interval TO -1;
SELECT pg_reload_conf();

-- BEGIN: Create table to split, along with other co-located tables. Add indexes, statistics etc.
    CREATE TABLE sensors(
        measureid         integer,
        eventdatetime     date,
        measure_data      jsonb,
        PRIMARY KEY (measureid, eventdatetime, measure_data))
    PARTITION BY RANGE(eventdatetime);

    -- Table access method is specified on child tables
    CREATE TABLE sensorscolumnar(
        measureid         integer,
        eventdatetime     date,
        measure_data      jsonb,
        PRIMARY KEY (measureid, eventdatetime, measure_data))
    PARTITION BY RANGE(eventdatetime);

    -- Create Partitions of table 'sensors'.
    CREATE TABLE sensors_old PARTITION OF sensors FOR VALUES FROM ('2000-01-01') TO ('2020-01-01');
    CREATE TABLE sensors_2020_01_01 PARTITION OF sensors FOR VALUES FROM ('2020-01-01') TO ('2020-02-01');
    CREATE TABLE sensors_news PARTITION OF sensors FOR VALUES FROM ('2020-05-01') TO ('2025-01-01');

    CREATE TABLE sensorscolumnar_old PARTITION OF sensorscolumnar FOR VALUES FROM ('2000-01-01') TO ('2020-01-01')  USING COLUMNAR;
    CREATE TABLE sensorscolumnar_2020_01_01 PARTITION OF sensorscolumnar FOR VALUES FROM ('2020-01-01') TO ('2020-02-01')  USING COLUMNAR;
    CREATE TABLE sensorscolumnar_news PARTITION OF sensorscolumnar FOR VALUES FROM ('2020-05-01') TO ('2025-01-01') USING COLUMNAR;

    -- Create index on parent and child partitions.
    CREATE INDEX index_on_parent ON sensors(lower(measureid::text));
    CREATE INDEX index_on_child ON sensors_2020_01_01(lower(measure_data::text));

    CREATE INDEX index_on_parent_columnar ON sensorscolumnar(lower(measureid::text));
    CREATE INDEX index_on_child_columnar ON sensorscolumnar_2020_01_01(lower(measure_data::text));

    ALTER INDEX index_on_parent ALTER COLUMN 1 SET STATISTICS 1000;
    ALTER INDEX index_on_child ALTER COLUMN 1 SET STATISTICS 1000;

    ALTER INDEX index_on_parent_columnar ALTER COLUMN 1 SET STATISTICS 1000;
    ALTER INDEX index_on_child_columnar ALTER COLUMN 1 SET STATISTICS 1000;

    -- Create statistics on parent and child partitions.
    CREATE STATISTICS s1 (dependencies) ON measureid, eventdatetime FROM sensors;
    CREATE STATISTICS s2 (dependencies) ON measureid, eventdatetime FROM sensors_2020_01_01;

    CREATE STATISTICS s1_c (dependencies) ON measureid, eventdatetime FROM sensorscolumnar;
    CREATE STATISTICS s2_c (dependencies) ON measureid, eventdatetime FROM sensorscolumnar_2020_01_01;

    CLUSTER sensors_2020_01_01 USING index_on_child;
    SELECT create_distributed_table('sensors', 'measureid');
    SELECT create_distributed_table('sensorscolumnar', 'measureid');

    -- create colocated distributed tables
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

    -- create reference tables
    CREATE TABLE reference_table (measureid integer PRIMARY KEY);
    SELECT create_reference_table('reference_table');

    SELECT shard.shardid, logicalrelid, shardminvalue, shardmaxvalue, nodename, nodeport
    FROM pg_dist_shard AS shard
    INNER JOIN pg_dist_placement placement ON shard.shardid = placement.shardid
    INNER JOIN pg_dist_node       node     ON placement.groupid = node.groupid
    INNER JOIN pg_catalog.pg_class cls     ON shard.logicalrelid = cls.oid
    INNER JOIN pg_catalog.pg_namespace ns  ON cls.relnamespace = ns.oid
    WHERE node.noderole = 'primary' AND ns.nspname = 'citus_split_test_schema_columnar_partitioned'
    ORDER BY logicalrelid, shardminvalue::BIGINT, nodeport;
-- END: Create table to split, along with other co-located tables. Add indexes, statistics etc.

-- BEGIN: Create constraints for tables.
    -- from parent to regular dist
    ALTER TABLE sensors ADD CONSTRAINT fkey_from_parent_to_dist FOREIGN KEY (measureid) REFERENCES colocated_dist_table(measureid);

    -- from parent to parent
    ALTER TABLE sensors ADD CONSTRAINT fkey_from_parent_to_parent FOREIGN KEY (measureid, eventdatetime) REFERENCES colocated_partitioned_table(measureid, eventdatetime);

    -- from parent to child
    ALTER TABLE sensors ADD CONSTRAINT fkey_from_parent_to_child FOREIGN KEY (measureid, eventdatetime) REFERENCES colocated_partitioned_table_2020_01_01(measureid, eventdatetime);

    ALTER TABLE sensors ADD CONSTRAINT fkey_from_parent_to_ref FOREIGN KEY (measureid) REFERENCES reference_table(measureid);

    -- from child to regular dist
    ALTER TABLE sensors_2020_01_01 ADD CONSTRAINT fkey_from_child_to_dist FOREIGN KEY (measureid) REFERENCES colocated_dist_table(measureid);

    -- from child to parent
    ALTER TABLE sensors_2020_01_01 ADD CONSTRAINT fkey_from_child_to_parent FOREIGN KEY (measureid,eventdatetime) REFERENCES colocated_partitioned_table(measureid,eventdatetime);

    -- from child to child
    ALTER TABLE sensors_2020_01_01 ADD CONSTRAINT fkey_from_child_to_child FOREIGN KEY (measureid,eventdatetime) REFERENCES colocated_partitioned_table_2020_01_01(measureid,eventdatetime);

    ALTER TABLE sensors_2020_01_01 ADD CONSTRAINT fkey_from_child_to_ref FOREIGN KEY (measureid) REFERENCES reference_table(measureid);

    -- No support for foreign keys, unique constraints, or exclusion constraints in columnar tables.
    -- Please see: https://github.com/citusdata/citus/tree/main/src/backend/columnar/README.md

-- END: Create constraints for tables.

-- BEGIN: Load data into tables
    INSERT INTO reference_table SELECT i FROM generate_series(0,1000)i;
    INSERT INTO colocated_dist_table SELECT i FROM generate_series(0,1000)i;
    INSERT INTO colocated_partitioned_table SELECT i, '2020-01-05' FROM generate_series(0,1000)i;
    INSERT INTO sensors SELECT i, '2020-01-05', '{}' FROM generate_series(0,1000)i;
    INSERT INTO sensorscolumnar SELECT i, '2020-01-05', '{}' FROM generate_series(0,1000)i;
-- END: Load data into tables

-- BEGIN: Show the current state on workers
\c - - - :worker_1_port
    SET search_path TO "citus_split_test_schema_columnar_partitioned";
    SET citus.show_shards_for_app_name_prefixes = '*';
    SELECT tbl.relname, fk."Constraint", fk."Definition"
            FROM pg_catalog.pg_class tbl
            JOIN public.table_fkeys fk on tbl.oid = fk.relid
            WHERE tbl.relname like '%_89%'
            ORDER BY 1, 2;
    SELECT tablename, indexdef FROM pg_indexes WHERE tablename like '%_89%' ORDER BY 1,2;
    SELECT stxname FROM pg_statistic_ext
    WHERE stxnamespace IN (
        SELECT oid
        FROM pg_namespace
        WHERE nspname IN ('citus_split_test_schema_columnar_partitioned')
    )
    ORDER BY stxname ASC;

    \c - - - :worker_2_port
    SET search_path TO "citus_split_test_schema_columnar_partitioned";
    SET citus.show_shards_for_app_name_prefixes = '*';
    SELECT tbl.relname, fk."Constraint", fk."Definition"
            FROM pg_catalog.pg_class tbl
            JOIN public.table_fkeys fk on tbl.oid = fk.relid
            WHERE tbl.relname like '%_89%'
            ORDER BY 1, 2;
    SELECT tablename, indexdef FROM pg_indexes WHERE tablename like '%_89%' ORDER BY 1,2;
    SELECT stxname FROM pg_statistic_ext
    WHERE stxnamespace IN (
        SELECT oid
        FROM pg_namespace
        WHERE nspname IN ('citus_split_test_schema_columnar_partitioned')
    )
    ORDER BY stxname ASC;
-- END: Show the current state on workers

-- BEGIN: Split a shard along its co-located shards
\c - - - :master_port
    SET search_path TO "citus_split_test_schema_columnar_partitioned";
    SET citus.next_shard_id TO 8999000;
    SELECT nodeid AS worker_1_node FROM pg_dist_node WHERE nodeport=:worker_1_port \gset
    SELECT nodeid AS worker_2_node FROM pg_dist_node WHERE nodeport=:worker_2_port \gset

    SELECT pg_catalog.citus_split_shard_by_split_points(
        8970000,
        ARRAY['-2120000000'],
        ARRAY[:worker_1_node, :worker_2_node],
        'block_writes');
-- END: Split a shard along its co-located shards

-- BEGIN: Perform deferred cleanup.
SELECT public.wait_for_resource_cleanup();
-- END: Perform deferred cleanup.

-- BEGIN: Validate Shard Info and Data
    SELECT shard.shardid, logicalrelid, shardminvalue, shardmaxvalue, nodename, nodeport
    FROM pg_dist_shard AS shard
    INNER JOIN pg_dist_placement placement ON shard.shardid = placement.shardid
    INNER JOIN pg_dist_node       node     ON placement.groupid = node.groupid
    INNER JOIN pg_catalog.pg_class cls     ON shard.logicalrelid = cls.oid
    INNER JOIN pg_catalog.pg_namespace ns  ON cls.relnamespace = ns.oid
    WHERE node.noderole = 'primary' AND ns.nspname = 'citus_split_test_schema_columnar_partitioned'
    ORDER BY logicalrelid, shardminvalue::BIGINT, nodeport;

    SELECT count(*) FROM reference_table;
    SELECT count(*) FROM colocated_partitioned_table;
    SELECT count(*) FROM colocated_dist_table;
    SELECT count(*) FROM sensors;
    SELECT count(*) FROM sensorscolumnar;
-- END: Validate Shard Info and Data

-- BEGIN: Show the updated state on workers
    \c - - - :worker_1_port
    SET search_path TO "citus_split_test_schema_columnar_partitioned";
    SET citus.show_shards_for_app_name_prefixes = '*';
    SELECT tbl.relname, fk."Constraint", fk."Definition"
            FROM pg_catalog.pg_class tbl
            JOIN public.table_fkeys fk on tbl.oid = fk.relid
            WHERE tbl.relname like '%_89%'
            ORDER BY 1, 2;
    SELECT tablename, indexdef FROM pg_indexes WHERE tablename like '%_89%' ORDER BY 1,2;
    SELECT stxname FROM pg_statistic_ext
    WHERE stxnamespace IN (
        SELECT oid
        FROM pg_namespace
        WHERE nspname IN ('citus_split_test_schema_columnar_partitioned')
    )
    ORDER BY stxname ASC;

    \c - - - :worker_2_port
    SET search_path TO "citus_split_test_schema_columnar_partitioned";
    SET citus.show_shards_for_app_name_prefixes = '*';
    SELECT tbl.relname, fk."Constraint", fk."Definition"
            FROM pg_catalog.pg_class tbl
            JOIN public.table_fkeys fk on tbl.oid = fk.relid
            WHERE tbl.relname like '%_89%'
            ORDER BY 1, 2;
    SELECT tablename, indexdef FROM pg_indexes WHERE tablename like '%_89%' ORDER BY 1,2;
    SELECT stxname FROM pg_statistic_ext
    WHERE stxnamespace IN (
        SELECT oid
        FROM pg_namespace
        WHERE nspname IN ('citus_split_test_schema_columnar_partitioned')
    )
    ORDER BY stxname ASC;
-- END: Show the updated state on workers

-- BEGIN: Split a partition table directly
\c - - - :master_port
    SET search_path TO "citus_split_test_schema_columnar_partitioned";
    SET citus.next_shard_id TO 8999100;
    SELECT nodeid AS worker_1_node FROM pg_dist_node WHERE nodeport=:worker_1_port \gset
    SELECT nodeid AS worker_2_node FROM pg_dist_node WHERE nodeport=:worker_2_port \gset

    SELECT pg_catalog.citus_split_shard_by_split_points(
        8999002, -- sensors_old
        ARRAY['-2127770000'],
        ARRAY[:worker_1_node, :worker_2_node],
        'block_writes');
-- END: Split a partition table directly

-- BEGIN: Perform deferred cleanup.
SELECT public.wait_for_resource_cleanup();
-- END: Perform deferred cleanup.

-- BEGIN: Validate Shard Info and Data
    SELECT shard.shardid, logicalrelid, shardminvalue, shardmaxvalue, nodename, nodeport
    FROM pg_dist_shard AS shard
    INNER JOIN pg_dist_placement placement ON shard.shardid = placement.shardid
    INNER JOIN pg_dist_node       node     ON placement.groupid = node.groupid
    INNER JOIN pg_catalog.pg_class cls     ON shard.logicalrelid = cls.oid
    INNER JOIN pg_catalog.pg_namespace ns  ON cls.relnamespace = ns.oid
    WHERE node.noderole = 'primary' AND ns.nspname = 'citus_split_test_schema_columnar_partitioned'
    ORDER BY logicalrelid, shardminvalue::BIGINT, nodeport;

    SELECT count(*) FROM reference_table;
    SELECT count(*) FROM colocated_partitioned_table;
    SELECT count(*) FROM colocated_dist_table;
    SELECT count(*) FROM sensors;
    SELECT count(*) FROM sensorscolumnar;
-- END: Validate Shard Info and Data

-- BEGIN: Show the updated state on workers
    \c - - - :worker_1_port
    SET search_path TO "citus_split_test_schema_columnar_partitioned";
    SET citus.show_shards_for_app_name_prefixes = '*';
    SELECT tbl.relname, fk."Constraint", fk."Definition"
            FROM pg_catalog.pg_class tbl
            JOIN public.table_fkeys fk on tbl.oid = fk.relid
            WHERE tbl.relname like '%_89%'
            ORDER BY 1, 2;
    SELECT tablename, indexdef FROM pg_indexes WHERE tablename like '%_89%' ORDER BY 1,2;
    SELECT stxname FROM pg_statistic_ext
    WHERE stxnamespace IN (
        SELECT oid
        FROM pg_namespace
        WHERE nspname IN ('citus_split_test_schema_columnar_partitioned')
    )
    ORDER BY stxname ASC;

    \c - - - :worker_2_port
    SET search_path TO "citus_split_test_schema_columnar_partitioned";
    SET citus.show_shards_for_app_name_prefixes = '*';
    SELECT tbl.relname, fk."Constraint", fk."Definition"
            FROM pg_catalog.pg_class tbl
            JOIN public.table_fkeys fk on tbl.oid = fk.relid
            WHERE tbl.relname like '%_89%'
            ORDER BY 1, 2;
    SELECT tablename, indexdef FROM pg_indexes WHERE tablename like '%_89%' ORDER BY 1,2;
    SELECT stxname FROM pg_statistic_ext
    WHERE stxnamespace IN (
        SELECT oid
        FROM pg_namespace
        WHERE nspname IN ('citus_split_test_schema_columnar_partitioned')
    )
    ORDER BY stxname ASC;
-- END: Show the updated state on workers

--BEGIN : Cleanup
    \c - postgres - :master_port
    ALTER SYSTEM RESET citus.defer_shard_delete_interval;
    SELECT pg_reload_conf();
    DROP SCHEMA "citus_split_test_schema_columnar_partitioned" CASCADE;
--END : Cleanup
