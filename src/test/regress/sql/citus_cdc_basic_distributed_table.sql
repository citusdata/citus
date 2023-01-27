/*
Citus CDC basic Test to verify a distributed table's inserts are published correctly to a subscriber.
Here is a high level overview of test plan:
 1. Co-ordinator node: Create a table 'sensors' and add indexes and statistics on this table, create distributed table from sesnsors table.
 2. CDC node: Create a table 'sensors' and add indexes and statistics on this table, create subscription to connect to co-ordinator node.
 3. Co-ordinator node: Connect to co-ordinator node and populate the distributed table and store the current LSN values of worker nodes in variables.
 4. CDC node: Wait for the data to be replicated, select the count of rows to sensors table 
    and match it with the number of rows inserted in the distributed table.
 5. Clean up all the resources created.
*/

CREATE SCHEMA "citus_cdc_test_schema";
CREATE ROLE test_cdc_role WITH LOGIN;
GRANT USAGE, CREATE ON SCHEMA "citus_cdc_test_schema" TO test_cdc_role;
SET ROLE test_cdc_role;

-- BEGIN 1:	 Connect to the co-ordinator node and create table, indexes and statistics and create CDC replication slots in worker nodes.
\c - postgres - :worker_1_port
CREATE SCHEMA "citus_cdc_test_schema";
CREATE ROLE test_cdc_role WITH LOGIN;
GRANT USAGE, CREATE ON SCHEMA "citus_cdc_test_schema" TO test_cdc_role;
SET ROLE test_cdc_role;

\c - postgres - :worker_2_port
CREATE SCHEMA "citus_cdc_test_schema";
CREATE ROLE test_cdc_role WITH LOGIN;
GRANT USAGE, CREATE ON SCHEMA "citus_cdc_test_schema" TO test_cdc_role;
SET ROLE test_cdc_role;

\c - postgres - :master_port
SET search_path TO "citus_cdc_test_schema";
SET citus.next_shard_id TO 8981000;
SET citus.next_placement_id TO 8610000;
SET citus.shard_count TO 2;
SET citus.shard_replication_factor TO 1;

SELECT 1 FROM citus_set_coordinator_host('localhost', :master_port);
SELECT 1 FROM master_add_node('localhost', :worker_1_port);
SELECT 1 from master_add_node('localhost', :worker_2_port);	

CREATE TABLE sensors(
    measureid               integer,
    eventdatetime           date,
    measure_data            jsonb,
	meaure_quantity         decimal(15, 2),
    measure_status          char(1),
	measure_comment         varchar(44),
    PRIMARY KEY (measureid, eventdatetime, measure_data));
	
CREATE INDEX index_on_sensors ON sensors(lower(measureid::text));
ALTER INDEX index_on_sensors ALTER COLUMN 1 SET STATISTICS 1000;
CREATE INDEX hash_index_on_sensors ON sensors USING HASH((measure_data->'IsFailed'));
CREATE INDEX index_with_include_on_sensors ON sensors ((measure_data->'IsFailed')) INCLUDE (measure_data, eventdatetime, measure_status);
CREATE STATISTICS stats_on_sensors (dependencies) ON measureid, eventdatetime FROM sensors;

-- Create publication and logical replication slot for CDC client to connect to.
CREATE PUBLICATION cdc_publication FOR TABLE sensors;
SELECT pg_create_logical_replication_slot('cdc_replication_slot', 'citus', false, true);

-- Create the sensors distributed table.
SELECT create_distributed_table('sensors', 'measureid', colocate_with:='none');

\c - postgres - :worker_1_port
SET search_path TO "citus_cdc_test_schema";
CREATE PUBLICATION cdc_publication FOR TABLE sensors;
SELECT pg_create_logical_replication_slot('cdc_replication_slot', 'citus', false, true);

\c - postgres - :worker_2_port
SET search_path TO "citus_cdc_test_schema";
CREATE PUBLICATION cdc_publication FOR TABLE sensors;
SELECT pg_create_logical_replication_slot('cdc_replication_slot', 'citus', false, true);
-- END 1:


-- BEGIN 2:	 Connect to the CDC client node and create table to receive events published from distributed table, indexes and statistics
\c - postgres - :worker_3_port
CREATE SCHEMA "citus_cdc_test_schema";
SET search_path TO "citus_cdc_test_schema";

-- Disable Deferred drop auto cleanup to avoid flaky tests.
ALTER SYSTEM SET citus.defer_shard_delete_interval TO -1;
ALTER SYSTEM SET citus.override_table_visibility to off;
SELECT pg_reload_conf();

CREATE TABLE sensors(
    measureid               integer,
    eventdatetime           date,
    measure_data            jsonb,
	meaure_quantity         decimal(15, 2),
    measure_status          char(1),
	measure_comment         varchar(44),
    PRIMARY KEY (measureid, eventdatetime, measure_data));

CREATE INDEX index_on_sensors ON sensors(lower(measureid::text));
ALTER INDEX index_on_sensors ALTER COLUMN 1 SET STATISTICS 1000;
CREATE INDEX hash_index_on_sensors ON sensors USING HASH((measure_data->'IsFailed'));
CREATE INDEX index_with_include_on_sensors ON sensors ((measure_data->'IsFailed')) INCLUDE (measure_data, eventdatetime, measure_status);
CREATE STATISTICS stats_on_sensors (dependencies) ON measureid, eventdatetime FROM sensors;

-- Create subscriptions to receive events from worker nodes for the distributed table.
CREATE SUBSCRIPTION cdc_subscription_1
        CONNECTION 'host=localhost port=57637 user=postgres dbname=regression'
        PUBLICATION cdc_publication
               WITH (
                   create_slot=false,
                   enabled=true,
                   slot_name='cdc_replication_slot',
                   copy_data=false);

-- Create subscriptions to receive events from worker nodes for the distributed table.				   
CREATE SUBSCRIPTION cdc_subscription_2
        CONNECTION 'host=localhost port=57638 user=postgres dbname=regression'
        PUBLICATION cdc_publication
               WITH (
                   create_slot=false,
                   enabled=true,
                   slot_name='cdc_replication_slot',
                   copy_data=false);
-- END 2

-- BEGIN 3: connect to co-ordinator node and populate the distributed table and store the current LSN values of worker nodes in variables.
\c - postgres - :master_port
SET search_path TO "citus_cdc_test_schema";
INSERT INTO sensors 
	SELECT i, '2020-01-05', '{}', 11011.10, 'A', 'I <3 Citus' 
	FROM generate_series(0,1000)i;
	
SELECT COUNT(*) AS distribued_sensor_count FROM sensors \gset
	
\c - postgres - :worker_1_port
SELECT pg_current_wal_lsn()-'0/0' AS remote_lsn_1 \gset

\c - postgres - :worker_2_port
SELECT pg_current_wal_lsn()-'0/0' AS remote_lsn_2 \gset
-- END 3: connect to co-ordinator node and populate the distributed table and note down the LSN values of worker nodes in variables.
	

-- BEGIN 4: connect to CDC client node and wait for the subscriptions to catch up with the worker nodes.
\c - postgres - :worker_3_port
-- Create subscription to connect to co-ordinator and replicate the sensors table.
SET search_path TO "citus_cdc_test_schema";

-- This function waits for the subscription to catch up with the given remote node's LSN.
CREATE OR REPLACE FUNCTION citus_cdc_test_schema.wait_for_subscription_to_catchup(subscription_name text, remoteLsn bigint) RETURNS void AS $$
DECLARE
	receivedLsn bigint;
BEGIN
    EXECUTE FORMAT('SELECT min(latest_end_lsn)-''0/0'' FROM pg_stat_subscription WHERE subname = ''%s'';', subscription_name) INTO receivedLsn;
	WHILE receivedLsn IS NULL  OR remoteLsn > receivedLsn LOOP
		EXECUTE FORMAT('SELECT min(latest_end_lsn)-''0/0'' FROM pg_stat_subscription WHERE subname = ''%s'';', subscription_name) INTO receivedLsn;
		PERFORM pg_sleep(1);
    END LOOP;
END$$ LANGUAGE plpgsql;	

SELECT wait_for_subscription_to_catchup('cdc_subscription_1',:remote_lsn_1);
SELECT wait_for_subscription_to_catchup('cdc_subscription_2',:remote_lsn_2);

SELECT COUNT(*) AS cdc_client_sensor_count FROM sensors \gset
\echo "expected_sensor_count: " :distribued_sensor_count "cdc_client_sensor_count: " :cdc_client_sensor_count
-- END 4: connect to CDC client node and wait for the subscriptions to catch up with the worker nodes.

--Begin 5: Cleanup all the resources created.
DROP SCHEMA "citus_cdc_test_schema" CASCADE;
\c - postgres - :master_port
DROP SCHEMA "citus_cdc_test_schema" CASCADE;
--END : Cleanup
