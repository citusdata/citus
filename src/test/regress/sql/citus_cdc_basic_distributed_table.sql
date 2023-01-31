/*
Citus CDC basic Test to verify a distributed table's INSERT/UPDATE/DELETE operations are published correctly to a subscriber without duplication.
Here is a high level overview of test plan:
 1. Co-ordinator node: Create a table 'sensors' and add indexes and statistics on this table, create distributed table from sesnsors table.
 2. CDC client node: Create tables to receive CDC events published from worker nodes holding the distributed table.
 3. Co-ordinator node: Insert 1001 rows in distributed sensors table and make sure those inserts are replicated in CDC client.
 4. Co-ordinator node: Update 1001 rows in distributed sensors table and make sure those updates are replicated in CDC client.
 5. Co-ordinator node: Delete half the rows in distributed sensors table and make sure those deletes are replicated in CDC client.
 6. Clean up all the resources created.
*/

-- BEGIN 1: Co-ordinator node: Create a table 'sensors' and add indexes and statistics on this table, create distributed table from sesnsors table.

\c - postgres - :master_port
CREATE SCHEMA "citus_cdc_test_schema";
SET search_path TO "citus_cdc_test_schema";
SET citus.next_shard_id TO 8981000;
SET citus.next_placement_id TO 8610000;
SET citus.shard_count TO 2;
SET citus.shard_replication_factor TO 1;

SELECT 1 FROM citus_set_coordinator_host('localhost', :master_port);
SELECT 1 FROM master_add_node('localhost', :worker_1_port);
SELECT 1 from master_add_node('localhost', :worker_2_port);

SELECT nodeid AS worker_1_node FROM pg_dist_node WHERE nodeport=:worker_1_port \gset
SELECT nodeid AS worker_2_node FROM pg_dist_node WHERE nodeport=:worker_2_port \gset

-- Worker3 node will be used as the CDC client port for CDC tests.
\set cdc_client_port :worker_3_port

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

-- Create the sensors distributed table and create he publications and replication slots in all worker nodes.
SELECT create_distributed_table('sensors', 'measureid', colocate_with:='none');

\c - postgres - :worker_1_port
SET search_path TO "citus_cdc_test_schema";
CREATE PUBLICATION cdc_publication FOR TABLE sensors;
SELECT slot_name AS slot_name_1 FROM pg_create_logical_replication_slot(FORMAT('cdc_replication_slot%s', :worker_1_node), 'citus',false,true) \gset

\c - postgres - :worker_2_port
SET search_path TO "citus_cdc_test_schema";
CREATE PUBLICATION cdc_publication FOR TABLE sensors;
SELECT slot_name AS slot_name_2 FROM pg_create_logical_replication_slot(FORMAT('cdc_replication_slot%s', :worker_2_node), 'citus',false,true) \gset
-- END 1:


-- BEGIN 2:	 Connect to the CDC client node and create table to receive CDC events published from worker nodes holding the distributed table.
\c - postgres - :cdc_client_port
CREATE SCHEMA "citus_cdc_test_schema";
SET search_path TO "citus_cdc_test_schema";

-- Disable Deferred drop auto cleanup to avoid flaky tests.
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

-- This function waits for the subscription to catch up with the given remote node's LSN.
CREATE OR REPLACE FUNCTION wait_for_subscription_to_catchup(subscription_name text, remoteLsn bigint) RETURNS void AS $$
DECLARE
	receivedLsn bigint;
BEGIN
    EXECUTE FORMAT('SELECT min(latest_end_lsn)-''0/0'' FROM pg_stat_subscription WHERE subname = ''%s'';', subscription_name) INTO receivedLsn;
	WHILE receivedLsn IS NULL  OR remoteLsn > receivedLsn LOOP
		EXECUTE FORMAT('SELECT min(latest_end_lsn)-''0/0'' FROM pg_stat_subscription WHERE subname = ''%s'';', subscription_name) INTO receivedLsn;
		PERFORM pg_sleep(1);
    END LOOP;
END$$ LANGUAGE plpgsql;

-- Create subscriptions to receive events from worker nodes for the distributed table.
\set connection_string '\'user=postgres host=localhost port=' :worker_1_port ' dbname=regression\''
CREATE SUBSCRIPTION cdc_subscription_1
        CONNECTION :connection_string
        PUBLICATION cdc_publication
               WITH (
                   create_slot=false,
                   enabled=true,
                   slot_name=:slot_name_1,
                   copy_data=false);

-- Create subscriptions to receive events from worker nodes for the distributed table.
\set connection_string '\'user=postgres host=localhost port=' :worker_2_port ' dbname=regression\''
CREATE SUBSCRIPTION cdc_subscription_2
        CONNECTION :connection_string
        PUBLICATION cdc_publication
               WITH (
                   create_slot=false,
                   enabled=true,
                   slot_name=:slot_name_2,
                   copy_data=false);
-- END 2

-- BEGIN 3: Insert 1001 rows in distributed sensors table and make sure those inserts are replicated in CDC client.

\c - postgres - :master_port
SET search_path TO "citus_cdc_test_schema";
INSERT INTO sensors
	SELECT i, '2020-01-05', '{}', 11011.10, 'A', 'I <3 Citus'
	FROM generate_series(0,1000)i;

-- Export the source distributed table into a CSV file for comparison later with table formed using CDC subscriprions.
\COPY (select * from sensors ORDER BY measureid, eventdatetime, measure_data) TO 'results/sensors1.csv' DELIMITER ',' CSV HEADER;

\c - postgres - :worker_1_port
SELECT pg_current_wal_lsn()-'0/0' AS remote_lsn_1 \gset

\c - postgres - :worker_2_port
SELECT pg_current_wal_lsn()-'0/0' AS remote_lsn_2 \gset

\c - postgres - :cdc_client_port
-- Create subscription to connect to co-ordinator and replicate the sensors table.
SET search_path TO "citus_cdc_test_schema";

-- Wait for CDC subscriptions to catch up with publishers (worker nodes of distributed table)
SELECT wait_for_subscription_to_catchup('cdc_subscription_1',:remote_lsn_1);
SELECT wait_for_subscription_to_catchup('cdc_subscription_2',:remote_lsn_2);

-- Export the local table created by CDC subscriptions into a CSV file.
\COPY (select * from sensors ORDER BY measureid, eventdatetime, measure_data) TO 'results/sensors2.csv' DELIMITER ',' CSV HEADER;

-- Compare the dumps from original distributed table with the dump from the CDC subscribed table and make sure there are no differences.
\! diff results/sensors1.csv results/sensors2.csv

-- END 3:


-- BEGIN 4: Update 1001 rows in distributed sensors table and make sure those updates are replicated in CDC client.

\c - postgres - :master_port

SET search_path TO "citus_cdc_test_schema";
UPDATE sensors
	SET eventdatetime=NOW(),measure_data = jsonb_set(measure_data, '{val}', to_jsonb(measureid * 10)),
	meaure_quantity = (measureid  * 10) + 5,
	measure_status = CASE
		WHEN measureid % 2 = 0
			THEN 'y'
			ELSE 'n'
		END,
	measure_comment= 'Comment:' || measureid::text;
-- Export the source distributed table into a CSV file for comparison later with table forme	d using CDC subscriprions in CDC client node.
\COPY (select * from sensors ORDER BY measureid, eventdatetime, measure_data) TO 'results/sensors1.csv' DELIMITER ',' CSV HEADER;

\c - postgres - :worker_1_port
SELECT pg_current_wal_lsn()-'0/0' AS remote_lsn_1 \gset

\c - postgres - :worker_2_port
SELECT pg_current_wal_lsn()-'0/0' AS remote_lsn_2 \gset


-- Connect to the CDC client node and wait for the subscriptions to catch up with the worker nodes.
\c - postgres - :cdc_client_port
-- Create subscription to connect to co-ordinator and replicate the sensors table.
SET search_path TO "citus_cdc_test_schema";

--wait for CDC subscriptions to catch up with publishers (worker nodes of distributed table)
SELECT wait_for_subscription_to_catchup('cdc_subscription_1',:remote_lsn_1);
SELECT wait_for_subscription_to_catchup('cdc_subscription_2',:remote_lsn_2);

-- Export the local table created by CDC subscriptions into a CSV file.
\COPY (select * from sensors ORDER BY measureid, eventdatetime, measure_data) TO 'results/sensors2.csv' DELIMITER ',' CSV HEADER;

-- Compare the dumps from original distributed table with the dump from the CDC subscribed table and make sure there are no differences.
\! diff results/sensors1.csv results/sensors2.csv

-- END 4:

-- BEGIN 5: Delete half the rows in distributed sensors table and make sure those deletes are replicated in CDC client.

\c - postgres - :master_port

SET search_path TO "citus_cdc_test_schema";

DELETE FROM sensors	WHERE (measureid %2) == 0;

-- Export the source distributed table into a CSV file for comparison later with table forme	d using CDC subscriprions in CDC client node.
\COPY (select * from sensors ORDER BY measureid, eventdatetime, measure_data) TO 'results/sensors1.csv' DELIMITER ',' CSV HEADER;

\c - postgres - :worker_1_port
SELECT pg_current_wal_lsn()-'0/0' AS remote_lsn_1 \gset

\c - postgres - :worker_2_port
SELECT pg_current_wal_lsn()-'0/0' AS remote_lsn_2 \gset


-- Connect to the CDC client node and wait for the subscriptions to catch up with the worker nodes.
\c - postgres - :cdc_client_port
-- Create subscription to connect to co-ordinator and replicate the sensors table.
SET search_path TO "citus_cdc_test_schema";

--wait for CDC subscriptions to catch up with publishers (worker nodes of distributed table)
SELECT wait_for_subscription_to_catchup('cdc_subscription_1',:remote_lsn_1);
SELECT wait_for_subscription_to_catchup('cdc_subscription_2',:remote_lsn_2);

-- Export the local table created by CDC subscriptions into a CSV file.
\COPY (select * from sensors ORDER BY measureid, eventdatetime, measure_data) TO 'results/sensors2.csv' DELIMITER ',' CSV HEADER;

-- Compare the dumps from original distributed table with the dump from the CDC subscribed table and make sure there are no differences.
\! diff results/sensors1.csv results/sensors2.csv

-- END 5:

--Begin 6: Cleanup all the resources created.
DROP SCHEMA "citus_cdc_test_schema" CASCADE;
\c - postgres - :master_port
DROP SCHEMA "citus_cdc_test_schema" CASCADE;
--END 5 :
