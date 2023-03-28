# CDC test for inserts during create distributed table concurrently
use strict;
use warnings;

use Test::More;

use lib './t';
use cdctestlib;

use threads;


# Initialize co-ordinator node
our $select_stmt = qq(SELECT * FROM sensors ORDER BY measureid, eventdatetime, measure_data;);
my $add_local_meta_data_stmt = qq(SELECT citus_add_local_table_to_metadata('sensors'););
my $result = 0;

### Create the citus cluster with coordinator and two worker nodes
our ($node_coordinator, @workers) = create_citus_cluster(2,"localhost",57636);

our $node_cdc_client = create_node('cdc_client', 0, "localhost", 57639, "");

# Creeate the sensors table and ndexes.
my $initial_schema = "
        CREATE TABLE sensors(
        measureid               integer,
        eventdatetime           timestamptz,
        measure_data            jsonb,
        meaure_quantity         decimal(15, 2),
        measure_status          char(1),
        measure_comment         varchar(44),
        PRIMARY KEY (measureid, eventdatetime, measure_data));

        CREATE INDEX index_on_sensors ON sensors(lower(measureid::text));
        ALTER INDEX index_on_sensors ALTER COLUMN 1 SET STATISTICS 1000;
        CREATE INDEX hash_index_on_sensors ON sensors USING HASH((measure_data->'IsFailed'));
        CREATE INDEX index_with_include_on_sensors ON sensors ((measure_data->'IsFailed')) INCLUDE (measure_data, eventdatetime, measure_status);
        CREATE STATISTICS stats_on_sensors (dependencies) ON measureid, eventdatetime FROM sensors;";

$node_coordinator->safe_psql('postgres',$initial_schema);
$node_coordinator->safe_psql('postgres',$add_local_meta_data_stmt);
$node_cdc_client->safe_psql('postgres',$initial_schema);


create_cdc_publication_and_replication_slots_for_citus_cluster($node_coordinator,\@workers,'sensors');
connect_cdc_client_to_citus_cluster_publications($node_coordinator,\@workers,$node_cdc_client);

#insert data into the sensors table in the coordinator node before distributing the table.
$node_coordinator->safe_psql('postgres',"
	INSERT INTO sensors
SELECT i, '2020-01-05', '{}', 11011.10, 'A', 'I <3 Citus'
FROM generate_series(0,10)i;");

wait_for_cdc_client_to_catch_up_with_citus_cluster($node_coordinator,\@workers);

$result = compare_tables_in_different_nodes($node_coordinator,$node_cdc_client,'postgres',$select_stmt);
is($result, 1, 'CDC create_distributed_table - insert data');

sub create_distributed_table_thread() {
	$node_coordinator->safe_psql('postgres',"SELECT create_distributed_table_concurrently('sensors', 'measureid');");
}

sub insert_data_into_distributed_table_thread() {
	# Insert some data to the sensors table in the coordinator node.
	$node_coordinator->safe_psql('postgres',"
	 INSERT INTO sensors
		SELECT i, '2020-01-05', '{}', 11011.10, 'A', 'I <3 Citus'
		FROM generate_series(-10,-1)i;");
}

# Create the distributed table concurrently in a separate thread.
my $thr_create = threads->create(\&create_distributed_table_thread);
my $thr_insert = threads->create(\&insert_data_into_distributed_table_thread);
$thr_create->join();
$thr_insert->join();

wait_for_cdc_client_to_catch_up_with_citus_cluster($node_coordinator,\@workers);
$result = compare_tables_in_different_nodes($node_coordinator,$node_cdc_client,'postgres',$select_stmt);
is($result, 1, 'CDC create_distributed_table - insert data');


drop_cdc_client_subscriptions($node_cdc_client,\@workers);
done_testing();
