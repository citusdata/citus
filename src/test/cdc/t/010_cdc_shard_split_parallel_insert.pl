# Basic CDC test for create_distributed_table
use strict;
use warnings;

use Test::More;

use lib './t';
use cdctestlib;

use threads;

# Initialize co-ordinator node
my $select_stmt = qq(SELECT * FROM sensors ORDER BY measureid, eventdatetime, measure_data;);
my $result = 0;
my $citus_config = "
citus.shard_count = 2
citus.shard_replication_factor = 1
";
### Create the citus cluster with coordinator and two worker nodes
our ($node_coordinator, @workers) = create_citus_cluster(1,"localhost",57636, $citus_config);

my $command = "UPDATE pg_dist_node SET shouldhaveshards = true;";
$node_coordinator->safe_psql('postgres',$command);

our $node_cdc_client = create_node('cdc_client', 0, "localhost", 57639);

# Create the sensors table and ndexes.
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
$node_cdc_client->safe_psql('postgres',$initial_schema);

create_cdc_publication_and_slots_for_coordinator($node_coordinator,'sensors');
connect_cdc_client_to_coordinator_publication($node_coordinator, $node_cdc_client);
wait_for_cdc_client_to_catch_up_with_coordinator($node_coordinator);

create_cdc_slots_for_workers(\@workers);

# Distribut the sensors table to worker nodes.
$node_coordinator->safe_psql('postgres',"SELECT create_distributed_table('sensors', 'measureid');");

connect_cdc_client_to_workers_publication(\@workers, $node_cdc_client);
wait_for_cdc_client_to_catch_up_with_citus_cluster($node_coordinator, \@workers);

# Compare the data in the coordinator and cdc client nodes.
$result = compare_tables_in_different_nodes($node_coordinator,$node_cdc_client,'postgres',$select_stmt);
is($result, 1, 'CDC split test - distributed table create data');

# Insert some data to the sensors table in the coordinator node.
$node_coordinator->safe_psql('postgres',"
 INSERT INTO sensors
	SELECT i, '2020-01-05', '{}', 11011.10, 'A', 'I <3 Citus'
	FROM generate_series(-100,100)i;");

# Compare the data in the coordinator and cdc client nodes.
$result = compare_tables_in_different_nodes($node_coordinator,$node_cdc_client,'postgres',$select_stmt);
is($result, 1, 'CDC split test - distributed table insert data');


sub insert_data_into_distributed_table_thread() {
        # Insert some data to the sensors table in the coordinator node.
        $node_coordinator->safe_psql('postgres',"
        INSERT INTO sensors
                SELECT i, '2020-01-05', '{}', 11011.10, 'A', 'I <3 Citus'
                FROM generate_series(101,200)i;");
}

sub split_distributed_table_thread() {
        $node_coordinator->safe_psql('postgres',"
        SELECT citus_split_shard_by_split_points(102008,ARRAY['-50'],ARRAY[1,2], 'force_logical');");
}

# Create the distributed table concurrently in a separate thread.
my $thr_create = threads->create(\&split_distributed_table_thread);

# Insert some data to the sensors table in the coordinator node while the table is being distributed.
my $thr_insert = threads->create(\&insert_data_into_distributed_table_thread);

# Wait for the threads to finish.
$thr_create->join();
$thr_insert->join();

# Wait for the data changes to be replicated to the cdc client node.
wait_for_cdc_client_to_catch_up_with_citus_cluster($node_coordinator, \@workers);


# Compare the data in the coordinator and cdc client nodes.
$result = compare_tables_in_different_nodes($node_coordinator,$node_cdc_client,'postgres',$select_stmt);
is($result, 1, 'CDC split test - distributed table split data');

drop_cdc_client_subscriptions($node_cdc_client,\@workers);
done_testing();
