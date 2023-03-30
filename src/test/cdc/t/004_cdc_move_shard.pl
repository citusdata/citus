# Schema change CDC test for Citus
use strict;
use warnings;

use Test::More;

use lib './t';
use cdctestlib;


# Initialize co-ordinator node
my $select_stmt = qq(SELECT * FROM sensors ORDER BY measureid, eventdatetime, measure_data;);
my $result = 0;

### Create the citus cluster with coordinator and two worker nodes
our ($node_coordinator, @workers) = create_citus_cluster(2,"localhost",57636);

our $node_cdc_client = create_node('cdc_client', 0, "localhost", 57639);

print("coordinator port: " . $node_coordinator->port() . "\n");
print("worker0 port:" . $workers[0]->port() . "\n");
print("worker1 port:" . $workers[1]->port() . "\n");
print("cdc_client port:" .$node_cdc_client->port() . "\n");

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
$node_cdc_client->safe_psql('postgres',$initial_schema);

create_cdc_publication_and_slots_for_coordinator($node_coordinator,'sensors');
connect_cdc_client_to_coordinator_publication($node_coordinator, $node_cdc_client);
wait_for_cdc_client_to_catch_up_with_coordinator($node_coordinator);

create_cdc_slots_for_workers(\@workers);

#insert data into the sensors table in the coordinator node before distributing the table.
$node_coordinator->safe_psql('postgres',"
	INSERT INTO sensors
SELECT i, '2020-01-05', '{}', 11011.10, 'A', 'I <3 Citus'
FROM generate_series(0,100)i;");

$node_coordinator->safe_psql('postgres',"SET citus.shard_count = 2; SELECT create_distributed_table_concurrently('sensors', 'measureid');");

connect_cdc_client_to_workers_publication(\@workers, $node_cdc_client);
wait_for_cdc_client_to_catch_up_with_citus_cluster($node_coordinator, \@workers);

$result = compare_tables_in_different_nodes($node_coordinator,$node_cdc_client,'postgres',$select_stmt);
is($result, 1, 'CDC create_distributed_table - schema change before move');



my $shard_to_move = $node_coordinator->safe_psql('postgres',
        "SELECT shardid FROM citus_shards ORDER BY shardid LIMIT 1;");
my $host1 = $node_coordinator->safe_psql('postgres',
        "SELECT nodename FROM citus_shards ORDER BY shardid LIMIT 1;");
my $port1 = $node_coordinator->safe_psql('postgres',
        "SELECT nodeport FROM citus_shards ORDER BY shardid LIMIT 1;");

my $shard_last = $node_coordinator->safe_psql('postgres',
        "SELECT shardid FROM citus_shards ORDER BY shardid DESC LIMIT 1;");
my $host2 = $node_coordinator->safe_psql('postgres',
        "SELECT nodename FROM citus_shards ORDER BY shardid DESC LIMIT 1;");
my $port2 = $node_coordinator->safe_psql('postgres',
        "SELECT nodeport FROM citus_shards ORDER BY shardid DESC LIMIT 1;");

my $move_params = "select citus_move_shard_placement($shard_to_move,'$host1',$port1,'$host2',$port2,'force_logical');";
print("move_params: $move_params\n");
$node_coordinator->safe_psql('postgres',$move_params);

wait_for_cdc_client_to_catch_up_with_citus_cluster($node_coordinator,\@workers);


#wait_for_cdc_client_to_catch_up_with_workers(\@workers);
$result = compare_tables_in_different_nodes($node_coordinator,$node_cdc_client,'postgres',$select_stmt);
is($result, 1, 'CDC create_distributed_table - schema change and move shard');

drop_cdc_client_subscriptions($node_cdc_client,\@workers);
done_testing();
