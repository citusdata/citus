# Basic CDC test for create_distributed_table
use strict;
use warnings;

use Test::More;

use lib './t';
use cdctestlib;

# Initialize co-ordinator node
my $select_stmt = qq(SELECT * FROM sensors ORDER BY measureid, eventdatetime, measure_data;);
my $add_local_meta_data_stmt = qq(SELECT citus_add_local_table_to_metadata('sensors'););
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
$node_coordinator->safe_psql('postgres',$add_local_meta_data_stmt);
$node_cdc_client->safe_psql('postgres',$initial_schema);

create_cdc_publication_and_replication_slots_for_citus_cluster($node_coordinator,\@workers,'sensors');
connect_cdc_client_to_citus_cluster_publications($node_coordinator,\@workers,$node_cdc_client);

# Distribut the sensors table to worker nodes.
$node_coordinator->safe_psql('postgres',"SELECT create_distributed_table_concurrently('sensors', 'measureid');");

wait_for_cdc_client_to_catch_up_with_citus_cluster($node_coordinator,\@workers);

# Compare the data in the coordinator and cdc client nodes.
$result = compare_tables_in_different_nodes($node_coordinator,$node_cdc_client,'postgres',$select_stmt);
is($result, 1, 'CDC split test - distributed table create data');

# Insert some data to the sensors table in the coordinator node.
$node_coordinator->safe_psql('postgres',"
 SELECT alter_distributed_table('sensors', shard_count:=6, cascade_to_colocated:=true);");

wait_for_cdc_client_to_catch_up_with_citus_cluster($node_coordinator, \@workers);

# Compare the data in the coordinator and cdc client nodes.
$result = compare_tables_in_different_nodes($node_coordinator,$node_cdc_client,'postgres',$select_stmt);
is($result, 1, 'CDC split test - alter distributed table ');

#$node_cdc_client->safe_psql("postgres","alter subscription cdc_subscription refresh publication;");
$node_cdc_client->safe_psql("postgres","alter subscription cdc_subscription_1 refresh publication;");


#Drop the CDC client subscription and recreate them , since the
#alter_distributed_table has changed the Oid of the distributed table.
#So the  CDC client has to create Oid to table mappings again for
#CDC to work again.
drop_cdc_client_subscriptions($node_cdc_client,\@workers);
create_cdc_publication_and_replication_slots_for_citus_cluster($node_coordinator,\@workers,'sensors');
connect_cdc_client_to_citus_cluster_publications($node_coordinator,\@workers,$node_cdc_client);

# Insert some data to the sensors table in the coordinator node.
$node_coordinator->safe_psql('postgres',"
 INSERT INTO sensors
	SELECT i, '2020-01-05', '{}', 11011.10, 'A', 'I <3 Citus'
	FROM generate_series(0,10)i;");

# Wait for the data changes to be replicated to the cdc client node.
wait_for_cdc_client_to_catch_up_with_citus_cluster($node_coordinator, \@workers);

$result = compare_tables_in_different_nodes($node_coordinator,$node_cdc_client,'postgres',$select_stmt);
is($result, 1, 'CDC basic test - distributed table insert data');

drop_cdc_client_subscriptions($node_cdc_client,\@workers);

done_testing();
