# Basic CDC test for create_distributed_table
use strict;
use warnings;

use Test::More tests => 1;

use lib './t';
use cdctestlib;

# Initialize co-ordinator node
my $select_stmt = qq(SELECT * FROM sensors ORDER BY measureid, eventdatetime, measure_data;);
my $result = 0;

### Create the citus cluster with coordinator and two worker nodes
our ($node_coordinator, @workers) = create_citus_cluster(2,"localhost",57636);
our $node_cdc_client = create_node('cdc_client', 0, "localhost", 57639);

$node_coordinator->safe_psql('postgres',"CREATE TABLE reference_table(measureid integer PRIMARY KEY);");
$node_cdc_client->safe_psql('postgres',"CREATE TABLE reference_table(measureid integer PRIMARY KEY);");

create_cdc_publication_and_slots_for_coordinator($node_coordinator,'reference_table');
connect_cdc_client_to_coordinator_publication($node_coordinator, $node_cdc_client);
wait_for_cdc_client_to_catch_up_with_coordinator($node_coordinator);

# Create the reference table in the coordinator and cdc client nodes.
$node_coordinator->safe_psql('postgres',"SELECT create_reference_table('reference_table');");

# Insert data to the reference table in the coordinator node.
$node_coordinator->safe_psql('postgres',"INSERT INTO reference_table SELECT i FROM generate_series(0,100)i;");

create_cdc_publication_and_slots_for_workers(\@workers,'reference_table');
connect_cdc_client_to_workers_publication(\@workers, $node_cdc_client);
wait_for_cdc_client_to_catch_up_with_citus_cluster($node_coordinator, \@workers);



$node_coordinator->safe_psql('postgres',"INSERT INTO reference_table SELECT i FROM generate_series(101,200)i;");

wait_for_cdc_client_to_catch_up_with_citus_cluster($node_coordinator, \@workers);

# Compare the data in the coordinator and cdc client nodes.
my $ref_select_stmt = qq(SELECT * FROM reference_table ORDER BY measureid;);
$result = compare_tables_in_different_nodes($node_coordinator,$node_cdc_client,'postgres',$ref_select_stmt);
is($result, 1, 'CDC reference taable test');

drop_cdc_client_subscriptions($node_cdc_client,\@workers);
