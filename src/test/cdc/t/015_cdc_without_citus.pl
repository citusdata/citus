# Schema change CDC test for Citus
use strict;
use warnings;

use Test::More;

use lib './t';
use cdctestlib;


# Initialize co-ordinator node
my $select_stmt = qq(SELECT * FROM data_100008 ORDER BY id;);
my $result = 0;

### Create the citus cluster with coordinator and two worker nodes
our ($node_coordinator, @workers) = create_citus_cluster(2,"localhost",57636);

our $node_cdc_client = create_node('cdc_client', 0, "localhost", 57639);

print("coordinator port: " . $node_coordinator->port() . "\n");
print("worker0 port:" . $workers[0]->port() . "\n");
print("worker1 port:" . $workers[1]->port() . "\n");
print("cdc_client port:" .$node_cdc_client->port() . "\n");

my $initial_schema = "
        CREATE TABLE data_100008(
                id  integer,
                data integer,
                PRIMARY KEY (data));";

$node_coordinator->safe_psql('postgres','DROP extension citus');
$node_coordinator->safe_psql('postgres',$initial_schema);
$node_coordinator->safe_psql('postgres','ALTER TABLE data_100008 REPLICA IDENTITY FULL;');

$node_cdc_client->safe_psql('postgres',$initial_schema);


create_cdc_publication_and_slots_for_coordinator($node_coordinator,'data_100008');
connect_cdc_client_to_coordinator_publication($node_coordinator, $node_cdc_client);
wait_for_cdc_client_to_catch_up_with_coordinator($node_coordinator);

#insert data into the sensors table in the coordinator node before distributing the table.
$node_coordinator->safe_psql('postgres',"
  	INSERT INTO data_100008
  SELECT i, i*10
  FROM generate_series(-10,10)i;");


$result = compare_tables_in_different_nodes($node_coordinator,$node_cdc_client,'postgres',$select_stmt);
is($result, 1, 'CDC create_distributed_table - basic test');

$node_cdc_client->safe_psql('postgres',"drop subscription cdc_subscription");
done_testing();
