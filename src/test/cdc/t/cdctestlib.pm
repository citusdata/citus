use strict;
use warnings;

my $pg_major_version =  int($ENV{'pg_major_version'});
print("working with PG major version : $pg_major_version\n");
if ($pg_major_version >= 15) {
    eval "use PostgreSQL::Test::Cluster";
    eval "use PostgreSQL::Test::Utils";
} else {
    eval "use PostgresNode";
}


#use PostgresNode;
use DBI;

our $NODE_TYPE_COORDINATOR = 1;
our $NODE_TYPE_WORKER = 2;
our $NODE_TYPE_CDC_CLIENT = 3;

sub compare_tables_in_different_nodes
{
    my $result = 1;
    my ($node1, $node2, $dbname, $stmt) = @_;

    # Connect to the first database node
    my $dbh1 = DBI->connect("dbi:Pg:" . $node1->connstr($dbname));

    # Connect to the second database node
    my $dbh2 = DBI->connect("dbi:Pg:" . $node2->connstr($dbname));

    # Define the SQL query for the first database node
    my $sth1 = $dbh1->prepare($stmt);
    $sth1->execute();

    # Define the SQL query for the second database node
    my $sth2 = $dbh2->prepare($stmt);
    $sth2->execute();

    # Get the field names for the table
    my @field_names = @{$sth2->{NAME}};

    #$sth1->dump_results();
    #$sth2->dump_results();

    our @row1, our @row2;

    # Use a cursor to iterate over the first database node's data
    while (1) {

        @row1 = $sth1->fetchrow_array();
        @row2 = $sth2->fetchrow_array();
        #print("row1: @row1\n");
        #print("row2: @row2\n");

        # Use a cursor to iterate over the second database node's data
        if (@row1 and @row2) {
            my $field_count_row1 = scalar @row1;
            my $field_count_row2 = scalar @row2;
            if ($field_count_row1 != $field_count_row2) {
                print "Field count mismatch: $field_count_row1 != $field_count_row2 \n";
                print "First row: @row1\n";
                #print "Second row: @row2\n";
                for (my $i = 0; $i < scalar @row2; $i++) {
                    print("Field $i, field name: $field_names[$i], value: $row2[$i] \n");
                }
                $result = 0;
                last;
            }
            # Compare the data in each field in each row of the two nodes
            for (my $i = 0; $i < scalar @row1; $i++) {
                if ($row1[$i] ne $row2[$i]) {
                    print "Data mismatch in field '$field_names[$i]'\n";
                    print "$row1[$i] != $row2[$i]\n";
                    print "First row: @row1\n";
                    print "Second row: @row2\n";
                    $result = 0;
                    last;
                }
            }
        } elsif (@row1 and !@row2) {
            print "First node has more rows than the second node\n";
            $result = 0;
            last;
        } elsif (!@row1 and @row2) {
            print "Second node has more rows than the first node\n";
            $result = 0;
            last;
        } else {
            last;
        }
    }

    $sth1->finish();
    $sth2->finish();
    $dbh1->disconnect();
    $dbh2->disconnect();
    return $result;
}

sub create_node {
    my ($name,$node_type,$host, $port) = @_;

    our $node;

    if ($pg_major_version >= 15) {
        $PostgreSQL::Test::Cluster::use_unix_sockets = 0;
        $PostgreSQL::Test::Cluster::use_tcp = 1;
        $PostgreSQL::Test::Cluster::test_pghost = 'localhost';
        my %params = ( "port" => $port, "host" => "localhost");
        $node = PostgreSQL::Test::Cluster->new($name, %params);
    } else {
        $PostgresNode::use_tcp = 1;
        $PostgresNode::test_pghost = '127.0.0.1';
        my %params = ( "port" => $port, "host" => "localhost");
        $node = get_new_node($name, %params);        
    }
    print("node's port:" . $node->port . "\n");

    $port += 1;

    $node->init(allows_streaming => 'logical');
    if ($node_type == $NODE_TYPE_COORDINATOR || $node_type == $NODE_TYPE_WORKER) {
        $node->append_conf("postgresql.conf","
            citus.enable_replication_origin_session = on
            citus.override_table_visibility = off
        ");
    } 

    $node->start();

    if ($node_type == $NODE_TYPE_COORDINATOR || $node_type == $NODE_TYPE_WORKER) {
        $node->safe_psql('postgres', "CREATE EXTENSION citus;");
        my $value = $node->safe_psql('postgres', "SHOW citus.enable_replication_origin_session;");
        print("citus.enable_replication_origin_session value is $value\n")
    }
    
    return $node;
}

# Create a Citus cluster with the given number of workers
sub create_citus_cluster {
    my ($no_workers,$host,$port) = @_;
    my @workers = ();
    #my $localhost = "127.0.0.1";
    #my $port = 56365;
    my $node_coordinator = create_node('coordinator', $NODE_TYPE_COORDINATOR,$host, $port);
    my $coord_host = $node_coordinator->host();
    my $coord_port = $node_coordinator->port();
    $node_coordinator->safe_psql('postgres',"SELECT pg_catalog.citus_set_coordinator_host('$coord_host', $coord_port);");
    for (my $i = 0; $i < $no_workers; $i++) {
        $port = $port + 1;
        my $node_worker = create_node("worker$i", $NODE_TYPE_WORKER,"localhost", $port);
        my $node_worker_host = $node_worker->host();
        my $node_worker_port = $node_worker->port();
        $node_coordinator->safe_psql('postgres',"SELECT pg_catalog.citus_add_node('$node_worker_host', $node_worker_port);");
        push @workers, $node_worker;
    }
    return $node_coordinator, @workers;
}

sub prepare_workers_for_cdc_publication {
    my ($workersref) = $_[0];
    for (@$workersref) {
        my $pub = $_->safe_psql('postgres',"SELECT * FROM pg_publication WHERE pubname = 'cdc_publication';");
        if ($pub ne "") {
            $_->safe_psql('postgres',"DROP PUBLICATION IF EXISTS cdc_publication;");
        }
        $_->safe_psql('postgres',"CREATE PUBLICATION cdc_publication FOR TABLE sensors;");

        my $slot = $_->safe_psql('postgres',"select * from pg_replication_slots where  slot_name = 'cdc_replication_slot';");
        if ($slot ne "") {
            $_->safe_psql('postgres',"SELECT pg_catalog.pg_drop_replication_slot('cdc_replication_slot');");
        }
        $_->safe_psql('postgres',"SELECT pg_catalog.pg_create_logical_replication_slot('cdc_replication_slot','citus',false,true)");
    }
}

sub prepare_coordinator_for_cdc_publication {
    my $node_coordinator = $_[0];
    print("node node_coordinator connstr: \n" . $node_coordinator->connstr());
    $node_coordinator->safe_psql('postgres',"CREATE PUBLICATION cdc_publication FOR TABLE sensors;");
    $node_coordinator->safe_psql('postgres',"SELECT pg_catalog.pg_create_logical_replication_slot('cdc_replication_slot','citus',false,true)");
}

sub connect_cdc_client_to_citus_cluster_publications {
    my ($workersref) = $_[0];
    my ($node) = $_[1];

    my $i = 1;
    for (@$workersref) {
        my $conn_str = $_->connstr() . " dbname=postgres";
        my $subscription = 'cdc_subscription_' . $i;
        print "creating subscription $subscription for node$i: $conn_str\n";
        my $copy_data= 'copy_data=false';
        if ($i == 1) {
            $copy_data='copy_data=true';
        }
        my $subscription_stmt = "CREATE SUBSCRIPTION $subscription
            CONNECTION '$conn_str'
            PUBLICATION cdc_publication
            WITH (
                create_slot=false,
                enabled=true,
                slot_name=cdc_replication_slot,". $copy_data .");";
        
        $node->safe_psql('postgres',$subscription_stmt);
        $i++;
    }
}

sub connect_cdc_client_to_coordinator_publication {
    my $node_coordinator = $_[0];
    my $node_cdc_client = $_[1];

    my $conn_str = $node_coordinator->connstr() . " dbname=postgres";
    my $subscription = 'cdc_subscription';
    print "creating subscription $subscription for coordinator: $conn_str\n";
    $node_cdc_client->safe_psql('postgres',"
        CREATE SUBSCRIPTION $subscription
            CONNECTION '$conn_str'
            PUBLICATION cdc_publication
            WITH (
                create_slot=false,
                enabled=true,
                slot_name=cdc_replication_slot,
                copy_data=true);"
    );
}

sub wait_for_cdc_client_to_catch_up_with_workers {
        my ($workersref) = $_[0];
        my $i = 1;
        for (@$workersref) {
            my $subscription = 'cdc_subscription_' . $i;
            print "node$i: waiting for cdc client subscription $subscription to catch up\n";
            $_->wait_for_catchup($subscription);
            $i++;
        }        
}

sub wait_for_cdc_client_to_catch_up_with_coordinator {
        my $node_coordinator = $_[0];
        my $subscription = 'cdc_subscription';
        print "coordinator: waiting for cdc client subscription $subscription to catch up\n";
        $node_coordinator->wait_for_catchup($subscription);
}

