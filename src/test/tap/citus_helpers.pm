use strict;
use warnings;

my $pg_major_version =  int($ENV{'pg_major_version'} || 0);
if ($pg_major_version >= 15) {
    eval "use PostgreSQL::Test::Cluster";
    eval "use PostgreSQL::Test::Utils";
} else {
    eval "use PostgresNode";
}

our $NODE_TYPE_COORDINATOR = 1;
our $NODE_TYPE_WORKER = 2;

sub create_node {
    my ($name,$node_type,$host, $port, $config) = @_;
    my $node;
    if ($pg_major_version >= 15) {
        $PostgreSQL::Test::Cluster::use_unix_sockets = 0;
        $PostgreSQL::Test::Cluster::use_tcp = 1;
        $PostgreSQL::Test::Cluster::test_pghost = 'localhost';
        my %params = ( host => 'localhost' );
        $params{port} = $port if defined $port;
        $node = PostgreSQL::Test::Cluster->new($name, %params);
    } else {
        $PostgresNode::use_tcp = 1;
        $PostgresNode::test_pghost = '127.0.0.1';
        my %params = ( host => 'localhost' );
        $params{port} = $port if defined $port;
        $node = get_new_node($name, %params);
    }
    $port++ if defined $port;

    my $citus_config_options = "
max_connections = 100
max_wal_senders = 100
max_replication_slots = 100
log_statement = 'all'
ssl = off
logging_collector = on
log_directory = 'log'
log_filename = 'postgresql.log'
";

    if ($config) {
        $citus_config_options .= $config;
    }

    $node->init(allows_streaming => 'logical');
    if ($node_type == $NODE_TYPE_COORDINATOR || $node_type == $NODE_TYPE_WORKER) {
        $node->append_conf("postgresql.conf", "shared_preload_libraries = 'citus'\n" . $citus_config_options);
        $node->start();
        if (-f $node->data_dir . '/server.key') {
            chmod 0600, $node->data_dir . '/server.key';
        }
        $node->safe_psql('postgres', "CREATE EXTENSION citus;");
    }

    return $node;
}

sub create_citus_cluster {
    my ($num_workers,$host,$port,$citus_config) = @_;
    my @workers = ();
    my $node_coordinator;
    $node_coordinator = create_node('coordinator', $NODE_TYPE_COORDINATOR,$host, $port, $citus_config);
    my $coord_host = $node_coordinator->host();
    my $coord_port = $node_coordinator->port();
    $node_coordinator->safe_psql('postgres',"SELECT pg_catalog.citus_set_coordinator_host('$coord_host', $coord_port);");
    for (my $i = 0; $i < $num_workers; $i++) {
        $port = $port ? $port + 1 : undef;
        my $node_worker = create_node("worker$i", $NODE_TYPE_WORKER,"localhost", $port, $citus_config);
        my $node_worker_host = $node_worker->host();
        my $node_worker_port = $node_worker->port();
        $node_coordinator->safe_psql('postgres',"SELECT pg_catalog.citus_add_node('$node_worker_host', $node_worker_port);");
        push @workers, $node_worker;
    }
    return $node_coordinator, @workers;
}

1;
