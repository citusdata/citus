# Based on the beginning of postgres' 001_stream_rep.pl to verify columnar behaviour
# Minimal test testing streaming replication
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 4;

# Initialize master node
my $leader_node = get_new_node('node_one');
# A specific role is created to perform some tests related to replication,
# and it needs proper authentication configuration.
$leader_node->init(
	allows_streaming => 1,
	auth_extra       => [ '--create-role', 'repl_role' ]);
$leader_node->start;
my $backup_name = 'my_backup';

# Take backup
$leader_node->backup($backup_name);

# Create streaming standby linking to master
my $node_standby_1 = get_new_node('standby_1');
$node_standby_1->init_from_backup($leader_node, $backup_name,
	has_streaming => 1);
$node_standby_1->start;

# Take backup of standby 1 (not mandatory, but useful to check if
# pg_basebackup works on a standby).
$node_standby_1->backup($backup_name);

# Take a second backup of the standby while the master is offline.
$leader_node->stop;
$node_standby_1->backup('my_backup_2');
$leader_node->start;

# Create second standby node linking to standby 1
my $node_standby_2 = get_new_node('standby_2');
$node_standby_2->init_from_backup($node_standby_1, $backup_name,
	has_streaming => 1);
$node_standby_2->start;

$leader_node->safe_psql('postgres',
	"CREATE EXTENSION citus;");

# Create some content on master and check its presence in standby 1
$leader_node->safe_psql('postgres',
	"CREATE TABLE tab_int USING columnar AS SELECT generate_series(1,1002) AS a");

# Wait for standbys to catch up
$leader_node->wait_for_catchup($node_standby_1, 'replay',
	$leader_node->lsn('insert'));
$node_standby_1->wait_for_catchup($node_standby_2, 'replay',
	$node_standby_1->lsn('replay'));

my $result =
  $node_standby_1->safe_psql('postgres', "SELECT count(*) FROM tab_int");
print "standby 1: $result\n";
is($result, qq(1002), 'check streamed content on standby 1');

$result =
  $node_standby_2->safe_psql('postgres', "SELECT count(*) FROM tab_int");
print "standby 2: $result\n";
is($result, qq(1002), 'check streamed content on standby 2');

# Check that only READ-only queries can run on standbys
is($node_standby_1->psql('postgres', 'INSERT INTO tab_int VALUES (1)'),
	3, 'read-only queries on standby 1');
is($node_standby_2->psql('postgres', 'INSERT INTO tab_int VALUES (1)'),
	3, 'read-only queries on standby 2');
