# Minimal test testing streaming replication
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 2;

# Initialize single node
my $node_one = get_new_node('node_one');
$node_one->init();
$node_one->start;

# initialize the citus extension
$node_one->safe_psql('postgres', "CREATE EXTENSION citus;");

# create columnar table and insert simple data to verify the data survives a crash
$node_one->safe_psql('postgres', "
BEGIN;
CREATE TABLE t1 (a int, b text) USING columnar;
INSERT INTO t1 SELECT a, 'hello world ' || a FROM generate_series(1,1002) AS a;
COMMIT;
");

# simulate crash
$node_one->stop('immediate');
$node_one->start;

my $result = $node_one->safe_psql('postgres', "SELECT count(*) FROM t1;");
print "node one count: $result\n";
is($result, qq(1002), 'columnar recovered data from before crash');


# truncate the table to verify the truncation survives a crash
$node_one->safe_psql('postgres', "
TRUNCATE t1;
");

# simulate crash
$node_one->stop('immediate');
$node_one->start;

$result = $node_one->safe_psql('postgres', "SELECT count(*) FROM t1;");
print "node one count: $result\n";
is($result, qq(0), 'columnar recovered truncation');
