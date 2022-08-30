# Minimal test testing streaming replication
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More tests => 6;

# Initialize single node
my $node_one = PostgreSQL::Test::Cluster->new('node_one');
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

# test crashing while having an open transaction
$node_one->safe_psql('postgres', "
BEGIN;
INSERT INTO t1 SELECT a, 'hello world ' || a FROM generate_series(1,1003) AS a;
");

# simulate crash
$node_one->stop('immediate');
$node_one->start;

$result = $node_one->safe_psql('postgres', "SELECT count(*) FROM t1;");
print "node one count: $result\n";
is($result, qq(0), 'columnar crash during uncommitted transaction');

# test crashing while having a prepared transaction
$node_one->safe_psql('postgres', "
BEGIN;
INSERT INTO t1 SELECT a, 'hello world ' || a FROM generate_series(1,1004) AS a;
PREPARE TRANSACTION 'prepared_xact_crash';
");

# simulate crash
$node_one->stop('immediate');
$node_one->start;

$result = $node_one->safe_psql('postgres', "SELECT count(*) FROM t1;");
print "node one count: $result\n";
is($result, qq(0), 'columnar crash during prepared transaction (before commit)');

$node_one->safe_psql('postgres', "
COMMIT PREPARED 'prepared_xact_crash';
");

$result = $node_one->safe_psql('postgres', "SELECT count(*) FROM t1;");
print "node one count: $result\n";
is($result, qq(1004), 'columnar crash during prepared transaction (after commit)');

# test crash recovery with copied data
$node_one->safe_psql('postgres', "
\\copy t1 FROM stdin delimiter ','
1,a
2,b
3,c
\\.
");

# simulate crash
$node_one->stop('immediate');
$node_one->start;

$result = $node_one->safe_psql('postgres', "SELECT count(*) FROM t1;");
print "node one count: $result\n";
is($result, qq(1007), 'columnar crash after copy command');
