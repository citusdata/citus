# Minimal test testing streaming replication
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More tests => 2;

# Initialize single node
my $node_one = PostgreSQL::Test::Cluster->new('node_one');
$node_one->init();
$node_one->start;

# initialize the citus extension
$node_one->safe_psql('postgres', "CREATE EXTENSION citus;");

# create columnar table and insert simple data to verify the data survives a crash
$node_one->safe_psql('postgres', "
CREATE TABLE test_row(i int);
INSERT INTO test_row VALUES (1);
CREATE TABLE test_columnar_freeze(i int) USING columnar WITH(autovacuum_enabled=false);
INSERT INTO test_columnar_freeze VALUES (1);
");

my $ten_thousand_updates = "";

foreach (1..10000) {
    $ten_thousand_updates .= "UPDATE test_row SET i = i + 1;\n";
}

# 70K updates
foreach (1..7) {
    $node_one->safe_psql('postgres', $ten_thousand_updates);
}

my $result = $node_one->safe_psql('postgres', "
select age(relfrozenxid) < 70000 as was_frozen
  from pg_class where relname='test_columnar_freeze';
");
print "node one count: $result\n";
is($result, qq(f), 'columnar table was not frozen');

$node_one->safe_psql('postgres', 'VACUUM FREEZE test_columnar_freeze;');

$result = $node_one->safe_psql('postgres', "
select age(relfrozenxid) < 70000 as was_frozen
  from pg_class where relname='test_columnar_freeze';
");
print "node one count: $result\n";
is($result, qq(t), 'columnar table was frozen');

$node_one->stop('fast');

