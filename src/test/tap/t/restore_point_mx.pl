use strict;
use warnings;

# TAP: PITR to citus_create_restore_point across coordinator + MX workers
#
# Flow:
# 1) Create coordinator + 2 workers, metadata sync enabled (MX-like).
# 2) Insert committed rows (distributed/reference), CHECKPOINT, take base backups.
# 3) Insert post-backup rows (should be present after restore), create restore point.
# 4) Start worker-local transactions before RP in parallel sessions; COMMIT after RP (rows must vanish after restore).
# 5) Insert after-RP rows (must vanish after restore).
# 6) Stop cluster; restore from base backups; replay WAL via archive/restore_command to RP.
# 7) Assert: committed + post-backup rows exist; late-committed and after-RP rows are absent; cluster writable.

use lib './t';
use citus_helpers;
use Cwd qw(cwd);
use File::Path qw(make_path);
use IPC::Run qw(start pump finish);
use Time::HiRes qw(time sleep);
use Test::More tests => 20;

# local helper to avoid clobbering PostgreSQL::Test::Utils::pump_until
sub _pump_until_bg {
    my ($h, $outref, $regex, $timeout_s) = @_;
    my $end = time() + $timeout_s;
    while (time() < $end) {
        pump $h;
        return 1 if $$outref =~ $regex;
        sleep 0.1;
    }
    return 0;
}

sub start_bg_tx {
    my ($node, $tag, $stmts) = @_;
    if ($node->can('background_psql')) {
        my $bg = $node->background_psql('postgres');
        $bg->query_until(qr/${tag}_ready/, "BEGIN;\n$stmts\n\\echo ${tag}_ready\n");
        return { bg_psql => 1, obj => $bg, tag => $tag };
    }

    my $connstr = $node->connstr('postgres');
    my @cmd = (
        $node->psql_program,
        '-X', '-At', '-q',
        '-d', $connstr,
        '-v', 'ON_ERROR_STOP=1'
    );
    my ($in, $out, $err) = ("", "", "");
    my $h = start(\@cmd, \$in, \$out, \$err, init => sub { $SIG{PIPE} = 'IGNORE'; });
    $in .= "BEGIN;\n$stmts\n\\echo ${tag}_ready\n";
    _pump_until_bg($h, \$out, qr/${tag}_ready/, 10)
        or BAIL_OUT("$tag bg tx did not start: $err $out");
    return { handle => $h, in => \$in, out => \$out, err => \$err, tag => $tag };
}

sub commit_bg_tx {
    my ($bg) = @_;
    if ($bg->{bg_psql}) {
        $bg->{obj}->query_until(qr/$bg->{tag}_commit/, "COMMIT;\n\\echo $bg->{tag}_commit\n");
        $bg->{obj}->quit();
        return;
    }
    ${ $bg->{in} } .= "COMMIT;\n\\echo $bg->{tag}_commit\n";
    _pump_until_bg($bg->{handle}, $bg->{out}, qr/$bg->{tag}_commit/, 10)
        or BAIL_OUT("$bg->{tag} commit failed: ${ \$bg->{err} } ${ \$bg->{out} }");
    ${ $bg->{in} } .= "\\q\n";
    finish($bg->{handle});
}

# Create Citus cluster with coordinator + 2 workers
my ($node_coordinator, @workers);
eval {
    ($node_coordinator, @workers) = create_citus_cluster(2, 'localhost', undef, "");
};
BAIL_OUT("cluster creation failed: $@") if $@;

my $testdir = $ENV{TESTDIR} || cwd();
my $archive_coord = "$testdir/tmp_check/archive_coordinator";
my $archive_w0    = "$testdir/tmp_check/archive_worker0";
my $archive_w1    = "$testdir/tmp_check/archive_worker1";
make_path($archive_coord, $archive_w0, $archive_w1);

$node_coordinator->append_conf('postgresql.conf', qq(
archive_mode = on
archive_command = 'cp %p $archive_coord/%f'
archive_timeout = '1s'
log_min_messages = info
log_min_error_statement = info
));
$node_coordinator->restart();

$workers[0]->append_conf('postgresql.conf', qq(
archive_mode = on
archive_command = 'cp %p $archive_w0/%f'
archive_timeout = '1s'
log_min_messages = info
log_min_error_statement = info
));
$workers[0]->restart();

$workers[1]->append_conf('postgresql.conf', qq(
archive_mode = on
archive_command = 'cp %p $archive_w1/%f'
archive_timeout = '1s'
log_min_messages = info
log_min_error_statement = info
));
$workers[1]->restart();

# Enable metadata sync (MX-like)
foreach my $w (@workers) {
    my $port = $w->port();
    $node_coordinator->safe_psql('postgres', "SELECT start_metadata_sync_to_node('localhost', $port);");
}

# Create distributed & reference tables
$node_coordinator->safe_psql('postgres', q{
    CREATE TABLE dtx(id int PRIMARY KEY, payload text);
    SELECT create_distributed_table('dtx','id');
    CREATE TABLE rtx(id int PRIMARY KEY, payload text);
    SELECT create_reference_table('rtx');
});

# Wait metadata sync
foreach my $w (@workers) {
    $w->poll_query_until('postgres', "SELECT count(*)>0 FROM pg_dist_partition WHERE logicalrelid = 'dtx'::regclass;")
        or die "metadata did not sync to worker " . $w->name();
}

# Pre-backup data (distributed + reference)
$node_coordinator->safe_psql('postgres', "INSERT INTO dtx SELECT i, 'committed' FROM generate_series(1,100) i;");
$workers[0]->safe_psql('postgres', "INSERT INTO dtx SELECT i, 'committed_w0' FROM generate_series(201,250) i;");
$node_coordinator->safe_psql('postgres', "INSERT INTO rtx SELECT i, 'ref_committed' FROM generate_series(1,20) i;");

diag("CHECKPOINT before base backups");
$node_coordinator->safe_psql('postgres', 'CHECKPOINT');
$node_coordinator->safe_psql('postgres', "SELECT run_command_on_workers('CHECKPOINT');");

diag("Base backups");
$node_coordinator->backup('coord_mx_rp');
$workers[0]->backup('w0_mx_rp');
$workers[1]->backup('w1_mx_rp');

diag("Post-backup data");
$workers[1]->safe_psql('postgres', "INSERT INTO dtx SELECT i, 'post_backup' FROM generate_series(1001,1010) i;");
$node_coordinator->safe_psql('postgres', "INSERT INTO rtx SELECT i, 'ref_post_backup' FROM generate_series(21,30) i;");

diag("Start worker background tx before restore point");
my $w0_bg = start_bg_tx($workers[0], 'w0', "INSERT INTO dtx VALUES (3001, 'late_w0');\nINSERT INTO rtx VALUES (41, 'late_ref_w0');");
my $w1_bg = start_bg_tx($workers[1], 'w1', "INSERT INTO dtx VALUES (3002, 'late_w1');\nINSERT INTO rtx VALUES (42, 'late_ref_w1');");

diag("Restore point after post-backup data");
$node_coordinator->safe_psql('postgres', "SELECT citus_create_restore_point('mx_rp');");

diag("Commit worker background tx after restore point");
commit_bg_tx($w0_bg);
commit_bg_tx($w1_bg);

is($node_coordinator->safe_psql('postgres', "SELECT count(*) FROM dtx WHERE payload='late_w0';"), '1', 'late w0 row visible before shutdown');
is($node_coordinator->safe_psql('postgres', "SELECT count(*) FROM dtx WHERE payload='late_w1';"), '1', 'late w1 row visible before shutdown');
is($node_coordinator->safe_psql('postgres', "SELECT count(*) FROM rtx WHERE payload='late_ref_w0';"), '1', 'late ref w0 row visible before shutdown');
is($node_coordinator->safe_psql('postgres', "SELECT count(*) FROM rtx WHERE payload='late_ref_w1';"), '1', 'late ref w1 row visible before shutdown');

diag("After-restore-point data (excluded)");
$node_coordinator->safe_psql('postgres', "INSERT INTO dtx SELECT i, 'after_rp' FROM generate_series(2001,2005) i;");
$node_coordinator->safe_psql('postgres', "INSERT INTO rtx SELECT i, 'ref_after_rp' FROM generate_series(31,35) i;");

# Stop original cluster before restore (ports free for restore)
$node_coordinator->stop('fast');
$_->stop('fast') for @workers;

# Restore coordinator (use original port so pg_dist_node matches)
my $coord_port = $node_coordinator->port();
my @worker_ports = map { $_->port() } @workers;
my $coord_restore = PostgreSQL::Test::Cluster->new('coord_restore', port => $coord_port);
$coord_restore->init_from_backup($node_coordinator, 'coord_mx_rp', has_restoring => 1);
$coord_restore->append_conf('postgresql.conf', qq(
recovery_target_name = 'mx_rp'
recovery_target_action = 'promote'
restore_command = 'cp $archive_coord/%f %p'
));
$coord_restore->command_ok([ 'chmod', '600', $coord_restore->data_dir . '/server.key' ], 'chmod server.key');
$coord_restore->start() or BAIL_OUT("coord_restore start failed");

# Restore workers
my @workers_restore;
for my $i (0 .. $#workers) {
    my $wr = PostgreSQL::Test::Cluster->new("w${i}_restore", port => $worker_ports[$i]);
    $wr->init_from_backup($workers[$i], "w${i}_mx_rp", has_restoring => 1);
    my $archive_dir = $i == 0 ? $archive_w0 : $archive_w1;
    $wr->append_conf('postgresql.conf', qq(
recovery_target_name = 'mx_rp'
recovery_target_action = 'promote'
restore_command = 'cp $archive_dir/%f %p'
    ));
    $wr->command_ok([ 'chmod', '600', $wr->data_dir . '/server.key' ], 'chmod server.key');
    $wr->start() or BAIL_OUT("worker${i}_restore start failed");
    push @workers_restore, $wr;
}

# Verify restored state
is($coord_restore->safe_psql('postgres', "SELECT count(*) FROM dtx WHERE payload='committed';"), '100', 'pre-restore coordinator rows present');
is($coord_restore->safe_psql('postgres', "SELECT count(*) FROM dtx WHERE payload='committed_w0';"), '50', 'pre-restore worker0 rows present');
is($coord_restore->safe_psql('postgres', "SELECT count(*) FROM dtx WHERE payload='post_backup';"), '10', 'post-backup rows present');
is($coord_restore->safe_psql('postgres', "SELECT count(*) FROM dtx WHERE payload='after_rp';"), '0', 'after-rp rows absent');
is($coord_restore->safe_psql('postgres', "SELECT count(*) FROM dtx WHERE payload='late_w0';"), '0', 'late_w0 row absent after restore');
is($coord_restore->safe_psql('postgres', "SELECT count(*) FROM dtx WHERE payload='late_w1';"), '0', 'late_w1 row absent after restore');
is($coord_restore->safe_psql('postgres', "SELECT count(*) FROM rtx WHERE payload='ref_committed';"), '20', 'reference pre rows present');
is($coord_restore->safe_psql('postgres', "SELECT count(*) FROM rtx WHERE payload='ref_post_backup';"), '10', 'reference post-backup rows present');
is($coord_restore->safe_psql('postgres', "SELECT count(*) FROM rtx WHERE payload='late_ref_w0';"), '0', 'late_ref_w0 row absent after restore');
is($coord_restore->safe_psql('postgres', "SELECT count(*) FROM rtx WHERE payload='late_ref_w1';"), '0', 'late_ref_w1 row absent after restore');
is($coord_restore->safe_psql('postgres', "SELECT count(*) FROM rtx WHERE payload='ref_after_rp';"), '0', 'reference after-rp rows absent');

# Cluster writable
ok(eval { $coord_restore->safe_psql('postgres', "INSERT INTO dtx VALUES (9999, 'after-restore');"); 1 }, 'cluster writable after restore');

eval { $coord_restore->stop('fast'); 1 };
eval { $_->stop('fast') for @workers_restore; 1 };

pass('restore_point_mx tap test completed');
