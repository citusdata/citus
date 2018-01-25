#!/usr/bin/perl -w
#----------------------------------------------------------------------
#
# pg_regress_multi.pl - Test runner for Citus
#
# Portions Copyright (c) 2012-2016, Citus Data, Inc.
# Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/test/regress/pg_regress_multi.pl
#
#----------------------------------------------------------------------

use strict;
use warnings;

use Fcntl;
use Getopt::Long;
use File::Spec::Functions;
use File::Path qw(make_path remove_tree);
use Config;

sub Usage()
{
    print "pg_regress_multi - Citus test runner\n";
    print "\n";
    print "Usage:\n";
    print "  pg_regress_multi [MULTI OPTIONS] -- [PG REGRESS OPTS]\n";
    print "\n";
    print "Multi Options:\n";
    print "  --isolationtester   	Run isolationtester tests instead of plain tests\n";
    print "  --vanillatest       	Run postgres tests with citus loaded as shared preload library\n";
    print "  --bindir            	Path to postgres binary directory\n";
    print "  --libdir            	Path to postgres library directory\n";
    print "  --postgres-builddir 	Path to postgres build directory\n";
    print "  --postgres-srcdir   	Path to postgres build directory\n";
    print "  --pgxsdir           	Path to the PGXS directory\n";
    print "  --load-extension    	Extensions to install in all nodes\n";
    print "  --server-option     	Config option to pass to the server\n";
    print "  --valgrind          	Run server via valgrind\n";
    print "  --valgrind-path     	Path to the valgrind executable\n";
    print "  --valgrind-log-file	Path to the write valgrind logs\n";
    print "  --pg_ctl-timeout    	Timeout for pg_ctl\n";
    print "  --connection-timeout	Timeout for connecting to worker nodes\n";
    exit 1;
}

# Option parsing
my $isolationtester = 0;
my $vanillatest = 0;
my $followercluster = 0;
my $bindir = "";
my $libdir = undef;
my $pgxsdir = "";
my $postgresBuilddir = "";
my $postgresSrcdir = "";
my $majorversion = "";
my @extensions = ();
my @userPgOptions = ();
my %dataTypes = ();
my %fdws = ();
my %fdwServers = ();
my %functions = ();
my %operators = ();
my $valgrind = 0;
my $valgrindPath = "valgrind";
my $valgrindLogFile = "valgrind_test_log.txt";
my $pgCtlTimeout = undef;
my $connectionTimeout = 5000;

my $serversAreShutdown = "TRUE";
my $usingWindows = 0;

if ($Config{osname} eq "MSWin32")
{
	$usingWindows = 1;
};

GetOptions(
    'isolationtester' => \$isolationtester,
    'vanillatest' => \$vanillatest,
    'follower-cluster' => \$followercluster,
    'bindir=s' => \$bindir,
    'libdir=s' => \$libdir,
    'pgxsdir=s' => \$pgxsdir,
    'postgres-builddir=s' => \$postgresBuilddir,
    'postgres-srcdir=s' => \$postgresSrcdir,
    'majorversion=s' => \$majorversion,
    'load-extension=s' => \@extensions,
    'server-option=s' => \@userPgOptions,
    'valgrind' => \$valgrind,
    'valgrind-path=s' => \$valgrindPath,
    'valgrind-log-file=s' => \$valgrindLogFile,
    'pg_ctl-timeout=s' => \$pgCtlTimeout,
    'connection-timeout=s' => \$connectionTimeout,
    'help' => sub { Usage() });

# Update environment to include [DY]LD_LIBRARY_PATH/LIBDIR/etc -
# pointing to the libdir - that's required so the right version of
# libpq, citus et al is being picked up.
#
# XXX: There's some issues with el capitan's SIP here, causing
# DYLD_LIBRARY_PATH not being inherited if SIP is enabled. That's a
# know problem, present in postgres itself as well.
if (defined $libdir)
{
    $ENV{LD_LIBRARY_PATH} = "$libdir:".($ENV{LD_LIBRARY_PATH} || '');
    $ENV{DYLD_LIBRARY_PATH} = "$libdir:".($ENV{DYLD_LIBRARY_PATH} || '');
    $ENV{LIBPATH} = "$libdir:".($ENV{LIBPATH} || '');
    $ENV{PATH} = "$libdir:".($ENV{PATH} || '');
}

# Put $bindir to the end of PATH. We want to prefer system binaries by
# default (as e.g. new libpq and old psql can cause issues), but still
# want to find binaries if they're not in PATH.
if (defined $bindir)
{
    $ENV{PATH} = ($ENV{PATH} || '').":$bindir";
}


# Most people are used to unified diffs these days, rather than the
# context diffs pg_regress defaults to.  Change default to avoid
# everyone having to (re-)learn how to change that setting.  Also add
# a bit more context to make it easier to locate failed test sections.
$ENV{PG_REGRESS_DIFF_OPTS} = '-dU10';

my $plainRegress = "";
my $isolationRegress = "";

if ($usingWindows)
{
	$plainRegress = "$bindir\\pg_regress.exe";
	$isolationRegress = "$bindir\\pg_isolation_regress.exe";
}
else
{
	$plainRegress = "$pgxsdir/src/test/regress/pg_regress";
	$isolationRegress = "${postgresBuilddir}/src/test/isolation/pg_isolation_regress";
}

if ($isolationtester && ! -f "$isolationRegress")
{
    die <<"MESSAGE";

isolationtester not found at $isolationRegress.

isolationtester tests can only be run when source (detected as ${postgresSrcdir})
and build (detected as ${postgresBuilddir}) directory corresponding to $bindir
are present.

Additionally isolationtester in src/test/isolation needs to be built,
which it is not by default if tests have not been run. If the build
directory is present locally
"make -C ${postgresBuilddir} all" should do the trick.
MESSAGE
}

my $vanillaRegress = catfile("${postgresBuilddir}", "src", "test", "regress", "pg_regress");
if ($vanillatest && ! -f "$vanillaRegress")
{
    die <<"MESSAGE";

pg_regress (for vanilla tests) not found at $vanillaRegress.

Vanilla tests can only be run when source (detected as ${postgresSrcdir})
and build (detected as ${postgresBuilddir}) directory corresponding to $bindir
are present.
MESSAGE
}

# If pgCtlTimeout is defined, we will set related environment variable.
# This is generally used with valgrind because valgrind starts slow and we
# need to increase timeout.
if (defined $pgCtlTimeout)
{
    $ENV{PGCTLTIMEOUT} = "$pgCtlTimeout";
}

# We don't want valgrind to run pg_ctl itself, as that'd trigger a lot
# of spurious OS failures, e.g. in bash. So instead we have to replace
# the postgres binary with a wrapper that exec's valgrind, which in
# turn then executes postgres.  That's unfortunately at the moment the
# only reliable way to do this.
sub replace_postgres
{
    if (-e catfile("$bindir", "postgres.orig"))
    {
	print "wrapper exists\n";
    }
    else
    {
	print "moving $bindir/postgres to $bindir/postgres.orig\n";
	rename catfile("$bindir", "postgres"), catfile("$bindir", "postgres.orig")
	    or die "Could not move postgres out of the way";
    }

    sysopen my $fh, catfile("$bindir", "postgres"), O_CREAT|O_TRUNC|O_RDWR, 0700
	or die "Could not create postgres wrapper at $bindir/postgres";
    print $fh <<"END";
#!/bin/bash
exec $valgrindPath \\
    --quiet \\
    --suppressions=${postgresSrcdir}/src/tools/valgrind.supp \\
    --trace-children=yes --track-origins=yes --read-var-info=no \\
    --leak-check=no \\
    --error-markers=VALGRINDERROR-BEGIN,VALGRINDERROR-END \\
    --log-file=$valgrindLogFile \\
    $bindir/postgres.orig \\
    "\$@"
END
    close $fh;
}

# revert changes replace_postgres() performed
sub revert_replace_postgres
{
    if (-e catfile("$bindir", "postgres.orig"))
    {
	print "wrapper exists, removing\n";
	print "moving $bindir/postgres.orig to $bindir/postgres\n";
	rename catfile("$bindir", "postgres.orig"), catfile("$bindir", "postgres")
	    or die "Could not move postgres back";
    }
}

# always want to call initdb under normal postgres, so revert from a
# partial run, even if we're now not using valgrind.
revert_replace_postgres();

# Set some default configuration options
my $masterPort = 57636;
my $workerCount = 2;
my @workerPorts = ();

for (my $workerIndex = 1; $workerIndex <= $workerCount; $workerIndex++) {
    my $workerPort = $masterPort + $workerIndex;
    push(@workerPorts, $workerPort);
}

my $followerCoordPort = 9070;
my @followerWorkerPorts = ();
for (my $workerIndex = 1; $workerIndex <= $workerCount; $workerIndex++) {
    my $workerPort = $followerCoordPort + $workerIndex;
    push(@followerWorkerPorts, $workerPort);
}

my $host = "localhost";
my $user = "postgres";
my @pgOptions = ();

# Postgres options set for the tests
push(@pgOptions, '-c', "listen_addresses=${host}");
# not required, and we don't necessarily have access to the default directory
push(@pgOptions, '-c', "unix_socket_directories=");
push(@pgOptions, '-c', "fsync=off");
push(@pgOptions, '-c', "shared_preload_libraries=citus");
push(@pgOptions, '-c', "wal_level=logical");

# Citus options set for the tests
push(@pgOptions, '-c', "citus.shard_count=4");
push(@pgOptions, '-c', "citus.shard_max_size=1500kB");
push(@pgOptions, '-c', "citus.max_running_tasks_per_node=4");
push(@pgOptions, '-c', "citus.expire_cached_shards=on");
push(@pgOptions, '-c', "citus.task_tracker_delay=10ms");
push(@pgOptions, '-c', "citus.remote_task_check_interval=1ms");
push(@pgOptions, '-c', "citus.shard_replication_factor=2");
push(@pgOptions, '-c', "citus.node_connection_timeout=${connectionTimeout}");

if ($followercluster)
{
  push(@pgOptions, '-c', "max_wal_senders=10");
  push(@pgOptions, '-c', "hot_standby=on");
  push(@pgOptions, '-c', "wal_level=replica");
}

# disable automatic distributed deadlock detection during the isolation testing
# to make sure that we always get consistent test outputs. If we don't  manually
# (i.e., calling a UDF) detect the deadlocks, some sessions that do not participate
# in the deadlock may interleave with the deadlock detection, which results in non-
# consistent test outputs.
# since we have CREATE/DROP distributed tables very frequently, we also set
# shard_count to 4 to speed up the tests.
if($isolationtester)
{
   push(@pgOptions, '-c', "citus.log_distributed_deadlock_detection=on");
   push(@pgOptions, '-c', "citus.distributed_deadlock_detection_factor=-1");
   push(@pgOptions, '-c', "citus.shard_count=4");
}

# Add externally added options last, so they overwrite the default ones above
for my $option (@userPgOptions)
{
	push(@pgOptions, '-c', $option);
}

#define data types as a name->definition
%dataTypes = ('dummy_type', '(i integer)',
               'order_side', ' ENUM (\'buy\', \'sell\')',
               'test_composite_type', '(i integer, i2 integer)',
               'bug_status', ' ENUM (\'new\', \'open\', \'closed\')');

# define functions as signature->definition
%functions = ('fake_fdw_handler()', 'fdw_handler AS \'citus\' LANGUAGE C STRICT;',
               'equal_test_composite_type_function(test_composite_type, test_composite_type)',
               'boolean AS \'select $1.i = $2.i AND $1.i2 = $2.i2;\' LANGUAGE SQL IMMUTABLE RETURNS NULL ON NULL INPUT;');


%operators = ('=', '(LEFTARG = test_composite_type, RIGHTARG = test_composite_type, PROCEDURE = equal_test_composite_type_function, HASHES)');

#define fdws as name->handler name
%fdws = ('fake_fdw', 'fake_fdw_handler');

#define server_name->fdw
%fdwServers = ('fake_fdw_server', 'fake_fdw');

# Cleanup leftovers and prepare directories for the run
if (-e catfile('tmp_check', 'tmp-bin'))
{
	remove_tree(catfile('tmp_check', 'tmp-bin')) or die "Could not remove tmp-bin directory";
}

if (-e catfile('tmp_check', 'master'))
{
	remove_tree(catfile('tmp_check', 'master')) or die "Could not remove master directory";
}

for my $port (@workerPorts)
{
	if (-e catfile("tmp_check", "worker.$port"))
	{
    		remove_tree(catfile("tmp_check", "worker.$port")) or die "Could not remove worker directory";
	}
}

if (-e catfile("tmp_check", "master-follower"))
{
	remove_tree(catfile("tmp_check", "master-follower")) or die "Could not remove master directory";
}

for my $port (@followerWorkerPorts)
{
	if (-e catfile("tmp_check", "follower.$port"))
	{
	    remove_tree(catfile("tmp_check", "follower.$port")) or die "Could not remove worker directory";
	}
}

# Prepare directory in which 'psql' has some helpful variables for locating the workers
make_path(catfile("tmp_check", "tmp-bin")) or die "Could not create tmp_bin directory $!\n";

my $psql_name = "psql";
if ($usingWindows)
{
	$psql_name = "psql.cmd";
}

sysopen my $fh, catfile("tmp_check", "tmp-bin", $psql_name), O_CREAT|O_TRUNC|O_RDWR, 0700
	or die "Could not create psql wrapper";
if ($usingWindows)
{ 
    print $fh "\@echo off\n";
}
print $fh catfile($bindir, "psql")." ";
print $fh "--variable=master_port=$masterPort ";
print $fh "--variable=follower_master_port=$followerCoordPort ";
print $fh "--variable=default_user=$user ";
print $fh "--variable=SHOW_CONTEXT=always ";
for my $workeroff (0 .. $#workerPorts)
{
	my $port = $workerPorts[$workeroff];
	print $fh "--variable=worker_".($workeroff+1)."_port=$port ";
}
for my $workeroff (0 .. $#followerWorkerPorts)
{
	my $port = $followerWorkerPorts[$workeroff];
	print $fh "--variable=follower_worker_".($workeroff+1)."_port=$port ";
}

if ($usingWindows)
{
	print $fh "--variable=dev_null=\"/nul\" ";
	print $fh "--variable=temp_dir=\"%TEMP%\\\"";
}
else
{
	print $fh "--variable=dev_null=\"/dev/null\" ";	
	print $fh "--variable=temp_dir=\"/tmp/\"";
}


if ($usingWindows)
{
	print $fh "%*\n"; # pass on the commandline arguments
}
else
{
	print $fh "\"\$@\"\n"; # pass on the commandline arguments
}
close $fh;

make_path(catfile('tmp_check', 'master', 'log')) or die 'Could not create master directory';
for my $port (@workerPorts)
{
    make_path(catfile("tmp_check", "worker.$port", "log"))
        or die "Could not create worker directory";
}

if ($followercluster)
{
    make_path(catfile('tmp_check', 'master-follower', 'log')) or die "Could not create follower directory";
    for my $port (@followerWorkerPorts)
    {
        make_path(catfile("tmp_check", "follower.$port", "log"))
            or die "Could not create worker directory";
    }
}

# Create new data directories, copy workers for speed
system(catfile("$bindir", "initdb"), ("--nosync", "-U", $user, "--encoding", "UTF8", catfile("tmp_check", "master", "data"))) == 0
    or die "Could not create master data directory";

if ($followercluster)
{
  # This is only necessary on PG 9.6 but it doesn't hurt PG 10
  open(my $fd, ">>", catfile("tmp_check", "master", "data", "pg_hba.conf"))
    or die "could not open pg_hba.conf";
  print $fd "\nhost replication postgres 127.0.0.1/32 trust";
  close $fd;
}

if ($usingWindows)
{
	for my $port (@workerPorts)
	{
		system(catfile("$bindir", "initdb"), ("--nosync", "-U", $user, "--encoding", "UTF8", catfile("tmp_check", "worker.$port", "data"))) == 0
		    or die "Could not create worker data directory";
	}	
}
else
{
	for my $port (@workerPorts)
	{
	    system("cp", ("-a", catfile("tmp_check", "master", "data"), catfile("tmp_check", "worker.$port", "data"))) == 0
	        or die "Could not create worker data directory";
	}
}


# Routine to shutdown servers at failure/exit
sub ShutdownServers()
{
    if ($serversAreShutdown eq "FALSE")
    {
        system(catfile("$bindir", "pg_ctl"),
               ('stop', '-w', '-D', catfile('tmp_check', 'master', 'data'))) == 0
            or warn "Could not shutdown worker server";

        for my $port (@workerPorts)
        {
            system(catfile("$bindir", "pg_ctl"),
                   ('stop', '-w', '-D', catfile("tmp_check", "worker.$port", "data"))) == 0
                or warn "Could not shutdown worker server";
        }

        if ($followercluster)
        {
            system(catfile("$bindir", "pg_ctl"),
                   ('stop', '-w', '-D', catfile('tmp_check', 'master-follower', 'data'))) == 0
                or warn "Could not shutdown worker server";

            for my $port (@followerWorkerPorts)
            {
                system(catfile("$bindir", "pg_ctl"),
                       ('stop', '-w', '-D', catfile("tmp_check", "follower.$port", "data"))) == 0
                    or warn "Could not shutdown worker server";
            }
        }
        $serversAreShutdown = "TRUE";
    }
}

# Set signals to shutdown servers
$SIG{INT} = \&ShutdownServers;
$SIG{QUIT} = \&ShutdownServers;
$SIG{TERM} = \&ShutdownServers;
$SIG{__DIE__} = \&ShutdownServers;

# Shutdown servers on exit only if help option is not used
END
{
    if ($? != 1)
    {
        ShutdownServers();
    }

    # At the end of a run, replace redirected binary with original again
    if ($valgrind)
    {
	revert_replace_postgres();
    }
}

# want to use valgrind, replace binary before starting server
if ($valgrind)
{
    replace_postgres();
}

# Signal that servers should be shutdown
$serversAreShutdown = "FALSE";

# Start servers
if(system(catfile("$bindir", "pg_ctl"),
       ('start', '-w',
        '-o', join(" ", @pgOptions)." -c port=$masterPort",
       '-D', catfile('tmp_check', 'master', 'data'), '-l', catfile('tmp_check', 'master', 'log', 'postmaster.log'))) != 0)
{
  system("tail", ("-n20", catfile("tmp_check", "master", "log", "postmaster.log")));
  die "Could not start master server";
}

for my $port (@workerPorts)
{
    if(system(catfile("$bindir", "pg_ctl"),
           ('start', '-w',
            '-o', join(" ", @pgOptions)." -c port=$port",
            '-D', catfile("tmp_check", "worker.$port", "data"),
            '-l', catfile("tmp_check", "worker.$port", "log", "postmaster.log"))) != 0)
    {
      system("tail", ("-n20", catfile("tmp_check", "worker.$port", "log", "postmaster.log")));
      die "Could not start worker server";
    }
}

# Setup the follower nodes
if ($followercluster)
{
    # This test would run faster on PG10 if we could pass --no-sync here but that flag
    # isn't supported on PG 9.6. In a year when we drop support for PG9.6 add that flag!
    system(catfile("$bindir", "pg_basebackup"),
           ("-D", catfile("tmp_check", "master-follower", "data"), "--host=$host", "--port=$masterPort",
            "--username=$user", "-R", "-X", "stream")) == 0
      or die 'could not take basebackup';

    for my $offset (0 .. $#workerPorts)
    {
        my $workerPort = $workerPorts[$offset];
        my $followerPort = $followerWorkerPorts[$offset];
        system(catfile("$bindir", "pg_basebackup"),
               ("-D", catfile("tmp_check", "follower.$followerPort", "data"), "--host=$host", "--port=$workerPort",
                "--username=$user", "-R", "-X", "stream")) == 0
            or die "Could not take basebackup";
    }

    if(system(catfile("$bindir", "pg_ctl"),
           ('start', '-w',
            '-o', join(" ", @pgOptions)." -c port=$followerCoordPort",
           '-D', catfile('tmp_check', 'master-follower', 'data'), '-l', catfile('tmp_check', 'master-follower', 'log', 'postmaster.log'))) != 0)
    {
      system("tail", ("-n20", catfile("tmp_check", "master-follower", "log", "postmaster.log")));
      die "Could not start master follower server";
    }

    for my $port (@followerWorkerPorts)
    {
        if(system(catfile("$bindir", "pg_ctl"),
               ('start', '-w',
                '-o', join(" ", @pgOptions)." -c port=$port",
                '-D', catfile("tmp_check", "follower.$port", "data"),
                '-l', catfile("tmp_check", "follower.$port", "log", "postmaster.log"))) != 0)
        {
          system("tail", ("-n20", catfile("tmp_check", "follower.$port", "log", "postmaster.log")));
          die "Could not start follower server";
	}
    }
}

###
# Create database, extensions, types, functions and fdws on the workers,
# pg_regress won't know to create them for us.
###
for my $port (@workerPorts)
{
    system(catfile($bindir, "psql"),
           ('-X', '-h', $host, '-p', $port, '-U', $user, "-d", "postgres",
            '-c', "CREATE DATABASE regression;")) == 0
        or die "Could not create regression database on worker";

    for my $extension (@extensions)
    {
        system(catfile($bindir, "psql"),
               ('-X', '-h', $host, '-p', $port, '-U', $user, "-d", "regression",
                '-c', "CREATE EXTENSION IF NOT EXISTS $extension;")) == 0
            or die "Could not create extension on worker";
    }

    foreach my $dataType (keys %dataTypes)
    {
        system(catfile($bindir, "psql"),
                ('-X', '-h', $host, '-p', $port, '-U', $user, "-d", "regression",
                 '-c', "CREATE TYPE $dataType AS $dataTypes{$dataType};")) == 0
            or die "Could not create TYPE $dataType on worker";
    }

    foreach my $function (keys %functions)
    {
        system(catfile($bindir, "psql"),
                ('-X', '-h', $host, '-p', $port, '-U', $user, "-d", "regression",
                 '-c', "CREATE FUNCTION $function RETURNS $functions{$function};")) == 0
            or die "Could not create FUNCTION $function on worker";
    }

    foreach my $operator (keys %operators)
    {
        system(catfile($bindir, "psql"),
                ('-X', '-h', $host, '-p', $port, '-U', $user, "-d", "regression",
                 '-c', "CREATE OPERATOR $operator $operators{$operator};")) == 0
            or die "Could not create OPERATOR $operator on worker";
    }

    foreach my $fdw (keys %fdws)
    {
        system(catfile($bindir, "psql"),
                ('-X', '-h', $host, '-p', $port, '-U', $user, "-d", "regression",
                 '-c', "CREATE FOREIGN DATA WRAPPER $fdw HANDLER $fdws{$fdw};")) == 0
            or die "Could not create foreign data wrapper $fdw on worker";
    }

    foreach my $fdwServer (keys %fdwServers)
    {
        system(catfile($bindir, "psql"),
                ('-X', '-h', $host, '-p', $port, '-U', $user, "-d", "regression",
                 '-c', "CREATE SERVER $fdwServer FOREIGN DATA WRAPPER $fdwServers{$fdwServer};")) == 0
            or die "Could not create server $fdwServer on worker";
    }
}

# Prepare pg_regress arguments
my @arguments = (
    "--host", $host,
    '--port', $masterPort,
    '--user', $user,
#    '--bindir', 'C:\Users\Administrator\Downloads\pg-64\bin',
    '--bindir', catfile("tmp_check", "tmp-bin")
);

# Add load extension parameters to the argument list
for my $extension (@extensions)
{
    push(@arguments, "--load-extension=$extension");
}

# Append remaining ARGV arguments to pg_regress arguments
push(@arguments, @ARGV);

my $startTime = time();

# Finally run the tests
if ($vanillatest)
{
    $ENV{PGHOST} = $host;
    $ENV{PGPORT} = $masterPort;
    $ENV{PGUSER} = $user;

    system("make", ("-C", catfile("$postgresBuilddir", "src", "test", "regress"), "installcheck-parallel")) == 0
    or die "Could not run vanilla tests";
}
elsif ($isolationtester)
{
    push(@arguments, "--dbname=regression");
    system("$isolationRegress", @arguments) == 0
      or die "Could not run isolation tests";
}
else
{
    system("$plainRegress", @arguments) == 0
	or die "Could not run regression tests";
}

my $endTime = time();

print "Finished in ". ($endTime - $startTime)." seconds. \n";

exit 0;
