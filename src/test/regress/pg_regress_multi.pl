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
use File::Basename;
use File::Spec::Functions;
use File::Path qw(make_path remove_tree);
use Config;
use POSIX qw( WNOHANG mkfifo );
use Cwd 'abs_path';

my $regressdir = (File::Spec->splitpath(__FILE__))[1];


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
    print "  --mitmproxy        	Start a mitmproxy for one of the workers\n";
    exit 1;
}

my $TMP_CHECKDIR = 'tmp_check';
my $TMP_BINDIR = 'tmp-bin';
my $MASTERDIR = 'master';
my $MASTER_FOLLOWERDIR = 'master-follower';

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
my $synchronousReplication = "";
my @extensions = ();
my @userPgOptions = ();
my %fdws = ();
my %fdwServers = ();
my %functions = ();
my $valgrind = 0;
my $valgrindPath = "valgrind";
my $valgrindLogFile = "valgrind_test_log.txt";
my $pgCtlTimeout = undef;
my $connectionTimeout = 5000;
my $useMitmproxy = 0;
my $mitmFifoPath = catfile($TMP_CHECKDIR, "mitmproxy.fifo");

my $serversAreShutdown = "TRUE";
my $usingWindows = 0;
my $mitmPid = 0;

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
    'mitmproxy' => \$useMitmproxy,
    'help' => sub { Usage() });

# Update environment to include [DY]LD_LIBRARY_PATH/LIBDIR/etc -
# pointing to the libdir - that's required so the right version of
# libpq, citus et al is being picked up.
#
# XXX: There's some issues with el capitan's SIP here, causing
# DYLD_LIBRARY_PATH not being inherited if SIP is enabled. That's a
# known problem, present in postgres itself as well.
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
#
# Also, ignore whitespace, without this the diffs on windows are unreadable
$ENV{PG_REGRESS_DIFF_OPTS} = '-dU10 -w';

my $plainRegress = "";
my $isolationRegress = "";
my $pgConfig = "";

if ($usingWindows)
{
	$plainRegress = "$bindir\\pg_regress.exe";
	$isolationRegress = "$bindir\\pg_isolation_regress.exe";
	$pgConfig = "$bindir\\pg_config.exe";
}
else
{
	$plainRegress = "$pgxsdir/src/test/regress/pg_regress";
	$isolationRegress = "${postgresBuilddir}/src/test/isolation/pg_isolation_regress";
	$pgConfig = "$bindir/pg_config";

	if (-x "$pgxsdir/src/test/isolation/pg_isolation_regress")
	{
		$isolationRegress = "$pgxsdir/src/test/isolation/pg_isolation_regress";
	}
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
my $vanillaSchedule = catfile(dirname("${pgxsdir}"), "regress", "parallel_schedule");

if ($vanillatest && ! (-f "$vanillaRegress" or -f "$vanillaSchedule"))
{
    die <<"MESSAGE";

pg_regress (for vanilla tests) not found at $vanillaRegress.

Vanilla tests can only be run when source (detected as ${postgresSrcdir})
and build (detected as ${postgresBuilddir}) directory corresponding to $bindir
are present.
MESSAGE
}

if ($useMitmproxy)
{
  system("mitmdump --version") == 0 or die "make sure mitmdump is on PATH";
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
    --max-stackframe=16000000 \\
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

# n.b. previously this was on port 57640, which caused issues because that's in the
# ephemeral port range, it was sometimes in the TIME_WAIT state which prevented us from
# binding to it. 9060 is now used because it will never be used for client connections,
# and there don't appear to be any other applications on this port that developers are
# likely to be running.
my $mitmPort = 9060;

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
if (! $vanillatest)
{
    push(@pgOptions, '-c', "extra_float_digits=0");
}

my $sharedPreloadLibraries = "citus";

# check if pg_stat_statements extension is installed
# if it is add it to shared preload libraries
my $sharedir = `$pgConfig --sharedir`;
chomp $sharedir;
my $pg_stat_statements_control = catfile($sharedir, "extension", "pg_stat_statements.control");
if (-e $pg_stat_statements_control)
{
	$sharedPreloadLibraries .= ',pg_stat_statements'; 
}

# check if hll extension is installed
# if it is add it to shared preload libraries
my $hll_control = catfile($sharedir, "extension", "hll.control");
if (-e $hll_control)
{
  $sharedPreloadLibraries .= ',hll';
}
push(@pgOptions, '-c', "shared_preload_libraries=${sharedPreloadLibraries}");

push(@pgOptions, '-c', "wal_level=logical");

# Citus options set for the tests
push(@pgOptions, '-c', "citus.shard_count=4");
push(@pgOptions, '-c', "citus.shard_max_size=1500kB");
push(@pgOptions, '-c', "citus.max_running_tasks_per_node=4");
push(@pgOptions, '-c', "citus.expire_cached_shards=on");
push(@pgOptions, '-c', "citus.sort_returning=on");
push(@pgOptions, '-c', "citus.task_tracker_delay=10ms");
push(@pgOptions, '-c', "citus.remote_task_check_interval=1ms");
push(@pgOptions, '-c', "citus.shard_replication_factor=2");
push(@pgOptions, '-c', "citus.node_connection_timeout=${connectionTimeout}");

# we disable slow start by default to encourage parallelism within tests
push(@pgOptions, '-c', "citus.executor_slow_start_interval=0ms");

if ($useMitmproxy)
{
  # make tests reproducible by never trying to negotiate ssl
  push(@pgOptions, '-c', "citus.node_conninfo=sslmode=disable");
}
elsif ($followercluster)
{
  # follower clusters don't work well when automatically generating certificates as the
  # followers do not execute the extension creation sql scripts that trigger the creation
  # of certificates
  push(@pgOptions, '-c', "citus.node_conninfo=sslmode=prefer");
}

if ($useMitmproxy)
{
  if (! -e $TMP_CHECKDIR)
  {
    make_path($TMP_CHECKDIR) or die "could not create $TMP_CHECKDIR directory";
  }
  my $absoluteFifoPath = abs_path($mitmFifoPath);
  die 'abs_path returned empty string' unless ($absoluteFifoPath ne "");
  push(@pgOptions, '-c', "citus.mitmfifo=$absoluteFifoPath");
}

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
   push(@pgOptions, '-c', "citus.metadata_sync_interval=1000");
   push(@pgOptions, '-c', "citus.metadata_sync_retry_interval=100");
}

# Add externally added options last, so they overwrite the default ones above
for my $option (@userPgOptions)
{
	push(@pgOptions, '-c', $option);
}

# define functions as signature->definition
%functions = ('fake_fdw_handler()', 'fdw_handler AS \'citus\' LANGUAGE C STRICT;');

#define fdws as name->handler name
%fdws = ('fake_fdw', 'fake_fdw_handler');

#define server_name->fdw
%fdwServers = ('fake_fdw_server', 'fake_fdw');

# Cleanup leftovers and prepare directories for the run
if (-e catfile($TMP_CHECKDIR, $TMP_BINDIR))
{
	remove_tree(catfile($TMP_CHECKDIR, $TMP_BINDIR)) or die "Could not remove $TMP_BINDIR directory";
}

if (-e catfile($TMP_CHECKDIR, $MASTERDIR))
{
	remove_tree(catfile($TMP_CHECKDIR, $MASTERDIR)) or die "Could not remove $MASTERDIR directory";
}

for my $port (@workerPorts)
{
	if (-e catfile($TMP_CHECKDIR, "worker.$port"))
	{
    		remove_tree(catfile($TMP_CHECKDIR, "worker.$port")) or die "Could not remove worker directory";
	}
}

if (-e catfile($TMP_CHECKDIR, $MASTER_FOLLOWERDIR))
{
	remove_tree(catfile($TMP_CHECKDIR, $MASTER_FOLLOWERDIR)) or die "Could not remove $MASTER_FOLLOWERDIR directory";
}

for my $port (@followerWorkerPorts)
{
	if (-e catfile($TMP_CHECKDIR, "follower.$port"))
	{
	    remove_tree(catfile($TMP_CHECKDIR, "follower.$port")) or die "Could not remove worker directory";
	}
}

# Prepare directory in which 'psql' has some helpful variables for locating the workers
make_path(catfile($TMP_CHECKDIR, $TMP_BINDIR)) or die "Could not create $TMP_BINDIR directory $!\n";

my $psql_name = "psql";
if ($usingWindows)
{
	$psql_name = "psql.cmd";
}

sysopen my $fh, catfile($TMP_CHECKDIR, $TMP_BINDIR, $psql_name), O_CREAT|O_TRUNC|O_RDWR, 0700
	or die "Could not create psql wrapper";
if ($usingWindows)
{ 
    print $fh "\@echo off\n";
}
print $fh catfile($bindir, "psql")." ";
print $fh "--variable=master_port=$masterPort ";
print $fh "--variable=worker_2_proxy_port=$mitmPort ";
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
	print $fh "--variable=temp_dir=\"%TEMP%\" ";
	print $fh "--variable=psql=\"".catfile($bindir, "psql")."\" ";
}
else
{
	print $fh "--variable=dev_null=\"/dev/null\" ";	
	print $fh "--variable=temp_dir=\"/tmp/\" ";
	print $fh "--variable=psql=\"psql\" ";
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

make_path(catfile($TMP_CHECKDIR, $MASTERDIR, 'log')) or die "Could not create $MASTERDIR directory";
for my $port (@workerPorts)
{
    make_path(catfile($TMP_CHECKDIR, "worker.$port", "log"))
        or die "Could not create worker directory";
}

if ($followercluster)
{
    make_path(catfile($TMP_CHECKDIR, $MASTER_FOLLOWERDIR, 'log')) or die "Could not create $MASTER_FOLLOWERDIR directory";
    for my $port (@followerWorkerPorts)
    {
        make_path(catfile($TMP_CHECKDIR, "follower.$port", "log"))
            or die "Could not create worker directory";
    }
}

# Create new data directories, copy workers for speed
system(catfile("$bindir", "initdb"), ("--nosync", "-U", $user, "--encoding", "UTF8", catfile($TMP_CHECKDIR, $MASTERDIR, "data"))) == 0
    or die "Could not create $MASTERDIR data directory";

if ($usingWindows)
{
	for my $port (@workerPorts)
	{
		system(catfile("$bindir", "initdb"), ("--nosync", "-U", $user, "--encoding", "UTF8", catfile($TMP_CHECKDIR, "worker.$port", "data"))) == 0
		    or die "Could not create worker data directory";
	}	
}
else
{
	for my $port (@workerPorts)
	{
	    system("cp", ("-a", catfile($TMP_CHECKDIR, $MASTERDIR, "data"), catfile($TMP_CHECKDIR, "worker.$port", "data"))) == 0
	        or die "Could not create worker data directory";
	}
}


# Routine to shutdown servers at failure/exit
sub ShutdownServers()
{
    if ($serversAreShutdown eq "FALSE")
    {
        system(catfile("$bindir", "pg_ctl"),
               ('stop', '-w', '-D', catfile($TMP_CHECKDIR, $MASTERDIR, 'data'))) == 0
            or warn "Could not shutdown worker server";

        for my $port (@workerPorts)
        {
            system(catfile("$bindir", "pg_ctl"),
                   ('stop', '-w', '-D', catfile($TMP_CHECKDIR, "worker.$port", "data"))) == 0
                or warn "Could not shutdown worker server";
        }

        if ($followercluster)
        {
            system(catfile("$bindir", "pg_ctl"),
                   ('stop', '-w', '-D', catfile($TMP_CHECKDIR, $MASTER_FOLLOWERDIR, 'data'))) == 0
                or warn "Could not shutdown worker server";

            for my $port (@followerWorkerPorts)
            {
                system(catfile("$bindir", "pg_ctl"),
                       ('stop', '-w', '-D', catfile($TMP_CHECKDIR, "follower.$port", "data"))) == 0
                    or warn "Could not shutdown worker server";
            }
        }
        if ($mitmPid != 0)
        {
            # '-' means signal the process group, 2 is SIGINT
            kill(-2, $mitmPid) or warn "could not interrupt mitmdump";
        }
        $serversAreShutdown = "TRUE";
    }
}

# setup the signal handler before we fork
$SIG{CHLD} = sub {
 # If, for some reason, mitmproxy dies before we do, we should also die!
  while ((my $waitpid = waitpid(-1, WNOHANG)) > 0) {
    if ($mitmPid != 0 && $mitmPid == $waitpid) {
      die "aborting tests because mitmdump failed unexpectedly";
    }
  }
};

if ($useMitmproxy)
{
  if (! -e $mitmFifoPath)
  {
    mkfifo($mitmFifoPath, 0777) or die "could not create fifo";
  }

  if (! -p $mitmFifoPath)
  {
    die "a file already exists at $mitmFifoPath, delete it before trying again";
  }

  system("lsof -i :$mitmPort");
  if (! $?) {
    die "cannot start mitmproxy because a process already exists on port $mitmPort";
  }

  if ($Config{osname} eq "linux")
  {
    system("netstat --tcp -n | grep $mitmPort");
  }
  else
  {
    system("netstat -p tcp -n | grep $mitmPort");
  }

  my $childPid = fork();

  die("Failed to fork\n")
   unless (defined $childPid);

  die("No child process\n")
    if ($childPid < 0);

  $mitmPid = $childPid;

  if ($mitmPid eq 0) {
    print("forked, about to exec mitmdump\n");
    setpgrp(0,0); # we're about to spawn both a shell and a mitmdump, kill them as a group
    exec("mitmdump --rawtcp -p $mitmPort --mode reverse:localhost:57638 -s $regressdir/mitmscripts/fluent.py --set fifo=$mitmFifoPath --set flow_detail=0 --set termlog_verbosity=warn >proxy.output 2>&1");
    die 'could not start mitmdump';
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

# enable synchronous replication if needed
if ($followercluster)
{
    $synchronousReplication = "-c synchronous_standby_names='FIRST 1 (*)' -c synchronous_commit=remote_apply";
}

# Start servers
if(system(catfile("$bindir", "pg_ctl"),
       ('start', '-w',
        '-o', join(" ", @pgOptions)." -c port=$masterPort $synchronousReplication",
       '-D', catfile($TMP_CHECKDIR, $MASTERDIR, 'data'), '-l', catfile($TMP_CHECKDIR, $MASTERDIR, 'log', 'postmaster.log'))) != 0)
{
  system("tail", ("-n20", catfile($TMP_CHECKDIR, $MASTERDIR, "log", "postmaster.log")));
  die "Could not start master server";
}

for my $port (@workerPorts)
{
    if(system(catfile("$bindir", "pg_ctl"),
           ('start', '-w',
            '-o', join(" ", @pgOptions)." -c port=$port $synchronousReplication",
            '-D', catfile($TMP_CHECKDIR, "worker.$port", "data"),
            '-l', catfile($TMP_CHECKDIR, "worker.$port", "log", "postmaster.log"))) != 0)
    {
      system("tail", ("-n20", catfile($TMP_CHECKDIR, "worker.$port", "log", "postmaster.log")));
      die "Could not start worker server";
    }
}

# Setup the follower nodes
if ($followercluster)
{
    system(catfile("$bindir", "pg_basebackup"),
           ("-D", catfile($TMP_CHECKDIR, $MASTER_FOLLOWERDIR, "data"), "--host=$host", "--port=$masterPort",
            "--username=$user", "-R", "-X", "stream", "--no-sync")) == 0
      or die 'could not take basebackup';

    for my $offset (0 .. $#workerPorts)
    {
        my $workerPort = $workerPorts[$offset];
        my $followerPort = $followerWorkerPorts[$offset];
        system(catfile("$bindir", "pg_basebackup"),
               ("-D", catfile($TMP_CHECKDIR, "follower.$followerPort", "data"), "--host=$host", "--port=$workerPort",
                "--username=$user", "-R", "-X", "stream")) == 0
            or die "Could not take basebackup";
    }

    if(system(catfile("$bindir", "pg_ctl"),
           ('start', '-w',
            '-o', join(" ", @pgOptions)." -c port=$followerCoordPort",
           '-D', catfile($TMP_CHECKDIR, $MASTER_FOLLOWERDIR, 'data'), '-l', catfile($TMP_CHECKDIR, $MASTER_FOLLOWERDIR, 'log', 'postmaster.log'))) != 0)
    {
      system("tail", ("-n20", catfile($TMP_CHECKDIR, $MASTER_FOLLOWERDIR, "log", "postmaster.log")));
      die "Could not start master follower server";
    }

    for my $port (@followerWorkerPorts)
    {
        if(system(catfile("$bindir", "pg_ctl"),
               ('start', '-w',
                '-o', join(" ", @pgOptions)." -c port=$port",
                '-D', catfile($TMP_CHECKDIR, "follower.$port", "data"),
                '-l', catfile($TMP_CHECKDIR, "follower.$port", "log", "postmaster.log"))) != 0)
        {
          system("tail", ("-n20", catfile($TMP_CHECKDIR, "follower.$port", "log", "postmaster.log")));
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

    foreach my $function (keys %functions)
    {
        system(catfile($bindir, "psql"),
                ('-X', '-h', $host, '-p', $port, '-U', $user, "-d", "regression",
                 '-c', "CREATE FUNCTION $function RETURNS $functions{$function};")) == 0
            or die "Could not create FUNCTION $function on worker";
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
    '--bindir', catfile($TMP_CHECKDIR, $TMP_BINDIR)
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
	$ENV{VANILLATEST} = "1";

	if (-f "$vanillaSchedule")
	{
	    rmdir "./testtablespace";
	    mkdir "./testtablespace";

	    my $pgregressdir=catfile(dirname("$pgxsdir"), "regress");
	    system("$plainRegress", ("--inputdir",  $pgregressdir),
	           ("--schedule",  catfile("$pgregressdir", "parallel_schedule"))) == 0
		or die "Could not run vanilla tests";
	}
	else
	{
	    system("make", ("-C", catfile("$postgresBuilddir", "src", "test", "regress"), "installcheck-parallel")) == 0
		or die "Could not run vanilla tests";
	}
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
