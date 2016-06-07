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


sub Usage()
{
    print "pg_regress_multi - Citus test runner\n";
    print "\n";
    print "Usage:\n";
    print "  pg_regress_multi [MULTI OPTIONS] -- [PG REGRESS OPTS]\n";
    print "\n";
    print "Multi Options:\n";
    print "  --bindir            Path to postgres binary directory\n";
    print "  --libdir            Path to postgres library directory\n";
    print "  --pgxsdir           Path to the PGXS directory\n";
    print "  --load-extension    Extensions to install in all nodes\n";
    print "  --server-option     Config option to pass to the server\n";
    exit 1;
}

# Option parsing
my $bindir = "";
my $libdir = undef;
my $pgxsdir = "";
my $majorversion = "";
my @extensions = ();
my @userPgOptions = ();
my %dataTypes = ();
my %fdws = ();
my %fdwServers = ();
my %functions = ();

GetOptions(
    'bindir=s' => \$bindir,
    'libdir=s' => \$libdir,
    'pgxsdir=s' => \$pgxsdir,
    'majorversion=s' => \$majorversion,
    'load-extension=s' => \@extensions,
    'server-option=s' => \@userPgOptions,
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

# Set some default configuration options
my $masterPort = 57636;
my $workerCount = 2;
my @workerPorts = ();

for (my $workerIndex = 1; $workerIndex <= $workerCount; $workerIndex++) {
    my $workerPort = $masterPort + $workerIndex;
    push(@workerPorts, $workerPort);
}

my $host = "localhost";
my $user = "postgres";
my @pgOptions = ();

# Postgres options set for the tests
push(@pgOptions, '-c', "listen_addresses='${host}'");
# not required, and we don't necessarily have access to the default directory
push(@pgOptions, '-c', "unix_socket_directories=");
push(@pgOptions, '-c', "fsync=off");
push(@pgOptions, '-c', "shared_preload_libraries=citus");
push(@pgOptions, '-c', "max_prepared_transactions=100");

# Citus options set for the tests
push(@pgOptions, '-c', "citus.shard_max_size=300kB");
push(@pgOptions, '-c', "citus.max_running_tasks_per_node=4");
push(@pgOptions, '-c', "citus.expire_cached_shards=on");
push(@pgOptions, '-c', "citus.task_tracker_delay=10ms");
push(@pgOptions, '-c', "citus.remote_task_check_interval=1ms");

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
%functions = ('fake_fdw_handler()', 'fdw_handler AS \'citus\' LANGUAGE C STRICT;');

#define fdws as name->handler name
%fdws = ('fake_fdw', 'fake_fdw_handler');

#define server_name->fdw
%fdwServers = ('fake_fdw_server', 'fake_fdw');

# Cleanup leftovers and prepare directories for the run
system("rm", ('-rf', 'tmp_check/tmp-bin')) == 0 or die "Could not remove tmp-bin directory";

system("rm", ('-rf', 'tmp_check/master')) == 0 or die "Could not remove master directory";
for my $port (@workerPorts)
{
    system("rm", ('-rf', "tmp_check/worker.$port")) == 0 or die "Could not remove worker directory";
}

# Prepare directory in which 'psql' is a wrapper around 'csql', which
# also adds some variables to csql.
system("mkdir", ('-p', "tmp_check/tmp-bin")) == 0
	or die "Could not create tmp-bin directory";
sysopen my $fh, "tmp_check/tmp-bin/psql", O_CREAT|O_TRUNC|O_RDWR, 0700
	or die "Could not create psql wrapper";
print $fh "#!/bin/bash\n";
print $fh "exec $bindir/csql ";
print $fh "--variable=master_port=$masterPort ";
for my $workeroff (0 .. $#workerPorts)
{
	my $port = $workerPorts[$workeroff];
	print $fh "--variable=worker_".($workeroff+1)."_port=$port ";
}
print $fh "\"\$@\"\n"; # pass on the commandline arguments
close $fh;

system("mkdir", ('-p', 'tmp_check/master/log')) == 0 or die "Could not create master directory";
for my $port (@workerPorts)
{
    system("mkdir", ('-p', "tmp_check/worker.$port/log")) == 0
        or die "Could not create worker directory";
}

# Create new data directories, copy workers for speed
system("$bindir/initdb", ("--nosync", "-U", $user, "tmp_check/master/data")) == 0
    or die "Could not create master data directory";

for my $port (@workerPorts)
{
    system("cp -a tmp_check/master/data tmp_check/worker.$port/data") == 0
        or die "Could not create worker data directory";
}

# Initialize master's worker list
for my $port (@workerPorts)
{
    system("echo $host $port >> tmp_check/master/data/pg_worker_list.conf") == 0
        or die "Could not initialize master's worker list";
}

# Routine to shutdown servers at failure/exit
my $serversAreShutdown = "FALSE";
sub ShutdownServers()
{
    if ($serversAreShutdown eq "FALSE")
    {
        system("$bindir/pg_ctl",
               ('stop', '-w', '-D', 'tmp_check/master/data')) == 0
            or warn "Could not shutdown worker server";

        for my $port (@workerPorts)
        {
            system("$bindir/pg_ctl",
                   ('stop', '-w', '-D', "tmp_check/worker.$port/data")) == 0
                or warn "Could not shutdown worker server";
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
}

# Start servers
system("$bindir/pg_ctl",
       ('start', '-w',
        '-o', join(" ", @pgOptions)." -c port=$masterPort",
       '-D', 'tmp_check/master/data', '-l', 'tmp_check/master/log/postmaster.log')) == 0
    or die "Could not start master server";

for my $port (@workerPorts)
{
    system("$bindir/pg_ctl",
           ('start', '-w',
            '-o', join(" ", @pgOptions)." -c port=$port",
            '-D', "tmp_check/worker.$port/data",
            '-l', "tmp_check/worker.$port/log/postmaster.log")) == 0
        or die "Could not start worker server";
}

###
# Create database, extensions, types, functions and fdws on the workers,
# pg_regress won't know to create them for us.
###
for my $port (@workerPorts)
{
    system("$bindir/psql",
           ('-h', $host, '-p', $port, '-U', $user, "postgres",
            '-c', "CREATE DATABASE regression;")) == 0
        or die "Could not create regression database on worker";

    for my $extension (@extensions)
    {
        system("$bindir/psql",
               ('-h', $host, '-p', $port, '-U', $user, "regression",
                '-c', "CREATE EXTENSION IF NOT EXISTS \"$extension\";")) == 0
            or die "Could not create extension on worker";
    }

    foreach my $dataType (keys %dataTypes)
    {
        system("$bindir/psql",
                ('-h', $host, '-p', $port, '-U', $user, "regression",
                 '-c', "CREATE TYPE $dataType AS $dataTypes{$dataType};")) == 0
            or die "Could not create TYPE $dataType on worker";
    }

    foreach my $function (keys %functions)
    {
        system("$bindir/psql",
                ('-h', $host, '-p', $port, '-U', $user, "regression",
                 '-c', "CREATE FUNCTION $function RETURNS $functions{$function};")) == 0
            or die "Could not create FUNCTION $function on worker";
    }

    foreach my $fdw (keys %fdws)
    {
        system("$bindir/psql",
                ('-h', $host, '-p', $port, '-U', $user, "regression",
                 '-c', "CREATE FOREIGN DATA WRAPPER $fdw HANDLER $fdws{$fdw};")) == 0
            or die "Could not create foreign data wrapper $fdw on worker";
    }

    foreach my $fdwServer (keys %fdwServers)
    {
        system("$bindir/psql",
                ('-h', $host, '-p', $port, '-U', $user, "regression",
                 '-c', "CREATE SERVER $fdwServer FOREIGN DATA WRAPPER $fdwServers{$fdwServer};")) == 0
            or die "Could not create server $fdwServer on worker";
    }
}

# Prepare pg_regress arguments
my @arguments = (
    "--host", $host,
    '--port', $masterPort,
    '--user', $user
);

if ($majorversion eq '9.5')
{
    push(@arguments, '--bindir', "tmp_check/tmp-bin");
}
elsif ($majorversion eq '9.4')
{
    push(@arguments, '--psqldir', "tmp_check/tmp-bin");
}
else
{
    die "Citus is not compatible with the detected PostgreSQL version $majorversion";
}

# Add load extension parameters to the argument list
for my $extension (@extensions)
{
    push(@arguments, "--load-extension=$extension");
}

# Append remaining ARGV arguments to pg_regress arguments
push(@arguments, @ARGV);

my $startTime = time();

# Finally run the tests
system("$pgxsdir/src/test/regress/pg_regress", @arguments) == 0
    or die "Could not run regression tests";

my $endTime = time();

print "Finished in ". ($endTime - $startTime)." seconds. \n";

exit 0;
