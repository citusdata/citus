#!/usr/bin/env perl

# Generates all the .sql files and puts them into the requested directory

use strict;
use warnings;

use File::Copy "copy";
use File::Spec::Functions "catdir";

my $EXTENSION = "citus";

my $argcount = @ARGV;
die "need a single argument saying where to install the sql files\n" if ($argcount != 1);

my $prefix = $ARGV[0];
die "install destination must exist!\n" unless (-e $prefix);
my $dest = catdir($prefix, "share/postgresql/extension");
unless (-e $dest)
{
  $dest = catdir($prefix, "share/extension");
}
die "install destination must be a postgres installation!\n" unless (-e $dest);

# 1) check that we're installing into the real postgres installation

print "installing into $prefix\n";

# 2) parse Makefile's EXTVERSIONS to get the list of version pairs

die "could not find Makefile" unless (-e 'Makefile');
my $lines;
{
  open(MAKEFILE, "Makefile") or die "cannot open Makefile\n";
  local $/ = undef;  # remove the record separator just for this scope
  $lines = <MAKEFILE>;  # read all the lines at once into a single variable
  close(MAKEFILE);
}

my $versions;
if ($lines =~ /^EXTVERSIONS = ((?:[0-9. \t-]+(?:\\\n)?)*)/ms)
{
  $versions = $1;
}
else
{
  die "could not find EXTVERSIONS\n";
}

$versions =~ s/\\\n//g;  # remove all newlines
$versions =~ s/\t/ /g;   # also all tabs
$versions =~ s/\s+/ /g;  # and, finally, all doubled whitespace
print "found list of versions: $versions\n";

# 3) generate all the sql files

my @versions = split(' ', $versions);
my @files = ("$EXTENSION--5.0.sql");

die "could not find $EXTENSION.sql" unless (-e "$EXTENSION.sql");
copy("$EXTENSION.sql", "$EXTENSION--5.0.sql");

while (scalar @versions > 1)
{
  my $version = $versions[0];
  my $next = $versions[1];

  my $thisFile = "$EXTENSION--$version.sql";
  my $updateFile = "$EXTENSION--$version--$next.sql";
  my $nextFile = "$EXTENSION--$next.sql";

  print "creating $nextFile\n";

  copy($thisFile, $nextFile);

  open(NEXT, ">>", $nextFile) or die "could not open $nextFile";
  open(UPDATE, "<", $updateFile) or die "could not open $updateFile";
  while(my $line = <UPDATE>){
    print NEXT $line;
  }
  close(UPDATE);
  close(NEXT);

  push @files, $nextFile;
  shift @versions;
}

print "copying .sql files into $dest\n";

# 4) copy the sql files into the right place!
while(my $file = shift(@files))
{
  my $fullDest = catdir($dest, $file);
  copy($file, $fullDest);
}
