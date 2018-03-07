/* citus--7.3-3--7.3-4 */

/* helper functions for automated failure testing */

CREATE FUNCTION citus.run_command(text)
RETURNS void AS $$
  use strict;
  use warnings;
  use IO::Socket;

  my $command = $_[0];
  my $sockpath = '/home/brian/Work/citus/src/test/regress/gdb.sock';

  # open a connection to the daemon
  my $client = IO::Socket::UNIX->new(
    Type => SOCK_STREAM(), Peer => $sockpath,
  );

  # send a command to the daemon!
  $client->send("$command\n");
  $client->flush();

  # this is a bit of a crazy dance but it does the trick of ensuring the remote
  # end has received our command before we disconnect
  $client->shutdown(1);
  $client->read(my $ignored, 2048);
  $client->shutdown(0);
$$ LANGUAGE plperlu;

CREATE FUNCTION citus.gdb_attach(int)
RETURNS void AS $$
  use strict;
  use warnings;
  use IO::Socket;

  my $pid = $_[0];
  my $sockpath = '/home/brian/Work/citus/src/test/regress/gdb.sock';

  # open a connection to the daemon
  my $client = IO::Socket::UNIX->new(
    Type => SOCK_STREAM(), Peer => $sockpath,
  );

  # send a command to the daemon!
  $client->send("attach $pid\n");
  $client->flush();

  # this is a bit of a crazy dance but it does the trick of ensuring the remote
  # end has received our command before we disconnect
  $client->shutdown(1);
  $client->read(my $ignored, 2048);
  $client->shutdown(0);
$$ LANGUAGE plperlu;

CREATE FUNCTION citus.advisory_lock_at(text)
RETURNS void AS $$
  use strict;
  use warnings;
  use IO::Socket;

  my $breakpoint = $_[0];
  my $sockpath = '/home/brian/Work/citus/src/test/regress/gdb.sock';

  # open a connection to the daemon
  my $client = IO::Socket::UNIX->new(
    Type => SOCK_STREAM(), Peer => $sockpath,
  );

  # send a command to the daemon!
  $client->send("break\n");
  $client->flush();

  # this is a bit of a crazy dance but it does the trick of ensuring the remote
  # end has received our command before we disconnect
  $client->shutdown(1);
  $client->read(my $ignored, 2048);
  $client->shutdown(0);
$$ LANGUAGE plperlu;
