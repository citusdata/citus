Automated Failure testing
=========================

Automated Failure Testing works by inserting a network proxy (mitmproxy) between the Citus coordinator and one of the workers (connections to the other worker are left unchanged). The proxy is configurable, and sits on a fifo waiting for commands. When it receives a command over the fifo it reconfigures itself and sends back response. Regression tests which use automated failure testing communicate with mitmproxy by running special UDFs which talk to said fifo. The tests send commands such as "fail any connection which contain the string `COMMIT`" and then run SQL queries and assert that the coordinator has reasonable behavior when the specified failures occur.

**Table of Contents**

- [Getting Started](#getting-started)
- [Running mitmproxy manually](#running-mitmproxy-manually)
  - [Using Failure Test Helpers](#using-failure-test-helpers)
- [`citus.mitmproxy()` command strings](#citusmitmproxy-command-strings)
  - [Actions](#actions)
  - [Filters](#filters)
  - [Chaining](#chaining)
- [Recording Network Traffic](#recording-network-traffic)


## Getting Started

First off, to use this you'll need mitmproxy.
Currently, we rely on a [fork](https://github.com/thanodnl/mitmproxy/tree/fix/tcp-flow-kill) to run the failure tests.
We recommned using pipenv to setup your failure testing environment since that will handle installing the fork
and other dependencies which may be updated/changed.

Setting up pipenv is easy if you already have python and pip set up:
```bash
pip install pipenv
```

If the Pipfile requires a version you do not have, simply install that python version and retry.
Pipenv should be able to find the newly installed python and set up the environment.

Once you've installed it:

```bash
$ cd src/test/regress
$ pipenv --rm # removes any previous available pipenv
$ pipenv install  # there's already a Pipfile.lock in src/test/regress with packages
$ pipenv shell  # this enters the virtual environment, putting mitmproxy onto $PATH
```

That's all you need to do to run the failure tests:

```bash
$ make check-failure
```

## Running mitmproxy manually

```bash
$ mkfifo /tmp/mitm.fifo  # first, you need a fifo
$ cd src/test/regress
$ pipenv shell
$ mitmdump --rawtcp -p 9703 --mode reverse:localhost:9702 -s mitmscripts/fluent.py --set fifo=/tmp/mitm.fifo
```

The specific port numbers will be different depending on your setup. The above string means mitmdump will accept connections on port `9703` and forward them to the worker listening on port `9702`.

Now, open psql and run:

```psql
# UPDATE pg_dist_node SET nodeport = 9703 WHERE nodeport = 9702;
```

Again, the specific port numbers depend on your setup.

### Using Failure Test Helpers

In a psql front-end run
```psql
# \i src/test/regress/sql/failure_test_helpers.sql
```

> **_NOTE:_**  To make the script above work start psql as follows
> ```bash
> psql -p9700 --variable=worker_2_port=9702
> ```
> Assuming the coordinator is running on 9700 and worker 2 (which is going to be intercepted) runs on 9702

The above file creates some UDFs and also disables a few citus features which make connections in the background.

You also want to tell the UDFs how to talk to mitmproxy (careful, this must be an absolute path):

```psql
# SET citus.mitmfifo = '/tmp/mitm.fifo';
```

(nb: this GUC does not appear in `shared_library_init.c`, Postgres allows setting and reading GUCs which have not been defined by any extension)

You're all ready! If it worked, you should be able to run this:

```psql
# SELECT citus.mitmproxy('conn.allow()');
 mitmproxy
-----------

(1 row)
```

## `citus.mitmproxy()` command strings

Command strings specify a pipline. Each connection is handled individually, and the pipeline is called once for every packet which is sent. For example, given this string:

`conn.onQuery().after(2).kill()` -> kill a connection if three Query packets are seen

- `onQuery()` is a filter. It only passes Query packets (packets which the frontend sends to the backend which specify a query which is to be run) onto the next step of the pipeline.

- `after(2)` is another filter, it ignores the first two packets which are sent to it, then sends the following packets to the next step of the pipeline.

- `kill()` is an action, when a packet reaches it the connection containing that packet will be killed.

### Actions

There are 5 actions you can take on connections:


| Action              | Description                                                                                                                                                                                                                            |
|:--------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `conn.allow()`      | the default, allows all connections to execute unmodified                                                                                                                                                                              |
| `conn.kill()`       | kills all connections immediately after the first packet is sent                                                                                                                                                                       |
| `conn.reset()`      | `kill()` calls `shutdown(SHUT_WR)`, `shutdown(SHUT_RD)`, `close()`. This is a very graceful way to close the socket. `reset()` causes a RST packet to be sent and forces the connection closed in something more resembling an error.  |
| `conn.cancel(pid)`  | This doesn't cause any changes at the network level. Instead it sends a SIGINT to pid and introduces a short delay, with hopes that the signal will be received before the delay ends. You can use it to write cancellation tests.     |
| `conn.killall()`    | the `killall()` command kills this and all subsequent connections. Any packets sent once it triggers will have their connections killed.                                                                                               |

The first 4 actions all work on a per-connection basis. Meaning, each connection is tracked individually. A command such as `conn.onQuery().kill()` will only kill the connection on which the Query packet was seen. A command such as `conn.onQuery().after(2).kill()` will never trigger if each Query is sent on a different connection, even if you send dozens of Query packets.

### Filters

- `conn.onQuery().kill()`
  - kill a connection once a `Query` packet is seen
- `conn.onCopyData().kill()`
  - kill a connection once a `CopyData` packet is seen

The list of supported packets can be found in [structs.py](structs.py), and the list of packets which
could be supported can be found [here](https://www.postgresql.org/docs/current/static/protocol-message-formats.html)

You can also inspect the contents of packets:

- `conn.onQuery(query="COMMIT").kill()`
  - You can look into the actual query which is sent and match on its contents.
  - Note that this is always a regex
- `conn.onQuery(query="^COMMIT").kill()`
  - The query must start with `COMMIT`
- `conn.onQuery(query="pg_table_size\(")`
  - You must escape parens, since you're in a regex
- `after(n)`
  - Matches after the n-th packet has been sent:
- `conn.after(2).kill()`
  - Kill connections when the third packet is sent down them

There's also a low-level filter which runs a regex against the raw content of the packet:

- `conn.matches(b"^Q").kill()`
  - This is another way of writing `conn.onQuery()`
  - Note the `b`, it's always required.

### Chaining

Filters and actions can be arbitrarily chained:

- `conn.matches(b"^Q").after(2).kill()`
  - kill any connection when the third Query is sent

## Recording Network Traffic

There are also some special commands. This proxy also records every packet and lets you
inspect them:

- `recorder.dump()`
  - Emits a list of captured packets in `COPY` text format
- `recorder.reset()`
  - Empties the data structure containing the captured packets

Both of those calls empty the structure containing the packets, a call to `dump()` will only return the packets which were captured since the last call to `dump()` or `reset()`

Back when you called `\i sql/failure_test_helpers.sql` you created some UDFs which make using these strings easier. Here are some commands you can run from psql, or from inside failure tests:

- `citus.clear_network_traffic()`
  - Empties the buffer containing captured packets
- `citus.dump_network_traffic()`
  - Returns a little table and pretty-prints information on all the packets captured since the last call to `clear_network_traffic()` or `dump_network_traffic()`
