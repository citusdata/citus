# pg_send_cancellation

pg_send_cancellation is a program for manually sending a cancellation
to a Postgres endpoint. It is effectively a command-line version of
PQcancel in libpq, but it can use any PID or cancellation key.

We use pg_send_cancellation primarily to propagate cancellations between pgbouncers
behind a load balancer. Since the cancellation protocol involves
opening a new connection, the new connection may go to a different
node that does not recognize the cancellation key. To handle that
scenario, we modified pgbouncer to pass unrecognized cancellation
keys to a shell command.

Users can configure the cancellation_command, which will be run with:
```
<cancellation_command> <client ip> <client port> <pid> <cancel key>
```

Note that pgbouncer does not use actual PIDs. Instead, it generates PID and cancellation key together a random 8-byte number. This makes the chance of collisions exceedingly small.

By providing pg_send_cancellation as part of Citus, we can use a shell script that pgbouncer invokes to propagate the cancellation to all *other* worker nodes in the same cluster, for example:

```bash
#!/bin/sh
remote_ip=$1
remote_port=$2
pid=$3
cancel_key=$4

postgres_path=/usr/pgsql-14/bin
pgbouncer_port=6432

nodes_query="select nodename from pg_dist_node where groupid > 0 and groupid not in (select groupid from pg_dist_local_group) and nodecluster = current_setting('citus.cluster_name')"

# Get hostnames of other worker nodes in the cluster, and send cancellation to their pgbouncers
$postgres_path/psql -c "$nodes_query" -tAX | xargs -n 1 sh -c "$postgres_path/pg_send_cancellation $pid $cancel_key \$0 $pgbouncer_port"
```

One thing we need to be careful about is that the cancellations do not get forwarded
back-and-forth. This is handled in pgbouncer by setting the last bit of all generated
cancellation keys (sent to clients) to 1, and setting the last bit of all forwarded bits to 0.
That way, when a pgbouncer receives a cancellation key with the last bit set to 0,
it knows it is from another pgbouncer and should not forward further, and should set
the last bit to 1 when comparing to stored cancellation keys.

Another thing we need to be careful about is that the integers should be encoded
as big endian on the wire.
