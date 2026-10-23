# Metadata replay security

## The problem

Citus recreates distributed objects on worker nodes through a superuser
connection. PostgreSQL evaluates an expression wherever it parses one, so an
expression that a non-superuser tenant authored could run with the rights of the
replaying superuser.

The reported case was a column default, but the surface is much wider. An unknown
literal coerced to an array of a tenant owned domain runs the domain's `CHECK`
during parse analysis, and that construct is accepted in a column default, a
generated column, a check constraint, an index expression or predicate, a
statistics expression, a partition key or bound, a domain default, a function
parameter default, a function body, a view or materialized view definition, a
trigger `WHEN` clause, an RLS policy, and a publication row filter. The
publication case evaluates the expression even though PostgreSQL then rejects
the statement.

Enumerating those places is a list that fails open when it is incomplete, which
is why classification is inverted: see below.

## The boundary

Commands that can carry such an expression are prefixed with the owner of the
object being replayed:

```sql
DO 'BEGIN IF current_setting(''citus.metadata_replay_owner'', true) IS NULL
    THEN RAISE WARNING ''this node evaluates replayed expressions with the
    rights of the replaying superuser'';
    ELSE PERFORM set_config(''citus.metadata_replay_owner'', ''alice'', true);
    END IF; END';
CREATE TABLE ...;
DO '... clear the marker ...';
```

While the marker is set, the receiving node runs that one statement as the
object owner:

```c
SetUserIdAndSecContext(ownerId,
                       savedContext | SECURITY_LOCAL_USERID_CHANGE |
                       SECURITY_RESTRICTED_OPERATION);
```

Identity, security context and settings are restored afterwards, including on
error, and the marker is consumed so it applies to a single statement.

This is an identity change rather than an interception of function calls, which
is what makes it cover builtin functions, cached or inlined expressions, and
evaluation performed by the type system. A function-call hook sees none of
those: `fmgr` takes a fast path for builtins, a cached expression need not call
the guarded function again, and domain input is not a hooked call.
`SECURITY_RESTRICTED_OPERATION` additionally blocks `SET ROLE`,
`SET SESSION AUTHORIZATION` and protected settings.

## Which commands are marked

Every replayed command is marked with the owner of the object it recreates,
unless the object is already owned by a superuser or is of a kind that cannot
evaluate anything. The exceptions are named in `ReplayObjectIsExpressionFree()`:
collations, schemas, text search dictionaries and configurations, roles,
databases, extensions, foreign servers, sequences and composite types. Roles,
databases and extensions additionally have to stay privileged to replay at all.

An object kind that is not named there is marked. That way a kind nobody
considered, in this PostgreSQL release or a later one, is protected rather than
quietly exposed.

What decides marking is the connection a command travels on. Citus marks what it
sends over a connection it opens as the extension owner: node activation,
metadata sync, dependencies it creates ahead of a new object, and explicit
object repair. Commands sent over the user's own connection are not marked,
because they already carry that user's rights, and marking them would only add
a privilege requirement.

The line is therefore not "replay versus DDL a user ran". A tenant distributing
their own table makes Citus create that table's dependencies on every node, and
it opens the extension owner's connection to do so whenever none of them was
created in the same transaction. Those creates are marked.

## Adding an exception

`ReplayObjectIsExpressionFree()` is the only place that decides a kind does not
need the owner's rights, and adding to it is how a future kind is exempted. The
bar is a demonstration rather than an argument:

1. Build the object so a tenant owned domain is reachable from its definition,
   through an unknown array literal such as `'{x}'::tenant.checked_text[]`.
2. Recreate it the way replay does and confirm the domain's `CHECK` does not
   run. A scalar cast is not a sufficient test. PostgreSQL defers that one, so
   it stays quiet for kinds that are not safe and will mislead you.
3. Add the kind to the switch with its reason, and a case in
   `metadata_replay_security` that fails if it ever starts being marked.

A kind that cannot evaluate anything but whose owner Citus cannot read should be
left out of the list. It then reports itself, which is the intended outcome: the
operator learns that something replays privileged instead of assuming it does
not.

## Cost

Running a statement as the owner means the owner must hold the privileges that
statement needs. Replaying an object therefore requires its owner to be able to
recreate it, typically `CREATE` on the containing schema. If it cannot, replay
fails with `42501` rather than falling back to the rights of the replaying
session. Failing closed is deliberate: the alternative reopens the vulnerability.

This is a behaviour change for objects whose ownership was transferred to a
role that cannot create in that schema. Such an object replayed before this
change and fails after it, with an error naming the role and the missing grant.

## Nodes that predate this protection

The marker is emitted by the coordinator and applied by the worker, so both
sides must be upgraded before replay is protected. A worker that does not know
the marker would otherwise ignore it silently, so the emitted command reports
itself instead. It tests whether the setting exists rather than relying on an
error, because before PostgreSQL 15 an unknown `citus.` name is accepted as a
placeholder instead of being rejected, which would leave `require` reporting
nothing on exactly the nodes it is meant to catch.
`citus.metadata_replay_protection` selects the reaction:

- `warn` (default) reports the node and continues, so an upgrade in progress is
  visible but not broken;
- `require` refuses to replay to such a node;
- `off` disables the marker entirely.

Upgrade workers before the coordinator. Doing it the other way leaves a window
where the cluster looks patched while every worker replay is still unprotected.

The check reads presence, not enforcement, and the two can differ. Before
PostgreSQL 15 a `citus.` name left in `postgresql.conf` survives as a
placeholder on a node that does not register the real setting, and a placeholder
holds a value without applying anything. Such a node answers the check as though
it were protected, which is a limit on what `require` can promise. Detecting it
would take a probe that a placeholder cannot satisfy, such as setting a role
name that cannot exist and requiring the rejection the check hook produces.

To check which nodes enforce the protection:

```sql
SELECT nodename, nodeport, result
FROM run_command_on_workers(
  $$SELECT current_setting('citus.metadata_replay_owner', true) IS NOT NULL$$);
```

## Known limitations

- The boundary is applied by the utility hook, so a marked command that is not a
  utility statement is covered only through the utility statements it runs.
  `worker_create_or_replace_object()` has that shape, and the create it applies
  is marked because the marker is still set when it reaches the hook. An
  expression evaluated by such an outer statement itself would not be covered.
- Resolving a name collision on the receiving node renames or drops the object
  that is in the way with the rights of the replaying superuser. That statement
  carries no tenant expression, and the create that follows it is marked.
- `current_setting('is_superuser')` still reports `on` inside the restricted
  context, because `SetUserIdAndSecContext` does not update that GUC the way
  `SET ROLE` does. Permission checks use the switched user, so this is
  informational only.
- Nothing fails closed if a future PostgreSQL release evaluates a surface that
  is not marked today.
