# Manual MVU (major-version-upgrade) testing for backport branches

Backporting a SQL-schema change and propagating the upgrade ladder (see
`sql-schema-backport.md` §Step 5) only *claims* MVU safety. This doc is how you **prove** it
end-to-end on a real multi-node cluster: stand up N nodes at an OLD Citus major, then walk the
extension forward across both backport branches — and, one hop further onto the
`main`-targeting PR branch — with `ALTER EXTENSION citus UPDATE`, capturing `citus_version()` AND
`pg_extension.extversion` on every node after every hop.

This is the "manual testing for backport branches" step. Do it after CI is green, before
declaring a SQL-schema backport family done. A C-only backport does NOT need it.

## What this proves
- A cluster shipped at the 13.x tail (`default_version` of `release-13.2`, e.g. `13.4-1`) can
  still cross to 14.x — i.e. a future managed-service 13→14 MVU won't strand the customer.
- **Final hop:** a 14.x-tail cluster (e.g. `14.2-1`) can cross to the NEXT major on
  `main` (`15.0-1`) via the PR branch you opened against `main` — proving the SAME object also
  rides a future 14.x→15.0 MVU. `main` jumps `14.0-1 → 15.0-1` directly (it has NO 14.1/14.2
  *upgrade-in-line*), but it MUST carry the `14.2-1--14.1-1` and `14.1-1--14.0-1` **downgrades**
  so PG can route `14.2-1 → 14.1-1 → 14.0-1 → 15.0-1`. If that route is missing you get "no
  update path from 14.2-1 to 15.0-1" — a real MVU gap in the ladder propagation to `main`.
- The forward/downgrade **ladder** you propagated actually routes end to end.
- The backported object (UDF/GUC/column) is present and the cluster is functional after each jump.

## The single hard requirement: ONE PG, three Citus builds
`ALTER EXTENSION citus UPDATE` walks the version graph made of the `citus--A--B.sql` scripts
**installed in the current PG's `SHAREDIR/extension/`**. So all three Citus versions in the walk
(old baseline + both backport branches) must be installed, one at a time, into the SAME PG
install / SAME cluster data dirs. You swap the installed Citus between hops and restart the nodes;
the catalog stays put and `ALTER EXTENSION` migrates it.

Use ONE dedicated PG install as that single PG (its Citus gets overwritten repeatedly — that's the
point — then you restore it at the end, §Cleanup). Do the builds in **detached worktrees** so your
reviewed branch checkouts are never disturbed.

### `make install-all`, NOT `make install` — MANDATORY here
`make install` lays down only **forward** scripts. The cross-major route from the 13.x tail is
**not** a straight climb — there is no `13.4-1--14.x` edge. It goes:

```
13.4-1 → 13.3-1 → 13.2-1 → 14.0-1 → 14.1-1 → 14.2-1
  (down)   (down)   (BRIDGE up)  (up)    (up)
```

so it needs the **downgrade** scripts `13.4-1--13.3-1`, `13.3-1--13.2-1` and the cross-major
**bridge** `13.2-1--14.0-1` (all shipped under `sql/downgrades/` on `release-14.0`). `make install`
omits every downgrade → `ALTER EXTENSION citus UPDATE` fails with "no update path from 13.4-1 to
14.2-1". Always build the test PG with **`make install-all`** (it runs `install` +
`install-downgrades` for both the distributed and columnar extensions).

### PG version must be common to all builds
Baseline Citus must support the chosen PG. 13.0/13.2 support PG15/16/17; 14.0 supports 16/17/18;
`main` supports 16/17/18 → **PG 17** is the common denominator for all four. Pick a PG 17 install
so it works for the whole 13.0→13.4→14.2→15.0 walk.

## Recipe (verified with a 3-node cluster: citus 13.0.5 → 13.4.0 → 14.2.0 → 15.0devel)

Pick a shared PG (`SHARED=<path to your PG17 install>`), a spare coordinator port (e.g. `10600`;
workers take `+1/+2`), and the three source trees. Use detached worktrees so the reviewed
branch worktrees are never disturbed:

```bash
cd <main clone>            # holds all the branches/tags
git worktree add --detach /tmp/mvu/wt130 v13.0.5            # OLD baseline (a real tag)
git worktree add --detach /tmp/mvu/wt132 <bp-release-13.2-head-sha>
git worktree add --detach /tmp/mvu/wt140 <bp-release-14.0-head-sha>
git worktree add --detach /tmp/mvu/wtmain <bp-main-PR-head-sha>
```

Build helper — reconfigure each tree against the SHARED PG and `install-all`:

```bash
build() {  # build <worktree> ; installs into $SHARED
  cd "$1"
  ./configure PG_CONFIG="$SHARED/bin/pg_config" >/tmp/cfg.log 2>&1
  local JOBS; JOBS=$(nproc 2>/dev/null || sysctl -n hw.logicalcpu)   # portable: Linux || macOS
  make -sj"$JOBS"             >/tmp/mk.log  2>&1
  make -sj"$JOBS" install-all >/tmp/ins.log 2>&1
  ls "$SHARED/share/extension/" | grep -E '^citus--1[3-5]' | sort   # sanity: see the ladder
}
```
Re-use the same cluster data dirs across hops:

1. **Baseline 13.0.** `build /tmp/mvu/wt130`. Stand up a cluster on `$SHARED` with `citus_dev`
   (it `CREATE EXTENSION citus` at `default_version` = the currently-installed `13.0-1`). Verify
   `SELECT extversion FROM pg_extension WHERE extname='citus'` and `SELECT citus_version()` = 13.0
   on coordinator + both workers. (`citus_dev`'s default DB is `$USER`, NOT `postgres`.)
2. **Up to the 13.2 branch.** `build /tmp/mvu/wt132` (overwrites Citus in `$SHARED`, now
   `default_version = 13.4-1`, installs the 13.x up+down ladder). **Restart every node** so the new
   `.so` loads. Then, on EACH node: `ALTER EXTENSION citus UPDATE;` → catalog goes `13.0-1 → 13.4-1`
   in one call (walks the whole chain). Capture `citus_version()` per node — expect
   `13.4.0 ... (sha: <13.2-head>)`.
3. **Cross-major to the 14.0 branch.** `build /tmp/mvu/wt140` (`default_version = 14.2-1`, installs
   the full up+down+bridge ladder). **Restart every node.** On EACH node: `ALTER EXTENSION citus
   UPDATE;` → routes `13.4-1 → 13.3-1 → 13.2-1 → 14.0-1 → 14.1-1 → 14.2-1`. Capture
   `citus_version()` per node — expect `14.2.0 ... (sha: <14.0-head>)`.
4. **Functional check** on the upgraded cluster (coordinator): active `pg_dist_node`,
   `create_distributed_table` + INSERT + distributed `count(*)`, `pg_dist_shard` count, and that
   the backported object is present (e.g. `SELECT proname FROM pg_proc WHERE proname =
   'distribute_object' AND pronamespace = 'citus_internal'::regnamespace;`).
5. **One more hop onto the `main` PR branch.** Detached worktree at the `bp-<pr>-main`
   head, `build` it into the SAME `$SHARED` (overwrites Citus to `default_version = 15.0-1`,
   installs main's up+down ladder incl. the 14.x downgrades). **Restart every node.** On EACH node:
   `ALTER EXTENSION citus UPDATE;` → routes `14.2-1 → 14.1-1 → 14.0-1 → 15.0-1`. Re-run the
   functional check. Expect `Citus 15.0devel ... (sha: <main-PR-head>)` and the backported object
   still present at `15.0-1`.

**KEY NUANCE — `citus_version()` reports the BINARY, `extversion` reports the CATALOG.** After you
install a newer Citus + restart (but BEFORE `ALTER EXTENSION citus UPDATE`), `citus_version()`
already prints the new version (it is compiled into the `.so`), while `pg_extension.extversion` is
still the old catalog version. The migration is only proven by **`extversion` moving to the new
`default_version`**. On the first hops the jump is a whole major so `citus_version()` alone is
telling; on the `14.2 → 15.0devel` hop both 14.2 and 15.0 builds may print `15.0devel`-ish strings,
so capture `extversion` before+after (`14.2-1 → 15.0-1`) as the real evidence.

Run the same `ALTER EXTENSION citus UPDATE` on each node by connecting to each port directly
(coordinator does NOT auto-propagate the extension version to workers) — that's the "update all
nodes" the ask means.

## Environment notes (neutral, but they save time)
- **Put SQL in `.sql` files and run `psql -f`; put shell logic in `.sh` files and run them.**
  Piping SQL / loops inline through several shell layers (esp. cross-OS terminals) mangles quoting
  and `$USER`. If a script is authored on a different-newline OS, strip CRLF first (`sed -i
  's/\r$//' file`) or `set -o pipefail` may die with "invalid option name".
- **`citus_dev` records RELATIVE data-dir paths** (`cluster/<role>`). `pg_ctl -D <abs>` then fails
  with "could not access directory …/cluster/<role>". Fix: `cd` into the clusters parent and use
  `pg_ctl -D cluster/<role> …`.
- **If `pg_ctl -w start` blocks**, the `-w` readiness probe can hang against an `ssl=on` citus_dev
  config even though the server logged "ready to accept connections". Start WITHOUT `-w` and poll
  the TCP port yourself (`ss -ltn | grep :<port>`). `-w` on **stop** is fine.
- On slow disks the first post-restart checkpoint can take ~100s; judge node readiness by
  "database system is ready to accept connections" in the node logfile + the port listening, not by
  a `pg_ctl -w` wrapper returning.

## Cleanup / restore
- The shared PG's Citus was left at whatever you installed last (after the final main hop that
  is main's `15.0-1`). **Restore the env's own build:** rebuild the PG's normal Citus branch with
  `make -sj"$(nproc 2>/dev/null || sysctl -n hw.logicalcpu)" install-all` (verify `default_version` + the ladder edges are back).
- Stop the test cluster (`pg_ctl -D cluster/<role> -w -m fast stop` per node, or your cluster
  helper's stop command) and `git worktree remove` the throwaway build trees (+ `git worktree
  prune`) when done.

## Expected evidence shape (what a PASS looks like)
```
BASELINE (citus 13.0):    all nodes → extversion 13.0-1  | Citus 13.0.5 ... gitref: v13.0.5
after 13.2-branch UPDATE: all nodes → extversion 13.4-1  | Citus 13.4.0 ... (sha: <13.2 head>)
after 14.0-branch UPDATE: all nodes → extversion 14.2-1  | Citus 14.2.0 ... (sha: <14.0 head>)
after main-PR   UPDATE:   all nodes → extversion 14.2-1→15.0-1 | Citus 15.0devel ... (sha: <main head>)
functional (each hop): 3 active nodes, create_distributed_table OK, count matches, backported obj present
```
Report the per-node `extversion` (before+after) AND `citus_version()` for every hop — not a
summary — since the whole point is proving every node crossed each boundary.
