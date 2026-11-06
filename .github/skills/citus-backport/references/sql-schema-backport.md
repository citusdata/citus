# SQL-schema backport recipe (the hard case)

A C-only cherry-pick usually just applies. The moment a commit touches the SQL schema (new UDF,
new column, new migration step) you must **remap it to the target branch's real version**, because
`main`'s migration files are named for the NEXT major (e.g. `15.0-1`) and the release branch is on
a different `default_version`. Do NOT accept `main`'s files as-is.

## Step 0 — read the target version (drives everything)
On each target branch read `src/backend/distributed/citus.control`:

```
default_version = '<X.Y-Z>'
```

Then find that branch's current dev migration + downgrade under
`src/backend/distributed/sql/`:
- upgrade:   `citus--<prev>--<default_version>.sql`
- downgrade: `citus--<default_version>--<prev>.sql`

Worked example (versions as of this writing):

| Branch        | default_version | dev upgrade migration        | dev downgrade                | udf versioned file |
|---------------|-----------------|------------------------------|------------------------------|--------------------|
| release-13.2  | `13.4-1`        | `citus--13.3-1--13.4-1.sql`  | `citus--13.4-1--13.3-1.sql`  | `13.4-1.sql`       |
| release-14.0  | `14.2-1`        | `citus--14.1-1--14.2-1.sql`  | `citus--14.2-1--14.1-1.sql`  | `14.2-1.sql`       |
| main          | `15.0-1`        | `citus--14.0-1--15.0-1.sql`  | `citus--15.0-1--14.0-1.sql`  | `15.0-1.sql`       |

## Step 1 — remap the migration files
The main commit added its schema hunk to `citus--14.0-1--15.0-1.sql` (and the DROP to the matching
`15.0-1--14.0-1` downgrade), plus a versioned udf file `sql/udfs/<name>/15.0-1.sql`. On the target
branch:

1. `git rm` the main-only `15.0-1` upgrade + downgrade migration files if the cherry-pick created
   them (they don't belong on the release branch).
2. `git mv src/.../sql/udfs/<name>/15.0-1.sql  src/.../sql/udfs/<name>/<default_version>.sql`
   (rename the versioned udf file to the branch's version).
3. Add the `#include "udfs/<name>/<default_version>.sql"` line to the branch's **dev upgrade
   migration** (`citus--<prev>--<default_version>.sql`) — same include the main commit added to
   `14.0-1--15.0-1.sql`, just in the branch's file.
4. Add the reverse `DROP FUNCTION IF EXISTS <fully-qualified-signature>;` (or DROP for whatever
   object) to the branch's **dev downgrade** (`citus--<default_version>--<prev>.sql`).
5. Keep `sql/udfs/<name>/latest.sql` — refresh it to identical content as the versioned file.

`ci/check_migration_files.sh` ("Check for missing downgrade scripts") gates step 4 — every upgrade
step needs a matching downgrade. The CI `check-sql-snapshots` job validates these manual edits, so
green there is strong confirmation the remap is correct.

## Step 2 — fix `multi_extension.out`
`multi_extension` walks the whole upgrade+downgrade ladder and prints a "Snapshot of state at
`<version>`" section per version. The new object's row must land under the **target branch's**
version section (e.g. `13.4-1`), NOT main's `15.0-1`. The snapshot table's column widths are
dynamic — **regenerate the .out with the runner, don't hand-align** it. From `src/test/regress`:

```
python citus_tests/run_test.py multi_extension
```

The new line typically shows the new function/object with `(1 row)` (or the appropriate count) in
the target-version section.

## Step 3 — fix `upgrade_list_citus_objects.out` (edit BY CONSTRUCTION)
This test **cannot be run standalone** via `run_test.py` — it depends on a `citus_schema` created
by an earlier test in `after_pg_upgrade_with_columnar_schedule`; run alone it dies with
`ERROR: no schema has been selected to create in`. So validate its edit by construction:
- the new function line auto-merges alphabetically in the cherry-pick (leave it);
- fix only the trailing **row COUNT** = base_count + (number of new citus objects). Read the base
  count from the branch's own expected file. Observed: release-13.2 `378 → 379`, release-14.0
  `384 → 385` for one new UDF.

## Step 4 — build WITH downgrades, then test
`make install` does NOT install downgrade scripts, and `multi_extension` exercises the downgrade
path, so after every rebuild run both:

```
make -sj"$(nproc 2>/dev/null || sysctl -n hw.logicalcpu)" install
make -C src/backend/distributed install-downgrades
make -C src/backend/columnar    install-downgrades
```

(`columnar` is a SEPARATE extension with its own downgrades; omitting it surfaces as a
`citus_columnar` "no update path" error. `make install-all` runs both.)

Then run the feature's own test + `multi_extension`.

## Step 5 — propagate the upgrade ladder ACROSS branches (MVU safety)

Backporting the object DOWN to a release branch is not enough. An MVU replays the migration
**ladder**, so a new SQL object must be reachable on EVERY higher branch's ladder too — otherwise
a cluster upgraded 13.x→14.y would silently lose it. Net effect: **the same object is defined
MULTIPLE times per branch, once per major's introduction step.** This is intentional, not
duplication to "fix".

**Topology** (worked example: `citus_internal.distribute_object`, introduced on main at
`14.0-1--15.0-1`, backported to 13.2 at `13.3-1--13.4-1` and to 14.0 at `14.1-1--14.2-1`):

| Ladder step (+ its downgrade + versioned udf) | release-13.2 | release-14.0 | main |
|---|---|---|---|
| `13.3-1--13.4-1` (udf `13.4-1`) — 13.x introduction | ✅ (the backport itself) | ✅ copy here | ✅ copy here |
| `14.1-1--14.2-1` (udf `14.2-1`) — 14.x introduction | n/a | ✅ (the backport itself) | ✅ copy here |
| `14.0-1--15.0-1` (udf `15.0-1`) — main's own top    | n/a | n/a | ✅ already there (orig land) — LEAVE UNTOUCHED |

Rule of thumb: **a step introduced on major N must be copied onto every branch for major ≥ N.**
13.x paths → release-14.0 + main; 14.x paths → main; main's own top path stays as merged.

**How to do it (per higher branch, e.g. release-14.0 then main):**
1. This is a **SEPARATE commit** on top of the (unchanged) cherry-pick — never amend the
   cherry-pick. On main it's its own PR.
2. Copy the step + downgrade + versioned-udf files **byte-for-byte** from the branch that owns
   the introduction. `git show <branch>:<path>` reads any branch's blob without checking it out
   (worktrees of one clone share an object store, so this is trivial):
   ```
   git show bp-<pr>-release-13.2:src/backend/distributed/sql/citus--13.3-1--13.4-1.sql          > <same path>
   git show bp-<pr>-release-13.2:src/backend/distributed/sql/downgrades/citus--13.4-1--13.3-1.sql > <same path>
   git show bp-<pr>-release-13.2:src/backend/distributed/sql/udfs/<name>/13.4-1.sql             > <same path>
   ```
   (and the analogous `14.1-1--14.2-1` trio from `bp-<pr>-release-14.0` when copying onto main.)
3. **Keep the header verbatim** — even the `-- bump version to 13.4-1` line stays on downstream
   branches where 13.4 is not the default. Confirmed identical across branches in the #8621 UDF family.
4. **Purely additive.** Do NOT touch `citus.control` / `default_version`. Do NOT touch the
   `13.2-1--14.0-1` major-bridge migration. main's `latest.sql` still points at its own top
   version (15.0-1); leave it. `check_sql_snapshots.sh` verifies latest.sql == highest udf.
5. New step files contain ONLY the object being propagated (header + single `#include`); the
   downgrade is header + `DROP FUNCTION IF EXISTS <signature>;`. Don't drag along unrelated objects
   that happen to share the source branch's step file.
6. Build + `install-downgrades` + `multi_extension` on the higher branch (expect PASS, usually no
   `.out` change needed since the new sections are additive). Run the three migration CI checks
   locally before pushing: `ci/check_migration_files.sh`, `ci/check_sql_snapshots.sh`,
   `ci/disallow_c_comments_in_migrations.sh` (all must exit 0).
7. **Add `multi_extension.sql` ladder-test coverage for the new step** (or prove it's already
   covered) — see the dedicated section below.

**Template PR family** (study these to see the end-state, don't reverse-engineer from one PR):
find the original feature PR, then every PR that mentions it in title/desc. Worked example — the
`citus_internal.distribute_object` repair UDF, introduced on main in **#8621**: backported
one-branch-per-target via **#8684**→release-13.2 and **#8685**→release-14.0 (each branch also carries
the sibling GUC PR **#8625** `citus.allow_unsafe_insert_select_pushdown`, unsquashed as its own
commit), with the 13.x/14.x upgrade paths propagated to `main` in **#8686**. The main ladder PR
(#8686, "Add 13.x/14.x upgrade paths for citus_internal.distribute_object to main") is the model for
the main ladder commit's title/body; it's internal plumbing so usually **no `DESCRIPTION:` line**.

## multi_extension.sql ladder tests for new steps (off-walk detour pattern)

Adding a ladder step (Step 5 backport OR Step 5 propagation) is not fully covered until
`src/test/regress/sql/multi_extension.sql` exercises it — or you can prove the existing walk already
does. Model the additions on the template PR family, adapted per branch.

- **The test walks ONE path only** — the main upgrade path (`13.2 → 13.3 → 14.0 → 14.1 → 15.0`).
  A step you add for MVU may sit **off** that walk:
  - the 13.x maintenance-tail step `13.3-1--13.4-1` (off-walk on release-14.0 and main), and
  - the 14.x maintenance-tail step `14.1-1--14.2-1` (off-walk on **main**, whose walk jumps 14.1 → 15.0).
  Off-walk steps' upgrade AND downgrade scripts are otherwise **never executed** by the test.
- **On-walk / top-of-branch steps need NOTHING extra.** A branch's own `default_version` (13.4-1 on
  release-13.2, 14.2-1 on release-14.0, 15.0-1 on main) is already the target of the existing
  `-- Snapshot of state at X` block; the propagated object simply appears in that snapshot's `.out`.
  ⇒ release-13.2 needed **no** `multi_extension.sql` change; release-14.0 needed **one** detour
  (13.4-1); main needed **two** (13.4-1 and 14.2-1).
- **PATTERN = round-trip no-op → snapshot at the detour → rejoin the walk FROM the detour.** Match
  the shape of the existing on-walk blocks — visit the off-walk version the same way every other
  version is visited, so the additions look like they belong. Do NOT add editorial parentheticals
  (no "(13.x maintenance line, off the ... path)"). At the point the walk sits at the detour's
  PARENT, insert two blocks AND retarget the *existing* next round-trip so it bounces off the
  detour instead of the parent:
  ```sql
  -- Test downgrade to <parent> from <detour>
  ALTER EXTENSION citus UPDATE TO '<detour>';   -- e.g. 13.4-1
  ALTER EXTENSION citus UPDATE TO '<parent>';   -- e.g. 13.3-1
  -- Should be empty result since upgrade+downgrade should be a no-op
  SELECT * FROM multi_extension.print_extension_changes();

  -- Snapshot of state at <detour>
  ALTER EXTENSION citus UPDATE TO '<detour>';
  SELECT * FROM multi_extension.print_extension_changes();

  -- Test downgrade to <detour> from <next>   <-- was "<parent> from <next>"; retarget it
  ALTER EXTENSION citus UPDATE TO '<next>';    -- the next ON-WALK version (e.g. 14.0-1 / 15.0-1)
  ALTER EXTENSION citus UPDATE TO '<detour>';  -- bounce back to the detour, NOT the parent
  -- Should be empty result since upgrade+downgrade should be a no-op
  SELECT * FROM multi_extension.print_extension_changes();
  ```
  Block 1 (round-trip) proves the up+down pair is a perfect no-op. Block 2 (snapshot) shows the new
  object actually appearing at the detour version (`+ function citus_internal.distribute_object...`,
  `(1 row)`). The snapshot leaves the walk sitting at `<detour>` with the `print_extension_changes()`
  baseline `= {detour}`. Instead of adding a separate "return to parent" block, you **retarget the
  existing next round-trip** (which used to bounce `<parent> ↔ <next>`) so it bounces `<detour> ↔
  <next>` — this keeps the walk moving forward from the detour and lands it back at `<detour>` with
  baseline reset to `{detour}`.
- **The one intended downstream change:** because the walk now enters the following `-- Snapshot of
  state at <next>` block with baseline `{detour}` (not `{parent}`), that ONE snapshot shifts by
  exactly the detour's object, and nothing else changes:
  - If `<next>` does NOT contain the object (e.g. `14.0-1`, a 13.x-line object): the snapshot GAINS a
    `previous_object` row (the object shown as dropped when leaving the maintenance line).
  - If `<next>` DOES contain the object (e.g. `15.0-1` when the object is already at `14.2-1`): the
    snapshot LOSES the `current_object` row it used to show (it's no longer newly-added — it was
    already displayed at the detour snapshot).
  Both are self-consistent and desirable: the object is introduced exactly once, at its detour
  snapshot. Every block AFTER that `<next>` snapshot stays byte-identical (print resets baseline to
  `{next}` regardless). Regenerate `.out` and eyeball that only that single snapshot moved.
- **`.out` regen:** edit `.sql` → runner "fails" (writes `results/multi_extension.out`) →
  `cp results/multi_extension.out expected/multi_extension.out` → re-run → "All 1 tests passed".
  **Never hand-edit `.out`.**
- **`diff -w` header gotcha:** pg_regress compares with `diff -dU10 -w`, so snapshot-table header
  column widths / leading spaces are IGNORED for pass/fail. A regen may normalize a pre-existing
  HAND-EDITED narrow header (left over from an earlier `.out` hand-edit) to the runner's canonical
  wider spacing. Whitespace-only and harmless — **keep the canonical runner output**, note it in the
  commit message. Prove it's pre-existing: run the test on pristine HEAD (no change) — same wide
  header, still PASSES.
- **APPEND-ONLY once reviewed:** if the backport branch/PR has already been reviewed, do NOT
  force-push — add the `multi_extension` test work as a new commit on top.

## Flaky / gotcha notes for SQL backports
- **`multi_extension` flaky maintenance-daemon crash:** the issue #3409 `DROP EXTENSION` under a
  non-superuser role section can report "server process was terminated" once, then PASS on a clean
  re-run. If the PG log shows only a clean fast-shutdown (no PANIC / segfault / Assert), it's
  timing/flaky — re-run before investigating. Sanity-check by running the same test on the PRISTINE
  base branch.
- **`configure` gets regenerated by builds** — never part of a backport. `git checkout -- configure`
  before committing.
- **`git stash -u` mid-cherry-pick** drops `CHERRY_PICK_HEAD` but keeps the file changes; on restore
  you must `git commit` manually and re-add the original message + `(cherry picked from commit <sha>)`.
- **Regenerate .out with the runner, never hand-edit** — snapshot column widths and object counts
  are computed.
