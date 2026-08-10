# CI triage for backports — PG-version compat + the baseline-red release branches

Two independent things make backport CI confusing: (1) `main` might support different PG majors
than a release branch, so a "trivial C-only" cherry-pick can fail to COMPILE on an old PG; and
(2) the release branches are BASELINE-RED — several jobs fail on the pristine base branch with
no backport. Handle both before declaring a red "your fault".

## PG-version compat (the #1 non-obvious C-backport failure)
`main` may support different PG majors than the release branches, so code `main` wrote unconditionally
against a newer PG API breaks the older-PG build on a release branch. Check each branch's supported
PG set in `.github/workflows/build_and_test.yml` (the `pgNN_version` params block). Today, these are the
supported PG majors by main and two most recent release branches:

| Branch        | PG majors built      |
|---------------|----------------------|
| release-13.2  | **15**, 16, 17       |
| release-14.0  | 16, 17, 18 (no 15)   |
| main          | 16, 17, 18 (dropped 15) |

**Concrete example (confirmed):** `Query.rteperminfos` was added in PG16. `main` sets
`q->rteperminfos = NIL;` bare. On release-13.2 that is `error: 'Query' has no member named
'rteperminfos'` on the **PG15** build. Fix = the citus idiom the branch ALREADY uses everywhere
else for that field:

```c
#if PG_VERSION_NUM >= PG_VERSION_16
    q->rteperminfos = NIL;
#endif
```

Grep the target branch for the same symbol to copy its exact guard macro. **General rule:** after a
C cherry-pick, scan the added `+` lines for struct fields / API calls introduced in a PG major that
the branch's OLDEST PG predates, and guard them. **Backport to the oldest-PG branch first** to
surface these early. A single build environment builds ONE PG so it can't catch this locally — rely
on CI's per-PG `Build for PGNN` jobs.

### Reading a Build-for-PGNN failure mid-run
`gh run view --log` / `--log-failed` say "still in progress" until the WHOLE run finishes, but a
single finished job's log is fetchable immediately:

```
gh api repos/<your-fork>/citus/actions/jobs/<jobId>/logs > build.log
```

Then grep for `error:` / `make.*Error`. A `Build for PGxx` failure with a real `error:` line = a
genuine bug (fix it). An "exit code N" annotation with no compiler `error:` in the log = usually
infra/flaky (rerun). After fixing, **amend the single cherry-pick commit** (keep 1 commit/PR),
`git push --force-with-lease`, and re-dispatch:
`gh workflow run "Build & Test" --repo <your-fork>/citus --ref <branch>`.

## The release branches are BASELINE-RED
`release-13.2` and `release-14.0` have KNOWN-FAILING jobs on their own pristine upstream CI. Do NOT
panic when your backport branch is red — **subtract the baseline first.** As of this writing the
base branches fail these SAME jobs with no backport applied:

- **N-1 mixed-version jobs where the OLD lib runs on the COORDINATOR:**
  - Test Citus **Lib N-1** — check-multi, check-multi-1, check-add-backup-node
  - Test Citus **Coordinator N-1** — check-multi, check-multi-1, check-add-backup-node
  - (The SQL N-1 and Worker N-1 variants of the same tests PASS — old lib is only on workers there.)
- Occasional **Test flakyness (N)** shards and a stray check-enterprise / check-operations.
- **Test Citus Failure / check-query-generator** — a randomized differential fuzzer (compares
  distributed vs local results of random nested-CTE/join queries). Flaky by nature; a random query
  can trip `citus.max_intermediate_result_size` etc. **Tell:** one PG major fails while another PG
  major of the SAME commit passes. Not a backport regression.

**Root cause of the N-1 reds (not yours):** these base branches already carry in-flight
minor-targeted features (e.g. clone-promotion sequence *re-ranging*;
`citus_internal.delete_placement_metadata` behavior) whose NEW behavior lives only in the NEW lib.
When the OLD lib (the previous minor's binary) is loaded on the coordinator, the expected `.out`
(written for the new lib) mismatches → diff. Inherent to N-1 testing of in-flight features;
unrelated to a feature backport that doesn't touch those areas.

## How to PROVE a red is pre-existing (do this — gather the evidence)
1. **Upstream base-branch run is itself failing:**
   ```
   gh run list --repo citusdata/citus --branch release-13.2 --workflow "Build & Test" --limit 6
   ```
   Note the base-branch runs are themselves `failure`.
2. **Your failing set ⊆ upstream's failing set:**
   ```
   gh run view <upstreamRunId> --repo citusdata/citus --json jobs \
     --jq '.jobs[]|select(.conclusion=="failure")|.name'
   ```
   If your branch's failing job names are a subset, they're baseline, not yours.
3. **The diff cites a pre-existing feature, not your object:** download your failed job's artifact
   and read `regression.diffs`:
   ```
   gh run download <runId> --repo <your-fork>/citus -n "<artifactName>" -D <dir>
   ```
   Artifact naming: `<pgmajor>_<test>_[<citus_version>_]<libLabel>_<mode>`
   (e.g. `18_check-enterprise_14.1-1_all` = SQL N-1; `..._v14.1.0_coordinatoronly` = Coordinator
   N-1). Confirm the diff mentions the pre-existing feature (`re-ranged N sequence(s)`,
   `delete_placement_metadata`) and has **ZERO** mentions of your backported symbol.
4. **Best control:** run CI on your C-ONLY sibling backport (touches none of the SQL/dependency
   code). If it shows the IDENTICAL N-1 reds, that confirms the reds track the base branch, not the
   change.

## What WOULD be your fault (fix these)
- A red job whose `regression.diffs` names YOUR backported object **in a NORMAL (same-version)
  job** — check-multi/-mx/-1 on a supported PG. (See the N-1 handling immediately below.)
- A `Build for PGxx` with a real `error:` in its log.
- A NEW job red that is GREEN on the upstream base branch. If it's a NEW-GUC/UDF N-1 red, the fix is
  the schedule move below (not a test/output change); everything else, fix at the source.

Everything else on these branches is baseline noise.

## The NEW N-1 red a feature backport ADDS — move it to the N-1-excluded schedule
Backporting a feature that adds a **new GUC or UDF** whose regression test runs under an N-1
schedule (`multi_schedule` / `multi_1_schedule` → `check-multi`/`check-multi-1`) makes the **N-1
(mixed-version)** jobs show that test as a NEW red. The red itself is EXPECTED graceful-degradation
(not a defect), but the CLEAN fix is to relocate the test so N-1 never runs it — do NOT mangle the
reviewed test, fabricate output, or just "leave + document". Proven on both the 13.2 and 14.0
backports of the `citus.allow_unsafe_insert_select_pushdown` GUC:
- Test `allow_unsafe_insert_select_pushdown` (backported GUC `citus.allow_unsafe_insert_select_pushdown`).
- Under N-1 the OLD lib runs on the coordinator (`lib-v13.3.0` / `lib-v14.1.0`), which lacks the GUC.
  `SET citus.allow_unsafe_insert_select_pushdown` → `ERROR: invalid configuration parameter name ...
  "citus" is a reserved prefix`, then the EXPLAIN falls back to the DEFAULT (pull-to-coordinator) plan.
- **That fallback IS the N-1 contract** (old binary → GUC absent → safe pre-feature behavior); the diff
  *demonstrates* compatibility. But a red CI job is still noise, so move the test off the N-1 matrix.

**THE FIX: move the test line from its schedule into the placeholder section of
`multi_1_create_citus_schedule`.** The N-1 jobs' `make_targets` (build_and_test.yml Coordinator/Lib
N-1 blocks) list `check-multi`, `check-multi-1`, `check-add-backup-node`, ... but **NOT
`check-multi-1-create-citus`**, so any test in `multi_1_create_citus_schedule` is N-1-invisible while
STILL running under `check-multi-1-create-citus` on every normal PG job (coverage preserved). Steps:
1. Remove `test: <name>` from `multi_schedule` (mind the surrounding blank lines / parallel-group
   comments — don't strand a comment that referenced it).
2. Append a block under the placeholder header in `multi_1_create_citus_schedule`
   (`# Have this placeholder section at the end for the tests that need to be moved back ... to
   prevent N-1 test failures for the features that are not present in the minor version.`), mirroring
   the EXISTING `citus_cluster_changes_block` block right there — same `# ----------` fencing and
   phrasing so new additions look like the existing file.
3. `run_test.py` auto-discovers the new schedule (it prints `SCHEDULE: multi_1_create_citus_schedule`);
   the `.out` file is schedule-independent, so NO `.out` regen is needed — it's a pure schedule move.

**N-1 version label = the actual N-1 CI version = the PREVIOUS MINOR (it advances every release).**
Read the authoritative number from `build_and_test.yml`'s N-1 jobs — the `citus_version:` (SQL /
Worker / Coordinator N-1) and `citus_libdir:` (Lib / Coordinator N-1) pins, not from
maybe-stale comments at `multi_1_create_citus_schedule`. As of this writing: release-13.2 pins
`citus_version: "13.3-1"` / `citus_libdir: v13.3.0` (its `default_version` is 13.4-1, so N-1 = 13.3-1);
release-14.0 pins `14.1-1` / `v14.1.0` (default 14.2-1, N-1 = 14.1-1). The rule is simply
`default_version` minus one minor, and it is bumped whenever the branch ships a new minor.

- **Write the label with today's N-1 version.** For a GUC introduced in the branch's current
  `default_version` (e.g. 13.4 on release-13.2), the N-1 binary is the previous minor (13.3-1) and the
  GUC is absent there, so: `GUC introduced in 13.4; not present in the N-1 (13.3-1) Citus binary ...
  Move back to multi_schedule at Citus <next major, 14>.` On release-14.0 the corresponding numbers
  are 14.2 / N-1 14.1-1 / next major 15.
- **Sanity gate:** the move is only warranted when the object is genuinely ABSENT in that N-1 version
  (introduced in a minor strictly newer than N-1). If your object was introduced at or below N-1, the
  N-1 binary already has it — no red, no move.

- **Why a new UDF may NOT need this move** (asymmetry): a test already placed in
  `multi_1_create_citus_schedule` (e.g. `citus_internal_distribute_object`) is ALREADY N-1-invisible,
  so it never shows an N-1 red and needs no move. Grep which schedule your test is in first.
- Distinguish this N-1 red from a genuine bug: a same-version job (no `lib-vX` / `_coordinatoronly` /
  `_workeronly` / `_all` suffix on the artifact) naming your object IS yours — see the section above.
- Shipping: fold the move into a not-yet-reviewed fix commit (amend + `--force-with-lease`), or add a
  NEW appended commit if the tip has already been reviewed (plain push, no force).

## Genuinely-new failures a backport WILL introduce on a release branch (and the fix)
The release branch supports an OLDER PG than `main` (e.g. release-13.2 still builds **PG15**), and it
ships **version-pinned `expected/*.out`** files. A backport that is green on `main` can therefore go
red on the release branch for two reasons that ARE yours to fix (they are not baseline noise):

### (a) A shared helper's SIGNATURE changed → version-pinned .out that prints the signature mismatches
If your backport chain brings a new/edited SQL helper whose *signature* changed (e.g.
`explain_filter(text)` → `explain_filter(text, keep_numbers boolean DEFAULT false)`), any expected
`.out` that prints that signature in a `CONTEXT:` / `PL/pgSQL function ... line N` / error line will
mismatch. **`normalize.sed` normalizes only the line NUMBER (`line [0-9]+` → `line XX`), NOT the
signature text**, so the signature diff is real. Fix = regenerate/`sed`-update those signature
strings in the pinned `.out`. Find them all with `grep -rl "helper_name(oldargs)" expected/`.
- **Do NOT mistake a required helper for "over-reach" and revert it.** If the backported *test*
  calls the new form (`explain_filter($$...$$, true)`), the helper is REQUIRED; reverting it yields
  `ERROR: function ...(unknown, boolean) does not exist`. Before deleting an "extra" hunk a
  cherry-pick carried, check whether the backported test depends on it.

### (b) PG-version planner divergence on the older PG the release branch still supports
The same EXPLAIN can differ by PG major. Real example: PG15 omits a functionally-redundant trailing
sort column that PG16+ keeps — `Sort Key: q.batch` (PG15) vs `Sort Key: q.batch, q.text_id` (PG16+).
release-13.2 hits it on the PG15 `check-multi`; main/release-14.0 never see it (no PG15).
Fix, in the order the repo prefers:
1. **An explain helper from `main`'s `multi_test_helpers.sql`** — check whether one already
   normalizes the divergent bit. As of 13.x/14.x none normalizes Sort Key (there are
   `coordinator_plan`, `plan_normalize_memory`, `explain_with_pg16_subplan_format`,
   `explain_with_pg17_initplan_format`, and boolean `explain_has_distributed_subplan` /
   `explain_has_single_task` — the booleans would need an invasive test rewrite).
2. **MOVE the diverging blocks into a PG-version-guarded test file (PREFERRED on release-13.2 when
   the divergence is PG15-only).** release-13.2 ships `sql/pg16.sql`, which `\q`-skips below PG16
   (guard at top: `SELECT substring(:'server_version','\d+')::int >= 16 ... \if ... \else \q \endif`)
   AND already lives in `multi_1_create_citus_schedule` (the N-1-excluded schedule). So relocating the
   PG15-diverging EXPLAIN cases out of the normal test (e.g. `allow_unsafe_insert_select_pushdown.sql`)
   into a self-contained section appended to `pg16.sql` means: PG15 never runs them (no diff),
   PG16/17/18 still cover them, and — bonus — they leave the N-1 matrix too. Recipe:
   - Cut the exact diverging blocks from the source `.sql`; fix any dangling comment they leave behind.
   - Append AFTER `pg16.sql`'s final `DROP SCHEMA pg16 CASCADE;` a self-contained section: its OWN
     schema (e.g. `allow_unsafe_insert_select_pushdown_pg16`), a distinct `next_shard_id`
     (e.g. 14100000) to avoid shard-id clashes, recreate the tables/functions the blocks need, the
     moved EXPLAIN blocks, then `DROP SCHEMA ... CASCADE;`. It must not depend on the parent test's
     schema (schedules run independently).
   - Regenerate BOTH `.out`s (source shrinks, `pg16.out` grows) via the runner; never hand-edit.
3. Add a new **`normalize.sed` rule** or update an existing one to generalize the coverage, or add an
   alternative expected file as a last resort.

### (c) Flaky detector goes all-red because your changed-file set is widely depended-on
"Test flakyness (1..32)" computes a changed-test set by DEPENDENCY EXPANSION of the PR's changed
files. Editing a widely-depended helper/migration (`multi_test_helpers.sql`, a migration `.sql`,
`multi_extension.sql`) balloons the set across every shard and drags in inherently-flaky base tests
(`start_stop_metadata_sync`, `multi_metadata_sync`, ...). The shards only settle once your
*actually-changed* tests pass; the base-flaky tests pulled in are not yours to fix. Sanity check with
`gh pr view <n> --json files` — a helper appearing in the flaky `tests=` list but NOT in the PR's
file list is pure dependency-expansion, not a change you made.

**Do NOT hand-wave "it's flaky".** Two pieces of hard evidence, both cheap:
1. **Read the actual diff** from the shard's uploaded artifact (they DO upload one, named
   `test_flakyness_parallel_<N>`; see fetch recipe below). Confirm the failing test is a base test
   you did not touch AND the diff contains ZERO of your backported symbols. The signature of
   `--use-whole-schedule-line` running the whole schedule line in parallel so other tests consume
   colocation IDs first. Scan: `grep -c <your_symbol> regression.diffs` (want 0),
   `grep -oE "expected/[a-z_0-9]+[.]out"` (only the base test).
2. **Reproduce on the PRISTINE base** to prove pre-existing, not introduced. From `src/test/regress`:
   `python citus_tests/run_test.py <flaky_test> --repeat 8 --use-whole-schedule-line` on your branch
   head, then `git checkout <merge-base> && make -sj"$(nproc 2>/dev/null || sysctl -n hw.logicalcpu)" install` and run the SAME command. If the
   base fails the identical N/N under the identical flags, it's 100% pre-existing (checkout your
   branch + rebuild to restore afterward). Example proven this way: `start_stop_metadata_sync` failed
   8/8 on both `bp-release-13.2` tip and its pristine base, same colocationid/FK diff — not the
   backport's fault. NOTE the flaky detector's PG major varies — check the shard log's `PG_MAJOR:`
   line and reproduce on the SAME PG for an exact comparison.

### Shipping the fix
One NEW fix commit on top of the reviewed tip (do not rewrite reviewed SHAs). If a prior *bad* fix
commit is already on the remote, REPLACE it with `git push --force-with-lease=<branch>:<badsha>`
(the reviewed commits below it stay). Force-push freely only while the commits are still unreviewed.

## Fetching CI diffs reliably (tooling gotchas)
- `gh run view --job <id> --log-failed` streams often die with `stream error: stream ID 1; CANCEL` —
  retry or avoid.
- Jobs that upload artifacts (`NN_check-multi`, `NN_check-operations`, AND the flaky shards):
  list with a SINGLE page, not `--paginate` (`--paginate` concatenates JSON pages so a single `--jq`
  chokes with "Additional text after finished reading JSON content"):
  `gh api "repos/citusdata/citus/actions/runs/<run>/artifacts?per_page=100" --jq '.artifacts[]|select(.name=="15_check-multi").id'`
  then `gh api .../actions/artifacts/<id>/zip > x.zip` (this redirect is fine for the small per-artifact
  zip; read `regression.diffs` from it).
- Flaky-detector jobs DO upload a named artifact `test_flakyness_parallel_<N>` (contains the shard's
  `regression.diffs` under `__w/citus/citus/src/test/regress/`) — fetch it like the per-artifact zip
  above; that's the reliable way to read the flaky diff body. The per-job logs endpoint only NAMES the
  failing test (`SCHEDULE_LINE:` / `not ok`) and points at regression.diffs in the artifact.
- Do NOT re-encode a downloaded zip through a shell pipe that changes encoding (corrupts it); write it
  to disk directly. The whole-run `/logs` zip often arrives partial ("End of Central Directory record
  could not be found") — prefer the per-job / per-artifact endpoints above.

## Mechanics / gotchas
- Overall run status stays `queued`/`in_progress` until EVERY matrix job ends; per-job logs and
  finished-job artifacts are still fetchable mid-run (above).
- Dispatching several full matrix runs (~190 jobs each) saturates the fork runner pool and starves
  the job you actually care about; `gh run cancel <id>` the fully-triaged runs to free capacity for
  a critical re-verification.
