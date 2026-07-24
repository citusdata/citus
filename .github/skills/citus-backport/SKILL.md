---
name: citus-backport
description: >-
  Backport one or more merged citusdata/citus `main` commits/PRs onto the active release
  branches. Covers the release model, cherry-picking, remapping SQL schema changes to each
  branch's real default_version, propagating the SQL upgrade/downgrade ladder for
  Major-Version-Upgrade safety, building + running the relevant regression tests, producing one
  clean commit per PR per branch, and triaging release-branch CI (separating pre-existing
  baseline-red jobs from failures the backport introduced). USE WHEN asked to backport / port /
  cherry-pick a Citus change to release-13.2 / release-14.0 (or the newest two majors), when a
  backport hits a SQL migration / udf / multi_extension conflict, when a "trivial C-only" backport
  fails to compile on an older PG the release branch still supports (e.g. PG15 `rteperminfos`),
  when a backport branch's CI is red and you must prove which reds are pre-existing vs introduced,
  or when you must prove a SQL-schema backport is Major-Version-Upgrade-safe by walking a real
  multi-node cluster across the backport branches with `ALTER EXTENSION citus UPDATE`. DO NOT USE
  FOR non-Citus repos, for authoring brand-new features, or for reviewing a PR.
license: See the repository LICENSE file.
---

# Backport a Citus change to the release branches

This skill encodes how to backport merged `main` commit(s) to Citus release branches: the release
model, the SQL-schema remap recipe, the PG-version compatibility traps, and how to triage the
release branches' baseline-red CI. Follow the repository's normal build/test/style conventions
(see `CONTRIBUTING.md` and `src/test/regress/README.md`) on top of the guidance here.

## Reference deep-dives (load on demand)
All live next to this file under `references/`:
- **`references/sql-schema-backport.md`** — the hard case: a commit that changes the SQL schema
  (new UDF / column / migration). Exact file-by-file remap recipe, the `multi_extension.out` and
  `upgrade_list_citus_objects.out` edits, the `install-downgrades` trap, and the cross-branch
  **ladder-propagation** step (copy the introduction paths UP to every higher major for MVU safety).
- **`references/ci-triage.md`** — reading backport CI: the PG-version compile traps (main drops
  old PGs the release branch keeps), the N-1 mixed-version jobs, and the step-by-step proof that a
  red job is pre-existing baseline noise vs a real regression you introduced.
- **`references/manual-upgrade-testing.md`** — the manual MVU proof: stand up a multi-node cluster
  at an OLD Citus major, then `ALTER EXTENSION citus UPDATE` across the backport branches (13.x
  tail → cross-major → 14.x tail), capturing `citus_version()` + `pg_extension.extversion` per node
  per hop. The `make install-all` / single-shared-PG requirement.

A C-only backport usually needs only this hub + `ci-triage.md`. A SQL-schema backport needs all
four (add `manual-upgrade-testing.md` as the final proof once CI is green).

## Release model (internalize this first)
- **ONE branch per MAJOR.** `release-13.2` serves the WHOLE 13.x line; `release-14.0` serves ALL of
  14.x; `main` is the next major. Citus no longer cuts a branch per minor (this changed ~2026-05).
- **Backport FEATURES, not just bugfixes**, to the **newest two majors** (13 & 14 as of this
  writing).
- **Branch names LIE about the version.** `release-13.2` currently ships 13.4; `release-14.0` ships
  14.2. **NEVER infer the target minor from the branch name** — read `default_version` from
  `src/backend/distributed/citus.control` on each target branch. That value drives every SQL edit.
- **N-1 compatibility is required and CI-gated.** A backport is N-1-safe when it is purely
  ADDITIVE (new UDF / GUC / column) and does not change an existing UDF signature or wire format
  used cross-version. If it's additive, it's safe. See `references/ci-triage.md` for the jobs.

## Workflow (per PR, per target branch)
1. **Use an isolated build per target major.** A dedicated checkout/worktree + its own PostgreSQL
   install per release branch keeps builds/tests from clobbering each other and your main checkout.
   Two worktrees of the same clone SHARE one git object store — all branches are visible/pushable
   from either, and a `+`-prefixed branch in `git branch` is checked out in a sibling worktree.
2. **Make the SHAs reachable**, then `git checkout -b bp-<pr>-<release-branch> origin/<release-branch>`
   and `git cherry-pick -x <sha>` (the `-x` records `(cherry picked from commit <sha>)`).
3. **Resolve:**
   - **C-only:** usually applies clean or with trivial context conflicts. THEN check PG-version
     compat (`references/ci-triage.md` §PG-version) — the #1 non-obvious C-backport failure.
   - **SQL-schema:** migration / udf / `multi_extension.out` conflict → remap by hand per
     `references/sql-schema-backport.md`. Do NOT just accept `main`'s next-major (e.g. `15.0-1`) files.
4. **Build in the isolated env:** `make -sj"$(nproc 2>/dev/null || sysctl -n hw.logicalcpu)" install`. For any SQL change ALSO install
   downgrades (see `references/sql-schema-backport.md` — plain `make install` skips them and
   `multi_extension` will fail with cascading "no update path" errors otherwise; use
   `make install-all`).
5. **Test only what's relevant** (CI runs the whole suite): the feature's own test + `multi_extension`
   for SQL changes. From `src/test/regress`: `python citus_tests/run_test.py <test>` (it manages its
   own cluster). Regenerate any `expected/*.out` with the runner — **never hand-edit .out** (column
   widths / row counts are dynamic).
6. **Collapse to ONE commit per PR per branch.** Keep the original message; keep a `DESCRIPTION:`
   first line iff the change is user-facing (this feeds the changelog). `git checkout -- configure`
   if a build regenerated it (configure is never part of a backport). **Multiple PRs to the SAME
   target → ONE branch per target, not per PR** (unsquashed: one commit per PR, in main's
   chronological merge order; SQL-ladder commits sit right after their PR's cherry-pick). Name it
   `bp-release-<major>` (e.g. `bp-release-13.2`). Build cleanly from the SQL-heavy PR's branch, then
   cherry-pick the C-only PRs on top. **Cherry-pick an already-adapted backport commit WITHOUT
   `-x`** — its message already carries the single `(cherry picked from <orig>)` trailer; `-x` would
   double it.
7. **Push to your fork.** `bp-*` branches don't match CI's push trigger (`main`/`release-*` only),
   so kick CI with `gh workflow run "Build & Test" --repo <your-fork>/citus --ref <branch>`. Open
   PRs only when the change owner asks.
8. **Wait for CI and triage** per `references/ci-triage.md`. Separate baseline-red noise (N-1 /
   flaky / coverage) from **genuinely-new failures you introduced**. The two you WILL hit on a
   release branch: (a) a version-pinned `.out` that prints a shared helper's changed *signature*, and
   (b) a PG-version planner divergence on the OLDER PG the branch keeps but `main` dropped (PREFER a
   PG-guarded file like `pg16.sql` over a `<test>_N.out` alt file). A third you must NOT chase green
   by mangling the test: a new GUC/UDF whose test runs under N-1 shows a NEW red in the (non-blocking)
   **N-1 mixed-version** jobs — the old lib lacks it and falls back, which IS the N-1 contract. The
   clean fix is to move that test line into the `multi_1_create_citus_schedule` placeholder section
   (schedule mechanics + N-1 label rule in the hard-rules below and `ci-triage.md §"NEW N-1 red"`).
   **Fix-commit rule (all fixes):** pre-review → amend + `git push --force-with-lease`; post-review →
   add ONE new fix commit, never rewrite reviewed SHAs (replace a bad remote fix commit with
   `--force-with-lease=<branch>:<badsha>`). Report evidence; don't declare done on red.
9. **Propagate the ladder UP (SQL-schema backports only).** A new SQL object must also be reachable
   on every HIGHER major's upgrade ladder, or a future 13.x→14.y Major Version Upgrade loses it.
   As SEPARATE commits (its own PR on main), copy the introduction step/downgrade/udf files
   byte-for-byte onto release-14.0 (for a 13.x path) and main (for 13.x and 14.x paths, given today's
   two most recent major versions); main's own top path stays untouched. Full topology + recipe in
   `references/sql-schema-backport.md` §Step 5.
10. **Add `multi_extension.sql` ladder-test coverage** for every new step. The test only walks the
    main upgrade path, so a step OFF that path (a 13.x/14.x maintenance-tail detour) is never
    exercised unless you add a detour block matching the on-walk shape (and the file's existing
    comment style — no editorial parentheticals): a round-trip no-op + a snapshot at the detour,
    then retarget the *existing* next round-trip to bounce `<detour> ↔ <next>` rather than adding a
    separate "return" block. That shifts exactly one downstream snapshot; everything after stays
    byte-identical. A branch's own top/default_version is already covered by its snapshot block.
    Never hand-edit `.out`; prefer append-only on reviewed branches. Full pattern + the `diff -w`
    header gotcha in `references/sql-schema-backport.md`.
11. **Prove MVU safety on a real cluster (SQL-schema backports, after CI is green).** The ladder
    propagation only *claims* a future 13→14 Major Version Upgrade keeps the object; prove it. Stand
    up a multi-node cluster at an OLD major, then `ALTER EXTENSION citus UPDATE` on EVERY node across
    the backport branches (13.x tail → cross-major → 14.x tail → cross-major → main tail), capturing
    `citus_version()` and `pg_extension.extversion` per node per hop, plus a functional check that the
    backported object survived. Build all Citus versions into ONE shared PG with **`make install-all`**
    (plain `install` omits the downgrade + bridge scripts the cross-major route needs → "no update path").
    Full recipe and the single-PG requirement in `references/manual-upgrade-testing.md`.

## Hard rules / traps (full detail in the references)
- **Do NOT run `make reindent` in a build environment whose `citus_indent` version differs from the
  branch's** — a version-skewed formatter reformats 100+ unrelated files and errors out. Trust
  cherry-picked formatting; CI `check-style` verifies it. If you ran it by accident:
  `git reset --hard HEAD`.
- **`make install` does NOT install downgrade scripts.** For any SQL change run BOTH
  `make -C src/backend/distributed install-downgrades` and `make -C src/backend/columnar
  install-downgrades` (columnar is a separate extension). `make install-all` does both.
- **A single build environment builds ONE PG only** — it cannot catch a compile break on another PG
  the release branch supports. Rely on CI's per-PG `Build for PGNN` jobs; scan cherry-picked C for
  struct fields / APIs newer than the branch's OLDEST PG and add `#if PG_VERSION_NUM >= PG_VERSION_NN`.
- **The release branches are BASELINE-RED.** Several N-1 / flaky jobs fail on the pristine base
  branch with no backport. Subtract the baseline before blaming your change — prove it with the
  upstream `citusdata/citus` base-branch run. Details + exact commands in `references/ci-triage.md`.
- **A new SQL object is defined MULTIPLE times on purpose.** After ladder propagation the same UDF
  appears in the 13.x, 14.x AND main introduction steps (triple-defined on main). That is
  intentional MVU/N-1 safety, NOT duplication to collapse. Never "dedupe" it. Details:
  `references/sql-schema-backport.md` §Step 5.
- **Show evidence.** Present the proof (build-job outcomes, `regression.diffs` excerpts, the
  upstream-baseline comparison), not a bare "CI is green/red".
- **A backport onto an older-PG release branch surfaces failures `main` never saw** (release-13.2
  still builds PG15; the branch ships version-pinned `expected/*.out`). Two patterns: (1) a shared
  helper whose SIGNATURE you changed prints in a pinned `.out` (`normalize.sed` masks the line
  NUMBER, not the signature) — do NOT "fix" it by reverting the helper if the backported TEST needs
  it (it's required, not over-reach); (2) a PG15-vs-PG16+ EXPLAIN/planner diff → PREFER a PG-guarded
  file (`pg16.sql` `\q`-skips <PG16 and already lives in the N-1-excluded schedule) over a
  `<test>_0.out` alt. Full recipe: `references/ci-triage.md` §Genuinely-new failures.
- **A NEW GUC/UDF whose test runs under N-1 has a CLEAN fix, not "leave it".** Move the test line
  from its schedule into the `multi_1_create_citus_schedule` placeholder section (N-1 make_targets
  omit `check-multi-1-create-citus`). N-1 version label = the branch's CURRENT N-1 = the previous
  minor (13.3-1 on release-13.2, 14.1-1 on release-14.0), read from the live `citus_version:` /
  `citus_libdir:` pin in build_and_test.yml — not the (possibly-stale) number in the neighbor comment.
  Details: `references/ci-triage.md` §"NEW N-1 red".
