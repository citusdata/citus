---
name: citus-merge-loop
description: >-
  Take a list of citusdata/citus PR links or numbers and merge each one independently: sync it
  with its base branch, fix a red `check-style` job with `make reindent`, retry other red checks a
  bounded number of times, give up on a PR when a rule says to give up, then squash-merge with the
  PR description as the commit message. USE WHEN asked to "merge these PRs", "land this batch of
  PRs", "merge PR #1234 (and others)", or to run an unattended merge loop over several Citus PRs.
  DO NOT USE for reviewing a PR, for a single manual merge with no retry/give-up rules, or for
  non-Citus repos.
license: See the repository LICENSE file.
---

# Merge a batch of Citus PRs, one at a time

Input: one or more PR links or numbers. Process them **one at a time, independently** — finish
(merge or give up on) one PR completely before starting the next. Do not chain PRs onto each
other (do not base one PR's fix on another PR's branch).

Use the `gh` CLI for all GitHub calls. Every `gh pr ...` command accepts a PR number **or** a full
PR URL directly, so you never need to parse the input yourself: `gh pr view <input>`, `gh pr
checks <input>`, `gh pr merge <input>` all work either way.

Keep a running list of `{pr, outcome, reason}` as you go (a scratch note or a session SQL table
both work) so you can print the final report without having to re-look-up anything.

## Per-PR algorithm

For each PR, in order:

### 1. Skip already-merged PRs
`gh pr view <pr> --json state,isDraft,reviewDecision,mergeStateStatus,baseRefName,headRefOid,title,body`

- `state == "MERGED"` → record outcome `skipped (already merged)`, move to the next PR.

### 2. Give up if draft or not approved
- `isDraft == true` → record outcome `given up (draft)`, move to the next PR.
- `reviewDecision != "APPROVED"` → record outcome `given up (not approved)`, move to the next PR.
  `reviewDecision` is `APPROVED`, `CHANGES_REQUESTED`, `REVIEW_REQUIRED`, or empty (empty means the
  repo asks for no review at all). Treat every value other than `APPROVED` as not approved.

  You only need to test this once, here. Pushing to a PR does not dismiss an existing approval in
  this repo, so an approval you saw in this step is still valid later in the loop.

### 3. Sync-and-check loop
Repeat the steps below until **both** are true: the PR's `mergeStateStatus` is `CLEAN` (fully
merged up to date with its base, no conflicts) **and** every non-Codecov check is passing.
Re-fetch PR state at the top of every iteration — do not act on stale data.

**a. Bring the branch up to date with its target.**
If `mergeStateStatus` is `BEHIND`, run `gh pr update-branch <pr>` (this merges the base branch
into the PR branch on GitHub's side — no local checkout needed) and loop back to re-fetch state.

If `mergeStateStatus` is `DIRTY` (real merge conflict) or `BLOCKED` for a reason you cannot
resolve with the rules below, give up: record outcome `given up (merge conflict / blocked)`,
move to the next PR.

**b. Read every check.**
`gh pr checks <pr> --json name,bucket,link`
(`bucket` is one of `pass`, `fail`, `pending`, `skipping`, `cancel`.)

Do **not** pass `--required`. Branch protection here requires a single aggregate job named `CI`
that only mirrors the result of the jobs it depends on. `check-style` and the flakyness jobs never
appear in `--required` output, so the special-case rules below would be unreachable. Read every
check instead, so you can see and act on the individual job that actually failed.

- **Ignore checks whose name contains `codecov` (case-insensitive)**, regardless of whether their
  bucket is `pass`, `fail`, `pending`, `skipping`, or `cancel`. Codecov checks do not block
  merging in this repository.
- All non-Codecov checks are `pass` (or `skipping`, which does not block merge) and none are
  `pending` → sync-and-check loop is done, go to step 4.
- Any `pending` and nothing actionable to do → wait for up to 15mins (e.g. `gh pr checks <pr>
  --watch`), then loop back to re-fetch state. Do not wait on pending Codecov checks.
- Any non-Codecov `fail` or `cancel` → handle the failing ones as follows, most specific rule
  first:

  1. **A check named `check-style` failed** — this loop is responsible for setting up the
     environment; the reindent skill only works on whatever is already checked out. Concretely:

     - Check out the PR's own head branch, e.g. `gh pr checkout <pr>`.
     - Record the PR's own changed files, taken from the PR itself rather than from a local diff:
       ```bash
       gh pr diff <pr> --name-only > /tmp/pr-<pr>-files.txt
       ```
       Do not derive this list with `git diff ... origin/<base-branch>`. `gh pr checkout` does not
       refresh your local copy of the base branch, so a stale `origin/<base-branch>` would fold
       base-branch changes into the list and let an unrelated reindent slip through as "allowed".
     - Load and follow the standalone
       [`citus-check-style-reindent`](../citus-check-style-reindent/SKILL.md) skill as-is. It
       reads the formatter versions pinned by the checked-out branch itself and, if it
       finds a legitimate fix, stops after a **local** commit — it never pushes.
     - After the skill produces a commit, compare the reindent commit's changed files against
       the PR's own changed-files list from above. If the reindent commit touched any file the
       PR itself did not already touch, discard the commit (`git reset --hard HEAD^`) and give
       up: record outcome `given up (check-style: reindent touched unrelated files)`, move to
       the next PR.
     - Otherwise, push the local commit to the PR's head branch (`git push`) — pushing is this
       loop's decision, not the reindent skill's.

     Track how many times you have run this recipe **for this PR**. If check-style is still red
     after your **5th** attempt (whether the skill made no fix, or the fix still fails CI), give
     up: record outcome `given up (check-style)`, move to the next PR. Otherwise loop back to
     re-fetch state (the head commit has changed).

  2. **A check whose name contains "flaky" (e.g. a flakyness/flaky job) failed** — give up
     immediately, no retries: record outcome `given up (flaky job)`, move to the next PR.

  3. **Any other check failed or was canceled** — rerun the individual failed jobs, one at a time,
     each with its own counter.

     Do **not** run `gh run rerun <run-id> --failed`. That reruns every failed job in the whole
     workflow run at once, so unrelated failures get retriggered while only the one check you were
     looking at gets its counter incremented.

     Instead, for each failing check, resolve its run, look up that single job's `databaseId`, and
     rerun only that job:
     ```bash
     LINK=$(gh pr checks <pr> --json name,link -q '.[] | select(.name=="<check-name>") | .link')
     RUN_ID=$(echo "$LINK" | grep -oE 'runs/[0-9]+' | grep -oE '[0-9]+')
     JOB_ID=$(gh run view "$RUN_ID" --json jobs \
       -q '.jobs[] | select(.name=="<check-name>") | .databaseId')
     gh run rerun "$RUN_ID" --job "$JOB_ID"
     ```
     `--job` needs the job's `databaseId` from the API. The number in the job's browser URL is a
     different id and returns 404.

     Repeat that block once per failing check, so several failing checks in the same run each get
     rerun separately.

     Keep one independent counter per `(head commit SHA, check name)` pair — never a single shared
     counter. If one check has already failed **5 times** on this exact head commit, stop rerunning
     that check: record outcome `given up (check <name> failed 5+ times)`, move to the next PR. A
     new head commit (from step 3a or 3b.1) resets every counter for this PR.

     After triggering the reruns, loop back to re-fetch state and wait for them to finish.

### 4. Squash-merge
The PR is in sync with its base and every non-Codecov check is green. Merge it with the PR
description as the commit message, pinned to the exact commit you just validated:

```bash
BODY=$(gh pr view <pr> --json body -q .body)
SHA=$(gh pr view <pr> --json headRefOid -q .headRefOid)
gh pr merge <pr> --squash --body "$BODY" --match-head-commit "$SHA"
```

If `--match-head-commit` rejects the merge (head moved again in the small window between the
check and the merge), just loop back to step 3 and re-validate — do not force it.

Never record `merged` just because you issued the command. Check that `gh pr merge` exited
successfully, then confirm the PR really reached the merged state:

```bash
gh pr view <pr> --json state,mergedAt
```

- `gh pr merge` succeeded **and** `state == "MERGED"` → record outcome `merged`, move to the next
  PR.
- `gh pr merge` failed on a `--match-head-commit` mismatch → loop back to step 3 as described
  above.
- `gh pr merge` failed for any other reason (you lack merge permission on the repo, a branch
  protection rule rejected it, an API or network error), or it reported success but `state` is
  still not `MERGED` → record outcome `given up (merge failed: <error text>)`, move to the next
  PR. Do not retry blindly, and never report such a PR as merged.

## Final report
After every PR has been processed, print one line per PR: its number/link and its outcome
(`merged`, `skipped (already merged)`, or `given up (<reason>)`). List the given-up PRs together
at the end so they're easy to scan.
