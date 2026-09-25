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
`gh pr view <pr> --json state,isDraft,mergeStateStatus,baseRefName,headRefOid,title,body`

- `state == "MERGED"` → record outcome `skipped (already merged)`, move to the next PR.

### 2. Give up on drafts
- `isDraft == true` → record outcome `given up (draft)`, move to the next PR.

### 3. Sync-and-check loop
Repeat the steps below until **both** are true: the PR's `mergeStateStatus` is `CLEAN` (fully
merged up to date with its base, no conflicts) **and** every required check is passing. Re-fetch
PR state at the top of every iteration — do not act on stale data.

**a. Bring the branch up to date with its target.**
If `mergeStateStatus` is `BEHIND`, run `gh pr update-branch <pr>` (this merges the base branch
into the PR branch on GitHub's side — no local checkout needed) and loop back to re-fetch state.

If `mergeStateStatus` is `DIRTY` (real merge conflict) or `BLOCKED` for a reason you cannot
resolve with the rules below, give up: record outcome `given up (merge conflict / blocked)`,
move to the next PR.

**b. Read the required checks.**
`gh pr checks <pr> --required --json name,bucket,link`
(`bucket` is one of `pass`, `fail`, `pending`, `skipping`, `cancel`.)

- All `pass` (or `skipping`, which does not block merge) and none `pending` → sync-and-check loop
  is done, go to step 4.
- Any `pending` and nothing actionable to do → wait for up to 15mins (e.g. `gh pr checks <pr> --watch
  --required`), then loop back to re-fetch state.
- Any `fail` → handle the failing ones as follows, most specific rule first:

  1. **A check named `check-style` failed** — this loop is responsible for setting up the
     environment; the reindent skill only works on whatever is already checked out. Concretely:

     - Check out the PR's own head branch, e.g. `gh pr checkout <pr>`.
     - Record the PR's own changed files (`git diff --name-only
       $(git merge-base HEAD origin/<base-branch>)...HEAD`).
     - Load and follow the standalone
       [`citus-check-style-reindent`](../citus-check-style-reindent/SKILL.md) skill as-is. It
       reads the formatter version from the checked-out branch's own `STYLEGUIDE.md` and, if it
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

  3. **Any other required check failed** — rerun the failed check, not the whole PR. Find their
     respective run and rerun just the failed jobs in it:
     ```bash
     LINK=$(gh pr checks <pr> --json name,link -q '.[] | select(.name=="<check-name>") | .link')
     RUN_ID=$(echo "$LINK" | grep -oE 'runs/[0-9]+' | grep -oE '[0-9]+')
     gh run rerun "$RUN_ID" --failed
     ```
     Count reruns per `(head commit SHA, check name)` pair. If this exact check has already
     failed **5 times** on this exact head commit, do not rerun again — give up: record outcome
     `given up (check <name> failed 5+ times)`, move to the next PR. A new head commit (from step
     3a or 3b.1) resets the counter for that commit.

     After triggering a rerun, loop back to re-fetch state and wait for it to finish.

### 4. Squash-merge
The PR is in sync with its base and every required check is green. Merge it with the PR
description as the commit message, pinned to the exact commit you just validated:

```bash
BODY=$(gh pr view <pr> --json body -q .body)
SHA=$(gh pr view <pr> --json headRefOid -q .headRefOid)
gh pr merge <pr> --squash --body "$BODY" --match-head-commit "$SHA"
```

If `--match-head-commit` rejects the merge (head moved again in the small window between the
check and the merge), just loop back to step 3 and re-validate — do not force it.

Record outcome `merged`. Move to the next PR.

## Final report
After every PR has been processed, print one line per PR: its number/link and its outcome
(`merged`, `skipped (already merged)`, or `given up (<reason>)`). List the given-up PRs together
at the end so they're easy to scan.
