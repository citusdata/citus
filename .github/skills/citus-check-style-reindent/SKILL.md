---
name: citus-check-style-reindent
description: >-
  Fix a failing citusdata/citus `check-style` job by running `make reindent` with the exact
  uncrustify/citus_indent version the currently checked-out branch expects (read from its own
  `STYLEGUIDE.md`), then inspect and commit the resulting style-only changes locally. USE WHEN
  `check-style` fails on whatever Citus branch is already checked out, or when asked to run `make
  reindent` safely. Works entirely on the current branch: it does not fetch, check out, or compare
  against any other branch. This skill deliberately DOES NOT PUSH; it only commits locally. Push
  only if the caller separately tells you to.
license: See the repository LICENSE file.
---

# Fix Citus `check-style` with the current branch's own formatter

`check-style` runs `citus_indent` (built on top of `uncrustify`) plus the smaller style tools
called by `ci/fix_style.sh`. `uncrustify` changes formatting between releases, so a formatter
version that is correct for `main` may rewrite many unrelated files on a release branch.

This skill assumes you are **already on the branch you want to fix** (checking out the right
branch is the caller's job, not this skill's). Always use the formatter version documented by
**that same checked-out branch's own `STYLEGUIDE.md`**. Never use an arbitrary `citus_indent` or
`uncrustify` already installed on the machine if its version does not match.

## 1. Read the formatter version from the current branch

Read the working copy's own `STYLEGUIDE.md` directly — no `git fetch`, no `git show`, no `gh api`,
no other branch involved:

```bash
grep -A2 'Uncrustify changes the way' STYLEGUIDE.md
```

Find the line shaped like:

```bash
curl -L https://github.com/uncrustify/uncrustify/archive/uncrustify-<VERSION>.tar.gz | tar xz
```

`<VERSION>` is the exact `uncrustify` version for this run. Follow any newer installation
instructions in `STYLEGUIDE.md` if they differ from the example below.

## 2. Build the exact formatter in a scratch location

Use a throwaway directory. Do not overwrite or trust an existing formatter installation. The
`curl` here only downloads the `uncrustify` source release tarball; it is not a remote/branch
lookup.

```bash
curl -L https://github.com/uncrustify/uncrustify/archive/uncrustify-<VERSION>.tar.gz | tar xz
cd uncrustify-uncrustify-<VERSION>
mkdir build
cd build
cmake ..
make -j"$(nproc)"
sudo make install
cd ../..

git clone https://github.com/citusdata/tools.git
cd tools
make uncrustify/.install
```

If root installation is unavailable, install to a private prefix and put that prefix on `PATH`.
The important invariant is that `make reindent` resolves the current branch's exact formatter.

## 3. Run `make reindent`

Return to the checked-out Citus worktree and run:

```bash
make reindent
```

This runs `citus_indent`, `black`, `isort`, and the other fixers in `ci/fix_style.sh`.

## 4. Diff and commit

Inspect the complete result before committing:

```bash
git status --short
git diff --check
git diff
```

Formatter-version skew is the first suspect if unrelated files or unrelated lines changed. Do not
commit a broad rewrite. Re-check `STYLEGUIDE.md` and the formatter version, restore only the
changes made by this reindent run, and rerun with the correct tooling.

If there is no diff, report that `make reindent` made no changes and do not create an empty commit.

If the diff contains only the intended style corrections:

```bash
git add -A
git commit -m "Apply make reindent"
```

Stop after the local commit. **Do not push.** Pushing (and where the branch lives, and what
happens to CI afterward) is entirely the caller's decision — only push if separately instructed to.
