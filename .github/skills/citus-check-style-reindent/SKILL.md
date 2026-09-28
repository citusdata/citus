---
name: citus-check-style-reindent
description: >-
  Fix a failing citusdata/citus `check-style` job by running `make reindent` with the exact
  uncrustify/citus_indent versions the currently checked-out branch pins (read from its own
  `STYLEGUIDE.md` and its own `.github/workflows/build_and_test.yml`), then inspect and commit the
  resulting style-only changes locally. USE WHEN `check-style` fails on whatever Citus branch is
  already checked out, or when asked to run `make reindent` safely. Works entirely on the current
  branch: it does not fetch, check out, or compare against any other branch. This skill
  deliberately DOES NOT PUSH; it only commits locally. Push only if the caller separately tells
  you to.
license: See the repository LICENSE file.
---

# Fix Citus `check-style` with the current branch's own formatter

`check-style` runs `citus_indent` (built on top of `uncrustify`) plus the smaller style tools
called by `ci/fix_style.sh`. `uncrustify` changes formatting between releases, so a formatter
version that is correct for `main` may rewrite many unrelated files on a release branch.

This skill assumes you are **already on the branch you want to fix** (checking out the right
branch is the caller's job, not this skill's). Always use the formatter versions that **this same
checked-out branch** pins — see step 1 for the two places they are recorded. Never use an
arbitrary `citus_indent` or `uncrustify` already installed on the machine if its version does not
match.

## 1. Read the pinned formatter versions from the current branch

`check-style` pins **two** versions and you need both: the `uncrustify` release, and the
`citusdata/tools` release that `citus_indent` itself comes from. Read both out of the working copy
you are already on — no `git fetch`, no `git show`, no `gh api`, no other branch involved.

**a. The `uncrustify` version**, from `STYLEGUIDE.md`:

```bash
grep -A2 'Uncrustify changes the way' STYLEGUIDE.md
```

Find the line shaped like:

```bash
curl -L https://github.com/uncrustify/uncrustify/archive/uncrustify-<VERSION>.tar.gz | tar xz
```

`<VERSION>` is the exact `uncrustify` version for this run. Follow any newer installation
instructions in `STYLEGUIDE.md` if they differ from the example below.

**b. The `citusdata/tools` version**, from the workflow that runs `check-style`:

```bash
grep style_checker_tools_version .github/workflows/build_and_test.yml
```

CI does not build `citus_indent` from the tools repo's moving default branch. It runs the
`ghcr.io/citusdata/stylechecker` image built at that exact tools version, so `citus_indent` is
pinned just like `uncrustify` is. A newer `citus_indent` can produce output that the pinned CI
formatter still rejects, which looks like "I reindented and check-style is still red".

`<TOOLS_VERSION>` is the value of that key (for example `0.8.33`). The matching git tag in
`citusdata/tools` is that value prefixed with `v`, so `v<TOOLS_VERSION>` (for example `v0.8.33`).

## 2. Build the exact formatter in a scratch location

Use a throwaway directory. Do not overwrite or trust an existing formatter installation. The
`curl` and the `git clone` here only download pinned third-party sources; neither is a lookup of
another Citus branch.

```bash
curl -L https://github.com/uncrustify/uncrustify/archive/uncrustify-<VERSION>.tar.gz | tar xz
cd uncrustify-uncrustify-<VERSION>
mkdir build
cd build
cmake ..
make -j"$(nproc)"
sudo make install
cd ../..

git clone --branch v<TOOLS_VERSION> --depth 1 https://github.com/citusdata/tools.git
cd tools
make uncrustify/.install
```

Clone the pinned tag, never the default branch: the default branch moves ahead of what CI runs.

If root installation is unavailable, install to a private prefix and put that prefix on `PATH`.
The important invariant is that `make reindent` resolves both of the current branch's pinned
versions: the `uncrustify` release from `STYLEGUIDE.md`, and `citus_indent` from
`citusdata/tools` at `v<TOOLS_VERSION>`.

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
commit a broad rewrite. Re-check both pins from step 1 — the `uncrustify` version in
`STYLEGUIDE.md` and `style_checker_tools_version` in `.github/workflows/build_and_test.yml` —
restore only the changes made by this reindent run, and rerun with the correct tooling.

If there is no diff, report that `make reindent` made no changes and do not create an empty commit.

If the diff contains only the intended style corrections:

```bash
git add -A
git commit -m "Apply make reindent"
```

Stop after the local commit. **Do not push.** Pushing (and where the branch lives, and what
happens to CI afterward) is entirely the caller's decision — only push if separately instructed to.
