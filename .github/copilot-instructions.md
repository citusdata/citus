# Copilot instructions for citus

Follow the repository's normal contribution conventions: see `CONTRIBUTING.md` for
build/style/PR rules and `src/test/regress/README.md` for the regression-test workflow.

## Agent skills

This repo ships task-specific **agent skills** under `.github/skills/`. Before starting
a task that matches one, load that skill's `SKILL.md` and follow it so you don't re-derive
it. Each skill's `SKILL.md` frontmatter `description` states exactly when to use it; the
index below is a quick map.

| Task | Skill |
|------|-------|
| Backport a merged `main` commit/PR to a release branch (`release-13.2` / `release-14.0` / newest two majors) — including SQL-schema migrations, the upgrade/downgrade ladder, N-1 / Major-Version-Upgrade safety, and triaging release-branch CI | [`citus-backport`](skills/citus-backport/SKILL.md) |
| Merge a batch of Citus PRs one at a time: sync each with its base branch, fix a red `check-style` job, retry other red checks with give-up rules, then squash-merge with the PR description as the commit message | [`citus-merge-loop`](skills/citus-merge-loop/SKILL.md) |
| Fix a failing `check-style` job on whatever branch is already checked out, by rebuilding the exact `uncrustify`/`citus_indent` versions that branch itself pins (in its own `STYLEGUIDE.md` and `.github/workflows/build_and_test.yml`), then running `make reindent` and committing the style-only diff locally (no push, and no switching to or inspecting any other Citus branch) | [`citus-check-style-reindent`](skills/citus-check-style-reindent/SKILL.md) |

See `.github/skills/README.md` for the skills layout and how to add a new one.
