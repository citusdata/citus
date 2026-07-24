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

See `.github/skills/README.md` for the skills layout and how to add a new one.
