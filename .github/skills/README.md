# Agent skills for citus

This directory holds **agent skills** — folders of instructions (and optionally
scripts/resources) that an AI coding agent can load on demand when it works on a
particular kind of task in this repository. Skills let us capture hard-won,
repo-specific know-how once and have every agent (and human) reuse it, instead of
re-deriving it from scratch each time.

## Layout

Each skill lives in its own subdirectory containing a `SKILL.md`:

```
.github/skills/
├── README.md                     ← this file
└── citus-backport/
    ├── SKILL.md                  ← entry point (loaded first)
    └── references/               ← deep-dive docs loaded on demand
        ├── sql-schema-backport.md
        ├── ci-triage.md
        └── manual-upgrade-testing.md
```

- Subdirectory names are lowercase with hyphens.
- The entry file **must** be named `SKILL.md`.
- Keep `SKILL.md` a concise hub; push long procedures into `references/` files it
  links to, so the agent only pulls in what it needs.

## `SKILL.md` format

A `SKILL.md` is Markdown with YAML frontmatter:

```markdown
---
name: my-skill                 # required; lowercase-hyphenated, matches the folder
description: >-                # required; WHAT it does + WHEN to use it (drives auto-selection)
  One or two sentences describing the task this skill helps with and the
  trigger conditions that should make an agent load it.
license: See the repository LICENSE file.   # optional
---

# Human-readable title

Instructions, examples, and guidelines for the agent to follow…
```

The `description` is the most important field for discovery: agents match a user's
request against it, so state both the capability and the trigger phrases plainly.

## Where these are picked up

Project skills under `.github/skills/` are discovered automatically by GitHub
Copilot CLI. (The Agent Skills format is intentionally portable; other agent tools
may read `.claude/skills/` or `.agents/skills/` instead — see each tool's docs.)

`.github/copilot-instructions.md` (auto-loaded repo-wide) also carries a short
`task → skill` index so agents are pointed here even before they browse this
directory. When you add a skill, add a row there too.

## Adding a new skill

1. Create `./<skill-name>/SKILL.md` with valid frontmatter.
2. Put anything long or optional in `./<skill-name>/references/` and link to it
   from `SKILL.md`.
3. Keep it repo-specific and evidence-based; avoid machine- or user-specific paths
   so it works for anyone who clones citus.
4. Add a `task → skill` row to `.github/copilot-instructions.md` so agents discover it.
