# Title for the plan

## Prompt

Before responding to questions or discussion points in this
document, explore the client-python-k3s codebase thoroughly. Read
relevant source files, understand existing patterns (the
`shakenfist_client.plugin` entry point and Click command group in
`__init__.py`, the orchestration primitives and namespace-metadata
cluster state in `primitives.py`, agent operation handling and its
wait loops, the two-mode progress reporting in `progress.py`, the
release version caches), and ground your answers in what the code
actually does today. Do not speculate about the codebase when you
could read it instead. Where a question touches on external
concepts (the Shaken Fist API and in-guest agent, k3s, MetalLB,
Longhorn, helm), research as needed to give a confident answer.
Flag any uncertainty explicitly rather than guessing.

Consult `ARCHITECTURE.md` for the plugin structure, cluster
assembly flow, and agent requirements (the `sf-agent2` side
channel). Consult `AGENTS.md` for build and test commands and
project conventions. Remember that orchestration behaviour can
only be fully validated against a live Shaken Fist cluster; unit
tests mock the API surface.

Plan files live in `docs/plans/`, named `<topic>.md`, on the
branch that becomes the pull request, so that they publish with
the rest of the documentation. When we get to detailed planning, I
prefer a separate plan file per detailed phase. These separate
files should be named for the master plan, in the same directory,
and simply have `-phase-NN-descriptive` appended before the `.md`
file extension. Tracking of these sub-phases should be done via a
table like this in the master plan under the Execution section:

```
| Phase | Plan | Status |
|-------|------|--------|
| 1. Smoke tier | functional-ci-phase-01-smoke.md | Not started |
| 2. Merge tier | functional-ci-phase-02-merge.md | Not started |
| ...   | ...  | ...    |
```

I prefer one commit per logical change, and at minimum one commit
per phase. Do not batch unrelated changes into a single commit.
Each commit should be self-contained: it should build, pass tests,
and have a clear commit message explaining what changed and why.

## Situation

...

## Mission and problem statement

...

## Open questions

...

## Execution

...

## Administration and logistics

### Success criteria

We will know when this plan has been successfully implemented
because the following statements will be true:

* The code passes `tox -epy3`, `tox -eflake8` and
  `pre-commit run --all-files`.
* New code is compatible with Python >= 3.7 and the plugin still
  imports cleanly (`python3 -c 'import shakenfist_client_k3s'`) --
  a broken import takes the whole `sf-client` CLI down.
* There are unit tests for new parsing, error-handling and
  progress-reporting behaviour, in the existing testtools/stestr
  style with external APIs mocked.
* Lines are wrapped at 120 characters, single quotes for strings,
  double quotes for docstrings, no triple single quotes.
* Behaviour which can only be validated against a live Shaken
  Fist cluster has been exercised there (manually or via the
  functional CI) before merge.
* `ARCHITECTURE.md`, `README.md`, and `AGENTS.md` have been
  updated if the change adds or modifies modules or CLI commands.

### Future work

We should list obvious extensions, known issues, unrelated bugs
we encountered, and anything else we should one day do but have
chosen to defer to here so that we don't forget them.

...

### Bugs fixed during this work

This section should list any bugs we encounter during
development that we fixed. You should also scan the relevant
github bug tracker to see if there are any directly related
bugs that we should either resolve as part of this master
plan, or at least be aware of when planning. Remember that
issues for this plugin sometimes belong upstream (for example
shakenfist/shakenfist or shakenfist/agent-python); reference
cross-repository issues explicitly.

### Back brief

Before executing any step of this plan, please back brief the
operator as to your understanding of the plan and how the work
you intend to do aligns with that plan.
