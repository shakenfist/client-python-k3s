# Cumulative health signals: what the health verb cannot see

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

<!-- shared-block: plan-file-conventions v1 -->
Plan file conventions (shared block; do not edit -- the canonical
copy lives in shakenfist/development at
`templates/shared-blocks/plan-file-conventions.md`):

- All planning documents live in `docs/plans/`.
- Detailed planning gets one plan file per phase. Phase files are
  named for their master plan, sit in the same directory as it,
  and append `-phase-NN-descriptive` before the `.md` extension.
- The master plan tracks its phases in a table under its Execution
  section:

  | Phase | Plan | Status |
  |-------|------|--------|
  | 1. Schema migration | PLAN-thing-phase-01-schema.md | Not started |
  | 2. Public API | PLAN-thing-phase-02-api.md | Not started |

- One commit per logical change, and at minimum one commit per
  phase. Unrelated changes are not batched into a single commit.
  Each commit is self-contained: it builds, passes tests, and has
  a message explaining what changed and why.
<!-- shared-block-end -->

## Situation

This began as a placeholder recording a real problem and the evidence
for it. Its phases are planned one at a time; open questions 1 to 3
are answered by [phase 1's plan](PLAN-cumulative-health-signals-phase-01-agent-signals.md), and the
others are answered below.

`Cluster.health()` in `cluster.py` answers one question, and
answers it well: are this cluster's node instances up, and is the k3s
API responding. Per decision 7 of
[phase 3 of the library API plan][p3]
it returns structured data rather than text so that an Ansible module
can branch on it, and it deliberately repairs nothing, on the grounds
that a verb which silently fixes things cannot be used to decide
whether to fix things. `_node_health()` builds each
node's entry: `uuid`, `role`, `name`, `exists`, `state`,
`agent_state`, `healthy`.

Every one of those is a reading of *current* state, and that is the
gap. On 2026-10-03, while health testing the cluster 33fl's
`docs/plans/PLAN-k3s-ci-runners.md` intends to use,
`sf-client k3s health` reported a cluster healthy
minutes after its control plane node had been driven into global OOM,
had k3s restarted under it by systemd, and had spent about thirty
seconds refusing API connections. Nothing in the report was wrong.
Every node was up and the API was answering again by the time it was
asked. The verb simply has no way to say "and it was broken an hour
ago".

The signals that would have caught it are all cumulative -- a counter,
or a log entry -- and all cheap to read through the `sf-agent2` side
channel the plugin already uses for everything else:

- **`systemctl show k3s -p NRestarts`.** The single best signal. (On
  workers the unit is `k3s-agent`; see phase 1's survey finding 2.) It
  read 5 and then 7 across that testing while
  `systemctl show k3s -p ActiveState` said `active` throughout.
- **The kernel's OOM kill count.** Not from `dmesg`: during the same
  testing the `dmesg` count went *down* from 2 to 1 as the ring buffer
  wrapped, and a counter that can decrease is not a counter. This
  section used to say `journalctl -k` instead, which shares the flaw
  over a longer timescale; phase 1 reads the kernel's own counter,
  `oom_kill` in `/proc/vmstat` (its survey finding 3).
- **`MemAvailable` from `/proc/meminfo`.** Warning before the cliff
  rather than forensics after it.
- **The etcd data directory size.** 383 MB of data plus 80 MB of
  snapshots on a 2 GB node, growing, watched by nothing.
- **`lastState.terminated.reason == OOMKilled` on pods.** Via the API
  rather than the agent. Machine readable, timestamped, and it
  distinguishes real damage from the first-boot ordering races every
  cluster carries -- a distinction a plain restart count cannot make,
  and which 33fl's `tools/k3s-health-check.py` currently has to
  approximate with a pod age heuristic.

A second example from the same cluster, found incidentally:
`metallb-controller` had been logging `AdditionalAssignFailed` every
few minutes -- 184 occurrences over 25 days -- because a Service asked
for dual-stack against an IPv4-only pool. Harmless, and invisible to
every reading of current state.

[p3]: PLAN-library-api-and-collection-phase-03-missing-verbs.md

## Mission and problem statement

Let `health()` report what has happened to a cluster since it was last
looked at, not only what is true at the instant it is asked, so that a
daily poll can distinguish "fine" from "fine right now".

**Explicit non-goal: this plan does not automate recovery.** The
"repairs nothing" contract is the reason the verb is usable as an
input to a decision, and it should survive. Recovery is a separate
verb and, on current evidence, a separate plan -- see open question 4,
because the two node roles are not symmetrical and the control plane
has no recovery path at all today.

## Open questions

Each question carries its answer, or says where to find it.

1. **Where do cumulative signals live in the returned structure?**
   Per-node alongside `agent_state`, or under a new top-level key? The
   return value is a documented contract that phase 5's Ansible module
   branches on, so adding keys is cheaper than moving existing ones,
   but the shape should be decided once rather than grown.
   **Answered** by [phase 1 decision 1](PLAN-cumulative-health-signals-phase-01-agent-signals.md): per node, under a
   `signals` key with a fixed set of keys in every outcome.
2. **How is "since when" expressed?** This is the sharpest design
   question. A counter is only meaningful against a previous reading,
   so either the caller stores the baseline and the verb reports raw
   values, or the plugin stores last-seen values in cluster metadata
   and reports deltas. The second is friendlier and makes `health()`
   *write*, which collides with it being the read-only verb you reach
   for when you do not trust the cluster. Recommendation to be tested
   during planning: report raw cumulative values and let the caller
   diff, keeping the verb read-only. **Answered** by
   [phase 1 decision 2](PLAN-cumulative-health-signals-phase-01-agent-signals.md): as recommended, with the boot id
   reported so that a caller can tell when its baseline is void.
3. **Does `health()` gain opinions, or only facts?** `MemAvailable` is
   a number; "this node is about to OOM" is a judgement with a
   threshold in it. A verb that reports facts stays useful as
   workloads change; a verb with thresholds baked in starts lying when
   they are wrong. **Answered** by [phase 1 decision 3](PLAN-cumulative-health-signals-phase-01-agent-signals.md): facts
   only, and no signal changes `healthy`.
4. **What is the recovery story, and whose plan is it?** The roles are
   asymmetrical. A worker is already replaceable with `remove-worker`
   plus `expand-workers`, and 33fl's phase 4 conductor scale loop
   would do that as a matter of course. A control plane node is not
   replaceable at all: there is one etcd member and no verb to replace
   it, so `--control-plane-count 3` for quorum may be the whole
   answer. Decide whether that belongs here, in its own plan, or in
   neither. **Answered** by [phase 1 decision 10](PLAN-cumulative-health-signals-phase-01-agent-signals.md): its own plan,
   proposed once this plan's signals show how often it is needed; see
   Future work.
5. **Is any of this worth doing before the cluster it was found on is
   rebuilt?** The evidence above came from nodes of the old hardcoded
   2 vCPU / 2048 MB size. Once
   [node customisation](PLAN-node-customisation.md) lands and clusters are
   built at sane sizes, control plane OOM should become rare. Rare is
   not never, and a silent failure that happens rarely is worse than
   one that happens often, but it is a fair question whether this is
   next or much later. **Answered** by scheduling: node customisation
   landed (its push audit merged as `4a6f38e`), and the operator chose
   to start this plan next.

## Execution

Phases are planned one at a time, each in its own file, and each
corrects the rows after it when its survey finds them wrong. The push
audit row is mandatory.

| Phase | Plan | Status | Merged |
|-------|------|--------|--------|
| 1. Agent-read signals | [PLAN-cumulative-health-signals-phase-01-agent-signals.md](PLAN-cumulative-health-signals-phase-01-agent-signals.md) -- `NRestarts` from `k3s` or `k3s-agent` by role, the `/proc/vmstat` OOM kill count, `MemTotal` and `MemAvailable`, the boot id and boot time, and the etcd data and snapshot directory sizes on control plane nodes, read by one agent operation per healthy node under the existing probe budget and reported raw under each node's `signals`. `healthy` is unchanged. Answers open questions 1, 2 and 3. (This row used to read the OOM count from `journalctl -k`.) | Complete | 78c9df1 |
| 2. API-read signals | `OOMKilled` terminations, and whatever else the API exposes that current state discards, placed per node where they belong to a node (phase 1 decision 1). Includes the node `Ready` condition from [#76](https://github.com/shakenfist/client-python-k3s/issues/76), which is current state rather than history and is the one reading that may reasonably change `healthy` -- decide that here | Proposed | |
| 3. Live validation | Provoke each signal on a throwaway cluster and assert the verb reports it, in `tools/ci_deploy_test.sh` or by hand. 33fl's `tools/k3s-health-check.py` tier 3 is a ready-made way to provoke control plane OOM. Also confirm two things phase 1 took from source rather than observation: that restarting k3s by hand resets `NRestarts`, and that a pod exceeding its own memory limit increments `oom_kill` | Proposed | |
| 4. Push audit | Run `PUSH-AUDIT.md` over the accumulated diff of phases 1-3 against `develop` | Proposed | |

<!-- shared-block: plan-push-audit-phase v3 -->
Push audit phase (shared block; do not edit -- the canonical
copy lives in shakenfist/development at
`templates/shared-blocks/plan-push-audit-phase.md`):

- Every master plan ends with a phase that runs the repository's
  `PUSH-AUDIT.md` over the whole plan's work. It is the last row of
  the Execution table and it is not optional. The rule binds every
  plan that carries the phase, which is decidable from the plan file
  alone: a plan that is already `Complete`, `Abandoned` or
  `Superseded` and does not carry the phase is not reopened to
  acquire one, and a plan that has the phase runs it even if it
  reaches `Complete` before the phase does.
- That phase audits the accumulated diff of every phase in the plan
  against the default branch, not the diff of the last phase alone.
  Auditing one phase at a time would miss what the phases did to
  each other -- the duplicated helper that only exists once phases
  three and six have both landed, the doc page that phase two made
  wrong and phase five never revisited.
- Once the plan's phases have merged, a diff against the default
  branch is empty and would read as a clean audit. The range is not
  reliably derivable after the fact either: unrelated work lands on
  the default branch between phases, so anything anchored on "since
  the plan file appeared" is far too wide. It has to be recorded. As
  each phase lands, what put it on the default branch goes into the
  plan: the merge commit of its pull request, whose diff against its
  first parent is the whole of what landed, or -- where the phase
  landed directly -- every commit of the phase, or its `first..last`
  range. A single commit is only ever enough when it is a merge
  commit.
- Where the Execution phases are a table, that record is a `Merged`
  column, added last so that a row which omits it still reaches
  `Status`; where they are prose sections it is a `Merged:` line in
  the phase's own section. The `Status` column keeps its single
  vocabulary term and nothing else (see `plan-status-vocabulary`).
  A phase that landed in another repository records `<repo> <sha>
  (#pr)` and is audited against that repository's default branch, as
  part of the pull request that lands it; the plan's own push-audit
  phase cites that audit rather than re-running it.
- Phases that landed before the plan started recording them are
  reconstructed rather than left blank. Recover what you can from
  `gh pr list --state merged` and `git rev-list --first-parent`, and
  say in the plan that the range was reconstructed. Do not trust a
  path-filtered `git log` on its own: it lists the commits that
  touched a path without saying which arrived directly and which
  arrived inside a pull request, and recording a commit that came in
  under a merge audits one commit of that pull request rather than
  the pull request. A reconstructed record may be a summary table in
  the audit phase's own section rather than a column or a line in
  the Execution table, which keeps retrospective archaeology out of
  a table that tracks live status. Where a phase accreted over
  months of unrelated commits and no range is recoverable, say that
  instead and name the paths the audit read -- an audit that says
  what it could not scope is a result; one that silently audits
  nothing is not.
- Findings land as their own pull request against the default
  branch, and the plan is not complete until they are resolved or
  explicitly declined in writing. A finding that is declined says
  why, in the plan, where the next reader will find it.
- Where the audit finds nothing, record that in the plan in one
  sentence. It is a real result, and a run of them is the evidence
  for making the phase conditional rather than mandatory.
- A repository with no `PUSH-AUDIT.md` still carries the phase, and
  the phase says that the runbook does not exist yet and what was
  done instead. Silently omitting it is what let the audit go
  untriggered for as long as it did.
<!-- shared-block-end -->

!!! note "In this project"

    The Execution phases are a table, so the record goes in a
    `Merged` column after `Status`. Phases here land as pull
    requests against `develop`, so the cell normally holds the
    single merge commit of that pull request.


<!-- shared-block: plan-status-vocabulary v1 -->
Plan status vocabulary (shared block; do not edit -- the canonical
copy lives in shakenfist/development at
`templates/shared-blocks/plan-status-vocabulary.md`):

A status cell -- in the master plan's own Execution phase table, and
in the row `docs/plans/index.md` carries for the plan -- holds
exactly one of these terms and nothing else:

- `Proposed` -- written down as a concept, not yet scheduled.
- `Not started` -- scheduled, but no work has begun.
- `In progress` -- work has begun and has not finished.
- `Blocked` -- cannot proceed until something outside the plan
  changes. Say what, in the plan.
- `Complete` -- the work is done.
- `Abandoned` -- deliberately dropped without being done.
- `Superseded` -- replaced by another plan, which the plan names.

The term is the whole cell. No dates, no phase arithmetic, no
parenthetical qualifiers, no summary of what happened: a status is
read to decide whether a plan still wants attention, and prose in
that column has repeatedly grown until it could no longer be read
either by a person scanning the table or by tooling. Detail belongs
in the plan file, and a one-line summary belongs in the index's own
Intent column.

Matching is case-insensitive, so `In Progress` is accepted, but the
spelling above is the one to write.
<!-- shared-block-end -->

## Agent guidance

### Execution model

<!-- shared-block: subagent-execution-model v1 -->
Sub-agent execution model (shared block; do not edit -- the
canonical copy lives in shakenfist/development at
`templates/shared-blocks/subagent-execution-model.md`):

All implementation work is done by sub-agents, never in the
management session. The management session is reserved for
planning, review, and decision-making. This keeps the management
context lean and avoids drowning it in implementation diffs.

The workflow is:

1. **Plan** at high effort in the management session.
2. **Spawn a sub-agent** for each implementation step with the
   brief from the plan, at the recommended effort level and model.
3. **Review** the sub-agent's output in the management session.
   Check the actual files -- the sub-agent's summary describes
   what it intended, not necessarily what it did.
4. **Fix or retry** if the output is wrong. Diagnose whether the
   brief was insufficient (improve it) or the model was too light
   (upgrade it), then re-run.
5. **Commit** once the management session is satisfied.

This applies to all steps, including high-effort ones. If a
sub-agent cannot succeed even with a detailed brief and the right
model, that is a signal the brief needs improving, not that the
management session should do the implementation itself.

Use `isolation: "worktree"` for sub-agents when the change is
risky or experimental; the worktree is discarded if the output is
unsatisfactory. For safe, well-understood changes, sub-agents can
work directly in the main tree.
<!-- shared-block-end -->

### Planning effort

<!-- shared-block: plan-planning-effort v1 -->
Planning effort (shared block; do not edit -- the canonical copy
lives in shakenfist/development at
`templates/shared-blocks/plan-planning-effort.md`):

The master plan itself is always created at **high effort** -- it
requires broad codebase understanding, cross-referencing several
source files, and judgment calls about scope and sequencing.

Each phase plan states the recommended effort level for planning
that phase. Phases that turn on design decisions, cross-component
coordination, protocol changes, or subtle correctness questions
should be planned at high effort. Phases that are mechanical, or
that follow a pattern already established elsewhere in the
codebase, can be planned at medium effort.
<!-- shared-block-end -->

!!! note "In this project"

    Phases involving cluster assembly ordering, agent operation
    wait loops, or the namespace-metadata representation of
    cluster state should be planned at high effort -- these are
    the places where a mistake only shows up against a live
    Shaken Fist cluster. Phases that mirror an already
    established pattern -- adding a Click subcommand alongside
    an existing one, for example -- can be planned at medium
    effort.

### Step-level guidance

<!-- shared-block: subagent-step-guidance v1 -->
Sub-agent step guidance (shared block; do not edit -- the
canonical copy lives in shakenfist/development at
`templates/shared-blocks/subagent-step-guidance.md`):

Each phase plan includes a table like this:

| Step | Effort | Model | Isolation | Brief for sub-agent |
|------|--------|-------|-----------|---------------------|
| 1a | medium | sonnet | none | One-sentence summary of what to do and which files to touch |
| 1b | high | opus | worktree | Why this needs high effort: requires understanding X to do Y |

**Effort levels**, from cheapest to most thorough:

- **low** -- Purely mechanical changes: rename, reformat, add a
  log line, regenerate generated code. The brief is a complete
  instruction.
- **medium** -- The plan provides enough context to follow a clear
  brief. The sub-agent may read a few files, but the approach is
  already decided.
- **high** -- Requires reading several files, making judgment
  calls, or understanding non-obvious invariants. The sub-agent
  needs to think about edge cases.
- **xhigh** -- The setting for hard coding and agentic steps:
  long-horizon changes, or steps where the sub-agent must both
  research and implement.
- **max** -- Correctness matters more than cost. Expect
  diminishing returns and occasional overthinking; reserve it for
  steps where a wrong answer would be expensive to detect.

**Brief for sub-agent:** this is the key field. Write it as if
briefing a colleague who has never seen the codebase. Include what
to change, which files to touch, what patterns to follow, and any
non-obvious constraints.

A good brief front-loads the research the planner already did, so
the implementing agent does not repeat it. Instead of "add storage
functions for the new object", name the functions to add, the file
they belong in, the existing equivalent to mirror (with line
numbers), and any registration the change also needs.

The better the brief, the lower the effort level needed and the
lighter the model that can succeed.
<!-- shared-block-end -->

!!! note "In this project"

    A brief should say explicitly whether a step can be verified
    by unit tests alone or needs a live cluster, because the
    sub-agent cannot reach one and should not pretend otherwise.

    A worked brief for this codebase: instead of "improve
    progress reporting for node creation", write "extend the
    progress reporter in `shakenfist_client_k3s/progress.py` to
    emit a per-node phase label, keeping both existing output
    modes working. Call it from the node creation path in
    `shakenfist_client_k3s/primitives.py`. Add coverage to
    `shakenfist_client_k3s/tests/test_progress.py` in the
    existing testtools/stestr style, with the Shaken Fist API
    mocked."

### Model choice

<!-- shared-block: subagent-model-roster v1 -->
Sub-agent model roster (shared block; do not edit -- the canonical
copy lives in shakenfist/development at
`templates/shared-blocks/subagent-model-roster.md`):

The planner recommends which model is best suited to each step.
This is a judgment call, not a rigid rule -- the right model
depends on what the step requires, not on whether it is "planning"
or "implementation". The models available to sub-agents are:

- **fable** -- The most capable model available, for the hardest
  reasoning and the longest-horizon work: multi-step changes a
  single sub-agent must carry end to end, or steps whose
  correctness depends on holding a whole subsystem in mind at
  once. It costs materially more than opus, so reserve it for
  steps that have already defeated opus or are expected to.
- **opus** -- The default for steps needing deep reasoning,
  architectural understanding, subtle correctness judgment
  (locking, state machines, migrations), or intricate
  implementation that would be costly to debug if it were wrong.
- **sonnet** -- A good default for well-briefed implementation
  work. Faster and cheaper than opus, and effective when the plan
  front-loads the research and the brief leaves no broad judgment
  calls to make.
- **haiku** -- Suitable for purely mechanical tasks:
  search-and-replace, regenerating generated code, adding log
  lines, running commands. The brief must be a near-complete
  instruction.

Model choice interacts with effort level and brief quality. A
detailed brief compensates for a lighter model -- sonnet at medium
effort with a thorough brief often matches opus at medium effort
with a vague brief. The planner's job is to write briefs good
enough that the recommended model can succeed.

The model also determines the context window: fable, opus and
sonnet have 1M tokens, haiku has 200K. A step that must hold many
files in context at once may need one of the larger-context models
for that reason alone, even when the reasoning itself is
straightforward.

**When in doubt, skew to the more capable model.** Saving money
only matters if the outcome is still acceptable. A failed or
low-quality implementation wastes more time -- and therefore more
money -- than the heavier model would have cost. Recommend a
lighter model only when you are confident the brief is detailed
enough for it to succeed.
<!-- shared-block-end -->

### Management session review checklist

<!-- shared-block: plan-review-checklist v1 -->
Management session review checklist (shared block; do not edit --
the canonical copy lives in shakenfist/development at
`templates/shared-blocks/plan-review-checklist.md`):

After a sub-agent completes, the management session verifies:

- [ ] The files that were supposed to change actually changed --
      read them, do not trust the summary.
- [ ] No unrelated files were modified.
- [ ] The changes match the intent of the brief: not merely
      syntactically correct, but semantically right.
- [ ] The project's own pre-merge checks pass, including any
      generated code that has to be regenerated and committed
      (see the project-specific checks below).
- [ ] The commit message follows project conventions, including
      the `Co-Authored-By` line recording model, context window,
      and effort level.
<!-- shared-block-end -->

!!! note "In this project"

    The project-specific checks referred to above are:

    - [ ] The code passes `tox -epy3`, `tox -eflake8` and
          `pre-commit run --all-files`.
    - [ ] The plugin still imports cleanly (`python3 -c 'import
          shakenfist_client_k3s'`) -- a broken import takes the
          whole `sf-client` CLI down.

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

!!! note "In this project"

    The close-out sections below apply as written, with one
    addition: when scanning the issue tracker for related bugs,
    remember that issues for this plugin sometimes belong
    upstream (for example `shakenfist/shakenfist` or
    `shakenfist/agent-python`). Reference cross-repository
    issues explicitly.

<!-- shared-block: plan-closeout-sections v1 -->
Plan close-out sections (shared block; do not edit -- the
canonical copy lives in shakenfist/development at
`templates/shared-blocks/plan-closeout-sections.md`):

### Future work

We should list obvious extensions, known issues, unrelated bugs we
encountered, and anything else we should one day do but have
chosen to defer to here, so that we do not forget them.

* A recovery plan, separate from this one (open question 4, phase 1
  decision 10). Workers are already replaceable with `remove-worker`
  plus `expand-workers`; a control plane node is not replaceable at
  all, and `--control-plane-count 3` may be the whole answer there.
  Propose it once these signals have shown how often recovery is
  needed.
* Bound the health probes' agent operations with a short deadline, so
  that an abandoned probe expires in seconds rather than after the
  server's 600 second default. `shakenfist_client`'s
  `instance_execute()` has a `deadline_seconds` argument on its
  development branch but in no release; this needs the client floor
  raised once one ships (phase 1 survey finding 5).

### Bugs fixed during this work

This section should list any bugs we encounter during development
that we fixed. You should also scan the project's issue tracker,
where one exists, for directly related issues that we should
either resolve as part of this master plan or at least be aware of
while planning it.

...

### Back brief

Before executing any step of this plan, please back brief the
operator as to your understanding of the plan and how the work you
intend to do aligns with that plan.
<!-- shared-block-end -->
