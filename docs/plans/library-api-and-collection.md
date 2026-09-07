# Library API, missing verbs, and the shakenfist.k3s collection

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

This plan in particular turns a CLI-shaped package into a callable
one and ships an Ansible collection alongside it, so two further
bodies of code matter. Read `shakenfist_client/main.py` in
`client-python` for how the plugin is loaded, and
`sf_namespace._make_client()` in the `shakenfist.shakenfist`
collection for the connection-parameter pattern the new Ansible
module should mirror. Read
`33fl/docs/plans/PLAN-k3s-ci-runners.md` for the consumer
requirements that motivate the missing verbs -- design decisions 2
and 5 in particular -- rather than inferring them from this
document alone.

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

This package is CLI-shaped. Every command body in `__init__.py` holds
its own orchestration inline -- `k3s_create()` is roughly 190 lines at
`__init__.py:93-286` -- and every helper in `primitives.py` takes a
Click `ctx` as its first argument. There is no callable API, no
structured output, and no way to drive a cluster from anything that is
not `sf-client`.

Two consumers now want one:

- **An Ansible module.** `33fl/docs/plans/PLAN-k3s-ci-runners.md`
  design decision 5 wants cluster bringup driven from
  `static_runner.yml`, ensuring a cluster exists at a minimum shape.
- **Conductor.** The same plan's design decision 2 has conductor
  polling the cluster and adding or removing workers. Conductor is a
  Python daemon; it wants the library directly, not Ansible.

The coupling is shallower than it looks, though not as shallow as it
first appears. `ctx.obj` carries five fixed keys -- `CLIENT`,
`PROGRESS`, `VERBOSE`, `name`, `namespace` -- plus a sixth, dynamic
one: `get_cluster_metadata()` caches the cluster's namespace metadata
under `METADATA_KEY % name`, and `set_cluster_metadata()` writes
through to it (`primitives.py:27-53`). Five arguments therefore do not
replace the context object; a small state object does. Two things are
genuinely load-bearing, and both are fatal to a module that must own
stdout for its JSON result and fail structurally:

- **48 bare `print()` calls** -- 24 in `primitives.py` and 24 in
  `__init__.py`.
- **`sys.exit(1)` on every failure path**: seven in `primitives.py`
  (`:91`, `:114`, `:124`, `:170`, `:198`, `:283`, `:405`) and thirteen
  in `__init__.py` (`:136`, `:139`, `:148`, `:253`, `:268`, `:341`,
  `:346`, `:363`, `:386`, `:465`, `:485`, `:512`, `:537`).

`progress.Progress` already accepts a `stream` argument
(`progress.py:41`), so a collecting reporter can feed the `log` list
that all four `sf_*` modules in `shakenfist.shakenfist` return. It has
to be file-like rather than a list, because `Progress` decides between
its interactive and line modes by calling `stream.isatty()`.

Beyond the refactor, the plugin is missing verbs and carries
behaviours a daemon cannot tolerate. Re-verified against `develop` on
2026-09-03:

- **No `remove-worker`.** No cordon, no k8s Node object deletion, no
  removal from `worker_nodes` metadata, no VM delete. This is the
  entire conductor scale-down verb.
- **`install_workers()` is not incremental.** It runs the k3s agent
  install across *all* of `md['worker_nodes']`
  (`primitives.py:537-542`), so `expand-workers` reinstalls k3s on
  every already-joined worker.
- **No crash recovery.** `create` registers the cluster name in
  namespace metadata and then builds for minutes; a mid-create failure
  leaves a name-squatting cluster stuck in `state: initial` with
  `delete` as the only way out. `state` is only `initial` ->
  `created` -> `deleted` (`__init__.py:177`, `:279`, `:442`).
- **No health verb.** Nothing answers "are all nodes Ready" without
  hand-rolled kubectl.
- **MetalLB and Longhorn are unconditional** (`__init__.py:234-235`).
  A CI cluster of ephemeral pods does not want Longhorn.
- **Kubeconfig side effects are mandatory.** `create` writes
  `~/.kube/config` and needs a local `kubectl` to merge into an
  existing one, exiting 1 if absent (`__init__.py:249-253`). `delete`
  unconditionally shells `kubectl config unset` three times and exits
  1 on failure (`__init__.py:459-465`) -- *after* the VMs are already
  gone.
- **No manifest payload hook.** No way to install extra manifests or
  `HelmChart` resources at bootstrap.

Finally, the packaging story has a hole nobody had noticed: **this
package has never been released.** There are no git tags, and both
`shakenfist-client-k3s` and `shakenfist_client_k3s` 404 on PyPI.

## Mission and problem statement

Turn this package into something a daemon and an Ansible module can
drive, and ship an optional Ansible collection that exposes cluster
bringup -- without making the k3s feature mandatory for anyone
deploying Shaken Fist.

Done means: `primitives.py` and a new orchestration layer are callable
with a client and a cluster name, return values, and raise exceptions;
the missing verbs exist; `shakenfist.k3s` is published to Ansible
Galaxy; and `shakenfist-client-k3s` is on PyPI so the collection's
install instructions are true.

Constraints:

- **Optionality is the whole point.** `shakenfist.shakenfist` must not
  change, and must not learn about k3s. A deployment that does not
  want k3s installs neither the plugin nor the collection and is
  unaffected.
- **The CLI must keep working exactly as it does today.** The Click
  commands become thin wrappers; their output and exit codes are
  observable behaviour that CI and humans depend on.
- **Python >= 3.7**, single quotes, 120 columns, per `AGENTS.md`.
- **The plugin must never break `sf-client` startup.** It is imported
  unconditionally by the entry-point loader
  (`client-python/shakenfist_client/main.py:259-269`, unguarded), so
  top-level imports stay cheap and reliable.

## Decisions

- **The Ansible module ships as its own collection,
  `shakenfist.k3s`, built and published from this repository.** Not as
  a module inside `shakenfist.shakenfist`.

  Ansible's native unit of optional distribution is the collection,
  which mirrors exactly how this package is already an optional CLI
  plugin: installed or not, discovered by presence via the
  `shakenfist_client.plugin` entry point. The operator story becomes
  symmetric --
  `pip install shakenfist-client` plus
  `ansible-galaxy collection install shakenfist.shakenfist` for the
  core, and the same two commands with the k3s names for this. The
  `shakenfist` Galaxy namespace is already claimed and currently
  empty, so a second collection in it costs nothing
  administratively.

  `galaxy.yml` keeps `dependencies: {}`. The module talks to the API
  through `shakenfist_client` directly and uses nothing from
  `shakenfist.shakenfist`, so it stays usable against a cloud that was
  not deployed with the collection.

  The build machinery is copyable almost verbatim from
  `shakenfist/tools/build-collection.py` and the `build-collection` /
  `publish-collection` job pair in that repository's `release.yml`;
  both repositories already share the same release template.

- **The module ensures existence and shape; it never manages worker
  count.** Cluster state is read-modify-write namespace metadata with
  no locking, and conductor is the other writer. Splitting by
  convention -- module owns "exists at minimum shape", conductor owns
  worker membership -- avoids a two-writer race. This is
  `PLAN-k3s-ci-runners.md` design decision 5, and it is why the module
  is deliberately *not* a full declarative reconciler.

- **Failures raise, they do not exit.** A local exception hierarchy,
  caught and turned into `sys.exit(1)` by the Click layer and into
  `fail_json()` by the module layer.

## Alternatives considered and rejected

- **A module inside `shakenfist.shakenfist` that imports this package
  and fails gracefully when it is absent.** There is precedent for
  tolerating an absent optional dependency -- `roles/node/tasks/
  config.yml` wraps the `hashivault` lookup in `block`/`rescue` -- but
  that is for a third-party plugin, not first-party content. This
  option puts a module for an optional component in the mandatory
  collection, forces its `requirements.txt` to either over-declare or
  leave a confusing runtime error, turns collection-versus-plugin
  version skew into a support matrix, and makes the server repository
  carry tests for code it does not ship. It was genuinely better at
  one thing: a single collection to install.

- **A role that shells out to `sf-client k3s create` via
  `ansible.builtin.command`.** Cheapest to write, and it needs none of
  the refactor. Rejected: no check mode, no structured return, no
  idempotency, and `print()` plus `sys.exit(1)` is precisely the
  interface a module must not have.

## Open questions

1. **Should the collection be the first thing published into the
   `shakenfist` Galaxy namespace?** `shakenfist.shakenfist` has the
   publish job wired but has never shipped a version -- a
   collection-version search for the namespace returns zero.
   Recommendation: let a core release go first, so the token and
   namespace-permission path is validated on the component we
   understand best. Awaiting an answer; it only affects ordering.

2. **What does `remove-worker` do about the k8s Node object?**
   Deleting the VM without deleting the Node leaves NotReady
   tombstones (`PLAN-k3s-ci-runners.md` decision 2 names this). Doing
   it from the plugin means the plugin needs cluster credentials and a
   kubectl-equivalent, which cuts against making kubeconfig side
   effects opt-out. Recommendation: the plugin cordons and deletes the
   Node using the kubeconfig already in namespace metadata, via the
   API rather than a `kubectl` subprocess.

3. **Does `create` grow a `--no-metallb` as well as `--no-longhorn`?**
   The CI cluster needs MetalLB for the federation float
   (`PLAN-k3s-ci-runners.md` decision 4), so nothing needs it off
   today. Recommendation: implement both flags anyway, since the code
   paths are adjacent, but do not spend design effort on the MetalLB
   case.

## Execution

| Phase | Work | Status | Merged |
|-------|------|--------|--------|
| 1. Library API | [library-api-and-collection-phase-01-library-api.md](library-api-and-collection-phase-01-library-api.md) -- extract orchestration from the Click command bodies into a callable `Cluster` layer; replace `sys.exit(1)` with an exception hierarchy; route `print()` through a reporter; make the Click commands thin wrappers | In progress | |
| 2. Client construction | Honour the root `--apiurl`/`--key`/`--namespace` in `_bind_cluster_context()` (`__init__.py:36` and `:98` currently overwrite the root client, which already honours them at `client-python/shakenfist_client/main.py:228-237`; note the root's `--async` default is `pause` and this code needs `ASYNC_CONTINUE`), and add the `api_url`/`namespace`/`key` plus `suppress_configuration_lookup=True` path that `sf_namespace._make_client()` uses | Not started | |
| 3. Missing verbs | `remove-worker`; make `install_workers()` incremental; reconcile/crash recovery for `state: initial`; a health verb; `--no-longhorn`/`--no-metallb`; kubeconfig side effects opt-out (including the `kubectl config unset` calls in `Cluster.delete()`, which run without `capture_output` and so write to the process's real stdout, bypassing the reporter -- found during phase 1 and recorded in its plan under "The kubectl unset leak"); manifest payload hook | Not started | |
| 4. First release | Cut `v0.1.0` so `shakenfist-client-k3s` exists on PyPI and `setuptools_scm` has a real version to stamp | Not started | |
| 5. The collection | `shakenfist.k3s` with `sf_k3s_cluster`; `tools/build-collection.py`; `build-collection` and `publish-collection` jobs in `release.yml`; ansible-lint in pre-commit and CI; docs | Not started | |
| 6. Push audit | Run `PUSH-AUDIT.md` over the accumulated diff of phases 1-5 against `develop` | Not started | |

Phase ordering is forced by dependency rather than convenience.
Phase 1 is the keystone: phases 3 and 5 are both much easier after it,
and phase 5 is impossible before it. Phase 4 must precede phase 5
because the collection's `requirements.txt` names a PyPI package that
must exist. Phase 2 is separable but belongs before phase 5, because
the module's connection parameters are the same code path.

Phases 1 and 3 are independently useful to conductor even if the CI
runner migration never happens.

!!! note "In this project"

    The Execution phases are a table, so the record of what put
    each phase on the default branch goes in the `Merged` column
    after `Status`. Phases here land as pull requests against
    `develop`, so the cell normally holds the single merge commit
    of that pull request. Phase 6 audits the accumulated diff of
    phases 1-5 against `develop`, not the diff of phase 5 alone,
    which is why those cells have to be filled in as each phase
    lands rather than reconstructed afterwards.

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

!!! note "In this project"

    Phase 1 is the exception to spawning one sub-agent per step
    and moving on. It rewrites `primitives.py` and every Click
    command body at once, and the management session has to read
    the resulting diff against the pre-refactor behaviour rather
    than against the brief -- the CLI's output and exit codes are
    the contract being preserved, and a sub-agent summary cannot
    demonstrate that they were.

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
    Shaken Fist cluster. That covers phase 1 (the library API),
    and within phase 3 the incremental `install_workers()`,
    `remove-worker` and the `state: initial` reconcile, all of
    which are read-modify-write against namespace metadata that
    conductor also writes. Phases that mirror an already
    established pattern -- the `--no-longhorn` flag alongside an
    existing option, or the collection build machinery copied
    from `shakenfist/tools/build-collection.py` -- can be planned
    at medium effort.

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
    Every phase 3 verb is in the second category.

    A worked brief for this codebase: instead of "make worker
    installation incremental", write "change
    `install_workers()` in `shakenfist_client_k3s/primitives.py`
    (currently `primitives.py:537-542`, which loops over all of
    `md['worker_nodes']`) to take the list of workers to install
    as an argument, so `expand-workers` passes only the newly
    created nodes and `create` passes all of them. Keep the
    existing agent operation wait loop and progress reporting.
    Add coverage to the tests in `shakenfist_client_k3s/tests/`
    in the existing testtools/stestr style with the Shaken Fist
    API mocked; the end-to-end behaviour needs a live cluster and
    cannot be asserted here."

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
    - [ ] `sf-client k3s --help`, and the help for each
          subcommand, is unchanged from before the refactor.
    - [ ] From phase 5 on, `ansible-lint` passes over the
          collection.

## Administration and logistics

### Success criteria

We will know when this plan has been successfully implemented
because the following statements will be true:

* The code passes `tox -epy3`, `tox -eflake8` and
  `pre-commit run --all-files`, the last including ansible-lint
  over the new collection.
* There is new unit test coverage for the exception paths that
  replaced `sys.exit(1)`, for the reconcile logic, and for the
  reporter that replaced `print()`, in the existing
  testtools/stestr style with the Shaken Fist API mocked.
* New code is compatible with Python >= 3.7 and the plugin still
  imports cleanly (`python3 -c 'import shakenfist_client_k3s'`) --
  a broken import takes the whole `sf-client` CLI down.
* Lines are wrapped at 120 characters, single quotes for strings,
  double quotes for docstrings, no triple single quotes.
* `sf-client k3s --help` lists the same commands with the same
  output as before the refactor, and the commands' exit codes are
  unchanged.
* A cluster can be created, expanded, health-checked, shrunk and
  deleted entirely from Python, with no Click context and no
  `~/.kube/config` side effects.
* `ansible-galaxy collection install shakenfist.k3s` followed by a
  playbook using `shakenfist.k3s.sf_k3s_cluster` brings up a cluster,
  is idempotent on a second run, and supports `--check`.
* `pip install shakenfist-client-k3s` works from PyPI.
* Installing neither the plugin nor the collection leaves a Shaken
  Fist deployment unchanged: `shakenfist.shakenfist` is not modified
  by this plan.
* Behaviour which can only be validated against a live Shaken
  Fist cluster -- every phase 3 verb, and the collection's
  end-to-end bringup -- has been exercised there (manually or via
  the functional CI) before merge.
* User-visible changes are documented in `docs/`. `AGENTS.md` changes
  only if a *convention* changed; `ARCHITECTURE.md` only if the
  *shape of the system* changed; `README.md` only if the pitch,
  install story or documentation links changed.

!!! note "In this project"

    The close-out sections below apply as written, with one
    addition: when scanning the issue tracker for related bugs,
    remember that issues for this plugin sometimes belong
    upstream (for example `shakenfist/shakenfist` or
    `shakenfist/agent-python`). Reference cross-repository
    issues explicitly.

### Documentation index maintenance

This plan is registered in `docs/plans/index.md`, in the *Master
plans* table. Its `Status` cell there and the `Status` cells in the
Execution table above hold exactly one term from the shared
vocabulary in `PLAN-TEMPLATE.md` -- `Proposed`, `Not started`,
`In progress`, `Blocked`, `Complete`, `Abandoned` or `Superseded`
-- and nothing else. Anything else a reader needs belongs in this
file, with a one line summary in the index's `Intent` column.

### Future work

- Teaching conductor to use the new library API is tracked in
  `33fl/docs/plans/PLAN-k3s-ci-runners.md`, not here.
- A k3s version upgrade story. `update-os` covers the OS only, and
  open question 4 of the CI runners plan asks whether the control
  plane is rebuilt or upgraded in place.
- Shrinking control plane count, and any declarative reconciliation of
  worker count, are deliberately out of scope while conductor owns
  worker membership.

### Bugs fixed during this work

<!-- Record bugs found and fixed while executing this plan. -->

Nothing yet. Note that
[#41](https://github.com/shakenfist/client-python-k3s/issues/41) --
`k3s delete` destroying a pre-existing network passed to
`create --network` -- is adjacent to phase 3 but is tracked as its own
issue, not as part of this plan.

### Back brief

Before executing any step of this plan, please back brief the
operator as to your understanding of the plan and how the work you
intend to do aligns with that plan.
