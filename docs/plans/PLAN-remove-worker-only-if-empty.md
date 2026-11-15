# remove-worker: refuse a worker which still has work on it

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

<!-- shared-block: plan-file-conventions v2 -->
Plan file conventions (shared block; do not edit -- the canonical
copy lives in shakenfist/development at
`templates/shared-blocks/plan-file-conventions.md`):

- All planning documents live in `docs/plans/`.
- Detailed planning gets one plan file per phase. Phase files are
  named for their master plan, sit in the same directory as it,
  and append `-phase-NN-descriptive` before the `.md` extension.
- The master plan tracks its phases in a table under its Execution
  section. `Merged` is last, and the push audit is the last row,
  for the reasons given in `plan-push-audit-phase`:

  | Phase | Plan | Status | Merged |
  |-------|------|--------|--------|
  | 1. Schema migration | PLAN-thing-phase-01-schema.md | Not started | |
  | 2. Public API | PLAN-thing-phase-02-api.md | Not started | |
  | 3. Push audit | - | Not started | |

- One commit per logical change, and at minimum one commit per
  phase. Unrelated changes are not batched into a single commit.
  Each commit is self-contained: it builds, passes tests, and has
  a message explaining what changed and why.
<!-- shared-block-end -->


## Situation

`Cluster.remove_worker()` drains each worker it is given before it
deletes the k3s node object and the Shaken Fist instance behind it. The
drain is unforced (`kubectl drain --ignore-daemonsets
--delete-emptydir-data --timeout=300s`), and the method's docstring and
`docs/usage.md` both read as though that were a safety property: a pod
the drain cannot move stops the removal, uncordons the node and raises.

That reading is wrong, and the mistake is in what `kubectl drain`
refuses. Its unreplicated-pod filter asks whether the pod has a
`controllerRef` at all, and never what kind the reference names. A pod
owned by a ReplicaSet, a Job, or any custom resource which sets
`controller: true` on its owner reference is therefore evicted by an
unforced drain, which then exits 0. Only a pod with no controller at
all needs `--force`.

The practical case is a CI runner. GitHub's Actions Runner Controller
gives each ephemeral runner pod an owner reference of kind
`EphemeralRunner` with `controller: true`, so a `remove_worker()` of
the node it is on evicts a running job and reports success. A
server-side dry run against a live cluster confirms it: the node is
cordoned, `evicting pod arc-runners/k3s-static-hunkydory-runner-...` is
printed, and the exit code is 0.

The caller which needs the other answer is a reconcile loop which
scales a pool of worker nodes up and down with cluster demand -- the
library API's named use case, and the one for which `remove_worker([])`
is an accepted no-op. Such a caller cannot close the window itself. It
can ask Kubernetes, or a metrics endpoint, whether a worker is idle,
but a worker which is idle when it looks can be given a job before it
acts on what it saw, and the loop has no Kubernetes credentials of its
own with which to prevent that.

## Mission and problem statement

Give `remove_worker()` an option which refuses a worker that still has
work on it, and make that refusal trustworthy by checking after a
cordon of this method's own rather than before it. Kubernetes will not
schedule a new pod onto a cordoned node, so emptiness observed behind a
cordon stays true until something uncordons the node; emptiness
observed in front of one is a guess. `kubectl drain` cordons as its
first act, which is too late -- there is no moment between that cordon
and the eviction at which a caller can look -- so the option cordons
explicitly, lists the node's pods, and only then lets the existing
drain proceed, whose own cordon is by then a no-op.

Out of scope, deliberately:

* **Telling a busy runner pod from an idle one.** A generic k3s library
  has no business knowing what a CI runner is, and tolerating idle
  runner pods would reopen the race: a controller can assign a job to
  an already-registered idle runner at any moment, and cordoning the
  node it is on does not prevent that. Refusing on any live pod is the
  conservative primitive, and what the caller does about a refusal is
  the caller's problem.
* **A label selector, an allowlist, or any knob for which pods count.**
  The rule is DaemonSet-managed or finished, with no exceptions and
  nothing to configure.
* **Changing the default.** `remove-worker` is an imperative verb and
  stays one. Every existing caller, the Ansible collection among them,
  must see no change at all.
* **A `pods` verb, or any other new way to read a cluster.** The
  listing is internal to this check.

## Open questions

1. **Does the drain refuse an ARC ephemeral runner pod, so that this
   option is unnecessary?** *Answered: no, it evicts it.* The filter
   tests for the existence of a `controllerRef`, not its kind, and
   `EphemeralRunner` sets `controller: true`. Established by a
   server-side dry run against a live cluster before this plan was
   written, and the reason the plan exists.
2. **Should the check be a separate read verb, so the caller decides?**
   *Decided: no.* A caller which reads the node's pods and then calls
   `remove_worker()` has the race this exists to close, however it
   reads them. The check has to be on the far side of a cordon, and the
   cordon is here.
3. **What counts as empty?** *Decided:* any pod which is not
   DaemonSet-managed and has not reached `Succeeded` or `Failed` is in
   the way. A DaemonSet's pod goes with the node and the drain already
   ignores it. A finished pod is a record of work rather than work, and
   these genuinely sit on these nodes: every helm install a k3s cluster
   does leaves a `helm-install-*` Job pod behind for good, and a
   removal which those blocked would never succeed.
4. **What should an answer the check cannot read mean?** *Decided:* a
   refusal, never an empty node. Read as empty, unreadable output says
   "nothing is running here" about a node nobody looked at, on the one
   verb where that destroys something. Output which was not available
   to read, no output at all, and output holding a line the Kubernetes
   API would not have printed are each a refusal of their own, distinct
   from "the node is in use" because a caller scaling a pool expects
   the second and not the first.
5. **Does the refusal need its own exception class?** *Decided: yes,
   and it must be distinguishable by type.* The caller counts a refusal
   as a normal outcome and a drain which broke as a failure, and a
   message is not something to branch on.

## Execution

This plan is one pull request. Phase 1 is the work. Phase 2 is the push
audit, which runs over phase 1's branch diff against `develop` before
the branch is pushed. Both phases land together, so the plan closes
itself out in that pull request and records no `Merged` cell.

| Phase | Plan | Status | Merged |
|-------|------|--------|--------|
| 1. The emptiness option | This file, steps below | Complete | |
| 2. Push audit | `PUSH-AUDIT.md` over phase 1's diff against `develop`, before push | Complete | |

Plan phase 1 at high effort. The ordering of the cordon against the
listing is the whole of the option's value, and the pod listing is a
new parser of output from a node which may be unwell -- both areas this
project's guidance names as high effort.

### Push audit result

Run on 2026-11-16 over `4bd0696..f0debdf`, the whole of phase 1, by two
reviewers: code quality and correctness, and tests, security and
documentation. Both ran `tox -epy3` themselves. They raised two
blocking findings and seventeen advisory ones; the fixes landed on this
branch before the push.

* **The reasoning behind one refusal was wrong, in three places.**
  Both reviewers found it, independently. The `no_output` refusal
  argued that a command which printed nothing has not answered,
  "because a node always has at least its DaemonSet pods". That claim
  is false -- a worker which has just joined has no pods until its
  DaemonSets' are scheduled, and a cluster running no DaemonSets never
  has any -- and the code never implemented it either: an empty answer
  is read as a node with no pods, and only a result carrying neither
  output nor a blob refuses. The behaviour is right, because refusing
  an empty answer would refuse such a worker for good, which is worse
  for a pool-scaling caller than not having the option. So the
  docstring, `docs/usage.md` and `docs/library-api.md` now say what the
  code does, and a test asserts the empty answer is believed, named for
  the decision. One reviewer established from the server's own
  sidechannel that `no_output` guards a result shape Shaken Fist does
  not produce; it stays, described as the guard it is.
* **The plan claimed a push audit it had not run.** The row below said
  `Complete` with no result recorded, which is the one thing the
  `plan-push-audit-phase` block this plan carries requires.
* **The live tier asserted less than its comment claimed.** Everything
  which lands on the worker the functional tier removes is a DaemonSet
  pod, so the branch CI actually takes was the accepted removal, and it
  checked only the exit code -- a listing which always answered "empty"
  would have passed. It now greps for the check's own progress note, so
  the live tier says the template rendered, the output parsed and a
  verdict was reached. An unreadable answer also now fails with an
  accurate message rather than "failed, and not by refusing", which it
  had not.
* **A comment stated a mechanism Go does not have.** The guard around
  the ownership column was explained as text/template refusing to range
  over a missing field. One reviewer rendered the template with Go
  1.24, the way kubectl's printer does, over twenty API shapes: a range
  over a missing field is an empty range. The guard stays, for the
  reason the module actually has -- every nested read sits inside a
  test of its parent, which a reader can check by eye -- and the false
  sentence is gone. That rendering also confirmed the template parses
  and prints the columns the parser expects, which is what no unit test
  here can do.
* **Weak tests and a fragile fake.** The pod-listing fake
  reimplemented its parent's bookkeeping, so `failing_command` silently
  did not apply to the listing, and its per-node dispatch fell back to
  an empty node on a miss -- which is the assertion most of those tests
  are making. It delegates and raises on a miss now. Four assertions
  which could not have failed were strengthened or replaced, and tests
  added for a drain which fails after the check, for the refusal
  message with two pods and a pod with no phase, and for neither parser
  reading the other's lines.
* **Unreachable defences and duplicated prose.** The listing's result
  read had two fallbacks `reap_execute()` has already made impossible;
  it says what is true instead. The seventeen-line comment above the
  cordon was cut to eight, and `node_pods_command()`'s docstring stopped
  re-deriving the quoting and output-limit reasoning the constants it
  cites already carry.
* **`docs/usage.md` misquoted the refusal it shows.** Both reviewers
  caught it; the example is now the message `__str__` produces, with
  the exit code.

Nothing was declined. Security found nothing blocking in either review:
the node name is quoted once, the template is a module constant, the
only API-derived values printed are ones the API server validates and
the parser revalidates, a phase or an owner kind is compared against
this module's literals rather than printed, and a forged line reaches
`unreadable_lines` and refuses rather than reading as an empty node.

### Review round 1

The automated reviewer raised 1 fix, 3 document, 5 consider and 2 none
on `55f0571`. Everything but one `consider` was taken.

The `fix` was a documentation drift this plan's own push audit created:
the audit corrected the false claim that a node always has at least its
DaemonSets' pods in the docstring and both documents, and missed the
same sentence inside `WorkerPodsUnreadableError.no_output`'s message,
where it contradicted the class docstring forty lines above it. The
existing test asserted only `reason`, which is why the drift survived;
it now asserts a phrase of the message and the absence of the old
claim.

Three documentation items, all taken. The cordon's durability claim is
qualified -- a cordon stops the scheduler, not a pod created with
`spec.nodeName` already set nor one tolerating the unschedulable taint,
and the drain evicts those as it always did. The per-worker nature of
the check is stated in both documents, because `docs/usage.md` promises
fifteen lines earlier that validation is hoisted so a bad last uuid
removes nothing, and a reader carries that promise into a section where
it does not hold. And `unreadable_lines` is described as a count rather
than as the lines themselves.

Four `consider` items taken: a result carrying both an inline stream
and a blob now refuses as `ambiguous_output` rather than resolving to
an empty node (unreachable against Shaken Fist's documented result
shape, and the same guard as `no_output` for the same reason); the
progress note the live tier greps is a module constant a unit test
asserts, so rewording it fails the fast tier rather than the merge
queue; two more mutation entries cover the two refusals the push audit
had reasoned about wrongly, and a third covers the new one; and one
docstring line is re-wrapped.

The `consider` not taken is the unconditional uncordon. A refusal
uncordons the node through the guard every unfinished removal uses, and
that guard cannot tell this method's cordon from one an operator made
first, so a loop refusing every cycle returns a deliberately cordoned
worker to service every cycle. The blindness is pre-existing -- only
the frequency is new -- and the correct fix costs a read of
`spec.unschedulable` before the cordon plus a branch in a guard shared
with the drain path, which is a behaviour change to the default verb as
well. Documented in `remove_worker()`'s docstring, `docs/usage.md` and
`docs/library-api.md`, and filed as
[#173](https://github.com/shakenfist/client-python-k3s/issues/173).

### Review round 2

1 fix, 1 document, 6 consider and 2 none on `bf379da`. The fix and five
of the six considers are addressed; the sixth was already answered.

The fix was round 1's own doing. Three comments -- on the constant, in
the test and in `tools/ci_deploy_test.sh` -- claimed a unit test pinned
`NOTHING_IN_THE_WAY_NOTE`'s text, so that rewording it would fail the
fast tier. The assertion was `assertIn(constant, lines)`, which pins
that the note is built from the constant and says nothing about what the
constant says: rewording it passed `tox -epy3` and broke only the shell
script's literal `grep -qF`, which is exactly the failure the comments
claimed was prevented. A second assertion pins the text, and the
comments now say which assertion pins what.

The document item is the third way a pod can arrive behind the cordon,
and it is the sharpest of the round: the scheduler's `NodeUnschedulable`
filter runs against a cache snapshot and the API server does not
re-check `spec.unschedulable` when it accepts a binding, so a pod the
scheduler had already selected the node for lands after both the cordon
and the listing. The consequence is the one the other two bypasses have
-- the drain evicts it -- but the text had read as a guarantee
Kubernetes does not make, on the one property the option rests on.

Considers taken. `reap_execute()`'s new return value had no test that
could tell it from the submitted operation, because the stock fake
answers `instance_execute()` with a completed operation while the real
server answers 'queued' with no results at all; a fake modelled on
`fakes.HealthClient` now proves the check reads the operation as it
completed, and a mutation confirms the test fails against the submitted
one. A listing read back from a blob and then parsed had no test either,
which is the realistic shape for the busy node the check exists to
refuse. `WorkerNotEmptyError`'s message is capped at ten pods with a
count of the rest, since a node at Kubernetes' default limit of 110
would have rendered one line of tens of kilobytes into every log that
caught it, while the `pods` attribute stays complete. A refusal is now
reported as a refusal rather than through the uncordon's "the removal
did not finish" wording, which for a loop that refuses every cycle
announced the normal outcome as an incident. And one `docs/usage.md`
line is re-wrapped.

The consider partly taken is where exception message tests live. The
existing assertions are not moved -- they are made through
`remove_worker()`, which is where the behaviour is -- but the new
message tests are a `WorkerNotEmptyErrorTestCase` in
`tests/test_exceptions.py` beside the other eighteen, so a reader
looking for an exception's message finds it where the convention says.

Ten mutations now cover this change, each applied and caught. The two
`none` items were the already-filed uncordon (#173) and a security read
that found no new class of untrusted input.

### Phase 1 steps

| Step | Effort | Model | Isolation | Brief for sub-agent |
|------|--------|-------|-----------|---------------------|
| 1a | high | opus | worktree | The option. `exceptions.py` gains `WorkerNotEmptyError` (the refusal, carrying the node, the instance and the pods found) and `WorkerPodsUnreadableError` (a `_ReasonedK3sException` with a reason per unreadable answer). `cluster.py` gains `NODE_PODS_TEMPLATE`, `node_pods_command()`, `parse_node_pods()` and `pods_in_the_way()` beside the health probe's template and parser, following that parser's style -- validate every field, drop nothing silently -- plus `Cluster._pods_in_the_way()`, which submits the listing and reaps it. `remove_worker()` takes `only_if_empty=False` and, when it is set, runs `kubectl cordon`, then the listing, then the existing drain, all inside the existing uncordon guard. `reap_execute()` returns the operation it reaped so the listing's output can be read from the result it already checked. The Click verb gains `--only-if-empty`. Unit tests for: the refusal and that it leaves the node schedulable; the cordon preceding the listing; a DaemonSet-only node and a `Succeeded` pod proceeding; an idle pod refusing; each unreadable answer refusing; a refusal on the second of two workers leaving the first removed; and the default running exactly the two commands it always did. Regenerate `tests/cli_contract/remove-worker.txt` by invoking `--help`, not by hand. |
| 1b | medium | opus | worktree | Documentation and the live tier. `docs/usage.md` gains the flag under `remove-worker`: what counts as work, why the cordon comes first, and that an unreadable answer is a refusal. `docs/library-api.md` gains the new signature, both exceptions, and a paragraph telling a pool-scaling caller to treat `WorkerNotEmptyError` as a normal outcome. `remove_worker()`'s docstring stops implying the drain is what makes a removal safe. `tools/ci_deploy_test.sh` runs the flag against the real cluster, accepting either answer -- which pods sit on a given worker depends on where the scheduler put Longhorn's and Traefik's -- and asserting that a refusal uncordoned the node, because the template is rendered by a Go engine only there. `AGENTS.md` and `ARCHITECTURE.md` are left alone: no convention changed and the component inventory is the same. |
| 1c | medium | opus | worktree | `tools/mutation-check.py` entries for the properties this defends rather than adds: the cordon preceding the listing, the refusal on a live pod, DaemonSet and finished pods not counting, and an unreadable line refusing. Run it and record the result in the commit message. |

One commit per step, each passing `tox -epy3`, `tox -eflake8` and
`pre-commit run --all-files`, and the plugin must still import cleanly.


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

<!-- shared-block: plan-phase-landing v1 -->
Phase landing (shared block; do not edit -- the canonical copy
lives in shakenfist/development at
`templates/shared-blocks/plan-phase-landing.md`):

A plan's status and a repository's review state both live in files
that every branch would otherwise rewrite. Left alone, that turns
each of them into a merge-conflict hot spot, and it spends a pull
request and a full CI run on a change that is entirely prose.
Three rules keep them out of the way.

- **A phase is closed out in the first commit of the next phase,
  not in a pull request of its own.** By the time the next phase
  branches, the previous one has merged, so its merge commit is
  known and its `Merged` cell can record the thing the push-audit
  phase actually needs. This is the only ordering that works: a
  phase cannot record its own merge commit, and a separate
  close-out pull request buys that record at the price of a round
  trip. The close-out sets the finished phase's `Status` and
  `Merged` cells and the plan's row in `docs/plans/index.md`, and
  it is committed before the next phase's own work, so that the
  branch never claims the plan is further along than the default
  branch is.

- **The last phase closes itself out.** The push-audit phase is
  the last row of every plan, so no next phase will carry its
  close-out. Where the audit raises findings, the plan is not
  complete until they are resolved or declined, and those land as
  their own pull request after the audit phase has merged -- so
  that pull request is the carrier, and it can record the audit
  phase's merge commit, which by then is known. Where the audit
  finds nothing there is no carrier, and no follow-up pull
  request is opened for the sake of one cell: the phase sets its
  own `Status`, and the plan's index row, to `Complete` in its
  own pull request, and records no `Merged` cell. It is the only
  row permitted to omit one. The column exists so that the
  push-audit phase can reconstruct what to audit; the audit phase
  is last, so nothing ever reads its own row.

- **`REVIEWS.md` is not pruned or regenerated in a pull request
  that changes code or documentation.** Editing a reviewed file
  stales its mark, and adding or removing an in-scope file moves
  the header count, but neither is the landing pull request's
  business. `prune` regenerates the file whether or not it dropped
  anything, so the `prune-reviews` workflow heals both on the next
  push to the default branch. Pruning from a branch is also wrong
  more often than it is right, though not for the reason it first
  appears: `prune` compares each stamp against `HEAD`, which on a
  branch is the branch tip, so it drops the marks for the files the
  pull request itself touched while keeping marks the default
  branch has already pruned. Committing that state merges a review
  file computed from a stale tree, and can resurrect marks
  `prune-reviews` has already removed. Accumulated staleness is
  reported by the `review-coverage` audit, which recomputes
  coverage against `HEAD` and raises an issue once the backlog is
  worth a review session.

  **A review session is the exception**, and it is not optional
  tidiness: `stamp` regenerates `REVIEWS.md` as well as writing the
  marks, and the rows, the sidecars and the marks are committed
  together (see `docs/code-review-tracking.md`). Where a repository
  requires a pull request to reach its default branch, that is how
  a review session lands, so "not in a pull request" is about the
  kind of change, not the mechanism.

These rules assume phases land one after another. Where two phase
branches are open at once, each closes out only the phase it
directly follows.
<!-- shared-block-end -->

!!! note "In this project"

    This repository does not deploy review tracking: there is no
    `REVIEWS.md` and no `prune-reviews` workflow, so the third rule
    has nothing to act on here. The first two apply as written.

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

* **The functional tier cannot force either answer.** The live check
  accepts a removal and a refusal, because which pods the scheduler put
  on a given worker is not something the test controls. A tier which
  pinned a pod to the worker first could assert the refusal and the
  uncordon directly, and would be the stronger test.
* **Nothing exercises the floor this package claims.** The pod listing
  uses a go-template guarded the way the health probe's are, for the
  oldest kubectl this plugin supports, and neither is rendered by that
  kubectl in CI. This is the same gap as
  shakenfist/client-python-k3s#82 rather than a new one.
* **The listing is one more agent operation per worker removed.** For a
  caller removing workers one at a time that is the right trade, but a
  caller removing several would pay it per worker; there is no batching
  here and none is needed yet.
* **The live tier's refusal branch is about twenty-five lines of shell
  which will not run.** Because the worker it removes holds only
  DaemonSet pods, CI takes the accepted-removal branch; the refusal
  branch, its node-name extraction and its uncordon assertion were read
  by hand rather than executed. The tier which could force a refusal
  above would run them.
* **`cluster.py` is near six thousand lines, and this change made the
  seam clearer.** The Kubernetes reading layer -- the go-templates, the
  name, phase and status validators, and the two parsers -- is now
  roughly seven hundred lines of pure functions with no `Cluster`
  dependency and test cases of their own, which would move to a module
  of their own without touching a call site beyond an import. A split is
  its own change and its own review.
* **A pod whose DaemonSet has since been deleted reads as not in the
  way.** The check believes the owner reference; the drain resolves it,
  finds nothing and refuses to evict the pod unforced, so the removal
  fails with an uncordon rather than a destructive misread. Worth
  knowing rather than worth fixing here.

### Bugs fixed during this work

This section should list any bugs we encounter during development
that we fixed. You should also scan the project's issue tracker,
where one exists, for directly related issues that we should
either resolve as part of this master plan or at least be aware of
while planning it.

* None. The reading this change corrects -- that an unforced drain
  refuses to evict a controller-owned pod -- was in
  `remove_worker()`'s docstring and in `docs/usage.md` rather than in
  the code, which has always done what it says. Both now say what the
  drain does and does not promise.

### Back brief

Before executing any step of this plan, please back brief the
operator as to your understanding of the plan and how the work you
intend to do aligns with that plan.
<!-- shared-block-end -->
