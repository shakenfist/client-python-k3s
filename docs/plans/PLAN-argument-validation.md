# Argument validation: cluster names and counts

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

Nothing on the way into this plugin checks a cluster name, and only some
paths check a count. Issue #96 began as "`Cluster.create()` does not
range-check its counts". The phase 6 push audit of the library API plan
widened it to "nothing validates a name or a count on the way in". This
plan was written on 2026-10-07 against `develop` at `b615aa3`, and every
claim below was checked against that tree.

**Counts.** `k3s create`'s `--control-plane-count`, `--worker-count` and
`--metal-address-count`, `expand-workers`' `--worker-count` and
`expand-addresses`' `--address-count` are all bare `click.INT`
(`shakenfist_client_k3s/__init__.py:152-156, 447, 477`). Neither
`Cluster.create()` (`cluster.py:1921`) nor `expand_workers()`
(`cluster.py:2939`) nor `expand_addresses()` (`cluster.py:3246`) range
checks them. Today's outcomes:

- `create --control-plane-count 0` builds a cluster with no API server.
  The failure arrives tens of minutes later and does not name the option.
- `expand-workers --worker-count 0` prints `Added 0 workers`. A negative
  count does the same: `create_and_await_instances()` makes nothing.
- `expand-addresses --address-count -1` reaches
  `allocate_metallb_addresses(-1)`, where `range(-1)` is empty, and
  reports that no addresses were available.

The Ansible module refuses the create-time three before it builds a
client (`collection/plugins/modules/sf_k3s_cluster.py:602-628`, pinned
by `test_ansible_module.ShapeRangeTestCase`), and its comment points at
#96 as the place the check should really live. The node size floors are
stated a third time, as `click.IntRange(min=1)` on the six size options
(`__init__.py:192-209`), alongside the library's `validate_node_sizes()`
(`cluster.py:473`). The library's two front doors therefore disagree.

**Names.** The cluster name reaches four places unchecked:

1. **The namespace metadata key**, `METADATA_KEY =
   'orchestrated_k3s_cluster_%s'` (`cluster.py:50`). The two release
   caches live in the same namespace document under
   `orchestrated_k3s_cluster_k3s_version_cache` and
   `orchestrated_k3s_cluster_longhorn_version_cache`
   (`primitives.py:30-31`), so the cluster names `k3s_version_cache` and
   `longhorn_version_cache` produce the cache's own key. `create()`
   writes the cache before it reads the metadata, so it finds a document
   with no `state` and raises `ClusterInterruptedError.mid_create`.
   `delete()` raises a bare `KeyError`, which the CLI's handler does not
   catch, so the user sees a traceback. A library caller's
   `set_metadata()` overwrites the shared cache.
2. **Instance names**: `'k3s-%s-node-%03d'` (`cluster.py:970`). Shaken
   Fist refuses an instance name that is not a DNS host name, contains a
   dot, or is longer than 63 characters
   (`shakenfist/external_api/instance.py:755-762` in
   `shakenfist/shakenfist`). The template adds 13 characters to the
   name, or 14 once the serial passes 999. A cluster name of 51
   characters or more, or one containing a dot, an underscore or a
   space, is therefore refused at the first instance create. By then
   the name is registered and the node network has been allocated, so
   the operator is left with an interrupted cluster. Mixed case works,
   is documented (`docs/usage.md:392-397`), and has to keep working.
3. **Kubernetes node names.** k3s lowercases the host name, which is why
   mixed case works. Nothing else needs to be said here once (2) holds.
4. **The local kubeconfig.** `create()` names the user, context and
   cluster `fqcn = '<name>.<namespace>'` (`cluster.py:2306-2314`), and
   `delete()` removes them with `kubectl config unset users.<fqcn>`, and
   likewise for contexts and clusters (`cluster.py:2886-2937`). The
   comment there blames dotted names and points at #96. Validating the
   name is not enough to fix this, though, and this is the part the
   issue does not say. `unset` takes a property path, and kubectl's path
   parser (`findNameStep` in `navigation_step_parser.go`) takes the first
   segment after `users.` as the start of the map key. It then reads
   *each later segment* as a possible field name of the value type, and
   a segment that matches a field name ends the key. `fqcn` always has
   at least two segments, so a valid name in a namespace called
   `cluster`, `user`, `namespace`, `token`, `server` or another field
   name fails in exactly the same way. Shaken Fist namespace names allow
   letters, digits, hyphens and underscores
   (`shakenfist/external_api/auth.py:291-302`), and all of those are
   valid. When the unset fails, the cluster's instances, network and
   metadata are already gone, so re-running `delete` raises
   `ClusterNotFoundError` and the stale entries stay behind.

Clusters already exist under whatever names their operators gave them,
including any a new validator would refuse. A validator applied to every
verb would make them impossible to show, repair or delete.

Related, and not in scope: #72 (a create interrupted between its two
name-claiming writes), #112 (the module cannot pass sizes or k3s
configuration, which adds more numeric inputs that will want this
validation), and #82 (the Python floor this code must still meet:
`>=3.7`).

## Mission and problem statement

Make every name and count that reaches the library either valid or
refused with a reasoned error, before the API has been asked to do
anything. Give each rule exactly one home, in the library, so the CLI
and the Ansible module apply the same rule by calling the same function.
Separately, make `delete()`'s kubeconfig cleanup correct for every
cluster and namespace name that exists, so the cleanup no longer depends
on which names a validator happens to accept.

### Decisions

These are recommendations for the open questions below. Execution
proceeds on them unless the operator says otherwise.

1. **One pure validator for names, `validate_cluster_name(name)` in
   `cluster.py`**, beside `validate_node_sizes()` and in its style
   (pure, testable without a client, raises a reasoned exception). The
   rule is `^[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?$` and a length of at
   most 48 characters. Mixed case stays legal. The first and last
   characters must be alphanumeric, which matches a DNS label and keeps
   the `k3s-<name>-node` join unambiguous. 48 rather than 50 leaves room
   for node serials up to 99999 under Shaken Fist's 63-character limit.
   The limit is derived in a comment from the instance name template
   rather than written as a bare number. Underscores are refused, which
   makes both reserved-key collisions impossible.
2. **Only `create()` applies the full name rule.** Every other verb
   acts on a name that already exists, and refusing it would strand
   that cluster. It is checked at the top of `create()`, with the
   existing pure checks and before the release lookup, so a refused
   name has cost no API call.
3. **Every verb refuses the two reserved names.** `Cluster.__init__`
   refuses a name whose metadata key equals a key this package reserves
   in the namespace document. Those names could never have been
   created, so refusing them strands nothing, and it turns the delete
   traceback and the cache overwrite into a reasoned error on every
   path, the library's included. The reserved keys are listed once in
   `primitives.py`, next to the cache keys.
4. **One pure validator for counts, `validate_counts(**counts)`**,
   which raises a new `ShapeError` (decision 7 has the hierarchy). The
   floors are: `control_plane_count >= 1`, `worker_count >= 0` and
   `metal_address_count >= 0` on create, and a count `>= 1` on
   `expand_workers()` and `expand_addresses()`, where zero means the
   operator asked for nothing. `bool` and non-integers are refused for
   the reasons `validate_node_sizes()`' docstring gives.
5. **Each floor is stated in one place.** The CLI drops
   `click.IntRange(min=1)` from the six size options and leaves every
   count as `click.INT`, so a bad value now reaches the library. The
   error is then the library's reasoned message on stderr with exit
   code 1, where it used to be Click's usage error with exit code 2.
   That changes a CLI contract, so it is recorded in `docs/usage.md`
   and the `tests/cli_contract/` snapshots are regenerated. The module
   keeps refusing before it builds a client, but it does so by calling
   the library's pure validators directly, so its own floors table and
   its #96 comment go.
6. **Remove kubeconfig entries by name, not by property path.** Use
   `kubectl config delete-user`, `delete-context` and `delete-cluster`,
   which take the name as a single literal argument. A missing entry
   makes these fail, so first read the names that are present
   (`kubectl config view -o json`, parsed in Python) and delete only
   those. That keeps a second `delete` idempotent, as `unset` was. The
   `KubeconfigError` reasons stay as they are, with `unset_failed`
   renamed to describe the new command. Check kubectl's version floor
   for `delete-user` before committing to it (open question 2).
7. **The exceptions** follow the `_ReasonedK3sException` pattern
   (`exceptions.py:38`). `ClusterNameError` has the reasons
   `invalid_characters`, `too_long` and `reserved`. `ShapeError` has
   `below_floor` and `not_an_integer`. Both are added to the module's
   catch tuple (`sf_k3s_cluster.py:698`) and to
   `docs/library-api.md`'s exception table.

## Open questions

1. **Is refusing the name on create alone enough?** The alternative is a
   charset check on every verb with a grandfathering escape hatch. I
   think not: the only names the other verbs can mis-handle are the
   reserved ones (decision 3), and decision 6 removes the kubeconfig
   hazard for every name. Recommendation: create only.
2. **The kubectl version floor for `delete-user`.** `delete-cluster` and
   `delete-context` are old. `delete-user` arrived later, in kubectl
   v1.24 if I remember correctly; verify that before committing to it.
   The docs say nothing about a minimum kubectl today. If the floor is
   too new, the fallback is to edit the kubeconfig file in Python. That
   means honouring `KUBECONFIG`'s list of files, keeping the file's
   permissions, and writing it atomically, which is more code to own
   for a smaller gain. Recommendation: use `delete-user` if its floor
   is v1.24 or older, and document the floor in `docs/usage.md`.
3. **Exit code 2 to exit code 1 for out-of-range sizes on the CLI.**
   Decision 5 accepts this to get a single home for each floor. The
   alternative is to keep `IntRange` as a duplicate early check, which
   is the duplication #96 complains about. Recommendation: accept the
   change and document it.
4. **Is 48 characters too tight?** Nobody is known to use a longer name.
   Shaken Fist's own limit makes 50 the true ceiling at today's serial
   width, and anything longer already fails. Recommendation: 48.

## Execution

This plan is one pull request. Phase 1 is the work. Phase 2 is the push
audit, which runs over phase 1's branch diff against `develop` before
the branch is pushed. The two phases land together, so the plan closes
itself out in that pull request and records no `Merged` cell.

| Phase | Plan | Status | Merged |
|-------|------|--------|--------|
| 1. Argument validation | This file, steps below | Not started | |
| 2. Push audit | `PUSH-AUDIT.md` over phase 1's diff against `develop`, before push | Not started | |

### Phase 1 steps

| Step | Effort | Model | Isolation | Brief for sub-agent |
|------|--------|-------|-----------|---------------------|
| 1a | high | opus | none | Add `ClusterNameError` and `ShapeError` to `shakenfist_client_k3s/exceptions.py` as `_ReasonedK3sException` subclasses, mirroring `K3sConfigError` (`exceptions.py:651`) with its classmethod-per-reason shape and a docstring that lists every refusal and why. Add pure `validate_cluster_name(name)` and `validate_counts(**counts)` to `cluster.py` beside `validate_node_sizes()` (`cluster.py:473`), following decisions 1 and 4: rule `^[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?$`, at most 48 characters, the limit derived in a comment from `'k3s-%s-node-%03d'` at `cluster.py:970` and Shaken Fist's 63-character instance-name limit; refuse `bool` and non-`int` the way `validate_node_sizes()` does. List the reserved metadata keys once in `primitives.py` next to `K3S_VERSION_CACHE_KEY`, and have `Cluster.__init__` (`cluster.py:743`) raise `ClusterNameError.reserved` when `METADATA_KEY % name` is one of them. Unit tests in `tests/test_cluster.py` in the existing testtools style: every reason; mixed case accepted; 48 accepted and 49 refused; a dot, an underscore, a leading or trailing hyphen and an empty string refused; `True` and `2.0` refused as counts. Unit tests only. |
| 1b | high | opus | none | Call the validators. `create()` (`cluster.py:1921`) calls `validate_cluster_name(self.name)` and `validate_counts(control_plane_count=..., worker_count=..., metal_address_count=...)` with its other pure checks, before `primitives.get_k3s_release()`. `expand_workers()` and `expand_addresses()` call `validate_counts()` with a floor of 1 before they read the metadata. Update `test_a_name_in_the_cluster_list_with_no_metadata_is_still_taken` only if it uses a name the rule refuses. Add tests that each refusal makes no client call, using the fakes in `tests/fakes.py` the way the existing `NodeSizeError` tests do. Then the front doors: drop `click.IntRange(min=1)` from the six size options in `__init__.py:192-209`; in `collection/plugins/modules/sf_k3s_cluster.py` replace the floors loop at lines 602-628 with calls to the library validators, keeping the refusal before a client is built and keeping the `fail_json` shape, and add both exceptions to the catch tuple near line 698. `ShapeRangeTestCase`'s message assertions will change; update them to the library's wording without weakening what they check (still no `cluster_calls` or `client_calls`). Regenerate `tests/cli_contract/` snapshots by the method `docs/testing.md` gives. Run `tox -epy3`, not a bare `stestr run`: the module tests import the installed package (#106). Unit tests only. |
| 1c | high | opus | none | Replace `delete()`'s `kubectl config unset` loop (`cluster.py:2886-2937`) per decision 6: read the present user, context and cluster names with `kubectl config view -o json` (parse in Python; `capture_output=True` as now, for the same file-descriptor-1 reason the existing comment gives), then run `kubectl config delete-user`, `delete-context` and `delete-cluster` with `fqcn` as one argument, only for names present. First confirm the kubectl version that introduced `delete-user` (kubectl changelog or source) and report it; stop and say so if it is newer than v1.24. Rename `KubeconfigError.unset_failed` to a reason naming the delete command and keep the stderr carried on the exception. Replace the two long comments about `unset`'s grammar with one short comment about why names are passed literally, citing namespace names such as `cluster` as well as dotted names. Update `tests/test_library_api.py`'s subprocess fakes (around line 867) and add tests: a namespace called `cluster`; a second delete with the entries already gone doing nothing; a kubectl failure still raising `KubeconfigError`. Unit tests only; the live merge tier exercises the real kubectl on the next queue run, and the PR description should say so. |
| 1d | medium | sonnet | none | Documentation. In `docs/usage.md`: the cluster name rule and its reasons (Shaken Fist instance names, the reserved keys) under `create`; count floors on `create`, `expand-workers` and `expand-addresses`; the exit code change for out-of-range sizes; the kubectl floor from step 1c. In `docs/library-api.md`: both new exceptions in the exception table near line 291, and the validators if pure helpers are listed there. In `docs/collection.md` and the module's `DOCUMENTATION` block: the name rule on `name`. Leave `AGENTS.md` and `ARCHITECTURE.md` alone unless a convention or the system's shape changed; neither does here. |
| 1e | medium | sonnet | none | Add `tools/mutation-check.py` entries for the properties this defends rather than adds: the reserved-name refusal in `Cluster.__init__`, the create-time name check, and the zero floor on `control_plane_count`. Each entry names the test that must fail when the property is broken. Entries whose test lives in the Ansible module harness are marked to run through `tox -epy3`, as the existing ones are. Run the tool and record the result in the commit message. |

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

* #112: the module cannot pass node sizes or k3s configuration. Once it
  can, those values go through the validators this plan adds, so do it
  after this plan rather than alongside it.
* #72: a create interrupted between its two name-claiming writes. It
  touches the same `create()` preamble and the `delete()` not-found
  contract, but it is a separate decision.
* Shaken Fist itself does not stop a namespace name from matching a
  kubeconfig field name, and it has no reason to. Decision 6 makes that
  irrelevant here, so there is nothing to file upstream.
* Node serials above 99999 would exceed Shaken Fist's instance-name
  limit for a 48-character name. That is not a realistic count, and it
  is noted here only so the arithmetic in the comment is not mistaken
  for a guarantee.

### Bugs fixed during this work

This section should list any bugs we encounter during development
that we fixed. You should also scan the project's issue tracker,
where one exists, for directly related issues that we should
either resolve as part of this master plan or at least be aware of
while planning it.

* #96, in full, as widened by its audit comment.
* `delete()`'s kubeconfig cleanup fails for valid names in namespaces
  whose names match a kubeconfig field. This was found while planning,
  is not in #96's text, and is fixed by step 1c.

### Back brief

Before executing any step of this plan, please back brief the
operator as to your understanding of the plan and how the work you
intend to do aligns with that plan.
<!-- shared-block-end -->
