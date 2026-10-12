# Recoverable create: no name without state

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

Issue #72 says `Cluster.create()` claims a cluster's name in two writes:
first it appends the name to the namespace's `CLUSTER_LIST`
(`primitives.CLUSTER_LIST`, `orchestrated_k3s_clusters`), then it
writes the cluster's own metadata document. A create killed between the
two leaves a name that `create()` calls taken (`ClusterExistsError`) and
`delete()` calls nonexistent (`ClusterNotFoundError.does_not_exist`).
The only way out is to edit namespace metadata by hand.

On `develop` at `243a91c` the problem is broader than the issue says:

* **The window is wide.** The name is claimed at `cluster.py:2817`. The
  first `set_metadata()` is at `cluster.py:2926`. Between the two,
  `create()` allocates a node network and polls it every second until
  it reports `created` (`cluster.py:2820-2836`). The window lasts as
  long as network creation does.
* **It does not need an interruption.** Two failures inside the window
  are deterministic:
  * `create(network='typo')` claims the name, then raises
    `NetworkNotFoundError` at `cluster.py:2824`.
  * An unreadable or non-UTF-8 `sshkey` claims the name and allocates
    a network, then raises `SshKeyError.unreadable` at `cluster.py:2851`.
    The CLI's `click.Path(exists=True)` catches a missing file but not
    an unreadable one, and a library or Ansible caller has no such
    guard.

  So a typo is enough to burn a name. The comment at the top of
  `create()` (around `cluster.py:2724`) says the metadata is written
  "shortly after" the claim, and that `sshkey` "is read after the name
  and the network have been settled". The first claim is not true, and
  the second describes the bug as though it were a design choice.
* **It can leak a network.** When `create()` allocates the network
  itself, the network's uuid is recorded only in the metadata document.
  A create that stops before `cluster.py:2926` leaves a
  `k3s-<name>-node` network that nothing records.
* **The Ansible module calls the stuck state absent.** `_absent()` in
  `collection/plugins/modules/sf_k3s_cluster.py:578` asks
  `cluster.get_metadata() is None` and reports `changed=False`. The
  next `state: present` then fails with `ClusterExistsError`. The module
  claims a state that the next task disproves.
* **The docs call the window narrow.** `docs/usage.md:308-315` and
  `docs/library-api.md:335-348` both describe "the narrow window" and
  say no supported recovery exists.

What already works, and must keep working:

* `delete()` (`cluster.py:3538`) tolerates a metadata document that
  never left state `initial`: empty node lists, a missing network, a
  missing kubeconfig entry. `DeleteInterruptedClusterTestCase`
  (`tests/test_cluster.py:1019`) pins that.
* `delete()` already writes in the recoverable order: it releases the
  name, then removes the metadata. A second delete finishes an
  interrupted one, because `cluster.py:3700` tests for the list entry
  rather than assuming it. `DeleteReleasesTheNameBeforeTheMetadataTestCase`
  (`tests/test_cluster.py:4359`) pins that order.
* `create()` reads the metadata before the list (`cluster.py:2797-2813`).
  Metadata in any state other than `created` raises
  `ClusterInterruptedError.mid_create()`, whose message names
  `sf-client k3s delete <name>`.
* `test_a_name_in_the_cluster_list_with_no_metadata_is_still_taken`
  (`tests/test_cluster.py:1011`) pins today's behaviour so that this
  fix has an obvious test to update.
* `_owns_node_network()` (`cluster.py:3506`) already accepts a network
  named `k3s-<cluster>-node` as proof that create allocated it, for
  clusters built before `node_network_created` was recorded.
* `apiclient.get_network(network_ref, namespace=None)` accepts a name
  when the server has the `get-network-namespace` capability, so a
  network can be found by name within a namespace.

PR #120 (`longhorn-version-selection`) also edits `create()` before the
name claim: it moves the Longhorn lookup up. It is the open PR most
likely to conflict with this one, so this branch should rebase onto
`develop` once #120 merges.

## Mission and problem statement

Make every way a create can stop recoverable with `delete NAME`, and
stop the deterministic failures from reaching the API at all. The fix
has two halves:

1. **Prevent.** `create()` never advertises a name that has no metadata
   document. Every check that can fail before anything is built runs
   before the first write. The metadata document is written before the
   name joins the cluster list. An allocated network's uuid is recorded
   as soon as the API returns it.
2. **Recover.** `delete()` frees a name that is listed but has no
   metadata. Clusters stuck by earlier versions of this plugin exist
   now, and step 1 does nothing for them.

### Decisions

1. **Write the metadata before the list entry.** An interruption between
   the two then leaves a metadata document in state `initial` whose
   name is not listed. Every verb already handles that: `create()`
   raises `ClusterInterruptedError` and points at `delete`, and
   `delete()` runs to completion. This mirrors the order `delete()`
   already uses, and the reasoning in its comment at
   `cluster.py:3680-3693` is the same reasoning in reverse.
2. **Move the borrowed-network lookup and the ssh key read before any
   write.** Neither depends on anything created. The network lookup
   reads from the API, so it goes after `check_k3s_release()` rather
   than into the pure `validate_create_arguments()`. The ssh key read is
   local, so it goes beside the manifest read. Each gets a
   `..._registers_nothing` test in `tests/test_library_api.py`,
   alongside `test_an_invalid_size_registers_nothing` (line 511).
3. **Record the allocated network's uuid immediately.** Write
   `md['node_network']` with `set_metadata()` straight after
   `allocate_network()` returns, before the poll loop. When create
   allocates the network, the metadata document exists before the
   allocation, with `node_network: None` and `node_network_created`
   already set. A borrowed network is known before the first write, so
   that write already carries its uuid. The only window that still
   leaks is one API round trip. That leaks a network, not a name, and
   step 1b's by-name lookup recovers it.

   Step 1a found that the borrowed-network check never worked against
   a real server. `apiclient.get_network()` raises
   `ResourceNotFoundException` on a 404 rather than returning a falsy
   value, so `NetworkNotFoundError` was unreachable and callers got an
   exception that is not a `K3sClusterException`. Step 1a catches it
   and raises `NetworkNotFoundError`.
4. **`delete()` treats "listed, no metadata" as something it can clean
   up.** It removes the list entry and says plainly that it is freeing
   a name whose create never wrote any state. "Neither listed nor
   metadata" still raises `ClusterNotFoundError.does_not_exist`, so
   that contract is unchanged for every other caller.
5. **The Ansible module follows the library.** `_absent()` treats a
   listed name without metadata as present, so it calls `delete()` and
   reports `changed=True`.

Out of scope:

* Concurrent creates of the same name. The list read-modify-write was
  never atomic and this does not change that.
* Resuming a create (#48).

## Open questions

All three were settled on 2026-10-08, each as recommended. They are
kept here with their reasoning, and steps 1b and 1c implement the
answers.

1. **Should `delete()` remove an unrecorded `k3s-<name>-node` network
   when it frees a stuck name?** After decision 3, new creates cannot
   orphan a network without leaving metadata. Names stuck by earlier
   versions can still have one.
   *Decided:* yes. Look it up by name in the namespace and
   delete it if exactly one matches. `_owns_node_network()` already
   accepts that name as proof of ownership, and a stuck name cannot
   belong to a later cluster, because the stuck name is what stops a
   later cluster being created. If the server lacks
   `get-network-namespace`, report that a network may have been left
   behind and give its name, rather than failing the delete.
2. **What should `create()` raise for "listed, no metadata"?** Today it
   raises `ClusterExistsError`, whose message says only that the name
   is taken. That is the answer the issue complains about.
   *Decided:* `ClusterInterruptedError.mid_create(name, ...)`.
   It is an interrupted create, its message already names
   `sf-client k3s delete <name>`, and the Ansible module already turns
   it into "remove with state: absent". The state argument needs a
   value: `_interrupted_state()` already returns `'unknown'` for
   metadata with no state, and reusing that is the least new
   vocabulary. Check the class docstring and its readers in
   `__init__.py` and the module before settling it.
3. **Should `show`, `health`, `expand-*` and `remove-worker` say more
   than "does not exist" for a listed name without metadata?** They all
   raise `ClusterNotFoundError.does_not_exist`, and `k3s list` prints
   the name, so the operator is told both things at once.
   *Decided:* add one reason form to `ClusterNotFoundError`
   that names `delete NAME` as the way to free the name, and raise it
   from the shared metadata read. The exception class is unchanged, so
   a caller that catches `ClusterNotFoundError` is unaffected.

## Execution

This plan is one pull request. Phase 1 is the work. Phase 2 is the push
audit, which runs over phase 1's branch diff against `develop` before
the branch is pushed. Both phases land together, so the plan closes
itself out in that pull request and records no `Merged` cell.

| Phase | Plan | Status | Merged |
|-------|------|--------|--------|
| 1. Recoverable create | This file, steps below | Complete | |
| 2. Push audit | `PUSH-AUDIT.md` over phase 1's diff against `develop`, before push | Complete | |

Plan phase 1 at high effort. It changes the order of create's writes to
namespace metadata, which this project's guidance names as a
high-effort area.

### Push audit result

Run on 2026-10-12 over `1326a3b..e5acb4a`, the whole of phase 1, by
three reviewers: code quality and correctness, tests and security, and
documentation. They raised twelve findings that were fixed on this
branch before the push, in `0cefa5d..be09ee5`. One was declined.

* **Security.** Freeing a stuck name deleted any `k3s-<name>-node`
  network found in the namespace, even one another cluster recorded as
  its borrowed network. An operator who found the orphan and passed it
  to a new cluster with `--network` would have lost it, and the server
  deletes a network that still has interfaces. The network is now left
  in place, and the cluster that records it is named. The orphan delete
  also passes the namespace, so the server refuses a uuid outside it.
* **A leak the plan said was recovered was not.** A create stopped
  between allocating its network and recording the uuid left a document
  whose `node_network` was empty. `delete()`'s main path skipped the
  network, so it leaked for good, and the next create of that name made
  a second network with the same name. `delete()` now looks the network
  up by name in that case, through the same checked helper the
  stuck-name path uses.
* **A race.** "No metadata" came from a possibly cached read and
  "listed" from a later one. A delete racing a new create could have
  destroyed the new cluster's network. Both are now decided from one
  fresh read, and a document that has appeared since takes the normal
  delete path.
* **A cluster list that is not a list.** `sf-client namespace
  set-metadata` stores a string, and a string was matched by substring:
  `show ana` matched `banana`, and `delete ana` deleted `k3s-ana-node`
  before crashing. `primitives.cluster_list()` now validates the value,
  and every reader goes through it.
* **Error types.** A borrowed network name that matches several
  networks now raises `NetworkNotFoundError.ambiguous()` instead of an
  apiclient exception.
* **Tests.** The orphan network's name guard could not fail, because
  the fake was keyed by name. A test and a mutation-check entry now
  cover it. The mutation check has nine new entries and catches all
  139.
* **Functional coverage.** No live test would have failed before this
  change. `tools/ci_deploy_test.sh` now checks that a create naming a
  missing network is refused and leaves no name listed. It also sets up
  a stuck name with `tools/ci_seed_stuck_name.py` and checks that
  `delete` frees it and removes its orphan network. That is the only
  real run of the network lookup by name within a namespace. Neither
  block has run yet; the merge queue will be their first run.
* **Code and docs quality.** Small fixes:
  * `_listed()` now reuses `primitives.list_clusters()`.
  * The message for a stateless name now names both causes.
  * A double-quoted string is now single-quoted.
  * The module uses `cluster.namespace`.
  * A plan reference was removed from a test docstring.
  * The history of how a name gets stuck, retold about ten times, is
    now told once, in `delete()`'s docstring.
  * The docs now describe the ownership check, the network recovery and
    the two new exception reasons.

**Declined:** splitting `cluster.py`. It was already far past the
size threshold, this branch adds about 4%, and a split belongs in its
own change. The seam is recorded under Future work.

### Phase 1 steps

| Step | Effort | Model | Isolation | Brief for sub-agent |
|------|--------|-------|-----------|---------------------|
| 1a | high | opus | none | Reorder `Cluster.create()` in `shakenfist_client_k3s/cluster.py` per decisions 1-3. (1) Move the `sshkey` read (now around line 2845) up beside `validate_create_arguments()`, keeping its `SshKeyError.unreadable` handling. (2) Move the borrowed `network=` lookup (`get_network()` plus `NetworkNotFoundError`, around line 2822) to just after `check_k3s_release()` and before the name check. (3) Build the metadata dictionary before the name is claimed, with `node_network` set to the borrowed network's uuid or None, and `set_metadata()` it before the `CLUSTER_LIST` append. Keep the existing "metadata before list" read order at lines 2797-2813, and reuse the `md` variable name with care: today it holds the read result there. (4) After `allocate_network()` returns, set `md['node_network']` and `set_metadata()` before the poll loop. Rewrite the comment at the top of the method (around 2724) and the `# Ensure this name isn't already taken` block so they describe the new order and why. Update `ClusterExistsError`'s docstring if it describes the order. Tests: in `tests/test_library_api.py`, add `test_a_missing_network_registers_nothing` and `test_an_unreadable_sshkey_registers_nothing`, following `test_an_invalid_size_registers_nothing` (line 511), and assert that no `CLUSTER_LIST` write, no metadata write and no network allocation happened. In `tests/test_cluster.py`, use `ActionLogClient` (line 536) or a similar ordered log to assert that the metadata write precedes the list write, and that the allocated network's uuid is written before the first poll. Check that the metadata read-count assertions the `get_metadata()` docstring mentions (line 1290) still hold, or update them deliberately. Run `tox -epy3`, not a bare `stestr run` (#106). Unit tests only; the merge tier's create exercises the new order live on the next queue run. |
| 1b | high | opus | none | Teach `delete()` (`cluster.py:3538`) to free a listed name without metadata, per decision 4 and the answers to open questions 1-3. When `get_metadata()` is None, read the namespace's `CLUSTER_LIST`. If the name is listed, write one plain line through `self.reporter.write()` saying it is freeing a name whose create never recorded any state, then (per question 1) look for a `k3s-<name>-node` network in the namespace with `get_network(name, namespace=...)` and delete it if found, handling `IncapableException` and `ResourceNotFoundException`. Finally remove the list entry using the same delete-or-set logic as lines 3698-3711. Factor that into a helper rather than copying it. If the name is not listed, raise `does_not_exist` as now. Apply question 2's answer in `create()`, with state `'unknown'`, and question 3's to the shared not-found path used at lines 3160 and 3382 and in the other verbs, adding a reason classmethod in `exceptions.py` beside `does_not_exist`. Replace `test_a_name_in_the_cluster_list_with_no_metadata_is_still_taken` (`tests/test_cluster.py:1011`) with a test of the new exception. Add to `DeleteInterruptedClusterTestCase` (line 1019): a listed name without metadata is unlisted; the last such name deletes the list key; an orphaned `k3s-<name>-node` network is deleted; a server without the namespace capability still unlists the name; and an unlisted name without metadata still raises `does_not_exist`. Unit tests only. |
| 1c | medium | sonnet | none | In `collection/plugins/modules/sf_k3s_cluster.py`, make `_absent()` (line 578) treat a name in `primitives.list_clusters(client, namespace)` as present even when `get_metadata()` is None, so it calls `cluster.delete()` and reports `changed=True`; check mode reports `changed=True` without deleting. Check the present path's handling of whichever exception step 1b settled on, and make sure its message tells the play to use `state: absent`. Add module tests in `shakenfist_client_k3s/tests/test_ansible_module.py` in the existing harness style. Run them with `tox -epy3`: the harness imports the installed package (#106). Add a collection changelog fragment if the collection keeps them. Unit tests only. |
| 1d | medium | sonnet | none | Documentation. Rewrite the #72 paragraphs in `docs/usage.md` (around lines 308-323) and `docs/library-api.md` (around lines 335-348) to describe the new behaviour: create writes its state before claiming the name, every way it can stop is recovered by `delete NAME`, and `delete` frees names stuck by earlier versions. Remove "narrow window" and "no supported way". Update `docs/collection.md` if it describes `state: absent` for interrupted clusters. Rewrite the comment in `delete()` at `cluster.py:3680-3693` so that it no longer says the create side is unrecoverable. Leave `AGENTS.md`, `ARCHITECTURE.md` and `README.md` alone: no convention or component changes. |
| 1e | medium | sonnet | none | Add `tools/mutation-check.py` entries for the properties this change defends: metadata is written before the list entry (swap the two writes; the step 1a ordering test must fail); a missing borrowed network registers nothing (move the lookup back after the claim); and `delete()` frees a listed name without metadata (restore the unconditional `does_not_exist`). Entries whose test lives in the Ansible module harness run through `tox -epy3`, as the existing ones do. Run the tool and record its result in the commit message. |

One commit per step. Each must pass `tox -epy3`, `tox -eflake8` and
`pre-commit run --all-files`, and the plugin must still import cleanly.
The pull request closes #72.

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

* The node network poll in `create()` loops until the network reports
  `created`, and has no deadline and no exit on `error`. A network the
  API fails to create hangs `create` indefinitely. This is a separate
  bug, worth its own issue.
* Concurrent creates of one name both pass the list check. The
  read-modify-write on `CLUSTER_LIST` is not atomic, and this plan does
  not change that.
* Resuming an interrupted create rather than tearing it down is #48.
* `delete()`'s main path calls `delete_network()` without
  `namespace=`. The new orphan path passes it, so the server refuses a
  uuid outside the namespace. Passing it on the main path too would
  harden against a tampered `node_network`, but first check that every
  older cluster's network was allocated in the cluster's own namespace.
* With a malformed cluster list, `delete()`'s main path tears down the
  instances and network before `_unlist()` refuses the list. The
  document is left in state `deleted` and recovers once the list is
  repaired, but checking the list before the teardown would be cleaner.
* The ssh key being read before the first write has no mutation-check
  entry. Breaking it means moving code across several statements rather
  than making a minimal edit, so only the step 1a tests guard it. They
  were shown to fail against the old `create()`.
* `cluster.py` grew by about 4% and was already far past the size
  threshold. The teardown path (`_owns_node_network()`, `delete()`,
  `_unlist()`, `_free_unrecorded_name()`, the orphan network helper and
  the kubeconfig cleanup) is an existing seam for a split, in its own
  change.
* `collection/requirements.txt` asks for `shakenfist_client_k3s>=0.3.0`,
  and 0.3.0 is unreleased (the latest tag is v0.2.0), so it already
  covers the library behaviour the module now relies on. If 0.3.0 is
  released before this merges, raise the floor.

### Bugs fixed during this work

This section should list any bugs we encounter during development
that we fixed. You should also scan the project's issue tracker,
where one exists, for directly related issues that we should
either resolve as part of this master plan or at least be aware of
while planning it.

* #72, and the deterministic triggers found while planning it: a
  missing `network=` and an unreadable `sshkey` both claimed the name
  before failing.
* The Ansible module's `state: absent` reported a stuck name as absent.
* `NetworkNotFoundError` could never fire against a real server.
  `apiclient.get_network()` raises `ResourceNotFoundException` on a 404,
  so a typo in `network=` escaped as an apiclient exception past the
  command line's group handler. The unit fakes returned None, which is
  why no test noticed. A name matching several networks (a 400) now
  raises `NetworkNotFoundError.ambiguous()` as well.
* Stuck names had a second cause. Before 46485f9, `delete()` removed
  the metadata document before it unlisted the name. Every release
  contains that fix, so only pre-release installs are affected.
  `delete()` frees those names too, and removes their kubeconfig
  entries when asked.
* The Ansible module's `state: present` reported `changed=True` and
  "may be partly built" for a create refused before it wrote anything.
* A cluster list stored as a string, which is what
  `sf-client namespace set-metadata` writes, was matched by substring:
  `show ana` matched `banana`. It is now refused with
  `ClusterMetadataError.not_a_cluster_list`.

Related issues to be aware of: #48 (resumable create) builds on the
recovery this plan makes complete. PR #120 edits the same part of
`create()` and should merge first.

### Back brief

Before executing any step of this plan, please back brief the
operator as to your understanding of the plan and how the work you
intend to do aligns with that plan.
<!-- shared-block-end -->
