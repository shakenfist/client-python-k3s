# Lens 4c findings: tests

Returned as text by the step 4c lens (opus, high effort), as the plan's
decision 5 asks, and saved here by the management session. The
management session confirmed TC-1 (`ci_deploy_test.sh:540-543`), TC-2
(the template is generated per condition at `cluster.py:948`) and TC-9
(`ci_health_signals.py:463-466` and `:756`) against the worktree before
saving.

Scope: the union of `git diff <m>^1 <m>` for `78c9df1`, `00109a6` and
`bd9bead`, judged at `bd9bead`. Cited lines were checked with
`git blame`; none came from `243a91c` (#121). No file was mutated and no
suite was run; results rest on reading the code and `verification.md`.

**Summary.** The unit tests are strong and mock the boundary
throughout. The gaps are in the functional tier:

- `expand_workers()` waiting for Ready has no functional test (TC-1).
- Several report keys are never asserted live, so a regression that
  made them None would pass every live check (TC-3).
- One column swap in the go-template is pinned by nothing, unit or live
  (TC-2).
- One of phase 3 decision 5's refusals, the unregistered node, no longer
  holds (TC-4).

## Functional coverage, behaviour by behaviour

| Behaviour | Functional test that fails before and passes after |
|---|---|
| `signals`: `boot_id`, `k3s_unit`, `k3s_state`, `k3s_restarts`, `oom_kills`, memory, `etcd_bytes`, `etcd_snapshot_bytes`, `error` | 4a to 4e in `tools/ci_health_signals.py` |
| `signals.booted_at`, and `etcd_snapshot_bytes` None on a worker | **none** (TC-3) |
| `kubernetes.ready`, `disk_pressure`, `oom_killed` | 4a, 4b, 4d, 4f |
| `memory_pressure`, `pid_pressure` | 4f, only as `'False'`; a swap of the two columns is not caught (TC-2) |
| `registered: False`, `ready_since`, `unmatched_nodes` | **none** (TC-3, TC-4, TC-5) |
| Top-level `healthy` gaining "every node Ready" | 4d (`check_not_ready`) |
| `create()` waits for Ready | `ci_deploy_test.sh:675`, `health --strict` straight after the minimal create; race-dependent, not deterministic |
| `expand_workers()` waits for Ready | **none** (TC-1) |
| `health --strict` | 4d, in both directions |
| Probe budget and abandonment | not provoked (decision 5, still holds); 4a's `signals.error is None` proves a real node answers within the budget |
| Collection module's health output | none (#89), and thin unit tests (TC-10) |

## Findings

### TC-1 (fix): `expand_workers()` waiting for Ready has no functional test

- `tools/ci_deploy_test.sh:540-543`. `sf-client k3s expand-workers`
  (542) is followed at once by the script's own `wait_for_nodes 4`
  (543), which hides whether the verb waited. No `health --strict` runs
  afterwards before `remove-worker`.
- The main cluster's create has the same shape (`wait_for_nodes 3` at
  402 before `--strict` at 423). Only the minimal cluster's `--strict`
  at 675 pins create's wait, and that depends on timing.
- Fix: insert `sf-client k3s health "${CLUSTER}" --strict` between 542
  and 543, keeping `wait_for_nodes` as the independent count check.
  Needs a merge tier dispatch.
- Related (document): the `wait_for_nodes()` comment at 91-94 ("the
  newest node may not have yet [registered]") now describes a state
  these two verbs prevent. The lines predate this plan, but phase 2
  made the comment stale.

### TC-2 (consider): the condition column order in the go-template is unpinned

- `shakenfist_client_k3s/cluster.py:968-975`;
  `tests/test_cluster.py:4829-4845`. Nothing pins the order of the
  condition columns in `KUBERNETES_NODES_TEMPLATE` against
  `_KUBERNETES_RECORDS['node']`. Swapping MemoryPressure and
  PIDPressure passes every unit test and every live step.
- `test_every_condition_is_read` only counts occurrences, and
  `test_each_line_has_its_records_fields` only counts tabs. No Go engine
  renders the template in unit tests. 4f asserts both columns are
  `'False'`, so a swap changes nothing it reads. Other swaps are caught
  live.
- Fix: one unit test asserting
  `re.findall(r'eq \.type "(\w+)"', template)` equals the record's
  order, and the same for the `oom` line's fields. Optionally a mutation
  swapping the two columns.

### TC-3 (consider): several keys are never asserted live

- `tools/ci_health_signals.py:206-253` (`_baseline_node_problems`) and
  the step checks. The baseline does not check `booted_at`,
  `ready_since`, `unmatched_nodes`, whether `oom_killed` is a list, or
  the worker's `etcd_snapshot_bytes`. No step compares `booted_at`
  across steps or `ready_since` across 4d.
- Fix, all cheap: in the baseline, assert `booted_at` and
  `ready_since` are counts, `unmatched_nodes == []` (both CI cluster
  names are mixed case, which makes this a lowercasing check), and
  `oom_killed` is a list. Check `booted_at` unchanged wherever
  `_boot_problems` checks `boot_id`. After 4d's start, assert
  `ready_since` is later than the baseline's.

### TC-4 (consider): phase 3 decision 5's "unregistered node" refusal no longer holds

- The refusal says "the kubelet registers it again within seconds, so
  an assertion is a race". That is not true inside 4d's window, where
  `k3s-agent` is stopped.
- After `check_kubelet_silent` passes in `step_not_ready`
  (`ci_health_signals.py:735-739`): `kubectl delete node <worker>`, poll
  for `registered is False`, and assert `ready is None`, `healthy is
  False` and `--strict` exits 1. The existing start and
  `check_ready_again` then prove the node re-registers. k3s documents
  delete-and-restart as its rejoin path.
- Side effect: pod GC on the worker, which no later step depends on.
  Needs one dispatch to confirm.

### TC-5 (consider): `unmatched_nodes` via a hand-made Node object

- Decision 5 only considered deleting a Node object. A bare
  `{"kind": "Node", "metadata": {"name": "ci-ghost-<hex>"}}` applied with
  `kubectl apply` should appear in `unmatched_nodes` at once, and must
  leave `healthy` True: a deterministic check of both the listing and
  its "not judged" rule.
- Caveats: this rests on k3s's embedded cloud provider not deleting the
  node, and a DaemonSet pod (svclb) would sit Pending on it until the
  delete. Verify with one dispatch.

### TC-6 (consider): the CLI renderer's OOM and pressure-`True` branches never run on a real report

- `tools/ci_health_signals.py:670-716, 777-792`. `_health_exit_codes`
  is only called in 4d (739, 759). A renderer crash on a real
  `oom_killed` entry or `disk_pressure: 'True'` would go unnoticed.
- Fix: call `_health_exit_codes(name, 0)` in 4b after the entry is
  listed, and in 4f.

### TC-7 (consider): `disk_fill_command()`'s arithmetic and skip branch are untested

- `tools/ci_health_signals.py:594-612`;
  `tests/test_ci_health_signals.py:740-747`. The test runs only `sh -n`
  and checks for `'* 3 / 100'`. Live runs only take the fill branch.
- Fix: run the script with a fake `df` and `fallocate` on PATH, as
  `NodeSignalsCommandRunsTestCase` does for `systemctl`.

### TC-8 (consider): mutation entry gaps and overlaps

- `tools/mutation-check.py:783-830`. No two of the plan's entries
  falsify the same property.
- Overlaps: the `boot_id` entry is also pinned by
  `CheckOomKillTestCase.test_the_node_rebooted`. "expand-workers waits
  only for the workers it added" (635) names `ExpandWorkersTestCase`,
  which asserts on a patched `Cluster.await_nodes_ready`
  (`test_cluster.py:305`); the boundary-only
  `ExpandWorkersAwaitsNodesReadyTestCase.test_only_the_new_workers_are_waited_for`
  would be the better test to name. "health() sends exactly the
  Kubernetes probe command the fake answers" (477) fails most of
  `HealthKubernetesTestCase` by design.
- Gaps: no mutation covers `check_kubelet_silent` rejecting None
  (`NOT_READY_STATUSES`, line 127, pinned by
  `test_unread_is_not_an_answer`), `check_automatic_restart`'s exact +1,
  or `check_snapshot_saved`'s growth comparison.
- Fix: add `NOT_READY_STATUSES = ('False', 'Unknown', None)` as a
  mutation named against `CheckKubeletSilentTestCase`.
- Side note (none): the uncommitted-changes warning (879) checks only
  `PKG`, but entries now mutate `tools/` and `collection/` too.

### TC-9 (document): stale text after phase 3's live run

- `check_restarts_reset`'s docstring
  (`tools/ci_health_signals.py:463-466`) says the reset "has never been
  observed".
- Line 756 says "docs/library-api.md expects 0".
- `health()`'s docstring (`cluster.py:4228`) still hedges. Same as
  DOC-1.

### TC-10 (consider): the collection module's health result is thinly tested

- `collection/plugins/modules/sf_k3s_cluster.py:259-262, 584-602`;
  `tests/module_harness.py:301-315`. No module test pins that a NotReady
  node gives `health.healthy: false` with `changed: false` and no
  failure, which is the play gate the module's EXAMPLES document. None
  checks that `signals` and `kubernetes` reach the result. The module
  tests assert only `healthy` True (`test_ansible_module.py:455, 562`).
- Related: the harness fake returns every probe as `complete` at
  submission, and its `get_agent_operation` answers every uuid with the
  API probe's output. If submission were modelled as the real server
  does it, the Kubernetes probe would read the API output.
  `HealthClient` models this correctly; this fake does not.

### TC-11 (none): duplicate `oom` lines are both kept

`shakenfist_client_k3s/cluster.py:1339`. The parser appends without
de-duplicating, and `test_every_oom_kill_on_a_node_is_kept_in_order`
covers only two different containers. kubectl cannot print a duplicate.

### TC-12 (consider): the first-occurrence defence depends on the snapshot reading being printed last

`node_signals_command`; `tests/test_cluster.py:4241`. Only the
relative-directory branch pins the order (`endswith`). Nothing pins it
for an absolute directory, and no end-to-end run uses a snapshot
directory containing a newline followed by `boot_id=...`.
`test_the_first_occurrence_of_a_key_wins` is parser-only. Real risk is
low: the directory would have to exist on the node. Overlaps the
security lens.

## Existing issues rediscovered (not re-raised)

- **#101:** `health --strict` exiting 1 is now covered live by 4d.
  `update-os`, the `query-*` verbs and the census sync remain. TC-1 is a
  new gap, not #101.
- **#102:** `make_client()` with no arguments is exercised live. The
  root-option half remains.
- **#106:** affects the module harness phase 2 changed.
  `mutation-check.py`'s new `tools/` entries correctly use `stestr`.
- **#89:** no functional coverage of the collection module.
- **#105:** `parse_node_signals()` and `parse_kubernetes_readings()`
  assume `stdout` is a str or None; a non-str value from the agent
  raises out of `health()`.

## Checklist

- **Unit tests mock the boundary, not the code under test:** nothing
  found, apart from the `ExpandWorkersTestCase` patch in TC-8, which has
  a boundary-only twin.
- **Adversarial cases around agent and kubectl output:** truncated
  output, extra fields, non-integer counters, unusual case, duplicate
  node records and empty go-template output are all covered. Residuals
  are TC-11, TC-12 and #105.
- **`check_*` tests fail for the reason they claim:** nothing found.
  One minor untested guard: `check_automatic_restart` with a non-count
  `restarts_before`.
- **Mutation entries:** nothing redundant; see TC-8.
- **Skipped:** 0 under tox. Conditional skips (`_ToolTestCase`, the
  `*CommandRunsTestCase` classes, the TZ test) are reasonable.
  Go-template rendering is not unit-testable (TC-2 is the residual).
  Not live by decision: the configured `etcd-snapshot-dir`, the main and
  HA clusters (#110), and the declined provocations.
- **Phase 3 decision 5's refusals:** global OOM holds; Memory and PID
  pressure hold (PID pressure probably cannot be provoked without a
  `pid.available` eviction threshold, unverified for k3s); reboot holds,
  with `booted_at` in TC-3 as the cheap remnant; unregistered node does
  not hold (TC-4); probe failure holds. Missed cheap provocation: a
  ghost Node (TC-5).
- **Verification baseline:** nothing found.
