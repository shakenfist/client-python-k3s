# Cumulative health signals phase 3: live validation

## Prompt

Before responding to questions or discussion points in this document,
read [the master plan](PLAN-cumulative-health-signals.md), phase 1's
plan ([agent-read signals](PLAN-cumulative-health-signals-phase-01-agent-signals.md))
and phase 2's ([Kubernetes-read signals](PLAN-cumulative-health-signals-phase-02-kubernetes-signals.md)).
Then read `Cluster.health()`, `Cluster.execute_and_await()` and the
`signals` and `kubernetes` sections of `docs/library-api.md`, and
`tools/ci_deploy_test.sh` from the minimal cluster's create to its
delete. Ground every answer in what that code and the live runs
recorded in the two earlier phase plans actually show. Nothing in this
phase can be validated by unit tests alone. Its whole point is to
observe what systemd, the kernel and the kubelet do on a real node.
Say so wherever a step depends on it.

## Planning effort

High. The phase changes no product code. But each provocation depends
on how systemd, the kernel's OOM accounting, the kubelet's eviction
manager or the node lifecycle controller behaves on the k3s release the
merge tier happens to install. A wrong guess about any of them shows up
only as a failed 25 minute CI run. The provocations are planned here so
that the implementer does not have to work out that timing alone.

## Scope

In:

- Provoke, on a real cluster in the merge tier, every signal that
  phases 1 and 2 added and that can be provoked deterministically.
  Assert that `Cluster.health()` reports each one. Decision 4 lists
  them.
- Confirm or correct the two claims phase 1 took from source rather
  than observation: that systemd resets `NRestarts` when the unit is
  started by hand, and that `/proc/vmstat`'s `oom_kill` counts a pod
  killed for exceeding its own memory limit. Replace the hedged
  sentences in `docs/library-api.md` with whatever was observed.
- Exercise `sf-client k3s health --strict` exiting 1 on a cluster that
  really is unhealthy. Until now it has only been run in the direction
  that passes ([#101](https://github.com/shakenfist/client-python-k3s/issues/101)).

Out:

- Any change to `health()`, its return shape or `healthy`. If a
  provocation finds a bug in either, fix it on this branch only when
  the fix is small and plainly a defect, and record it under
  *Deviations*. Otherwise file it and decline the assertion.
- The provocations decision 5 declines.
- The other gaps #101 lists: `update-os`, the `query-*` verbs, and
  keeping the script in step with the command list.
- The root-option half of
  [#102](https://github.com/shakenfist/client-python-k3s/issues/102).
  Decision 2 covers its `make_client()` half as a side effect.

## What the survey found

### 1. The code is as phase 2 left it

`develop` at `00109a6` carries both phases. Their merge commits are
`78c9df1` and `00109a6`. The readings this phase provokes are:

- under each node's `signals`: `k3s_state`, `k3s_restarts`,
  `oom_kills`, `memory_total_bytes`, `memory_available_bytes`,
  `etcd_bytes`, `etcd_snapshot_bytes` and `boot_id`;
- under each node's `kubernetes`: `ready`, `disk_pressure` and
  `oom_killed` (each entry has `namespace`, `pod`, `container`,
  `restarts` and `finished_at`).

`healthy` at the top level requires every node `ready` to be `'True'`.
At node level, `healthy` does not look at Kubernetes at all.

### 2. Neither earlier phase saw a signal move

Phase 1's live run and phase 2's both recorded what a fresh cluster
looks like: 0 restarts, 0 OOM kills, every node Ready and no pressure.
They proved that the commands run and the templates render. Neither saw
a counter increase, a condition go `True`, or an `oom_killed` entry
appear. Phase 2's tests check that the go-templates render on a fresh
cluster, so its OOM and pressure branches have only ever run against
the fake.

### 3. The CLI has no machine-readable health output

`k3s_health()` (`shakenfist_client_k3s/__init__.py:358`) renders text
for a person and exits non-zero only with `--strict`. `_render_health()`'s
docstring tells callers that want to branch on the report to use the
library. A CI assertion on a single reading therefore either parses
rendered text or calls `Cluster.health()`. Parsing the text would make
the rendering a contract, which it is deliberately not.

### 4. CI can reach a node's shell only through the agent

`tools/ci_deploy_test.sh` uses local `kubectl` and `sf-client`, and it
runs nothing on a node. Two things can. `sf-client instance execute`
returns the operation, but the command line does not say whether it
waits. `Cluster.execute_and_await()` (`cluster.py:2446`) is the path
`create()` itself uses. It waits for the instances to go idle and
raises `CommandFailedError` on a non-zero exit. It returns no output,
and nothing in this phase needs any.

### 5. 33fl's tier 3 is not a reliable way to cause an OOM

The master plan's phase 3 row calls 33fl's `tools/k3s-health-check.py`
tier 3 "a ready-made way to provoke control plane OOM". Its own
docstring (`tier3_pod_churn()`) says otherwise. It found that an
identical 60-pod batch "had succeeded minutes earlier", that the same
workload "sometimes completes and sometimes OOMs", and that its
default batch size is conservative for exactly that reason. It also
spreads its pods over the workers, not the control plane. A CI
assertion cannot depend on a provocation that works some of the time.
I have corrected the master plan's row.

### 6. The minimal cluster is the right one to damage

The minimal cluster (`ci_deploy_test.sh:630`) has one control plane
node and one worker, at the 2048 MB default, with no MetalLB or
Longhorn. `node-taint: []` leaves its control plane schedulable. The
script exports its `KUBECONFIG` at line 676, and after that it only
runs read-only checks and the refusal tests until the delete at line
773. Nothing after line 773 reads its nodes. The main cluster carries
Longhorn and MetalLB, and stopping a worker's kubelet would upset both
in ways that have nothing to do with this phase.

### 7. The budget has room

The merge tier's `cluster_deploy` step has an 80 minute timeout
(`functional-tests.yml:255`). Phase 2's run took 18 minutes for the
whole job. Decision 4's sequence is bounded at about 12 minutes in the
worst case and should take about 4.

### 8. Tools are already unit tested by loading them from `tools/`

`tests/test_build_collection.py:35` loads `tools/build-collection.py`
with `importlib.util.spec_from_file_location()`. A new tool's pure
functions can be tested the same way.

### 9. Related issues

- [#101](https://github.com/shakenfist/client-python-k3s/issues/101):
  its second paragraph, "`health --strict` is only ever exercised in
  the direction that passes", is answered by decision 4d. The issue's
  other items stay open, so step 3c comments on the issue rather than
  closing it.
- [#102](https://github.com/shakenfist/client-python-k3s/issues/102):
  decision 2's tool calls `make_client()` with no arguments against
  the runner's real configuration, which is one of the two things the
  issue asks for. Step 3c comments on the issue. The root-option half
  is out of scope.
- [#106](https://github.com/shakenfist/client-python-k3s/issues/106):
  a bare `stestr run` tests the installed package for the Ansible
  module tests. This bit the end of phase 2's review, when a tox venv
  held a copy of the package that `tools/mutation-check.py` had
  mutated. It does not block this phase, but an implementer running
  stestr directly should reinstall first, or run `tox -epy3`.

## Decisions

### 1. The provocations run on the minimal cluster, last, and the last one is not undone

They go in one new step just before `status 'Delete the minimal
cluster'` (`ci_deploy_test.sh:773`), for survey finding 6's reasons.
Running last means the final provocation, disk pressure, which takes
the kubelet five minutes to clear, can be left in place for the delete
to clean up.

### 2. One Python tool reads `Cluster.health()` through `make_client()`

`tools/ci_health_signals.py CLUSTER_NAME` is one script, run as one
line of `ci_deploy_test.sh`. The project's rule is that anything
longer than a few lines lives in `tools/`, not in a workflow step. The
tool builds its client with `make_client()` and no arguments, then
calls `Cluster(client, name, client.namespace).health()`. That is the
documented library entry point, and it is what an Ansible play or a
daily poll calls. So the assertions read the same dict callers branch
on (survey finding 3), and the runner's real configuration goes
through `make_client()`'s auto-discovery (#102).

The tool can be run by hand against any throwaway cluster, so an
operator can repeat a failed CI run locally. Its docstring says it
damages the cluster it is pointed at.

### 3. Node commands go through `Cluster.execute_and_await()`

This is the agent path `create()` uses, so it has already been proven
live, and it adds no dependency. Every command starts with a word on
the agent's PATH (`systemctl`, `k3s`, `sh`), because the agent refuses
any other. A command that exits non-zero raises, and the tool lets the
exception end the run with its message.

### 4. What is provoked, in order

The worker means the minimal cluster's one worker, and the control
plane means its one control plane node. Both are found in the report
by `role`, never by name. Every wait is a poll of `health()` with a
bound, never a fixed sleep. When a wait gives up, it prints that
node's last `signals` and `kubernetes` entries and the top-level
`healthy`, `api` and `kubernetes`.

a. **Baseline.** Take one report and check it. `healthy` is True.
   On each node:
   - `boot_id` matches the UUID format, and `k3s_state` is `active`;
   - `k3s_unit` is `k3s` on the control plane and `k3s-agent` on the
     worker;
   - `oom_kills` and `k3s_restarts` are integers of 0 or more;
   - available memory is greater than 0 and no more than total
     memory;
   - `etcd_bytes` is greater than 0 on the control plane and None on
     the worker;
   - `ready` is `'True'` and `disk_pressure` is `'False'`.

   Keep the worker's `boot_id`, `oom_kills` and `k3s_restarts` as the
   baseline.

b. **A pod killed at its own memory limit.** Apply one pod with
   `kubectl apply -f -`, as 33fl's tier 3 does. The pod:
   - is called `ci-oom-<8 hex>` and lives in `default`;
   - sets `nodeName` to the worker, which bypasses the scheduler;
   - uses `restartPolicy: Never`;
   - runs one container, `hog`, on
     `registry.k8s.io/e2e-test-images/nginx:1.15-alpine`, with a
     `limits.memory` of `32Mi` and the command `['tail', '/dev/zero']`.

   `tail` buffers a line that never ends, so its memory grows without
   bound. The image is the one `ci-web` already uses
   (`ci_deploy_test.sh:429`), which avoids Docker Hub's rate limits.

   Poll `kubectl get pod` until the container's
   `state.terminated.reason` is `OOMKilled` (bound: 180 s). Then poll
   `health()` until the worker's `kubernetes.oom_killed` has an entry
   whose `namespace`, `pod` and `container` match (bound: 60 s). Then
   assert:
   - the entry's `finished_at` is an integer;
   - the worker's `oom_kills` is at least its baseline plus 1, and its
     `boot_id` is unchanged. This confirms the cgroup-kill claim;
   - the top-level `healthy` is still True, because an OOM kill does
     not affect it (phase 2 decision 6).

   Then delete the pod.

c. **An automatic restart.** On the worker, run `systemctl kill
   --kill-whom=main --signal=KILL k3s-agent`. The installer's unit
   has `Restart=always`, so systemd restarts it. Poll until the
   worker's `k3s_state` is `active` and `k3s_restarts` is its baseline
   plus 1 (bound: 120 s). Assert that `boot_id` is unchanged.
   `--kill-whom=main` kills only the k3s process. The unit's
   `KillMode=process` leaves the containers running.

d. **NotReady, and a restart by hand.** On the worker, run `systemctl
   stop k3s-agent`. Poll until the worker's `kubernetes.ready` is not
   `'True'` (bound: 180 s). The node lifecycle controller marks a
   silent kubelet `Unknown` once its grace period, 40 to 50 s
   depending on release, has passed. Then assert:
   - the top-level `healthy` is False;
   - the worker's node-level `healthy` is True (phase 2 decision 3);
   - the worker's `k3s_state` is `inactive`;
   - `sf-client k3s health NAME --strict` exits 1, and the same
     command without `--strict` exits 0. The tool runs both as
     subprocesses. This is #101's second paragraph.

   Then run `systemctl start k3s-agent`. Poll until `ready` is
   `'True'` (bound: 180 s), and assert `healthy` is True and `--strict`
   exits 0. Finally, check whether `k3s_restarts` is now 0, which is
   what the docs currently say happens. The value is printed either
   way.

   - If it is 0, the claim is confirmed and the assertion stays.
   - If it is not, the docs are wrong, not the code. Step 3b changes
     the assertion to what was observed, and step 3c corrects
     `docs/library-api.md`. Either way the assertion ends up pinning
     the observed behaviour, because what systemd does here is
     exactly what a new release could change.

e. **An etcd snapshot.** On the control plane, run `k3s etcd-snapshot
   save`. Assert that `etcd_snapshot_bytes` is now greater than its
   baseline. The minimal cluster sets no `etcd-snapshot-dir`, so this
   reads the default directory. The configured-directory branch stays
   covered by unit tests only, which is recorded under *Risks*.

f. **Disk pressure, which is not undone.** On the worker, run one
   `sh -c` command that uses `df` and `fallocate` to fill the root
   filesystem until 3% is free. k3s's kubelet evicts at 5% free, and
   the stock kubelet at 10%. Do not fill it further: the agent and the
   signals probe still need to work. Poll until the worker's
   `disk_pressure` is `'True'` (bound: 180 s). Then assert:
   - `ready` is still `'True'`;
   - the top-level `healthy` is still True, because pressure is
     reported, not judged (phase 2 decision 6);
   - `memory_pressure` and `pid_pressure` are `'False'`.

   The delete that follows removes the instance and the file with it.

The tool prints one line per step, with the readings it compared, so
the job log records what was observed even when every step passes.

### 5. What is not provoked, and why

- **Global OOM on the control plane, by any means** (survey finding
  5). It is non-deterministic by the account of the tool that found
  it. And the reading it would test, `oom_kill`, counts global kills
  by definition. The cgroup case was the one in doubt, and 4b covers
  it.
- **`MemoryPressure` and `PIDPressure`.** On a 2048 MB node, pushing
  memory toward the kubelet's 100 Mi eviction threshold is the same
  cliff edge as global OOM. Provoking PID pressure means a fork bomb
  on a shared runner. Both read through the same go-template branch
  as `DiskPressure`, which 4f covers.
- **A reboot, and so a new `boot_id`.** Everything a reboot would
  confirm is true by definition: `boot_id` is regenerated at boot, and
  `/proc/vmstat` and systemd's counters do not survive it. Testing it
  costs a reboot and a second agent wait for no new information. This
  is the decision a reviewer is most likely to dispute. The
  counter-argument is that callers' baseline rule rests on it. But
  that rule rests on kernel semantics, not on this plugin's code, and
  the plugin's half, reading the file, is already proven by every live
  run.
- **An unregistered node, and `unmatched_nodes`.** Deleting a Node
  object only opens a window of seconds before the kubelet registers
  it again. An assertion inside that window is a race.
- **A probe that does not answer.** Unit tests cover this. Provoking
  it means breaking kubectl on the control plane, which also breaks
  the `api` probe, so the assertion would not isolate anything.

### 6. Assertions in CI, permanently, not one run by hand

The master plan's row allowed either. These readings come from
systemd, the kernel and the kubelet, and the merge tier installs
whatever k3s release the channel currently resolves to. A check run
once by hand proves only that it worked on the day it ran. The cost is
about 4 minutes and some new timing-dependent waits on every merge.
Decision 4's generous bounds and printed readings are what keep those
waits from turning into a flaky test nobody can debug.

### 7. The tool's judgements are pure functions, unit tested

Each check in decision 4 is a function that takes a baseline report
and a current report and returns either nothing or a failure message.
The tool's main sequence only provokes, polls and calls those
functions. The functions are tested in
`tests/test_ci_health_signals.py` against hand-built reports, loading
the tool as survey finding 8 describes. `tools/mutation-check.py`
gains entries for the comparisons a quiet regression would break:
- the cgroup kill having to raise `oom_kills`;
- `boot_id` having to be unchanged;
- top-level `healthy` having to fall for a NotReady node while the
  node-level `healthy` stays True;
- pressure having to leave `healthy` alone.

The live behaviour itself can only be checked by step 3b's run.

## Step plan

| Step | Effort | Model | Isolation | Brief for sub-agent |
|------|--------|-------|-----------|---------------------|
| 3a | high | opus | none | Write `tools/ci_health_signals.py`, unit test its judgement functions, and call it from `tools/ci_deploy_test.sh`. Read decisions 1 to 7 of `docs/plans/PLAN-cumulative-health-signals-phase-03-live-validation.md` first. Decision 4 is the specification, provocation by provocation, with its bounds. The tool takes a cluster name. It builds its client with `make_client()` from `shakenfist_client_k3s.client` with no arguments, and reads reports with `Cluster(client, name, client.namespace).health()`. Node commands go through that `Cluster`'s `execute_and_await([uuid], [command])` (`cluster.py:2446`), which raises `CommandFailedError` on a non-zero exit. Every command must start with a word on the agent's PATH. Pod operations run as `subprocess` calls to `kubectl`, which uses the `KUBECONFIG` the script exports at `ci_deploy_test.sh:676`. Build the pod manifest as JSON piped to `kubectl apply -f -`, as 33fl's `tools/k3s-health-check.py` `tier3_pod_churn()` does. The `--strict` checks run `sf-client k3s health NAME [--strict]` as subprocesses and compare exit codes. Find the worker and the control plane in the report by `role` (`'worker'`, `'control_plane'`), never by name. Structure it as decision 7 says: one pure `check_*` function per assertion, each taking reports and returning None or a failure message; a `poll(predicate, bound, describe)` helper that re-reads `health()` every 5 seconds and, when the bound expires, raises with the node's last `signals` and `kubernetes` entries and the top-level `healthy`, `api` and `kubernetes`; and a `main()` that runs 4a to 4f in order, printing one line per step with the readings it compared. Decision 4d's `k3s_restarts == 0` check after the hand start must print the observed value whether it passes or fails, because step 3b relies on reading it. Do not catch exceptions to keep going: the first failure ends the run with exit 1. The module docstring says the tool damages the cluster it is pointed at, leaves the disk full, and exists to run immediately before that cluster's delete. In `tools/ci_deploy_test.sh`, add `status 'Provoke each health signal on the minimal cluster'` and `python3 tools/ci_health_signals.py "${MINIMAL_CLUSTER}"` immediately before `status 'Delete the minimal cluster'` (line 773), with a comment that points at this plan. Check whether the script runs from the repository root, and whether its `python3` is the venv's (`VENV=/tmp/venv-k3s-ci`, line 56, and the install at line 286). If either is not the case, call the tool by path and with the venv's interpreter. Add `shakenfist_client_k3s/tests/test_ci_health_signals.py`, loading the tool as `tests/test_build_collection.py:35` does. Test every `check_*` function: a report that passes, and for each condition the check enforces, a report that fails it with a message naming the reading. Also test that `poll()` gives up at its bound with the readings in its message, using a fake clock and a fake reader. Add `tools/mutation-check.py` entries for the four comparisons decision 7 names. `tox -epy3`, `tox -eflake8`, `pre-commit run --all-files` and `python3 tools/mutation-check.py` must pass. The tool's behaviour against a real cluster cannot be checked here, and must not be guessed at in a test: step 3b runs it. Commit subject: "Provoke each health signal in the merge tier." |
| 3b | -- | management session | -- | Live check. Push the branch and dispatch the merge tier (`functional-tests.yml`, `workflow_dispatch`) against it, as phase 2 step 2e did. Read the new step's output in the `cluster_deploy` log. Every step 4a to 4f must pass. If one fails because the plan guessed a behaviour wrong, such as a bound, a unit setting, an eviction threshold or `NRestarts`, change the tool to match what was observed. Record the change under *Deviations*, push, and dispatch again. If one fails because `health()` reports something wrong, apply the *Scope* rule: fix it here if it is small and plainly a defect, otherwise file it and decline that assertion. Record under *Live results*: the run URL; each step's printed readings; how long each poll took; and, explicitly, the observed `k3s_restarts` after the hand start and the `oom_kills` delta from the limit kill. Commit subject: "Record the live signal provocation run." |
| 3c | medium | sonnet | none | Bring the documentation in line with what step 3b observed; 3a and 3b have landed. Read *Live results* and *Deviations* in `docs/plans/PLAN-cumulative-health-signals-phase-03-live-validation.md` first. In `docs/library-api.md`, replace the sentence "systemd is understood to reset `NRestarts` when the unit is restarted by hand." (around line 494) with the observed behaviour stated as fact. Confirm or correct `oom_kills`' "including a pod exceeding its own memory limit" (around line 468) the same way. Neither should hedge any more. In `docs/testing.md`'s merge tier paragraph (around line 62), add one or two sentences saying that the minimal cluster is then damaged on purpose: a pod limit OOM kill, a k3s-agent kill, a stopped kubelet and a full disk, with `health()` asserted to report each, and `health --strict` asserted to exit 1 on the stopped kubelet. Update the paragraph's "A full run is 20-30 minutes" if 3b's run says otherwise. Do not change `AGENTS.md`, `ARCHITECTURE.md` or `README.md`: no convention or component changes. Then post one comment on each of #101 and #102, ending with "*(Assisted by Claude Code)*". The #101 comment says that `health --strict` exiting 1 is now covered live, links the run, and says the issue's other items remain. The #102 comment says that `make_client()` auto-discovery is now exercised live by the tool, and that the root-option half remains. Run `pre-commit run --all-files`. Commit subject: "Document the observed signal behaviour." |

## Risks and mitigations

- **A timing assumption is wrong, and the merge tier turns flaky.**
  The node lifecycle grace period, the kubelet's eviction cadence and
  systemd's `RestartSec` all vary by release. Mitigation: every wait
  is a poll whose bound is several times the expected time, so a slow
  run passes. A failed wait prints the readings, so a failure can be
  diagnosed from the log alone. Step 3b reads the actual times, and
  the bounds are tightened only if they are absurd.
- **The disk fill breaks the agent before the kubelet notices.** If
  the agent cannot work, the tool cannot see the pressure. Mitigation:
  3% left free, which is above k3s's 5% threshold for eviction but
  well clear of empty on a 50 GB disk (about 1.5 GB). The fill comes
  last, so a failure there loses nothing else. Step 3b's log shows
  whether the probe kept answering.
- **The OOM pod lands on the wrong node, or is killed some other
  way.** Mitigation: `nodeName` pins the pod. The tool waits for the
  container's own `OOMKilled` reason before it reads `oom_kills`, so
  a pod that failed to pull or was evicted fails the step with its
  real cause, not a misleading counter.
- **`NRestarts` does not reset on a hand start.** That would mean the
  docs are wrong, not the code. Mitigation: decision 4d and step 3c
  already plan for this result, and the assertion pins whatever was
  observed.
- **The configured `etcd-snapshot-dir` branch stays live-untested.**
  Neither CI cluster sets one. Accepted: the branch is a quoted path
  substitution, covered by phase 1's unit tests and mutation entries.
  Adding a snapshot directory to the minimal cluster's
  `--server-config` would cost one line, but it would also change
  what that cluster's other assertions are testing.
- **The tool's pure checks agree with the plan's assumptions, but the
  plan is wrong.** Unit tests cannot catch that. Mitigation: step 3b
  is the check, and the tool prints what it compared.

## Definition of done

- [ ] `tox -epy3`, `tox -eflake8` and `pre-commit run --all-files`
      pass. `python3 tools/mutation-check.py` reports every mutation
      caught, with more than the 85 on `develop` at `00109a6`.
- [ ] Nothing under `shakenfist_client_k3s/` except `tests/` changed,
      unless *Deviations* records why:
      `git diff develop --stat -- shakenfist_client_k3s ':!shakenfist_client_k3s/tests'`
      is empty.
- [ ] `tools/ci_deploy_test.sh` calls the tool exactly once, and the
      call is the last step before `Delete the minimal cluster`:
      `grep -n "ci_health_signals\|Delete the minimal cluster"
      tools/ci_deploy_test.sh` shows the two on consecutive `status`
      steps.
- [ ] *Live results* records a merge tier run against this branch in
      which every step from 4a to 4f passed. It quotes the printed
      readings, including the observed `k3s_restarts` after the hand
      start.
- [ ] No page hedges about `NRestarts` or cgroup OOM kills any more:
      `grep -rn "understood to" docs/library-api.md` finds nothing.
- [ ] `docs/testing.md` says that the minimal cluster is damaged on
      purpose, and that `--strict` is asserted to exit 1.
- [ ] #101 and #102 each carry a comment that links the run and says
      what is still open.

## Back brief

Before executing any step of this plan, back brief the operator on
your understanding of the plan and how the work you intend to do aligns
with it. Two points are cheap to agree now and expensive to change
after a 25 minute run:

- Decision 5, the provocations this phase declines, especially the
  reboot.
- Decision 6, keeping the assertions in CI on every merge.

Confirm both before step 3a starts.
