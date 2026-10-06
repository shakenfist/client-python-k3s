# Cumulative health signals phase 1: agent-read signals

## Prompt

Before responding to questions or discussion points in this document,
read `Cluster.health()`, `_node_health()`, `_probe_k3s_api()`,
`_unprobed()`, `await_execute()` and the agent operation state constants
in `shakenfist_client_k3s/cluster.py`, the two shell rules above
`heredoc()` in the same file, `k3s_health()` and `_render_health()` in
`shakenfist_client_k3s/__init__.py`, `HealthClient` in
`shakenfist_client_k3s/tests/fakes.py`, and `HealthTestCase` and
`HealthProbeIsSkippedTestCase` in `shakenfist_client_k3s/tests/test_cluster.py`.
Ground answers in what that code does today. The master plan is
[PLAN-cumulative-health-signals.md](PLAN-cumulative-health-signals.md);
its open questions 1, 2 and 3 are answered here, in the decisions below,
and the master plan now points at those answers rather than restating
them.

## Planning effort

High. The phase changes the shape of a documented return value that an
Ansible module and its plays branch on, multiplies the number of agent
operations `health()` submits from one to one per node, and reads
counters whose reset semantics can only be confirmed on a live node.

Review effort: high for step 1c, which wires the probes into
`health()` and owns the wall clock bound; medium for the rest.

## Scope

In:

- A read-only, per-node probe run through the agent on every node that
  `_node_health()` already judges able to answer, reporting: the boot
  id and boot time, the k3s systemd unit's active state and restart
  count, the kernel's OOM kill count, total and available memory, and
  -- on control plane nodes -- the size of the embedded etcd data
  directory and its snapshot directory.
- Those readings in `health()`'s report, under a new per-node
  `signals` key, as raw values.
- Rendering them in `sf-client k3s health`.
- Documenting them in the method docstring, `docs/usage.md`,
  `docs/library-api.md`, the `sf_k3s_cluster` module's `RETURN`
  block, and `docs/collection.md` where it describes the return.
- One live run showing real readings on both node roles.

Out:

- Anything read through the Kubernetes API -- pod `OOMKilled`
  terminations, the node `Ready` condition from
  [#76](https://github.com/shakenfist/client-python-k3s/issues/76).
  That is phase 2.
- Provoking each signal and asserting the verb reports it, and any new
  assertion in `tools/ci_deploy_test.sh`. That is phase 3.
- Any change to what `healthy` means, at node or cluster level (see
  decision 3).
- Thresholds, warnings or judgements of any kind (decision 3).
- Storing previous readings anywhere (decision 2).
- Recovery. The master plan's non-goal stands; see decision 10.
- Passing a short agent operation deadline so that an abandoned probe
  expires sooner. The client does not support it in any released
  version (survey finding 5).

## What the survey found

The master plan's phase 1 section was written as a placeholder before
anything was planned, so every claim it makes was checked against the
tree at `3c4ddd8`. Where a claim was wrong it has been corrected at its
source -- the master plan's Situation, open questions and Execution
table, and the plan's row in `docs/plans/index.md` -- in the same commit
as this file. A later step does not need to redo that.

### 1. The code is as described, at new lines

`Cluster.health()` is at `cluster.py:2511`, not `:1603`, and
`_node_health()` at `cluster.py:1401`, not `:849`; the node
customisation phases moved both. Each node entry still carries exactly
`uuid`, `role`, `name`, `exists`, `state`, `agent_state` and `healthy`
(`cluster.py:1418-1426`). The master plan now names the methods rather
than lines that will move again.

`health()` submits exactly one agent operation today, `kubectl get
nodes` on the first control plane node, through `_probe_k3s_api()`
(`cluster.py:1264`), and only when that node's entry is healthy
(`cluster.py:2635`). Workers are never asked anything through the
agent.

### 2. Workers do not run a unit called `k3s`

The master plan names `systemctl show k3s -p NRestarts` as the single
best signal. That is right on servers only. `install_k3s_component()`
runs the installer as `sh -s - <role>` (`cluster.py:1716-1727`), and
the k3s installer names the unit `k3s` for `server` and `k3s-agent`
for `agent`. On a worker, `systemctl show k3s` reports `LoadState=not-found`
and `NRestarts=0`, which reads as "never restarted". Decision 5 picks
the unit by role.

### 3. The journal is no more a counter than `dmesg` is

The master plan moved the OOM count from `dmesg` to `journalctl -k`
because the ring buffer wrapped and the `dmesg` count went down. The
journal has the same flaw on a longer timescale: journald vacuums old
entries when it reaches its size limit, so a count over it can also
decrease, and on a volatile journal it resets at every boot. Counting
also means scanning and pattern matching the journal inside a 30 second
budget on a node that may be short of memory.

The kernel keeps the counter itself. `/proc/vmstat` has carried
`oom_kill` since Linux 4.13, incremented in `__oom_kill_process()` for
every OOM kill. It is O(1) to read, monotonic within a boot, and resets
only at reboot -- which the boot id detects. Debian 12's kernel is 6.1.
Decision 4 uses it. One consequence is worth stating: it counts every
OOM kill, global and cgroup alike, so a pod killed for exceeding its own
limit increments it too. Telling those apart is what phase 2's
`OOMKilled` reading is for.

### 4. The cluster always runs embedded etcd, at a fixed data directory

`install_control_plane()` sets `cluster-init` on the first server
(`cluster.py:1536`), so every cluster this plugin builds runs embedded
etcd and every server node carries an etcd member. `data-dir` is in
`K3S_SERVER_OWNED_KEYS` (`cluster.py:218`), so the data directory is
always `/var/lib/rancher/k3s/server/db/etcd`. `etcd-snapshot-dir` is
*not* owned: a caller's `server_config` can move snapshots, and it is
recorded in metadata as `server_config` (`cluster.py:2238`). Decision 6
honours it.

### 5. The client can bound an agent operation, but not in any release

`shakenfist_client`'s `instance_execute()` gained a `deadline_seconds`
argument in client-python commits `0b39248` and `95c5371`, which no
release tag contains; the 0.8.3 the tox environment installs takes only
`(instance_ref, command_line)`. This package's floor is `>= 0.7.7`.
Passing a short deadline would let an abandoned probe expire in seconds
rather than after the server's 600 second default, which matters more
now that a probe is submitted per node. It is recorded in the master
plan's Future work rather than done here, because doing it means raising
the client floor to a release that does not exist yet.

### 6. The fake cannot tell two commands apart

`HealthClient.instance_execute()` (`tests/fakes.py:287`) answers every
command with the same scripted `probe_*` result. Once `health()` runs
two different commands it must route by command line, and its
`max_agent_operation_reads` guard (60 reads) has to allow for one
pending operation per node rather than one in total.

### 7. Related issues

- [#76](https://github.com/shakenfist/client-python-k3s/issues/76),
  `health()` reports healthy while k3s nodes are NotReady. A per-node
  reading from the Kubernetes API that the issue proposes folding into
  `healthy`. It belongs with phase 2's API-read signals, and the master
  plan's phase 2 row now says so.
- [#105](https://github.com/shakenfist/client-python-k3s/issues/105),
  agent operation results are trusted in some places and guarded in
  others. The new probe reads `results` too; decision 9 makes it share
  the guarded reader `_probe_k3s_api()` already has rather than add a
  sixth reader. It does not fix #105's two unguarded call sites.
- [#101](https://github.com/shakenfist/client-python-k3s/issues/101),
  `health --strict` has no coverage in `ci_deploy_test.sh`. Untouched;
  phase 3 is where this plan adds live assertions.

### 8. Open question 5 has been answered by scheduling

Open question 5 asked whether any of this is worth doing before the
cluster it was found on is rebuilt at a sane size. Node customisation
has landed (its push audit merged as `4a6f38e`), so clusters can now be
sized, and the operator has chosen to start this plan anyway. The master
plan records the answer.

## Decisions

### 1. Signals live on each node, under `signals`

Every reading this phase takes is a property of one machine, so it goes
on that machine's entry in `nodes`, as a nested dict under a new
`signals` key, rather than in a new top-level map keyed by uuid. Nested
rather than flat beside `agent_state` so that the seven existing node
keys stay exactly as they are, and so that "was this node probed, and if
not why" has one place to live, as it does for `api`.

`signals` is present on every node entry, always with the same keys, in
every outcome: probed, skipped because the node is unhealthy, skipped
because the instance is gone, or failed. That is the rule `_unprobed()`
already follows for `api`, for the same reason: a caller must not have
to work out what happened from which keys exist.

```python
'signals': {
    'probed': bool,                 # the command ran at all
    'error': str or None,           # why not, or why it failed
    'boot_id': str or None,         # /proc/sys/kernel/random/boot_id
    'booted_at': int or None,       # /proc/stat btime, Unix seconds
    'k3s_unit': str,                # 'k3s' or 'k3s-agent', by role
    'k3s_state': str or None,       # systemd ActiveState
    'k3s_restarts': int or None,    # systemd NRestarts
    'oom_kills': int or None,       # /proc/vmstat oom_kill, since boot
    'memory_total_bytes': int or None,
    'memory_available_bytes': int or None,
    'etcd_bytes': int or None,      # control plane only
    'etcd_snapshot_bytes': int or None,
}
```

A reading which could not be taken is `None` on its own; it does not
void the others, and it does not set `error`. `error` is for the probe
as a whole -- not run, timed out, operation failed, non-zero exit.
`k3s_restarts` and `k3s_state` are `None` when the unit's `LoadState` is
not `loaded`, because `systemctl show` reports `NRestarts=0` for a unit
that does not exist and zero is a claim. `etcd_*` are always `None` on
workers. Memory is converted from `/proc/meminfo`'s kB to bytes so that
every size in the report is in one unit.

Phase 2 inherits the principle rather than a structure: a reading that
belongs to a node goes on the node.

### 2. Raw cumulative values; the caller diffs; `health()` stays read-only

This answers the master plan's sharpest question, and it is the decision
a reviewer is most likely to push back on, because the friendlier
alternative -- the plugin remembers the last reading in cluster metadata
and reports "3 restarts since you last asked" -- is genuinely friendlier.

It is declined because it makes `health()` write. `health()` is the verb
you reach for when you do not trust the cluster, `HealthClient` exists
to assert it makes no metadata writes at all, and the Ansible module
calls it on every `state: present` run. A health check that writes has
three new failure modes (the write fails; two pollers race and each
sees half the delta; a library caller's dry-run reasoning stops holding)
on the verb whose whole contract is that it has no side effects beyond
its probes. It would also define "since when" as "since anyone last
called it", which is meaningless when the CLI, a daily poll and an
Ansible play all call it.

What the caller needs to diff correctly is a way to know when a
baseline is void, so the report carries it:

- `boot_id` changes at every boot, and every reading here except the
  etcd sizes resets at boot. A changed `boot_id` means: discard the
  baseline, and treat the current value as the delta. `booted_at` is
  the same fact in a form a human can read, and an unexpected reboot is
  itself a finding the current report cannot show.
- A counter lower than its baseline with an unchanged `boot_id` has been
  reset by some other route -- systemd is understood to clear
  `NRestarts` when an operator restarts a unit by hand, which phase 3
  confirms. The same rule applies.

That rule is short enough to document once, in `docs/library-api.md`,
and the master plan's mission is satisfied by a daily poller that stores
yesterday's report.

### 3. Facts, not opinions; `healthy` does not change

No reading feeds into the node's `healthy` or the report's `healthy`,
and nothing carries a threshold. `healthy` is what the Ansible module's
documented `that: cluster.health.healthy` gate and `--strict` branch
on. A restart count with no baseline cannot be judged -- 33fl's cluster
carries restarts from first-boot ordering races 25 days old, and its
health check had to grow a pod age heuristic to stop crying wolf about
them. `MemAvailable` without a workload-specific threshold cannot be
judged either. A failed signals probe on a node that is otherwise up
also leaves it healthy: the probe failing is reported in
`signals['error']`, and folding it in would let a slow `du` flip
`--strict`.

The judgement belongs to the caller, who knows its baseline and its
workload. Changing `healthy` is #76's question, for phase 2, where the
reading in question is current state rather than history.

### 4. The OOM count is `/proc/vmstat`'s `oom_kill`

Not `journalctl -k`, as the master plan proposed, for survey finding
3's reasons: it is the counter, it is O(1), and it resets only when
`boot_id` changes. The docstring and docs say it counts all kernel OOM
kills, cgroup limit kills included.

### 5. The k3s unit is chosen by role

`k3s` for control plane nodes, `k3s-agent` for workers (survey finding
2), and the report names which it read in `k3s_unit`, so that a reader
does not have to know the installer's naming. The command reads
`LoadState`, `ActiveState` and `NRestarts` in one `systemctl show`.

### 6. etcd is read on control plane nodes, honouring `etcd-snapshot-dir`

`du -sb` of `/var/lib/rancher/k3s/server/db/etcd`, and of the snapshot
directory: the recorded `server_config`'s `etcd-snapshot-dir` when it
has one, otherwise `/var/lib/rancher/k3s/server/db/snapshots`. Metadata
written before `server_config` existed is read as `{}`. The
caller-supplied path is quoted with `shlex.quote()`, per rule 1 at the
top of `cluster.py`; no other value is interpolated into the command.
A directory that does not exist reports `None`, not 0.

### 7. One probe per eligible node, always on

The same gate as the API probe: a node is probed only when its entry is
`exists` and `healthy`, because an agent operation queued against a
disconnected agent never leaves its queued state. A skipped node's
`signals` says why, in the same words `health()` uses for a skipped API
probe.

There is no flag to turn the probe off. A verb with modes is the "shape
grown rather than decided" problem the master plan's open question 1
warned about, and the cost -- one short read-only command per healthy
node, run concurrently under the existing budget -- is small. The
visible cost is that an abandoned probe now leaves up to one queued
operation per node rather than one in total, which delays a later
`expand_workers()` or `update_os()` by at most the server's deadline;
the docs that describe the single abandoned probe today are updated to
say so.

### 8. One deadline for every probe, with `kubectl` submitted first

`health()` takes one deadline, `HEALTH_PROBE_TIMEOUT_SECONDS` from
before its first submission, and every probe -- the `kubectl` one and
every node's signals -- is waited for against it. Everything is
submitted before anything is waited for, so the bound on a cluster of
any size stays at one budget, not one per node. The `kubectl` probe is
submitted first, so that on the first control plane node, where both
run, the established finding is not queued behind a `du`.

This shifts the `kubectl` probe's budget to start slightly earlier, by
the time it takes to submit the other operations. That is a few API
calls and is accepted.

### 9. Both probes share one submit-and-collect pair

`_probe_k3s_api()` currently submits, waits, and turns every possible
ending into a dict -- API refusal, still pending, failed, an unknown
state, completed with no result, non-zero exit -- with a comment on each
branch. The signals probe needs every one of those branches. Rather than
copy them, step 1a splits `_probe_k3s_api()` into a submit half and a
collect half that take an instance and command and a deadline, with no
behaviour change, and the signals probe is built on the same pair. The
`kubectl`-specific wording in the error messages takes the command as a
parameter. Existing `HealthTestCase` and `HealthProbeIsSkippedTestCase`
tests pass unchanged; that is step 1a's acceptance test.

### 10. Recovery gets its own plan, later

Open question 4. Recovery is not this plan: the master plan's non-goal
stands, and the two roles need different answers -- a worker is already
replaceable, a control plane node is not replaceable at all. It is
recorded in the master plan's Future work as a candidate plan of its
own, to be proposed once this plan's signals have shown how often it is
needed.

### 11. No CI assertions in this phase; one live run instead

The merge tier's `tools/ci_deploy_test.sh` already runs `sf-client k3s
health --strict` on two clusters (lines 349 and 571), so dispatching it
against this branch prints real readings for both roles in the job log
without any script change. Step 1f does that and records the run in this
file. Asserting on the readings, and provoking them, is phase 3's job;
adding assertions here would mean writing them twice.

## Step plan

| Step | Effort | Model | Isolation | Brief for sub-agent |
|------|--------|-------|-----------|---------------------|
| 1a | high | opus | none | Refactor only; no behaviour change. In `shakenfist_client_k3s/cluster.py`, split `_probe_k3s_api()` (line 1264) into two methods: `_submit_probe(instance_uuid, command)`, which calls `self.client.instance_execute()` and returns either the operation or a probe dict built for the `apiclient.APIException` branch, and `_collect_probe(instance_uuid, command, aop, deadline)`, which waits with `await_execute(aop, timeout=max(0, deadline - time.monotonic()))` and turns the outcome into the probe dict through the existing branches (still pending, failed state, unrecognised state, no result, exit code). `deadline` is a `time.monotonic()` value. Keep every existing comment, moving each with its branch; make the messages that name `kubectl` take `command` instead, so that they read identically for the `kubectl` command. `_probe_k3s_api()` becomes a thin wrapper that builds the command, takes `deadline = time.monotonic() + HEALTH_PROBE_TIMEOUT_SECONDS`, and calls the two. Do not change `health()` yet. The acceptance test is that `tox -epy3` passes with no test edits; if a test needs changing, stop and report why instead. Commit subject: "Split the health probe into submit and collect." |
| 1b | high | opus | none | Add the signals command and its parser, as pure module-level functions in `cluster.py` beside `heredoc()`, unused by `health()` for now. Read decisions 1, 4, 5 and 6 of `docs/plans/PLAN-cumulative-health-signals-phase-01-agent-signals.md` first. `node_signals_command(role, snapshot_dir=None)` returns one shell command line that prints `key=value` lines: `boot_id` from `/proc/sys/kernel/random/boot_id`; `booted_at` from `/proc/stat`'s `btime`; `oom_kills` from `/proc/vmstat`'s `oom_kill`; `memory_total_kb` and `memory_available_kb` from `/proc/meminfo`; then `systemctl show <unit> -p LoadState -p ActiveState -p NRestarts` (which itself prints `Key=Value` lines) with unit `k3s` for `control_plane` and `k3s-agent` for `worker`; and for `control_plane` only, `etcd_bytes` and `etcd_snapshot_bytes` from `du -sb ... 2>/dev/null \| cut -f1` on `/var/lib/rancher/k3s/server/db/etcd` and on `snapshot_dir` or `/var/lib/rancher/k3s/server/db/snapshots`. Each reading must be independent -- one failing source prints an empty value, not a failed command -- and the command must exit 0 when every source is readable. `snapshot_dir` is caller data and goes through `shlex.quote()` per rule 1 near `cluster.py:299`; nothing else is interpolated. Define the unit names and default paths as named constants with a comment saying where each comes from. `parse_node_signals(stdout, role)` returns the reading keys of decision 1's `signals` dict (everything except `probed` and `error`): integers parsed with `int()`, a missing or unparsable value as `None`, kB converted to bytes, `k3s_state` and `k3s_restarts` `None` unless `LoadState` is `loaded`, `etcd_*` `None` for workers whatever the output says, `k3s_unit` always set from the role, unknown keys ignored. It must never raise on any string. Add tests in `shakenfist_client_k3s/tests/test_cluster.py` in the existing testtools style: the command for each role (unit name, etcd present or absent), the snapshot override quoted (use a path containing a space and a `$`), and the parser on a realistic server output, a realistic worker output, a `not-found` unit, empty output, garbage values, and a value with trailing whitespace. Commit subject: "Add the node signals command and parser." |
| 1c | high | opus | none | Wire the signals into `health()` (`cluster.py:2511`), per decisions 1, 3, 7, 8 and 9 of the phase plan; 1a and 1b have landed. Take one `deadline` before any submission. Submit the `kubectl` probe first (when it would be run today), then a signals probe for every node entry that `exists` and is `healthy`, using `_submit_probe()`; then collect the `kubectl` probe and every signals probe with `_collect_probe()` against the one deadline. A collected signals probe becomes `{'probed', 'error'}` plus `parse_node_signals(stdout, role)`; parse even when the exit code is non-zero, and set `error` as the API probe does. A node not probed gets every reading key `None`, `k3s_unit` set from its role, `probed` False and an `error` that says why -- "this instance no longer exists" or the same "not in a state which can answer: instance X, agent Y" wording the API probe's skip uses; factor a helper rather than writing the dict twice. Attach the result to each node entry as `signals`. Pass the recorded `server_config`'s `etcd-snapshot-dir` (with `md.get('server_config') or {}`) to the command builder for control plane nodes. Do not change how `healthy` is computed anywhere. Update `health()`'s docstring: the report schema gains `signals` with the comments from decision 1, a paragraph on reading counters (decision 2's boot_id and lower-than-baseline rule), a sentence that signals never affect `healthy`, and the abandoned-probe paragraph updated for one operation per probed node. In `tests/fakes.py`, make `HealthClient` route by command line: commands starting `kubectl ` keep the existing `probe_*` behaviour; other commands answer from new per-instance `signals_*` attributes (stdout, return code, state, raises) with a realistic default stdout for each role, and raise the read-count guard so it scales with the number of pending operations rather than assuming one. Tests: every node entry carries `signals` with exactly decision 1's keys in the healthy, gone, unready and probe-failed cases; signals are not probed on unhealthy or missing nodes; every `instance_execute` happens before the first `get_agent_operation` (record call order in the fake); the `kubectl` command is the first submitted; with every operation pending, `health()` returns after one budget, not one per node (patch `HEALTH_PROBE_TIMEOUT_SECONDS` small and count reads, or patch `time.monotonic`); a failed or timed-out signals probe leaves `healthy` True; `health()` still makes no metadata writes; a recorded `etcd-snapshot-dir` reaches the command quoted. Run `tox -epy3` and confirm the Ansible module tests in `test_ansible_module.py`, including `test_no_cluster_secret_reaches_the_result_of_a_health_report`, still pass. Commit subject: "Report cumulative node signals from health." |
| 1d | medium | sonnet | none | Render `signals` in `_render_health()` in `shakenfist_client_k3s/__init__.py` (line 362). After each existing node line, write one indented line. When probed, give in order: `booted <UTC ISO 8601 from booted_at>`, `<k3s_unit> <k3s_state>, <k3s_restarts> restarts`, `<oom_kills> OOM kills`, `<available> of <total> MiB available`, and on control plane nodes `etcd <n> MiB, snapshots <n> MiB`; any `None` value renders as `unknown` rather than being dropped, so the line keeps its shape. MiB is bytes // 1048576. When not probed, write `signals: not read (<error>)`; when probed with an error, add ` (<error>)` after the readings. Nothing is judged: no markers, no colour, no thresholds. Keep the existing rule that `None` never renders as the string "None". Add cases to `HealthCommandTestCase` and `HealthRenderingReporterTestCase` in `shakenfist_client_k3s/tests/test_commands.py`: a healthy two-role cluster, a node whose signals were skipped, and a node with some readings `None`. `tests/cli_contract/health.txt` must not change, because no option changes. Commit subject: "Render node signals in k3s health." |
| 1e | medium | sonnet | none | Document the signals, keeping one definition of each key and pointing to it rather than repeating it. `docs/library-api.md`: in the section around line 354 that describes `health()`'s probe, add a subsection on `signals` -- what each reading is and where it comes from, that every reading is raw and cumulative and `health()` stores nothing, the rule for diffing against a baseline (a changed `boot_id`, or a counter lower than its baseline, voids the baseline and the current value is the delta), that `oom_kills` counts every kernel OOM kill including a pod exceeding its own limit, that signals never affect `healthy`, and that an abandoned probe now leaves up to one queued operation per probed node; update the existing abandoned-probe paragraph to match. `docs/usage.md` `### health NAME` (line 446): update the example output with a signals line under each node, matching what step 1d renders, add one paragraph saying what the line shows and linking to the library-api section for meanings, and correct the paragraph at line 484 for one operation per node. `collection/plugins/modules/sf_k3s_cluster.py` `RETURN`: the `nodes` description gains the `signals` dict (short, pointing at `docs/library-api.md`), and line 310's "submits one read-only agent operation" becomes one per healthy node plus the `kubectl` one. Check `docs/collection.md` for any statement that health submits one operation and correct it. Run `tox -epy3` (the module documentation is parsed by tests) and `pre-commit run --all-files`. Commit subject: "Document the node signals in health." |
| 1f | -- | management session | -- | Live check. Push the branch and dispatch the merge tier workflow against it (as node customisation phase 3 did), then read the `Verify health reports a healthy cluster` and minimal cluster health steps in the job log. Record in this file, under a new *Live results* heading: the run URL, and the rendered signals line for one control plane node and one worker. Confirm every reading is non-`None` on both roles, `k3s_unit` is `k3s-agent` on the worker, `etcd_bytes` is plausibly non-zero on the control plane, and `oom_kills` is an integer. If a reading is `None` on a real node, that is a parser or command bug: fix it in a follow-up commit on this branch before review, not in phase 3. Commit subject: "Record the live signals run." |

## Risks and mitigations

- **The command reads differently on a real node than in the fakes.**
  The parser is tested against output the planner wrote, not output a
  node produced. Mitigation: step 1f, in the management session, runs
  the merge tier against the branch and refuses to proceed with any
  `None` reading on either role.
- **`NRestarts` resets on a manual restart, and `oom_kill` counts
  cgroup kills.** Both are the planner's reading of systemd and kernel
  source rather than something observed on these nodes: systemd should
  clear the counter when a unit is started by an explicit job rather
  than by its `Restart=` policy, and the kernel should count a pod's
  limit kill in `oom_kill`. Decision 2's lower-than-baseline rule covers it,
  and the docs say so. Mitigation: phase 3 restarts k3s by hand and provokes a
  pod limit kill on a throwaway cluster, and confirms both; the master
  plan's phase 3 row names it.
- **More abandoned operations.** Up to one per probed node, each held
  until the server's 600 second deadline. Mitigation: decision 7's
  doc updates in step 1e, and survey finding 5's deadline argument
  recorded in the master plan's Future work for when a client release
  carries it. The reviewer of 1e checks that no page still says
  "one operation".
- **A secret leaks into the module's return.** The report grows by
  data read from nodes. Nothing read is a credential, and the only
  caller value used is the snapshot directory path. Mitigation: 1c
  must run the existing
  `test_no_cluster_secret_reaches_the_result_of_a_health_report`
  unchanged; the reviewer of 1c checks that no raw command output is
  stored in the report.
- **The single deadline starves the `kubectl` probe.** If submission
  were slow, the `kubectl` probe could time out where it did not
  before. Mitigation: decision 8 submits it first; 1c's test that
  everything is submitted before anything is awaited also shows that
  submission costs no waiting.
- **The fake's routing hides a regression in the existing probe.**
  Mitigation: 1a lands first with the existing tests passing
  unchanged, and 1c keeps `probe_*` behaviour for `kubectl` commands,
  so every existing `HealthTestCase` assertion still exercises the same
  path.

## Definition of done

- [ ] `tox -epy3`, `tox -eflake8` and `pre-commit run --all-files` pass,
      and `python3 -c 'import shakenfist_client_k3s'` succeeds.
- [ ] Step 1a's commit touches no file under
      `shakenfist_client_k3s/tests/`:
      `git show --stat <1a sha> -- shakenfist_client_k3s/tests` is
      empty.
- [ ] No CLI option changed:
      `git diff develop -- shakenfist_client_k3s/tests/cli_contract/`
      is empty.
- [ ] A test asserts that every node entry carries a `signals` dict with
      exactly decision 1's twelve keys, in the healthy, gone, unready
      and probe-failed cases.
- [ ] A test asserts that, with every operation pending, `health()`
      returns within one probe budget whatever the node count.
- [ ] `healthy` is computed exactly as on `develop`:
      `git diff develop -- shakenfist_client_k3s/cluster.py | grep
      "^[-+].*'healthy':"` shows no change to either expression.
- [ ] No page still describes `health()` as submitting a single agent
      operation: `grep -rn -i "one read-only agent operation\|one agent
      operation" docs collection` finds nothing that says so.
- [ ] Each of the twelve keys is defined in exactly one prose place
      (`docs/library-api.md`) and in the `health()` docstring schema;
      `docs/usage.md` and the module `RETURN` point at the library page
      rather than redefining them.
- [ ] The *Live results* section records a merge tier run against this
      branch with no `None` reading on either role.
- [ ] The master plan's open questions 1, 2, 3 and 5 point at their
      answers, and its Future work names the recovery plan (decision
      10) and the agent operation deadline (survey finding 5).

## Back brief

Before executing any step of this plan, back brief the operator on
your understanding of it and how the work you intend to do aligns with
it.

One gate on top of that: decisions 1 to 3 fix a return shape that the
Ansible module documents and plays will branch on, and they are cheap
to change now and expensive after step 1e has documented them in four
places. Confirm them with the operator before step 1b starts. Step 1a
is safe to run before that confirmation, since it changes no behaviour.
