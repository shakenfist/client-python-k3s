# Cumulative health signals phase 2: Kubernetes-read signals

## Prompt

Before responding to questions or discussion points in this document,
read `Cluster.health()`, `_node_health()`, `_submit_probe()`,
`_collect_probe()`, `_unprobed()`, `_cannot_answer()`,
`node_signals_command()` and `parse_node_signals()` in
`shakenfist_client_k3s/cluster.py`, the node naming comment in
`remove_worker()`, `install_workers()` and `expand_workers()` in the same
file, `_render_health()` and `_render_signals()` in
`shakenfist_client_k3s/__init__.py`, and `HealthClient` in
`shakenfist_client_k3s/tests/fakes.py`. Ground answers in what that code
does today. The master plan is
[PLAN-cumulative-health-signals.md](PLAN-cumulative-health-signals.md),
and phase 1's plan,
[PLAN-cumulative-health-signals-phase-01-agent-signals.md](PLAN-cumulative-health-signals-phase-01-agent-signals.md),
sets the conventions this phase follows: one shared probe deadline,
every key present in every outcome, raw values the caller diffs, and
parsers that validate rather than trust.

## Planning effort

High. This phase changes what the top level `healthy` means -- the
value `--strict`, the Ansible module's documented `assert`, and two CI
gates branch on -- in a library that has a release on PyPI (`v0.2.0`).
It also makes `create()` and `expand_workers()` wait for something they
have never waited for, which is cluster assembly ordering, the area the
master plan says to plan at high effort.

Review effort: high for steps 2b and 2c; medium for the rest.

## Scope

In:

- A second read-only probe on the first control plane node, beside the
  existing `kubectl get nodes` one, which reads from the Kubernetes API:
  each Kubernetes node's `Ready`, `MemoryPressure`, `DiskPressure` and
  `PIDPressure` conditions, with when `Ready` last changed, and every
  container whose current or most recent termination was `OOMKilled`,
  with the node its pod was scheduled on.
- Those readings reported per node under a new `kubernetes` key, the
  probe's own outcome and any Kubernetes node no instance accounts for
  under a new top level `kubernetes` key.
- The top level `healthy` gaining one term: every node is registered
  with Kubernetes and `Ready` (#76).
- `create()` and `expand_workers()` waiting for every node they add to
  register and become `Ready` before returning, so that the new term
  does not turn "create, then check health" into a race.
- Rendering in `sf-client k3s health`, documentation, and one live merge
  tier run recording real readings.

Out:

- Kubernetes Events, including the MetalLB `AdditionalAssignFailed`
  example in the master plan: survey finding 4.
- Container restart counts other than `OOMKilled`: decision 7.
- Folding pressure conditions, OOM kills or unmatched Kubernetes nodes
  into `healthy`: decision 6.
- Provoking any of these on a live cluster and asserting the report
  sees it: phase 3.
- Changing the existing `api` probe or its `stdout`: decision 2.
- Recovery of any kind: the master plan's non-goal.

## What the survey found

The master plan's phase 2 row and its Situation section have been
corrected where this survey found them wrong, in the same commit as this
file; the corrections are noted below so that a later step does not redo
them.

### 1. The code is as phase 1 left it

`health()` is at `cluster.py:3024`, `K3S_API_PROBE_COMMAND` at
`cluster.py:140`, `_submit_probe()` at `cluster.py:1691`,
`_collect_probe()` at `cluster.py:1732`, `_node_health()` at
`cluster.py:1907`, and `_render_health()` at `__init__.py:363`.
`HealthClient` (`tests/fakes.py:237`) routes a submitted command by
whether it starts with `kubectl ` (`tests/fakes.py:411`): every
`kubectl` command is treated as the API probe, so a second `kubectl`
probe would be answered with the API probe's scripted output. The fake
has to route by the exact command (step 2c).

### 2. "Via the API" means kubectl through the agent

The master plan said `OOMKilled` would be read "via the API rather than
the agent". The plugin has no Kubernetes client, and none of its
dependencies is one. Every Kubernetes read and write it makes -- the
probe, `rollout status`, `drain`, `delete node`, `helm` -- is a command
run through the agent on the first control plane node with
`--kubeconfig /etc/rancher/k3s/k3s.yaml`. It never assumes the client
host can reach the cluster's API, and an operator running `sf-client`
from outside the node network often cannot. This phase reads the same
way: the distinction from phase 1 is *what answers* (the API server,
about every node, from one place) rather than *how the plugin reaches
it*. The master plan's row and Situation bullet now say so, and the
phase is named "Kubernetes-read" rather than "API-read" so that it is
not confused with the `api` key that already exists.

### 3. `lastState` is the latest termination, not a history

A container's `lastState.terminated` holds only its most recent
termination. A container killed for OOM and since restarted cleanly
still shows `OOMKilled` there; one that later exits for any other reason
does not, and a deleted pod takes its record with it. So this reading is
"containers whose latest termination was an OOM kill, and when", not a
count. That is still what the master plan wanted it for: it is
timestamped (`finishedAt`), so a caller diffing against yesterday's
report can tell a new kill from an old one, and it names the container,
which phase 1's `oom_kills` counter cannot. `restartCount` beside it is
cumulative for the pod's lifetime. A container in `CrashLoopBackOff`
after an OOM kill has its kill in `lastState` and a `waiting` state; one
that has just been killed and not yet restarted has it in `state`. The
probe reads both, and init containers as well.

### 4. Events expire in an hour

The master plan's second example, MetalLB logging
`AdditionalAssignFailed` 184 times over 25 days, is visible only as
Kubernetes Events. The API server keeps an Event for `--event-ttl`,
which defaults to one hour and which k3s does not change, so a daily
poll sees at most the last hour of them, and none of the 25 days. That
example stays out of reach of `health()` for a reason this phase cannot
change; it is recorded in the master plan's Future work rather than
silently dropped.

### 5. Nothing waits for a node to become Ready

`grep -rn "condition=Ready\|kubectl wait" shakenfist_client_k3s/*.py`
finds only a comment. `create()` installs k3s on each node and moves on;
the MetalLB `rollout status` that follows counts only nodes that have
already registered, and `--no-metallb` skips even that. Every caller
that needs Ready nodes waits itself: `tools/ci_deploy_test.sh`'s
`wait_for_nodes()` (line 90) exists for exactly this. And one caller
does not wait: the minimal cluster block runs `sf-client k3s health
--strict` at line 606, *before* its `wait_for_nodes 2` at line 610. Once
`healthy` requires Ready nodes (decision 6), that line races the
worker's registration. Decision 8 is the answer.

### 6. A Kubernetes node is named after its instance, lowercased

`remove_worker()`'s comment above its name resolution (around
`cluster.py:3718`) establishes this: Shaken Fist sets the guest's
hostname from the instance name, k3s registers the node under the
hostname, and kubelet lowercases it. `remove_worker()` already matches
on `inst['name'].lower()`. This phase matches the same way, and reads
the name from the instance rather than rebuilding it from
`md['node_serial']`, for the reason that comment gives.

### 7. `healthy` is a released contract

`v0.2.0` is tagged and on PyPI. Its `healthy` does not consider Kubernetes
node readiness, and `docs/usage.md` (around line 532) says so
explicitly and links #76. Callers that branch on it today: `--strict`
(both CI clusters, `ci_deploy_test.sh:364` and `:606`), the Ansible
module's documented `assert` example (`sf_k3s_cluster.py:249`), and
33fl. Narrowing it is a behaviour change, and the pull request's title
and description must say so, so that it reaches the next release's
notes.

### 8. 33fl's tier 1 health check cannot fail

Not this repository's bug, but found while reading the caller this plan
was written for. `tools/k3s-health-check.py`'s `tier1_cluster_health()`
runs `sf-client k3s health` without `--strict` and fails only on a
non-zero exit code. Without `--strict` the command exits 0 for any
cluster it could describe, so that check passes for an unhealthy
cluster. Its `tier1_nodes_ready()` covers readiness separately, which
is why this has not mattered yet. Raised in 33fl rather than fixed
here, as [Mach33Labs/33fl#938](https://github.com/Mach33Labs/33fl/issues/938).

### 9. Command output size is not bounded anywhere we control

The agent returns a command's whole stdout (`agent-python`,
`daemon.py`, `communicate()` with no limit), and no limit on the size of
an operation's stored result was found on the server side either. That
is not evidence there is none. `kubectl get pods -A -o json` on a busy
cluster is megabytes. The probe therefore prints one short line per
node and one per `OOMKilled` container, rendered by a go-template on the
API side, so its output grows with what it reports rather than with the
number of pods.

### 10. Related issues

| Issue | Relation |
|---|---|
| [#76](https://github.com/shakenfist/client-python-k3s/issues/76) | `healthy` ignores node readiness. Closed by this phase (decision 6). |
| [#105](https://github.com/shakenfist/client-python-k3s/issues/105) | Agent operation results trusted in some places. The new parser follows phase 1's validating parser rather than adding a trusting one. |
| [#101](https://github.com/shakenfist/client-python-k3s/issues/101) | `health --strict` has no coverage in the merge tier. Unchanged here; phase 3. |
| [#110](https://github.com/shakenfist/client-python-k3s/issues/110) | No HA control plane in CI. The probe runs on the first control plane node only, as the `api` probe does, so HA does not change it. |

## Decisions

### 1. Kubernetes is read with kubectl on the first control plane node

Survey finding 2. One probe, through the agent on
`md['control_plane_nodes'][0]`, under the same gate as the `api` probe:
only when that node's entry `exists` and is `healthy`. One node answers
for every node, because the API server is the authority on all of them.
That is the difference from phase 1's signals, which each node reports
about itself.

### 2. A second probe; `api` is untouched

The `api` probe's `stdout` is `kubectl get nodes`' human table, which
`docs/library-api.md` documents and the CLI prints under the report. It
is the most useful thing in the report for a person, and a released
contract for a program. Changing that command to emit machine-readable
output would change both. So the new readings come from a second
command, `K3S_KUBERNETES_PROBE_COMMAND`, submitted immediately after the
`api` probe and before any node's signals (phase 1 decision 8's order,
extended), and collected against the same deadline. The cost is one more
agent operation on the first control plane node, and one more abandoned
operation in the worst case; the docstring and docs that count them are
updated.

Folding the two probes into one later is possible, once a release can
change `api['stdout']`; that is recorded in Future work rather than done
here.

### 3. Readings go on each node; the probe's outcome goes at the top

A reading about one node goes on that node (phase 1 decision 1's
principle), under a new `kubernetes` key, not under `signals`:
`signals` is documented as what a node says about itself, read through
its own agent, and as never affecting `healthy`. Both stop being true
for `ready`. Keys, the same in every outcome:

```python
'kubernetes': {
    'registered': bool or None,     # a Kubernetes node has this node's name
    'ready': str or None,           # Ready condition status: 'True', 'False', 'Unknown'
    'ready_since': int or None,     # its lastTransitionTime, Unix seconds
    'memory_pressure': str or None, # MemoryPressure status, likewise
    'disk_pressure': str or None,
    'pid_pressure': str or None,
    'oom_killed': list or None,     # see decision 5
}
```

Every value is None when the probe did not answer, and `oom_killed` is
None rather than `[]`, because an empty list is a claim that nothing was
killed. When the probe answered but no Kubernetes node has this name,
`registered` is False and the conditions are None.

Condition statuses stay the strings Kubernetes uses rather than becoming
bools, because `Unknown` is a third value that means something specific
(the node controller has stopped hearing from the kubelet), and a bool
would have to lie about it in one direction or the other.

The probe's own outcome is cluster-wide, so it goes in one place, a new
top level key:

```python
'kubernetes': {
    'probed': bool,                 # the command ran at all
    'answered': bool,               # ...and it exited zero
    'error': str or None,           # why not
    'unmatched_nodes': list or None,  # Kubernetes node names no instance accounts for
}
```

`unmatched_nodes` is None when the probe did not answer, for the same
reason. A Kubernetes node that no instance in the metadata accounts for
-- typically a node object left behind by an instance deleted out of
band -- is reported there rather than dropped, because dropping it would
be a silent skip that looks exactly like "there are none".

### 4. Nodes are matched on the lowercased instance name

Survey finding 6, and `remove_worker()`'s precedent. A node whose
instance is gone has no name to match, so its readings are None, and
any Kubernetes node left over goes into `unmatched_nodes`.

### 5. `oom_killed` lists containers whose latest termination was an OOM kill

Survey finding 3. Each element is:

```python
{
    'namespace': str,
    'pod': str,
    'container': str,
    'restarts': int,        # restartCount, for the pod's lifetime
    'finished_at': int,     # the termination's finishedAt, Unix seconds
}
```

Placed on the node named by the pod's `spec.nodeName`. A container is
listed if its `state.terminated.reason` or its
`lastState.terminated.reason` is `OOMKilled`, in `containerStatuses` or
`initContainerStatuses`. The docs say plainly that this is the latest
termination of containers that still exist, not a count, and how a
caller tells a new kill from one it has already seen (a later
`finished_at` for the same namespace, pod and container). An element
whose node is not a cluster node goes nowhere and is not counted.
Whether such a thing can happen is unclear, and a pod on an unmatched
node is already visible through `unmatched_nodes`.

### 6. `healthy` requires every node Ready, and nothing else new

This is the decision a reviewer is most likely to push back on, because
it changes a released contract (survey finding 7). The top level
`healthy` becomes:

```python
not interrupted
and all(node['healthy'] for node in nodes)
and api['answered']
and kubernetes['answered']
and all(node['kubernetes']['ready'] == 'True' for node in nodes)
```

It is taken anyway because the alternative is worse: `--strict` is
named and documented as a gate, it reports a cluster whose kubelets are
all `NotReady` as healthy, and every caller that has noticed has added
its own `kubectl wait` to make up for it. #76 asked for exactly this
term.

The node level `healthy` does **not** change. It is Shaken Fist's view
of the instance, and it is also the gate that decides whether a node's
agent is asked anything. A `NotReady` kubelet with a working agent is
precisely the node whose signals are worth reading. The renderer shows
readiness per node, so the reason a cluster is unhealthy while every node
line is healthy is on the screen.

Not folded in, each for a stated reason:

- Pressure conditions. They taint the node `NoSchedule`, which is
  degraded rather than down, and whether that matters depends on the
  workload. Facts, not opinions (phase 1 decision 3).
- `oom_killed`. It is history, and history cannot be judged without a
  baseline.
- `unmatched_nodes`. A stale node object is debris rather than a broken
  cluster. Reported, so it cannot hide.

A probe that did not answer makes `healthy` False. Readiness that could
not be read is not readiness. Unlike a failed signals probe, which phase
1 deliberately kept out of `healthy`, this probe *is* the term.

### 7. Restart counts other than OOM kills are declined

A container's `restartCount` is cumulative only while its pod lives, so
a sum over pods goes down when a pod is deleted, which is the `dmesg`
lesson from phase 1 again. A per-container list of every restart is the
whole pod table under another name. 33fl's pod-age heuristic for telling
recent crash loops from old first-boot races is a judgement, so it stays
with the caller. `restarts` appears only where it qualifies an
`oom_killed` entry. Recorded in Future work.

### 8. `create()` and `expand_workers()` wait for their nodes to be Ready

Survey finding 5. Once decision 6 lands, "create, then check health" --
the Ansible example and `ci_deploy_test.sh:606` both do it -- races the
last node's registration. Saying "wait first" in the docs is the
`kubectl wait` workaround #76 complained about, moved into every
caller. So the verbs that add nodes wait for them, as this repo's CI
already waits after them.

A new `await_nodes_ready(instance_uuids)` runs one command per node on
the first control plane node through `execute_and_await()`. The command
first polls until the node object exists, because `kubectl wait` fails
immediately on a node that has not registered yet (the reason
`wait_for_nodes()` polls the count first). It then runs `kubectl wait
--for=condition=Ready node/<name> --timeout=300s`. Names come from the
instances, lowercased, and go through `shlex.quote()` (rule 1). It is
called once in `create()`, after the last k3s install and before MetalLB,
for every node, and in `expand_workers()` after `install_workers()`, for
the new workers. A node that never becomes Ready makes the verb fail
after five minutes, naming the node, rather than return a cluster that
`health()` would immediately call unhealthy. For `create()` that leaves
an interrupted cluster, which is the truth about it.

This makes `create()` slower by however long the last node takes to go
Ready -- seconds on the clusters the merge tier builds, since MetalLB and
Longhorn come after it. `ci_deploy_test.sh` keeps its own
`wait_for_nodes()`, which also checks node counts.

### 9. The probe output is one validated line per fact

Survey finding 9. The command runs `kubectl get nodes` and `kubectl get
pods -A`, each with a `-o go-template` that prints tab-separated
records: a `node` line per node (name, then the four condition statuses,
then `Ready`'s `lastTransitionTime`), and an `oom` line per `OOMKilled`
container (node, namespace, pod, container, restarts, finishedAt). The
template is a constant, and no caller data enters the command, so no
quoting is needed. The two `kubectl` calls are joined so that the
command exits non-zero if either fails, because unlike phase 1's
independent readings, a half-read here would be wrong: a node list with
no pod list reads as "no OOM kills".

`parse_kubernetes_readings(stdout)` follows phase 1's parser rules.
Unknown record types are ignored. A field that fails validation drops
the whole record, not just the field. Node, namespace and pod names must
be Kubernetes names (a lowercase RFC 1123 subdomain, at most 253
characters; container names and namespaces are labels, at most 63).
Statuses must be `True`, `False` or `Unknown`. Timestamps must be RFC
3339 UTC, as the API emits them, and are converted to Unix seconds.
Integers use phase 1's `NODE_SIGNAL_INTEGER_RE`. For a duplicate node
name, the first record wins. It never raises on any string.

### 10. One live run, no new CI assertions

As phase 1 decision 11: the merge tier already runs `health --strict` on
both clusters, and line 606 now exercises decision 8 for free. Step 2e
dispatches it against the branch and records the readings. Provoking a
`NotReady` node or an OOM kill is phase 3's job.

## Step plan

| Step | Effort | Model | Isolation | Brief for sub-agent |
|------|--------|-------|-----------|---------------------|
| 2a | high | opus | none | Add the Kubernetes probe command and its parser as pure module-level functions in `shakenfist_client_k3s/cluster.py`, beside `node_signals_command()` and `parse_node_signals()`, which they mirror. Nothing calls them yet. Read decisions 3, 5 and 9 of `docs/plans/PLAN-cumulative-health-signals-phase-02-kubernetes-signals.md` first. `K3S_KUBERNETES_PROBE_COMMAND` is a constant shell command: `kubectl get nodes` and `kubectl get pods -A`, both with `--kubeconfig /etc/rancher/k3s/k3s.yaml` and `-o go-template=...`, joined with `&&`. The nodes template prints, for each node, a line `node<TAB>name<TAB>Ready<TAB>MemoryPressure<TAB>DiskPressure<TAB>PIDPressure<TAB>Ready lastTransitionTime`. Range over `.status.conditions`, testing `.type`; print an empty field for a missing condition. The pods template uses a variable for the pod (`{{$pod := .}}`) so the inner ranges over `.status.containerStatuses` and `.status.initContainerStatuses` can print `oom<TAB>nodeName<TAB>namespace<TAB>pod<TAB>container<TAB>restartCount<TAB>finishedAt` when `state.terminated.reason` or `lastState.terminated.reason` is `OOMKilled`. When both are `OOMKilled`, report `state`'s, the newer of the two. Guard every nested field with `{{if ...}}` so a missing one does not make the template fail. Use `{{"\t"}}` and `{{"\n"}}` for separators. The templates contain `$` and double quotes, so single quote each `-o go-template=` argument in the shell command, and keep single quotes out of the templates. Comment where each field comes from. `parse_kubernetes_readings(stdout)` returns `{'nodes': {name: {'ready', 'ready_since', 'memory_pressure', 'disk_pressure', 'pid_pressure'}}, 'oom_killed': {node_name: [ {namespace, pod, container, restarts, finished_at} ]}}`. Use the validation rules of decision 9: names against compiled regexes (a lowercase RFC 1123 subdomain of at most 253 characters for node and pod names; an RFC 1123 label of at most 63 for namespaces and containers), statuses in `{'True', 'False', 'Unknown'}` or None when empty, RFC 3339 `YYYY-MM-DDTHH:MM:SSZ` converted with `calendar.timegm`, and integers with `NODE_SIGNAL_INTEGER_RE`. A record with any invalid field is dropped whole, the first record for a node name wins, unknown record types are ignored, and the parser never raises on any string, including None. Tests go in `shakenfist_client_k3s/tests/test_cluster.py`, in the style of `ParseNodeSignalsTestCase`: realistic output for a two node cluster with one OOM kill; a missing condition; `Unknown` status; an invalid name, status, timestamp and integer, each dropping its record; a duplicate node; an `oom` line for a node with no `node` line; empty and None input; trailing whitespace. Add a test that the template renders, by running it through a real Go template engine if one is available, or otherwise by asserting its structure. If neither is practical, say so, and step 2e is the check. Add `tools/mutation-check.py` entries for at least: the drop-whole-record rule, the first-record-wins rule, the status allow list, the name regex length cap, and the timestamp conversion. Unit tests only; no live cluster is reachable. Commit subject: "Add the Kubernetes probe command and parser." |
| 2b | high | opus | none | Make `create()` and `expand_workers()` wait for their nodes to become Ready, per decision 8 of the phase plan. Add `Cluster.await_nodes_ready(instance_uuids)` in `shakenfist_client_k3s/cluster.py`. Resolve each instance's Kubernetes node name as `remove_worker()` does: `inst.get('name')` lowercased, read from the instance, with that method's comment as the reference rather than copying it. Factor a shared helper if one falls out cleanly. Then call `execute_and_await([md['control_plane_nodes'][0]], commands)` with one command per node: a bounded poll (for example 60 attempts, 5 seconds apart) until `kubectl get node <name>` succeeds, then `kubectl wait --for=condition=Ready node/<name> --timeout=300s`, both with `--kubeconfig /etc/rancher/k3s/k3s.yaml`, and the name passed through `shlex.quote()` (rule 1). Write the command so it exits non-zero, naming the node, when the poll gives up. Emit a progress phase ("Waiting for N nodes to become Ready"). Call it in `create()` once, after the last k3s install (after `install_workers()`, at `cluster.py:2246`, or after the control plane installs when there are no workers) and before MetalLB, for every node. Call it in `expand_workers()` (`cluster.py:3598`) after `install_workers()`, for the new workers only. Read how `create()` records interruption, and confirm that a failure here leaves the cluster interrupted at a sensible state with nothing half-written; say what you found in the commit message. Update the fakes in `tests/fakes.py` so existing create and expand tests still pass with the new commands. Add tests: the commands carry each node's lowercased name, quoted; the wait comes after the last install and before MetalLB, with call order recorded; `expand_workers()` waits only for the new workers; a failing wait raises and leaves the cluster interrupted. Update the `create()` and `expand_workers()` docstrings and `docs/library-api.md` and `docs/usage.md` wherever they say what those verbs return after. Add mutation-check entries for the name quoting and for the wait being called in each verb. Unit tests only; step 2e checks it live. Commit subject: "Wait for new nodes to become Ready." |
| 2c | high | opus | none | Wire the Kubernetes probe into `health()` (`cluster.py:3024`), per decisions 1 to 6 of the phase plan; 2a and 2b have landed. After the `api` probe is submitted, and under the same gate, submit `K3S_KUBERNETES_PROBE_COMMAND` on the first control plane node with `_submit_probe()`, before any signals probe. Collect it after the `api` probe with `_collect_probe(..., name='the Kubernetes probe')` against the shared deadline. Build the top level `kubernetes` report (`probed`, `answered`, `error`, `unmatched_nodes`) from the probe. Reuse `_unprobed()`'s skip wording via `_cannot_answer()` for the gated case, and the no-control-plane wording for a cluster with none. Give every node entry a `kubernetes` dict with exactly decision 3's keys, matching on the lowercased instance name (a node whose instance is gone has no name, so its readings are None). Put every Kubernetes node no entry matched into `unmatched_nodes`, sorted. When the probe did not answer, every reading is None, including `oom_killed` and `unmatched_nodes`. When it answered, an unmatched node entry has `registered` False and `oom_killed` `[]`. Add the two new terms to the top level `healthy` (decision 6), and leave the node level `healthy` alone. In `tests/fakes.py`, make `HealthClient` route `K3S_API_PROBE_COMMAND` exactly to the existing `probe_*` behaviour, `K3S_KUBERNETES_PROBE_COMMAND` exactly to new `kubernetes_*` attributes with realistic default output matching the fake's node names, and anything else to signals. Update the docstring schema, the `healthy` paragraph, the probe count in the abandoned-probe paragraph (up to one per probed node plus two), and the probe order comment. Tests: every node carries `kubernetes` with exactly the keys in every outcome (healthy, gone, unready, probe failed, probe skipped, no control plane); the top level `kubernetes` likewise; `healthy` is False for a `NotReady` node, an `Unknown` one, an unregistered one, and a failed probe, and True when all are `True`; a pressure condition, an OOM kill and an unmatched node each leave it True; node level `healthy` is unchanged by a `NotReady` node; the submission order is `api`, Kubernetes, then signals; the wall clock bound test still holds with the extra probe; no metadata writes. Confirm the Ansible module tests in `test_ansible_module.py` still pass, including the secret-leak test. Add mutation-check entries for each new `healthy` term, for node level `healthy` not taking `ready`, for `oom_killed` being None and not `[]` when unread, and for the submission order. Commit subject: "Report Kubernetes node readiness from health." with `Fixes #76` in the body. |
| 2d | medium | sonnet | none | Render and document the Kubernetes readings; 2c has landed. In `_render_health()` (`shakenfist_client_k3s/__init__.py:363`), add one more indented line under each node, after its signals line, in the style of `_render_signals()`. When read, it is `kubernetes: Ready since <UTC ISO>` (or `NotReady (False) since ...` / `NotReady (Unknown) since ...`), then `, <Condition>` for each pressure condition whose status is `True`, or `, no pressure`; and then one further indented line per `oom_killed` entry: `OOM killed: <namespace>/<pod> <container> at <UTC ISO>, <n> restart(s)`. When unregistered, it is `kubernetes: not registered`; when unread, `kubernetes: not read (<top level kubernetes error>)`. After the `api` lines, write `  Kubernetes: unmatched nodes <a>, <b>` when there are any, and `  Kubernetes probe: did not answer (<error>)` when it did not. Values of the wrong type render as `unknown`, as `_render_signals()` does. Nothing is judged and there are no thresholds. Add cases to `HealthCommandTestCase` and `HealthRenderingReporterTestCase` in `tests/test_commands.py`. `tests/cli_contract/health.txt` must not change. Documentation: in `docs/library-api.md`, add a `### What kubernetes reports` section after `### What signals reports` (line 408), covering decisions 3, 5 and 6: every key; that `oom_killed` is latest-termination rather than a count, and how to tell a new kill from a seen one; and what `healthy` now requires. Correct every place that lists `healthy`'s terms or counts health's agent operations. In `docs/usage.md`, rewrite the "Be precise about what healthy means" paragraph (around line 532), which currently says NotReady nodes report healthy and links #76, and update the example output to include the new lines. In `collection/plugins/modules/sf_k3s_cluster.py`, update `RETURN` (`nodes` gains `kubernetes`, a top level `kubernetes`, and `healthy`'s description), pointing at `docs/library-api.md` rather than redefining keys. `grep -rn "NotReady\|#76\|issues/76" docs collection shakenfist_client_k3s` must find nothing that still says readiness is ignored. Run `tox -epy3` (module docs are parsed by tests) and `pre-commit run --all-files`. Commit subject: "Render and document Kubernetes readiness." |
| 2e | -- | management session | -- | Live check. Push the branch and dispatch the merge tier workflow against it, as phase 1 step 1f did. Read the `health --strict` steps for both clusters in the job log. The minimal cluster's at line 606 runs before any `wait_for_nodes`, so it passing is the live test of decision 8. Record under *Live results*: the run URL; how long `create()`'s new Ready wait took on each cluster (from the progress phases); the rendered Kubernetes line for a control plane node and a worker; and whether any `oom_killed` entry or unmatched node appeared. Every node must read `registered` True and `ready` `'True'`. A None reading on a real cluster, or a go-template that kubectl rejects, is a bug to fix on this branch before review, not in phase 3. Commit subject: "Record the live Kubernetes readings run." |

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| The go-template is wrong in a way unit tests cannot see: a field path, a nil guard, kubectl's template dialect. | Step 2a tests the template through a real Go template engine if one is available, and says so if not. Step 2e is the decisive check: any template error fails both `health --strict` steps, and the management session reads the rendered lines, not just the exit code. |
| Narrowing `healthy` breaks a caller that relied on the old meaning. | Survey finding 7 lists the known callers. Both CI gates run in step 2e, and the create wait (decision 8) removes the race the Ansible example would hit. The pull request title and description name the behaviour change so that it reaches the release notes, and the management session checks this before opening the pull request. 33fl gets a note (survey finding 8). |
| The create wait makes a cluster that would have worked fail instead, because a node is slow to go Ready on a loaded hypervisor. | Up to five minutes of polling plus five of `kubectl wait`, the same budget the MetalLB rollout waits already use. A node that is not Ready in ten minutes is broken. Step 2e records the real wait time, so there is data before anyone tunes it. |
| Matching on the lowercased instance name misses a node whose hostname cloud-init set differently. | It is the same rule `remove_worker()` has used in production. A miss shows up as `registered` False plus an entry in `unmatched_nodes`, which is visible rather than silent, and step 2e checks both are clean on two real clusters. |
| The extra probe pushes a slow cluster past the shared budget, abandoning the probe that now decides `healthy`. | The probe is two `kubectl get` calls with filtered output; the `api` probe beside it already makes the same API round trip. It is submitted second, ahead of every signals probe. Step 2c keeps phase 1's wall clock test. If step 2e shows it slow, that is a finding to record, not to tune silently. |
| The fake routes by exact command, so a test could pass against a command the code no longer sends. | Step 2c's fake compares against the module constants, not literals, and a mutation-check entry changes the constant to confirm a test fails. |

## Definition of done

- [x] `tox -epy3`, `tox -eflake8` and `pre-commit run --all-files` pass,
      `python3 -c 'import shakenfist_client_k3s'` succeeds, and
      `python3 tools/mutation-check.py` reports every mutation caught,
      with the count higher than the 43 on `develop` at `78c9df1`.
- [x] No CLI option changed:
      `git diff develop -- shakenfist_client_k3s/tests/cli_contract/`
      is empty.
- [x] The `api` probe is unchanged:
      `git diff develop -- shakenfist_client_k3s/cluster.py | grep
      "^[-+]K3S_API_PROBE_COMMAND"` prints nothing.
- [x] A test asserts every node entry carries `kubernetes` with exactly
      decision 3's keys, and the report a top level `kubernetes` with
      exactly its keys, in every outcome listed in step 2c's brief.
- [x] A test asserts `healthy` is False for each of a `NotReady`, an
      `Unknown`, an unregistered node and an unanswered probe, and that
      node level `healthy` is not.
- [x] Every `kubectl wait` or node-name command added interpolates a
      name only through `shlex.quote()`: `git diff develop --
      shakenfist_client_k3s/cluster.py | grep "^+.*node/%s\|get node %s"`
      shows each such format fed by a quoted value.
- [x] No page still says `healthy` ignores Kubernetes readiness:
      `grep -rn -i "notready\|issues/76" docs collection` finds nothing
      that does.
- [x] `oom_killed` and `ready` are each defined in exactly one prose
      place (`docs/library-api.md`) and in `health()`'s docstring
      schema.
- [x] *Live results* records a merge tier run against this branch in
      which both `health --strict` steps passed, including the minimal
      cluster's at `ci_deploy_test.sh:606`.
- [x] The master plan's Future work carries Events (survey finding 4),
      non-OOM restart counts (decision 7) and merging the two kubectl
      probes (decision 2). #76 is closed by the merge.

## Live results

Step 2e dispatched the merge tier's `functional-tests.yml` against this
branch at `bdb0652`:
[run 37668454070](https://github.com/shakenfist/client-python-k3s/actions/runs/37668454070).
Its cluster deployment job passed, including both `health --strict`
steps. Its sanity checks failed only on the sdist size gate in
`tools/check-wheel-build.sh`, which `431579e` raised; see *Deviations*.

The Ready wait, from the progress phases:

| Cluster | Verb | Nodes | Wait |
|---|---|---|---|
| ciMixed | `create` | 3 | 6 s |
| ciMixed | `expand-workers` | 1 | 6 s |
| ciMinimal | `create` | 2 | 6 s |

The minimal cluster's worker reports `Ready since 19:10:26`, and its
`create` returned at 19:10:42. The `health --strict` at
`ci_deploy_test.sh:606` ran straight after, before any
`wait_for_nodes`, and its `kubectl get nodes` shows the worker 18
seconds old. Before decision 8, that check raced the worker's
registration.

The rendered Kubernetes lines, from the main (`ciMixed`) cluster:

```
    [ok] k3s-ciMixed-node-001 (e8a46b92-..., control plane): instance created, agent ready
        booted 2026-10-07T18:54:34Z, k3s active, 0 restarts, 0 OOM kills, 2711 of 3914 MiB available, etcd 138 MiB, snapshots 0 MiB
        kubernetes: Ready since 2026-10-07T18:58:16Z, no pressure
    [ok] k3s-ciMixed-node-002 (844a6f96-..., worker): instance created, agent ready
        booted 2026-10-07T18:56:28Z, k3s-agent active, 0 restarts, 0 OOM kills, 2285 of 2971 MiB available
        kubernetes: Ready since 2026-10-07T18:59:27Z, no pressure
```

Every node on both clusters read `registered` and `Ready`. No
`OOM killed` line, unmatched node or `Kubernetes probe: did not
answer` line appeared. The instance names are mixed case
(`k3s-ciMixed-node-001`) and the Kubernetes nodes lowercase
(`k3s-cimixed-node-001`), so the lowercased match (decision 4) worked
on a real cluster. kubectl accepted both go-templates on k3s
`v1.36.5+k3s1`. Each `health` call took about 15 seconds of wall time.

## Deviations and bugs fixed during this work

- **One Ready command for all nodes (step 2b).** The brief asked for one
  command per node, with a 300 second registration poll and a 300 second
  `kubectl wait`. The sub-agent found that Shaken Fist expires an agent
  operation 600 seconds after submission
  (`AGENT_OPERATION_DEFAULT_DEADLINE`), and commands in one operation
  run one after another. So even one slow node could be expired before
  kubectl said why. Review sent it back. It is now one command for every
  node: 24 polls 5 seconds apart, then one `kubectl wait` of 300 seconds,
  about 535 seconds worst case for any node count. A test recomputes that
  from the constants. The poll's `--request-timeout` is 5 seconds rather
  than 10, so that a hung API server also fits.
- **The command starts with `printf` (step 2b).** The agent refuses a
  command whose first word is not on `PATH`, so the command cannot open
  with a shell loop.
- **`node_name_for_instance()` and `NodeUnnamedError` (step 2b).** The
  name rule `remove_worker()` used is now a shared helper. Its comment
  moved into the helper's docstring, so this plan's pointers to
  `remove_worker()`'s comment are now one step indirect. A nameless
  instance raises a new exception rather than `WorkerUnnamedError`,
  because it may be a control plane node.
- **The template guards (step 2a).** kubectl 1.21's `eq` fails the whole
  template on a missing operand, so every operand is tested first.
  `restartCount` is guarded with `exists` rather than `if`, because `if`
  is false for 0. Both were found by rendering the templates with real
  kubectl against throwaway k3s servers. An OOM line with no
  `finishedAt` is dropped, because decision 5 types the time as an int.
- **Duplicate lowercased names (step 2c).** Two entries that lowercase to
  the same name both read None, and the name is not reported as
  unmatched, because one node object cannot be attributed to either.
- **`oom_killed` on an unregistered node (step 2c)** carries the kills
  filed under its name, normally `[]`, rather than always `[]`. An empty
  list would deny a kill whose pod names the node.
- **`kubernetes['answered']` is implied** by the readiness term, so no
  mutation can catch its removal alone. It is kept because decision 6
  lists it.
- **The renderer never says "no pressure" unless all three conditions
  read `False` (step 2d).** An `Unknown` or missing condition renders
  as unknown rather than as fine.
- **The sdist size gate.** The merge tier failed the sdist byte bound
  (2355987 against 2200000). The growth is source and plans, spread over
  the phases, so `431579e` raised the bound to 2800000 as the gate's
  comment directs.

## Back brief

Before executing any step of this plan, back brief the operator on
your understanding of it and how the work you intend to do aligns with
it.

One gate on top of that: decisions 3, 6 and 8 change a released
contract, and they are cheap to change now and expensive once step 2d
has documented them. Decision 3 is the return shape. Decision 6 is what
`healthy` and `--strict` mean. Decision 8 is that `create()` now waits.
Confirm them with the operator before step 2b starts. Step 2a is safe to
run before that confirmation, since nothing calls what it adds.
