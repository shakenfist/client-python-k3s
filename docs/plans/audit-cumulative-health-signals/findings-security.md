# Lens 4e findings: security

Returned as text by the step 4e lens (opus, high effort), as the plan's
decision 5 asks, and saved here by the management session. The
management session confirmed SEC-1 against Shaken Fist's source
(`shakenfist/daemons/sidechannel/main.py:854-861` in a local checkout:
stdout over `10 * constants.KiB` becomes `stdout_blob`, with no
`stdout` key) and against `_collect_probe()` (`cluster.py:2644`, which
reads only `result.get('stdout')`). `grep -rn stdout_blob
shakenfist_client_k3s` finds nothing.

Judged at `bd9bead`, with every cited line assigned to a merge by
`git blame`; none comes from `243a91c` (#121). The lens also read, read
only, Shaken Fist `066816ee3` (`daemons/sidechannel/main.py` and the
smoke test `test_agentops.py`), agent-python `9a84f23` (`daemon.py`),
and Kubernetes' `pkg/apis/core/validation/validation.go`. Throwaway
probe scripts ran under `.tox/py3/bin/python` and local `dash`; nothing
touched a cluster.

| Id | Rating | One line |
|---|---|---|
| SEC-1 | fix | Kubernetes probe output over 10 KiB becomes a blob, and `health()` then reports every node "not registered" and no OOM kills |
| SEC-2 | consider | Two printed go-template fields are not validated by the API server, so a compromised worker can forge other nodes' readings, `oom_killed` entries and `unmatched_nodes` |

Nothing in scope lets a value the plugin did not choose reach a node's
shell unquoted, and nothing puts a secret into a report, a progress
line, an exception or the CI log.

## Findings

### SEC-1 (fix): an over-10 KiB probe answer reads as "no nodes and no kills"

**Where:** `cluster.py:2644` reads the probe's stdout (pre-existing,
`8ba16a09`); `cluster.py:2776-2781` parses it and `cluster.py:1069-1073`
says "nothing bounds output" (both #122).

**Claim.** Shaken Fist's sidechannel stores any execute stdout longer
than 10 KiB as a blob. The result then carries `stdout_blob` and **no
`stdout` key**, and Shaken Fist's own smoke test asserts
`'stdout' not in aop['results']['0']`. This has been the behaviour
since v0.7.0 (`8f1adf4d6`), so every server the plugin supports does it.

`_collect_probe()` reads `result.get('stdout')`, gets None, and still
sets `answered` True on return code 0. `_kubernetes_from_probe()` then
parses None as empty ("An answer which printed no record at all is
believed"). For every node: `registered: False` with every condition
None, and `oom_killed: []`, which claims nothing was killed. At the top
level: `unmatched_nodes: []`, `answered: True`, `error: None`, and
`healthy` False. That is the "empty list claims something nobody read"
outcome the design takes pains to avoid, with no error saying why.
Phase 2's survey finding 9 said no server-side limit was found. There
is one, and it is a reroute, not a cap.

**Evidence.** A node line is about 72 characters, so about 142 nodes
cross 10 KiB with no attacker at all. An `oom` line is about 82
characters, so about 120 OOM-killed containers cross it. With names at
their maximum lengths (pod 253, namespace 63, container 63) an `oom`
line is about 430 characters, so about 24 containers cross it, and one
pod can hold all of them. A tenant who can create pods can therefore
blind the probe deliberately, and the probe then reports
`oom_killed: []` for exactly the kills that caused it. Feeding
`_kubernetes_from_probe()` `{'answered': True, 'stdout': None}` returned
`unmatched_nodes: []`, and `registered: False` and `oom_killed: []` for
both nodes.

**Fix direction.** In `_collect_probe()`, treat a result with
`stdout_blob` (or with no `stdout` despite exiting 0) as not answered,
with an error that names the blob. Or fetch the blob with a size cap,
as `await_fetch()` does for `content_blob`. Either way, pin it with a
`HealthClient` fake result carrying `stdout_blob`. The same root cause
makes the pre-existing `api` probe's `kubectl get nodes` output vanish
on a large cluster; that code is not this plan's, but the same fix
covers it. The signals probe prints about 400 bytes, so only a node
lying about itself can reach the limit there.

### SEC-2 (consider): unvalidated go-template fields let a worker forge other nodes' records

**Where:** `cluster.py:949` (condition `.status`, printed by
`_node_condition_template()`), `cluster.py:1020` (container status
`.name`, printed by `_oom_line_template()`), `cluster.py:1330-1339` (the
line split and first-record-wins), and `cluster.py:1112-1117` (the
comment claiming "the API cannot hold a name either refuses"). All
#122.

**Claim.** The parser assumes no printed field can hold a tab or
newline. Node and pod names, `spec.nodeName`, namespace,
`restartCount` and the timestamps are all validated or typed by the
API server. Two printed fields are not:

- Node condition `.status`: `ValidateNode` and `ValidateNodeUpdate` do
  not check conditions ("anyone can update node status").
- Container status `.name`: `ValidatePodStatusUpdate` says there is no
  check that container statuses belong to containers in the pod spec,
  and that "consumers of those fields must account for unexpected
  data".

Root on a worker holds that node's kubelet credentials, which under
NodeRestriction can patch the node's own status and the status of pods
bound to it. text/template prints strings verbatim, so an embedded
`\n` starts a new record. Python's `splitlines()` also splits on `\r`,
`\x0b`, `\x0c`, `\x1c` to `\x1e`, `\x85`, U+2028 and U+2029.

**Evidence.** With node 002's Ready status set to a string carrying
`\n` and forged `node` and `oom` lines, `parse_kubernetes_readings()`
returned node 003 as `ready: 'True'` when its real line said `False`
(nodes are listed sorted, so the forged line comes first and wins), a
nonexistent node 999 that `health()` would list in `unmatched_nodes`,
and a forged `oom_killed` entry on control plane node 001. A container
status name carrying `\noom\t…` forged an `oom` entry on node 003.

**Impact.** A compromised worker can report any later-numbered node as
Ready, flipping `healthy` and `--strict` to True while that node is
down, and can add OOM entries and `unmatched_nodes` names. Forged
values still pass the validators, so nothing harmful reaches a
terminal. Before this plan, a worker could only lie about itself. It
needs root on a node, hence `consider`, but it is a defect in this
plan's code, and the comment at 1112-1117 states a premise that is
false for container status names.

**Fix direction.** Print every free-form field as
`{{printf "%q" ...}}`, which escapes tabs, newlines and the Unicode line
separators, and require the quoted form in the validators. Or print a
status only through `eq .status "True"` and its siblings. Splitting on
`'\n'` instead of `splitlines()` is not enough on its own.

## Existing issues (rediscovered, not re-raised)

- **#105 (agent results trusted).** Not worse in the probe path: the
  signals and Kubernetes probes read `results` only through the guarded
  `_collect_probe()`. `await_nodes_ready()` (`cluster.py:3266`, #122)
  is a new caller of `execute_and_await()` → `reap_execute()`, whose
  hard subscripts #105 names, in the same shape as every install
  command `create()` already runs. SEC-1 is a different defect: the read
  is guarded; "absent means empty" is what is wrong.
- **Pre-existing, not this plan's code.** `_render_health()` writes
  `api['stdout']` and `api['stderr']` to the terminal raw
  (`__init__.py:437-441`, `8ba16a09`), so a compromised first control
  plane node can put escape sequences there. The new `signals` and
  `kubernetes` rendering does not share the problem. No issue found for
  it.

## Safe by construction

**Outbound: values the plugin did not choose, reaching a node's shell.**

- **Cluster name and namespace** appear in no command these phases
  added. `K3S_KUBERNETES_PROBE_COMMAND` is a module constant, and both
  templates are `shlex.quote()`d and contain no single quote.
- **Node names** enter only `nodes_ready_command()`, which quotes each
  once and uses them only as `"$node"`, a printf argument or
  `grep -F --` input. They are `k3s-<validated name>-node-NNN`
  lowercased, so cannot start with `-`.
- **The etcd snapshot directory**, the one caller value in the signals
  command, is passed on only as a `str`, `shlex.quote()`d, placed after
  `--`, refused if relative, and run inside `"$( )"`. Under dash,
  `/a)b`, `'; echo PWNED #`, `$(...)`, a backtick form and an embedded
  newline all arrived as literal text. A newline cannot replace a
  reading, because every known key is printed earlier and the first
  occurrence wins. It never reaches the report or an error message.
- **Role** is validated by `k3s_unit_for_role()`, and `_proc_field()`
  interpolates only module literals.

**Inbound: forging, exceptions, slowness, terminals.**

- Signals cannot forge another node: each probe is fetched by its own
  operation uuid and filed under the instance it ran on, and every
  value passes a length- and charset-capped regex.
- Most Kubernetes fields cannot be forged; SEC-2 covers the two that
  can.
- No exception escapes. Neither parser raises for any `str`, including
  lone surrogates, NUL, ESC and 100k tabs. A non-`str` stdout cannot
  occur (`ExecuteReply.stdout` is a protobuf `string`). Non-UTF-8 output
  is refused by the agent's protobuf constructor, and the probe reports
  the failed operation rather than raising.
- No slowness: probe waits share one bounded deadline, the parsers are
  linear (1 MB of crafted input in about 3 ms), and the name regex's
  length lookahead bounds backtracking.
- No escape sequences reach a terminal or Ansible from the new code:
  every value is validated, error strings combine literals with values
  Shaken Fist types, and the Kubernetes probe's stdout is left out of
  the report.

**Secrets.** `health()` reads only `etcd-snapshot-dir` from
`server_config` and never reports it. The new commands carry no token
and name `/etc/rancher/k3s/k3s.yaml` by path only. Progress and debug
lines carry uuids and node names. `ci_health_signals.py`'s failure
output never dumps metadata or kubeconfig contents, and the pod
manifest goes over stdin.

**`tools/ci_health_signals.py`.** Every subprocess is an argv list with
no local shell. `disk_fill_command()` interpolates an int with `%d` and
a quoted constant path, then quotes the whole script for `sh -c`.
Destructive node commands go by instance uuid taken from `health()` on
the named cluster, and kubectl's `KUBECONFIG` is guarded by
`kubectl get node <name>` before anything is created. A same-named
cluster in another namespace would pass that guard, but only the 32Mi
OOM pod would land there, and 4b would fail before any destructive
step: `none`.

**path-traversal-review v1.** No local path in scope is built from
outside data. The snapshot directory is a remote path, read only by
`du`, chosen by the caller, quoted, and refused when relative.
