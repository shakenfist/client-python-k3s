# Missing verbs phase 3: the verbs a daemon needs

## Prompt

Before responding to questions or discussion points in this document,
explore the client-python-k3s codebase thoroughly. Read
`shakenfist_client_k3s/cluster.py` in full -- every item in this phase
touches it -- and read `progress.py`, `exceptions.py` and the Click
command bodies in `__init__.py` for the patterns phases 1 and 2
established. Ground your answers in what the code does today rather
than in what this plan or the master plan says about it.

Two external documents matter. `33fl/docs/plans/PLAN-k3s-ci-runners.md`
is the consumer: design decisions 2, 3 and 5 and the "Upstream
prerequisites" section state what conductor actually needs, and this
phase exists to deliver six of those seven prerequisites. The master
plan `library-api-and-collection.md` carries the phase row this plan
expands, the sub-agent execution model, and the model roster.

Remember that orchestration behaviour can only be fully validated
against a live Shaken Fist cluster; unit tests mock the API surface.
Three items here are read-modify-write against namespace metadata that
conductor also writes, so reason about interleaving explicitly rather
than assuming this process is the only writer.

## Planning effort

**High.** The master plan calls for it by name: "within phase 3 the
incremental `install_workers()`, `remove-worker` and the `state:
initial` reconcile, all of which are read-modify-write against
namespace metadata that conductor also writes". The four additive
items -- optional Longhorn and MetalLB, the kubeconfig opt-out, the
health verb and the manifest hook -- are medium-effort work that
follows patterns already in the tree.

## Scope

In scope, as the nine items the master plan's phase 3 row lists:

1. `install_workers()` installs only the nodes it is given.
2. A `remove-worker` verb.
3. Detection and safe teardown of a cluster left mid-create.
4. `--no-longhorn` and `--no-metallb`.
5. Opt-out kubeconfig side effects, including the `kubectl config
   unset` stdout leak phase 1 pinned.
6. A health verb.
7. A manifest payload hook.
8. `GroupCatchClusterExceptions` printing to `sys.stderr`.
9. `Cluster.get_progress()`'s lazy default carrying a real
   `total_phases`.

Explicitly out of scope:

- **Resuming an interrupted create.** Decision 5 scopes item 3 to
  detection and teardown, and records why.
- **Shrinking the control plane**, and any declarative reconciliation
  of worker count. The master plan's Future work already places both
  out of scope while conductor owns worker membership, and
  `remove-worker` is an imperative verb, not a reconciler.
- **A k3s version upgrade story.** Master plan Future work.
- **The first PyPI release and the collection.** Phases 4 and 5.

## What the survey found

The phase 3 row was written before phases 1 and 2 executed. Five
findings, two of which change what the phase is.

**1. `expand-workers` reinstalls k3s on every existing worker.** This
is the most important finding, and the row understates it: it lists
"make `install_workers()` incremental" as a missing feature, and it is
a live-cluster defect. `create_and_await_instances()`
(`cluster.py:293-313`) appends each new instance UUID to
`md['worker_nodes']` and saves the metadata as it goes. `expand_workers()`
(`:875-889`) then calls `install_workers()` (`:416-420`), which runs
`install_k3s_component(md['worker_nodes'], ...)` over the *whole* list.
Adding one worker to a five-worker cluster therefore re-runs
`curl -sfL https://get.k3s.io | ... sh -s - agent` on all six, on nodes
that are running workloads. Nothing in the tree prevents this and no
test covers it.

**2. Cluster state is written but never read.** `md['state']` is set to
`initial` at `:633`, `created` at `:735` and `deleted` at `:842`, and
no code anywhere reads it -- every `['state']` read in the package is an
instance or agent-operation state from the API. So item 3 is not
"finish a state machine", it is "start reading the one we already
write". `create()`'s existing guard (`:590-593`) refuses any name
already in the cluster list or holding metadata, so a cluster stranded
in `initial` blocks its own name until someone deletes it, with an
error that says only that it exists.

**3. The three phase-1 leftovers are accurately described and still
present.** The `kubectl config unset` loop at `:858-873` still passes
no `capture_output`, and its comment points here; `KubectlUnsetLeakTestCase`
(`tests/test_library_api.py:448-512`) pins the leak and names the
conditions under which it should be deleted. `GroupCatchClusterExceptions.invoke()`
(`__init__.py:105-113`) still prints to stdout. `get_progress()`
(`cluster.py:121-132`) still builds its lazy `Progress` with no
`total_phases`, so a library caller invoking a mid-level method gets
`[n]` rather than `[n/total]`.

**4. Longhorn and MetalLB are unconditional, and the hook has a shape
to copy.** `create()` calls `setup_metallb()` and `setup_longhorn()`
back to back at `:689-690` with no way to decline either. The consumer
plan's note that helm is already in use holds, but **its file
references are stale**: it cites `primitives.py:478` and `:620-647`,
and phase 1 moved all of that into `cluster.py` -- `extrepo enable helm`
is now at `:359-360`, and the `helm repo add` / `helm --kubeconfig ...
install` pairs at `:496-498` (MetalLB) and `:519-523` (Longhorn).
`primitives.py` is 209 lines and holds only namespace lookups and the
two version caches. That repository is not ours to edit; the
correction is recorded here.

**5. Nothing is released, so the library's defaults are still free.**
The consumer plan confirms there are no git tags and
`shakenfist-client-k3s` 404s on PyPI, and phase 4 is the first release.
The only caller of `Cluster.create()` and `Cluster.delete()` today is
this package's own CLI. Decision 6 depends on that.

Findings 1, 2 and 4 are corrected at source in the master plan's phase
3 row as part of this planning commit; a later step should not redo
that.

## Decisions this plan already takes

**1. Phase 3 stays one phase, sequenced in two groups with a gate.**
Nine items is large, and splitting would renumber phases 4, 5 and 6
across the master plan, the index and the consumer plan that cites
them. Phase 2 already showed a phase can land over several pull
requests (`1c32d12`, `4e76704`, `871f6ee`, `539b50d`), so the phase is a
unit of planning rather than of review. Steps 3a-3c are the
metadata-correctness group and land first; a back brief gates the rest.

**2. `install_workers()` takes the nodes to install, rather than
gaining an incremental mode.** `create_and_await_instances()` already
knows exactly which UUIDs it created -- it builds `new_nodes` locally
and throws it away. Returning that list and passing it to
`install_workers()` fixes finding 1 at its cause and leaves no flag
for a caller to get wrong. `create()` continues to install every
worker because at create time every worker is new.

**3. `remove-worker` drains the node and removes it from k3s before
deleting the instance.** Deleting a Shaken Fist instance out from under
k3s leaves a `NotReady` node object in the cluster forever, and
workloads that were scheduled on it are only rescheduled once the node
controller's eviction timeout expires. `kubectl drain --ignore-daemonsets
--delete-emptydir-data` followed by `kubectl delete node`, run on a
control plane node through the agent, is what makes the verb safe to
call on a cluster with something running on it. This is the decision a
reviewer is most likely to want cheaper: conductor's workers are
ephemeral CI runners, so "just delete the instance" is tempting. It is
wrong for anyone who is not conductor, and the drain is three agent
commands.

**4. Worker selection is by instance UUID, not by name or by count.**
`remove-worker --worker <uuid>`, repeatable. A count would make the
verb a reconciler, which the master plan's Future work puts out of
scope; a name would need a lookup this package does not have.
Conductor knows the UUID because `expand-workers` reports it.

**5. Item 3 delivers detection and safe teardown, not resume.** This is
the decision most likely to be argued with, because the consumer plan
calls the prerequisite "idempotence/crash recovery". Resuming a
half-built cluster means a state machine with a resume point per phase
of `create()`, against metadata a second writer may have touched, and
validating it means deliberately killing a real create at eight
different points. Detection is most of the value for a tenth of the
work: `create()` distinguishes "a cluster of that name is already
built" from "a cluster of that name was interrupted, delete it and try
again", `delete()` is made to work on a cluster that never reached
`created`, and the health verb reports the state. Resume goes to the
master plan's Future work with this reasoning attached. If conductor
later needs resume, it will need it against a state machine that is
being read at all, which is what this step builds.

**6. The kubeconfig side effects default to off in the library and on
from the CLI.** `Cluster.create(write_kubeconfig=False)` and
`Cluster.delete(update_kubeconfig=False)`; the Click commands pass
`True` unless `--no-kubeconfig` is given, so the command line is
unchanged. A library whose default is to rewrite `~/.kube/config` is
surprising, and finding 5 is what makes the safer default free: there
are no released library callers to break. Taking the other default
"for symmetry with the CLI" would bake the surprise in permanently at
the one moment it costs nothing to avoid.

**7. The health verb returns structured data; the CLI renders it.**
`Cluster.health()` returns a dict -- per-node instance and agent state,
cluster metadata state, and whether the API address answers -- and
`k3s health` renders it. This is what phases 1 and 2 established, and
it is what makes the verb usable from the Ansible module in phase 5
without parsing text. It performs no repair: a verb that silently fixes
things cannot be used to decide whether to fix things.

**8. The manifest hook writes to k3s's auto-apply directory.**
`--manifest <path>`, repeatable, reading local files and writing them
into `/var/lib/rancher/k3s/server/manifests/` on the first control
plane node before k3s starts, which is the mechanism the consumer
plan's design decision 3 names. No templating and no ordering control:
k3s applies that directory itself, and a payload needing more than that
belongs in a helm chart the caller installs afterwards.

**9. The two stdout leaks are one step, not two.** Items 8 and 9 in the
scope list are both one-line behaviour corrections with a test each,
both left by phase 1, and neither is worth a sub-agent of its own.

## Step plan

| Step | Effort | Model | Isolation | Brief for sub-agent |
|------|--------|-------|-----------|---------------------|
| 3a | high | opus | none | **Commit subject: `Install k3s only on the new workers.`** Fix survey finding 1: `expand-workers` currently re-runs the k3s agent installer on every existing worker. In `shakenfist_client_k3s/cluster.py`, make `create_and_await_instances()` (`:293-313`) return its `new_nodes` list -- it already builds it and discards it. Change `install_workers()` (`:416-420`) to take an `instance_uuids` argument and pass that to `install_k3s_component()` instead of `md['worker_nodes']`; do not give it a default, so every call site has to say what it means. Update the two callers: `create()` at `:668` passes `md['worker_nodes']` (at create time every worker is new, and saying so explicitly is the point), and `expand_workers()` at `:888` passes the list returned by its `create_and_await_instances()` call at `:887`. Do not add an "incremental" flag -- decision 2. Then pin it: add a test to `tests/test_cluster.py` that builds a cluster whose metadata already lists three workers, calls `expand_workers(1)`, and asserts that the UUIDs passed to `install_k3s_component` are exactly the one new instance -- not four. Before committing, prove the test detects the bug: revert `install_workers()` to read `md['worker_nodes']`, confirm the new test fails, restore. Say so in the commit message. Touch nothing else in `cluster.py`. |
| 3b | high | opus | none | **Commit subject: `Add remove-worker.`** Add `Cluster.remove_worker(instance_uuids)` to `cluster.py` and a `k3s remove-worker` Click command. Per decision 3 the node is drained before the instance is deleted: for each UUID, run `kubectl drain <node> --ignore-daemonsets --delete-emptydir-data --kubeconfig /etc/rancher/k3s/k3s.yaml` then `kubectl delete node <node>` on `md['control_plane_nodes'][0]` through `execute_and_await()`, then `self.client.delete_instance(uuid)`, then remove the UUID from `md['worker_nodes']` and `set_metadata()`. The k3s node name is the instance's hostname; get it from `self.client.get_instance(uuid)` rather than assuming it matches the Shaken Fist instance name, and say in a comment which field you used and why. Raise from `exceptions.py` -- add a subclass if none fits -- when a UUID is not in `md['worker_nodes']`, and do it for every UUID before deleting anything, so a typo in the third of three does not leave the first two gone. Refuse to remove the last worker only if that is cheap to express; otherwise say in the docstring that it is the caller's business. Per decision 4 the CLI option is `--worker <uuid>`, `multiple=True`, required. Tests in `tests/test_cluster.py` and `tests/test_commands.py`: the happy path asserts drain and delete-node ran before `delete_instance`; a UUID not in the cluster raises and nothing is deleted; metadata afterwards lists the survivors in their original order. Add the command to the CLI contract fixtures under `tests/cli_contract/` if that directory pins the command list -- check before assuming. |
| 3c | high | opus | none | **Commit subject: `Notice a cluster that never finished.`** Survey finding 2: `md['state']` is written at `cluster.py:633`, `:735` and `:842` and read nowhere. Start reading it, per decision 5 -- detection and teardown only, no resume. Three changes. First, `create()`'s guard at `:590-593` currently raises `ClusterExistsError` for any name that is taken; when the existing metadata has `state` other than `created`, raise a distinct exception instead whose message says the cluster was interrupted mid-build, names its state, and names `sf-client k3s delete <name>` as the way forward. Add it to `exceptions.py` following the constructor-classmethod style already there. Second, make `delete()` work on a cluster that never reached `created`: read it and fix what breaks rather than assuming -- `md['kubeconfig']` may be absent, `control_plane_nodes` may be empty, and the `kubectl config unset` loop at `:858-873` runs regardless. Third, add the state to what `show()` (`:757-767`) reports. Tests in `tests/test_cluster.py`: creating over an `initial` cluster raises the new exception and its message names the delete command; deleting an `initial` cluster with one control plane node and no kubeconfig succeeds and clears the metadata; `show()` includes the state. Do not add a resume path, do not add a `--force` flag, and do not change the `created` and `deleted` writes. |
| -- | -- | -- | -- | **Back brief gate.** Steps 3a-3c change metadata handling on live clusters. Stop here, report what landed, and get agreement before starting 3d. |
| 3d | medium | sonnet | none | **Commit subject: `Make Longhorn and MetalLB optional.`** `create()` calls `setup_metallb(metal_address_count)` and `setup_longhorn()` unconditionally at `cluster.py:689-690`. Add `install_metallb=True` and `install_longhorn=True` parameters to `Cluster.create()` and `--no-metallb` / `--no-longhorn` flags to `k3s create`, following the `--refresh-version-cache/--no-refresh-version-cache` option style already at `__init__.py:157`. Two things to get right rather than guess: `create()` computes `total_phases` for its `Progress` -- find where and make the count follow the flags, or the phase numbering reads `[7/9]` and stops; and `--metal-address-count` is meaningless with `--no-metallb`, so decide whether that combination is an error or is ignored, implement it, and say which in the option help. Skipping Longhorn also means `setup_longhorn()`'s storage-class patch at `:529` does not run, so check whether anything later in `create()` assumes Longhorn is the default storage class. Tests: each flag skips exactly its own setup call and nothing else; the phase count matches the phases actually run. |
| 3e | medium | opus | none | **Commit subject: `Make the kubeconfig side effects optional.`** Per decision 6, `Cluster.create()` gains `write_kubeconfig=False` and `Cluster.delete()` gains `update_kubeconfig=False` -- note the defaults are off, which is the opposite of today's behaviour, and the Click commands pass `True` unless `--no-kubeconfig` is given, so the command line is unchanged. In `create()` that gates the local kubeconfig write and the `kubectl config view --flatten` merge (`:692-733`); the fetch of the kubeconfig into `md['kubeconfig']` (`:674-685`) stays unconditional, because `get_kubeconfig()` serves it and the Ansible module will want it. In `delete()` it gates the `kubectl config unset` loop (`:858-873`). While you are there, fix the leak that loop carries: add `capture_output=True` to the `subprocess.run()` call, since the three `Property "..." unset.` lines it currently writes to file descriptor 1 bypass the reporter entirely. Then delete `KubectlUnsetLeakTestCase` (`tests/test_library_api.py:448-512`) and remove the caveat it names from the create-through-delete stdout test -- that test's docstring at `:495-512` says exactly this and tells you what to remove. Update the `delete()` docstring at `:769-776`, which says "phase 3 makes it optional" in the present tense. Tests: `write_kubeconfig=False` writes no file and runs no `kubectl config view`; `update_kubeconfig=False` runs no `unset`; the CLI without `--no-kubeconfig` still does both. |
| 3f | medium | opus | none | **Commit subject: `Add a health verb.`** Per decision 7, add `Cluster.health()` returning a dict and a `k3s health` command rendering it. The dict carries the cluster's metadata `state`, and per node its UUID, its Shaken Fist instance state and agent state, and its role. Get those from `self.client.get_instance()` per UUID in `md['control_plane_nodes']` and `md['worker_nodes']`; `await_boot()` (`:180-195`) shows the fields and how they are read. Also report whether the k3s API answers, by running `kubectl get nodes` on the first control plane node through `execute_and_await()` and recording whether it succeeded -- but treat a failure there as a health finding, not an exception, which is the whole point of the verb. Do not repair anything. A cluster whose metadata is missing raises `ClusterNotFoundError` as the other verbs do. The CLI rendering goes through the reporter, not `print()`. Tests: a healthy cluster reports every node; an instance in `error` state is reported rather than raised; a missing cluster raises. |
| 3g | medium | opus | none | **Commit subject: `Add a manifest payload hook.`** Per decision 8, `Cluster.create()` gains `manifests=None` taking a list of local file paths, and `k3s create` gains `--manifest`, `multiple=True`, `type=click.Path(exists=True)`. Each file is read locally and written into `/var/lib/rancher/k3s/server/manifests/` on the first control plane node before k3s is installed there, so k3s auto-applies it on first start. `install_control_plane()` (`:335-382`) is where the first control plane node is prepared and where the k3s install command runs -- read it and place the write before the install, not after. Use the same agent mechanism the rest of the file uses to put content on a node; `install_control_plane()` already writes config files, so follow whatever it does rather than inventing a path. Destination filenames are the source basenames; a duplicate basename is an error raised before anything is written. Nothing is templated. Tests: two manifests are written before the k3s install command runs; duplicate basenames raise; no manifests means no write. |
| 3h | low | sonnet | none | **Commit subject: `Send error output to stderr.`** The two stdout leaks phase 1 left, per decision 9. First, `GroupCatchClusterExceptions.invoke()` in `shakenfist_client_k3s/__init__.py:105-113` prints the exception message with `print(str(e))`; send it to `sys.stderr` instead. Its comment says the message goes "on stdout where it has always gone", which was phase 1 deliberately not changing user-visible output; rewrite the comment to say why stderr is now right rather than deleting it. Check `tests/test_cli_errors.py` for assertions on `result.output` that will need to become `result.stderr`, and note that Click's `CliRunner` needs `mix_stderr=False` to separate them -- check the Click version in `pyproject.toml` before assuming the argument exists. Second, `Cluster.get_progress()` (`cluster.py:121-132`) builds its lazy `Progress` with no `total_phases`, so a library caller invoking a mid-level method directly gets `[n]` headers instead of `[n/total]`. Give it a real count. The honest number for a single method call is 1; if you can see a better one from the call sites, take it and say why in the docstring. Tests for both. |
| 3i | low | sonnet | none | **Commit subject: `Document the new verbs.`** Documentation only, no code. `docs/usage.md` gains `remove-worker` and `health` with examples, and the new `create` flags `--no-longhorn`, `--no-metallb`, `--no-kubeconfig` and `--manifest`. `docs/library-api.md` gains `remove_worker()` and `health()`, and -- this is the part not to skip -- a short subsection saying that `write_kubeconfig` and `update_kubeconfig` default to off for library callers and on from the command line, and why (decision 6). Add the interrupted-cluster error from 3c to whatever troubleshooting or errors section exists, or say in the commit message that none does. `AGENTS.md` gets a row only if a new file was added; it is an index, not a reference. `ARCHITECTURE.md` changes only if the component inventory changed -- a new verb on an existing class is not that. Do not touch `README.md`. |

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| Step 3a's fix is correct but the bug it fixes has already damaged a live cluster -- a reinstalled worker may be in a state no test covers. | Out of this phase's hands, but the management session should say so when the fix lands: anyone who has run `expand-workers` against a cluster they care about should check their workers. The fix stops it recurring; it does not repair. |
| Conductor writes the same namespace metadata, so 3a, 3b and 3c's read-modify-write cycles can lose a concurrent update. | Already true of every existing method and not made worse here. The sub-agent briefs say to reason about interleaving; if a step finds a case where this phase makes it *worse*, that is a stop-and-report, not a fix to improvise. Recorded as an open question below. |
| Decision 6 reverses the default for `write_kubeconfig`, which is a breaking change for any library caller. | Finding 5 establishes there are none: no tags, not on PyPI, and the CLI is the only caller in the tree. Phase 4 is the first release, so this lands before anyone can depend on it. If phase 4 slips behind phase 3 in a way that changes this, revisit. |
| Step 3b's drain can hang: `kubectl drain` blocks on a pod with no other node to go to. | `execute_and_await()` has the wait loop and the agent operation timeout; the brief should not add a second timeout. A drain that does not finish surfaces as an agent operation error, which is the right failure. Worth a note in the docstring that removing the last worker of a cluster with scheduled workloads will block. |
| Nine steps is a lot of surface for one phase, and the later ones are the least reviewed. | The back brief gate after 3c splits the phase where the risk is. Steps 3d-3i are additive and independently revertable. |

## Open questions

1. **Concurrent metadata writers.** Conductor and this package both
   read-modify-write the same namespace metadata key. Nothing in this
   phase makes that worse, and nothing in it fixes it. Does it need a
   compare-and-set before phase 5 ships an Ansible module that a
   playbook will run in parallel across hosts?
2. **Does `remove-worker` belong to conductor at all?** Decision 4
   keeps it imperative because the master plan puts worker-count
   reconciliation out of scope. If conductor would rather say "I want
   N workers", that is a different verb and a different phase.

## Definition of done

Each of these is checkable:

- `grep -n "md\['worker_nodes'\]" shakenfist_client_k3s/cluster.py`
  shows no occurrence inside `install_workers()`.
- A test fails if `install_workers()` is given every worker rather
  than the new ones, and the commit message for 3a records that this
  was demonstrated by reverting the fix.
- `sf-client k3s remove-worker --help` exists, and a test asserts that
  a UUID absent from `md['worker_nodes']` raises before any
  `delete_instance()` call.
- `md['state']` is read in at least `create()`, `delete()` and
  `show()`; `grep -n "state" shakenfist_client_k3s/cluster.py` no
  longer shows it written-only.
- Creating a cluster whose name holds `state: initial` metadata
  produces an error naming the delete command, asserted by a test on
  the message.
- `KubectlUnsetLeakTestCase` no longer exists, and the
  create-through-delete stdout test no longer carries its caveat.
- `grep -n 'print(' shakenfist_client_k3s/__init__.py` shows no
  unqualified `print()` in `GroupCatchClusterExceptions`.
- `Cluster.create()` and `Cluster.delete()` both have a test that
  passes the kubeconfig parameter false and asserts no `subprocess.run`
  call was made.
- Every new command appears in `docs/usage.md` and every new public
  method in `docs/library-api.md`; no fact about the kubeconfig
  defaults is stated differently in those two files.
- `pre-commit run --all-files` and `tox -epy3` pass.
- The master plan's phase 3 row no longer contains the three claims
  the survey corrected, and its Execution table records phase 3 as
  Complete with the merge commits that landed it.

## Back brief

Restate, before starting 3a: which of the nine items are metadata
read-modify-write and therefore in the gated group; what decision 5
scopes item 3 down to and why; and which default decision 6 reverses,
and what makes that safe. If any of those three readings differ from
this plan, say so before writing code.

The gate after 3c is a hard stop. Report what the three metadata steps
changed, and specifically whether any of them found a concurrency case
this phase makes worse, before starting 3d.
