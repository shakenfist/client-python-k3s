# Using the k3s orchestration as a library

The command bodies behind `sf-client k3s ...` are Python, not shell: a
caller which is not a terminal -- an Ansible module, a conductor
reconcile loop -- can drive a cluster directly, with no Click
involved and no dependency on `click.Context`. This page indexes that
API. The docstrings in `shakenfist_client_k3s/cluster.py` and
`shakenfist_client_k3s/exceptions.py` are the reference; this page is
the map.

## Constructing a `Cluster`

```python
from shakenfist_client_k3s.client import make_client
from shakenfist_client_k3s.cluster import Cluster

client = make_client()
cluster = Cluster(client, 'mycluster', client.namespace)
```

`Cluster(client, name, namespace, reporter=None)` takes an already
constructed `apiclient.Client`, the cluster's name, and the namespace
it lives in -- the same values `NAME` and `--namespace` supply on the
command line (see `docs/usage.md`). `reporter` defaults to one that
writes to real stdout; pass a `CollectingReporter` to capture output
instead (below).

A `Cluster` caches its own namespace metadata document for its
lifetime (`get_metadata()`, `set_metadata(md)`, `delete_metadata()`),
fetching it once on first read and writing straight through to the
API on every change. That cache is what keeps the number of namespace
metadata reads the same as when this state lived in a Click context;
namespace metadata is a single document conductor also writes, so
every extra read widens the window for losing someone else's update.
Build a fresh `Cluster` per operation rather than reusing one across
unrelated clusters or a long-lived process.

### Constructing the client

`make_client()` exists because `apiclient.Client` on its own is the
wrong thing to reach for here. Its default strategy is `ASYNC_BLOCK`,
under which the client waits for each operation internally and returns
only once it has finished -- so the orchestration's own wait loops
never run and the reporter has nothing to report. `make_client()` sets
`ASYNC_CONTINUE` instead, which is what makes progress visible at all.

```python
from shakenfist_client_k3s.client import make_client

# Discover configuration the way the CLI does.
client = make_client()

# Or supply it, and skip discovery entirely.
client = make_client(api_url='https://api.example.com',
                     namespace='mynamespace', key='mykey')
```

With no arguments it finds configuration exactly as `sf-client` does,
from the environment, `~/.shakenfist` and `/etc/sf/shakenfist.json`.
Supplying all three of `api_url`, `namespace` and `key` uses them
verbatim and suppresses that discovery.

Supplying only some of them is a `ValueError`. Falling back to
discovery in that case would hand back a client pointed at whatever
cloud the environment names rather than the one the caller asked for,
with nothing said about it -- which is the failure this package just
removed from the CLI. A caller building its arguments from partially
populated configuration is the likeliest one to hit it, so it fails
loudly instead. `apiclient.UnconfiguredException` propagates when
discovery finds nothing, rather than being translated into a message
or an exit code -- that choice belongs to the caller.

A caller which brings its own client instead must set
`async_strategy=apiclient.ASYNC_CONTINUE` on it, for the same reason.

The `sf-client k3s` commands do not use this factory. `sf-client`
builds a client from its own `--apiurl`, `--key` and `--namespace`
options and the plugin uses that one, which is what keeps those
options meaningful (see `docs/usage.md`).

### The `Cluster` methods

Each command's body is a method taking that command's options, minus
`name` and `namespace`, and returning a value instead of printing one:

| Method | Command it replaces |
|--------|---------------------|
| `create(control_plane_count, worker_count, metal_address_count, network=None, ...)` | `k3s create` |
| `get_kubeconfig()` | `k3s getconfig` |
| `show()` | `k3s show` |
| `health()` | `k3s health` |
| `delete()` | `k3s delete` |
| `expand_workers(worker_count)` | `k3s expand-workers` |
| `remove_worker(instance_uuids)` | `k3s remove-worker` |
| `expand_addresses(address_count)` | `k3s expand-addresses` |
| `update_os()` | `k3s update-os` |

Only these nine methods, plus `get_metadata()`,
`set_metadata(md)` and `delete_metadata()`, are this library's
stable public surface: this package is published to PyPI, so what
is public here is a compatibility surface for external callers. Everything else `Cluster` exposes --
`create_instance()`, `await_boot()`, `await_idle()`,
`await_fetch()`, `await_execute()`, `reap_execute()`,
`execute_and_await()`, `instance_os_update()`,
`install_control_plane()`, `install_k3s_component()`,
`install_extra_control_plane()`, `install_workers()`,
`allocate_metallb_addresses()`, `configure_metallb_addresses()`,
`setup_metallb()`, `setup_longhorn()`,
`create_and_await_instances()`, `start_progress()`,
`get_progress()`,
`_interrupted_state()` and `_require_usable()` -- is internal
orchestration the methods above are built from, not a supported
entry point, so treat it as unstable even though nothing today
stops a caller reaching it directly.

One module level name in `cluster.py` is on the stable side of that
line: `read_manifests(paths)`, which takes a list of local paths and
returns a list of `(basename, content)` pairs, raising `ManifestError`
for the first path it cannot stage. `create()` calls it before it
allocates anything, and it is public deliberately so that a caller
which wants to validate paths without building a cluster -- an Ansible
module in check mode, a form which wants to reject a file before the
operator waits twenty minutes -- can do the same check the real call
will do. It touches no cluster and no API client.

The argument checks `create()` and the expand verbs run first are
public pure functions too, for the same reason: `validate_cluster_name(name)`,
`validate_counts(floor, **counts)`, `validate_create_counts(...)`,
`validate_create_arguments(...)` (every check `create()` can make without
the API, in one call) and `CLUSTER_NAME_MAX_LENGTH`. They raise
`ClusterNameError`, `ShapeError` and the other errors above, touch no
cluster and no API client, and let an Ansible module in check mode or a
form refuse a bad request before the operator waits for it.

`read_k3s_config(path, role)` is public on the same terms. It takes the
path of a k3s configuration file and `'server'` or `'agent'`, and
returns the mapping the file holds, ready to pass as `server_config` or
`agent_config`. It raises `K3sConfigError` for a file it cannot read or
parse, which uses a YAML alias, or whose keys `create()` would refuse.
It touches no cluster and no API client. Those refusals protect the
plugin's own operations rather than the cluster, so a form taking
configuration from someone less trusted than the cluster's owner needs
an allowlist of its own; `docs/usage.md` explains why.

Two of these have shapes worth stating, because they are not what a
reader would guess. `install_workers()` takes the instance uuids to
install, with no default -- a caller which means "every worker" has to
say so -- rather than offering an incremental mode:
`create_and_await_instances()` already knows exactly which instances
it just made, so passing that list along is the whole of what a caller
needs. `install_control_plane()` takes an optional `manifests`
argument, the same list of local paths `create()` reads and forwards to
it. Skipping MetalLB or Longhorn, by contrast, is not a change to
`setup_metallb()` or `setup_longhorn()`
themselves -- they are exactly as unconditional as before -- it is
`create()` deciding whether to call them at all.

`create()`'s remaining keyword arguments (`refresh_version_cache`,
`release_channel`, `sshkey`, `install_metallb`, `install_longhorn`,
`manifests`) mirror the command's options of the same name --
`manifests` takes a list of local paths, where `--manifest` is given
once per file. So do the six sizing arguments, `control_plane_cpus`,
`control_plane_memory`, `control_plane_disk`, `worker_cpus`,
`worker_memory` and `worker_disk`, which are vCPUs, MB and GB
respectively and default to 2, 2048 and 50. `server_config` and
`agent_config` take a mapping of k3s configuration keys, applied to
every control plane node and every worker respectively and recorded so
that `expand_workers()` reuses the agent one; they default to None,
which means none, and `docs/usage.md` says which keys are refused. See
its docstring in `cluster.py` for the full signature, and
`docs/usage.md` for what each command does; this page does not restate
it.

### Kubeconfig side effects default off in the library

`create(write_kubeconfig=...)` and `delete(update_kubeconfig=...)`
govern the only two things either call does to the machine it runs on
rather than to the cluster: writing and merging `~/.kube/config`, and
shelling out to `kubectl config delete-context`, `delete-user` and
`delete-cluster`. Both default to `False` here,
which is the one place a `Cluster` method's default differs from what
`sf-client k3s` does -- the command line passes `True` unless
`--no-kubeconfig` was given, so `sf-client k3s` behaves as it always
has, while a library caller's `~/.kube/config` is left alone unless it
asks.

The asymmetry is deliberate (decision 6 of
`docs/plans/PLAN-library-api-and-collection-phase-03-missing-verbs.md`).
Both side effects run on the calling machine, not on the cluster, and a
library whose default is to rewrite the caller's `~/.kube/config` is
surprising: an Ansible module or a conductor reconcile loop calling
`create()` from inside a process that manages its own kubectl
configuration should not find that file edited unless it said so.
The default was chosen before the first PyPI release, while
`sf-client k3s` was the only caller in the tree and reversing it cost
nothing. Taking the other default "for symmetry with the CLI" would
have made the surprise permanent at the one moment avoiding it was
free.

The cluster's kubeconfig is recorded in `md['kubeconfig']` regardless of
`write_kubeconfig`, and `get_kubeconfig()` serves it either way -- only
the local file write, the `kubectl config view --flatten` merge, and
the `kubectl config delete-*` cleanup are gated, never the credentials
themselves.

`k3s list`, `query-k3s-version` and `query-longhorn-version` name no
cluster, so they stay module level functions rather than `Cluster`
methods: `primitives.list_clusters(client, namespace)`,
`primitives.get_k3s_release(client, namespace, reporter, ...)` and
`primitives.get_longhorn_release(client, namespace, reporter, ...)`.

## The reporter

Every message this library produces -- progress phases, wait-loop
statuses, debug lines -- goes through a reporter rather than `print()`.
A reporter only needs to be file-like: `write()`, `flush()` and
`isatty()`, because `Progress` already writes to a stream and a
reporter is passed as that stream.

- `progress.Reporter` is the default. It writes to real `sys.stdout`
  and delegates `isatty()` to it, so a `Cluster` built with no
  reporter behaves exactly like the CLI: in-place status updates on a
  terminal, one line per change otherwise.
- `progress.CollectingReporter` accumulates everything written instead
  of printing it, and its `isatty()` always answers `False`, which is
  what keeps `Progress` in line mode when nothing is watching a
  terminal. `getvalue()` returns everything written as one string;
  `lines` splits it into a list with no line endings, for a caller
  (an Ansible module's `log` field, for instance) that wants
  individual entries.

Both take `verbose=True` to also emit `debug()` lines; without it
`debug()` is silent.

### What the output looks like, and when

`Progress` has two modes, and which one you get depends on the
reporter's `isatty()` and on `verbose`.

On a terminal, and not verbose, a wait block is rendered as one line per
item and rewritten in place with ANSI cursor movement, truncated to the
terminal width. Otherwise -- a pipe, a CI log, a `CollectingReporter`,
or `verbose=True`, whose debug lines would interleave badly with cursor
movement -- a status line is printed only when it changes, with a
heartbeat reprint every 60 seconds so a log still shows liveness during
a long wait.

Either way each status carries how long the item has been in *that*
status rather than how long the wait has run, so a stalled command shows
up as a growing elapsed time next to one item while its neighbours
advance. An idle wait names the agent command currently executing, not
a bare operation count. If a single agent command runs for more than
five minutes a one-off note says it may be stalled; the note does not
reset the per-item timers it is drawing attention to, and on a terminal
it is printed above the status block, which is then redrawn below it.

Every elapsed time is measured on the monotonic clock, so an NTP
correction part way through a twenty minute install cannot make a phase
appear to take a negative amount of time, move a timeout, or fire a
stall note.

## Exceptions

Every failure this library detects and reports is a
`K3sClusterException` subclass; `apiclient` exceptions, and
`OSError`/`yaml.YAMLError` from local file and subprocess work (a
`~/.kube/config` write, a malformed kubeconfig), propagate unchanged
rather than being wrapped. Every
`K3sClusterException` subclass's `__str__` renders exactly the text
`sf-client k3s ...` printed before this line existed as an exception
at all -- catching the base class and printing `str(e)` reproduces
the CLI's own error output for the failures this library detects.
`K3sClusterException` deliberately does not subclass anything in
`shakenfist_client.apiclient`: catching one hierarchy never catches
the other, so a caller can tell "the cluster API rejected this"
apart from "Shaken Fist itself is unreachable" -- though neither
hierarchy catches an `OSError` or `YAMLError` escaping from local
work, so a caller that wants to catch everything needs a third
`except` clause.

`make_client()` can also raise `ValueError`, when some but not all of
`api_url`, `namespace` and `key` are supplied (see "Constructing the
client" above). That is a programming error in the caller rather than
a cluster failure, which is why it is not a `K3sClusterException`, and
a correct caller never needs to catch it.

| Exception | Raised when |
|-----------|-------------|
| `ClusterExistsError` | `create()` is called for a name already holding a finished cluster |
| `ClusterInterruptedError` | `create()` is called for a name holding a cluster that never finished being built, or `expand_workers()`, `remove_worker()` or `expand_addresses()` is called against one |
| `NetworkNotFoundError` | `create(network=...)` names a network that does not exist |
| `ClusterNotFoundError` | the named cluster does not exist -- see method docstrings for which |
| `ClusterIncompleteError` | `get_kubeconfig()` is called on a cluster that exists but has not finished `create()` |
| `WorkerNotFoundError` | `remove_worker()` is given a uuid that is not one of the cluster's workers |
| `WorkerUnnamedError` | `remove_worker()` finds a worker whose instance record has no name, so the k3s node it became cannot be identified |
| `ManifestError` | `create(manifests=...)` is given a path that cannot be staged: wrong suffix, a basename that is not a plain filename, a duplicate basename, unreadable or not decodable as UTF-8, not valid YAML or JSON, or a line colliding with the staging marker |
| `GuestFileError` | a file this library writes onto a cluster node carries a line equal to the heredoc marker used to write it, so writing it would run the rest as commands on the node. The values that reach these bodies come from the namespace metadata document, which is third-party writable |
| `SshKeyError` | `create(sshkey=...)` is given a path that cannot be read or decoded as UTF-8 |
| `NodeSizeError` | `create()` is given a node size that is not a positive integer (a bool counts as not), before anything is built or the name is registered |
| `ClusterNameError` | `create()` is given a name that cannot become a node's instance name, before anything is built or the name is registered: `invalid_characters` (not a string, empty, or not ASCII letters, digits and hyphens starting and ending alphanumeric), `too_long` (over `CLUSTER_NAME_MAX_LENGTH`, 48). Any verb, `create()` included, raises `reserved` for `k3s_version_cache` and `longhorn_version_cache`, which collide with the release caches' metadata keys |
| `ShapeError` | `create()`, `expand_workers()` or `expand_addresses()` is given a count that is not an integer (`not_an_integer`; a bool counts as not) or is below its floor (`below_floor`): 1 for `control_plane_count` and for the two expand counts, 0 for `worker_count` and `metal_address_count` on `create()`. Raised before anything is built |
| `K3sConfigError` | `create(server_config=..., agent_config=...)` is given a mapping that cannot be used -- not a mapping, a non-string key, a key containing `=`, a value that does not survive a JSON round trip, a key the plugin owns (or k3s's alias for one), or text that collides with the heredoc marker -- or `read_k3s_config()` cannot read or parse its file, before anything is built or the name is registered |
| `UnsupportedReleaseError` | `create()` resolves a k3s release older than `v1.21.1+k3s1`, or one it cannot parse, right after the channel lookup and before anything is built or the name is registered |
| `ComponentNotInstalledError` | a verb needs an optional component the cluster was built without -- `expand_addresses()` against a cluster created with `install_metallb=False` |
| `ClusterMetadataError` | a value read back out of the namespace metadata cannot be used -- `expand_addresses()` finds something in `routed_addresses` that is not an IP address, or the API hands back one that is not. Raised before any address is routed, because the addresses are charged for and the configuration they go into is written afterwards |
| `ReleaseLookupError` | the k3s or Longhorn release lookup fails or returns nothing usable |
| `AgentOperationError` | a Shaken Fist agent operation finishes without doing its work -- `error`, or `expired` when Shaken Fist took its wall clock budget away |
| `CommandFailedError` | an agent command completes with a non-zero return code |
| `KubeconfigError` | a local `~/.kube/config` merge fails (`merge_failed`), or `delete()`'s cleanup cannot read the kubeconfig or remove an entry from it, or either needs a local `kubectl` and there is none. A failed write of the file itself is an `OSError` |

Each exception's docstring in `shakenfist_client_k3s/exceptions.py`
names the exact call site and the attributes it carries; several are
built through classmethods (`ClusterNotFoundError.not_found(name)`,
`KubeconfigError.merge_failed(...)`, and so on) rather than their
constructors, because one class covers several call sites whose
message text differs.

Where a class has classmethods, **they are the interface and the
constructor is not**. The eight classes that carry a `reason` share a
base whose `__init__` takes `(reason, message, **fields)`, so every
attribute past `message` is keyword-only, and the set of attributes a
class carries is its `FIELDS` tuple rather than a parameter list.
Catching these and reading their attributes is supported; constructing
one positionally is not, and the shared base is free to move a field
between classes without that being a breaking change. `ClusterInterruptedError` carries the state the
cluster was left in (`state`) and, for its `not_usable()` form, which
verb refused to run (`verb`); both of its messages name `sf-client k3s
delete <name>` as the way out, because what is built is detection and
teardown rather than a way to resume a half built cluster -- see
decision 5 of
`docs/plans/PLAN-library-api-and-collection-phase-03-missing-verbs.md`. One
gap that decision does not close: a `create()` interrupted between
claiming its name and writing that cluster's own metadata document
leaves a name `delete()` reports as not found at all, with no supported
way to free it from this library --
[shakenfist/client-python-k3s#72](https://github.com/shakenfist/client-python-k3s/issues/72).
`health()` deliberately does not raise `ClusterInterruptedError`:
reporting on an interrupted cluster, rather than refusing to look at
it, is what that verb is for.

`delete()` has the same two writes as `create()` in the other order --
it releases the name from the cluster list before it removes the
cluster's metadata document -- so an interrupted `delete()` leaves a
document whose name is already free, and calling `delete()` again
finishes the job. That is why #72 is a create-side gap rather than a
gap on both sides.

`AgentOperationError` carries the operation `state` which brought it
about, and renders it only when it is not `error`. Shaken Fist gives
every agent operation a deadline (600 seconds unless the creator asked
for something else) and moves one that overruns it to `expired`, which
it documents as deliberately distinct from `error`: "run it again with
a longer deadline" and "the command is broken" are different next
steps. Every wait in this library enumerates the states an operation
can be in, rather than naming the two endings it used to expect, so
`expired` and `deleted` end a wait instead of spinning in one.

A state this library has never heard of is handled differently by the
two kinds of wait, which is worth knowing if you are reading the
source. A wait for an instance to become idle keeps waiting and says
so, because an unrecognised state is more likely a new way of being in
flight than a new ending, and starting the next command over one that
is still running would corrupt the node. A wait for a command's output
raises, because that output exists only for `complete`. Neither can
wedge: the server moves every operation out of whatever state it is in
within its deadline. `health()`'s probe names the state too, rather
than reporting an ending it does not recognise as a completion with no
result.

`health()` is the exception to all of that, in the other direction: its
`kubectl` probe carries a timeout, and it is skipped altogether when
the node it would run on is not up. Both are reported as
`api['probed'] is False` with an explanatory `api['error']`, and
neither raises. Every probe -- the `kubectl` one and one signals probe
per healthy node (see "What `signals` reports", below) -- is submitted
before any is waited for, and all of them are waited for under one
shared budget, so the waiting is bounded by one budget on a cluster of
any size. That bounds the waiting, not the call: on top of it come one
API round trip per node to read its instance, one per probe to submit
it, and the reads of each operation, all made one after another, so on
a slow Shaken Fist API the wall time can exceed the budget. The budget
is a ceiling, not a cost: every command runs on its node while earlier
ones are waited for, and one which has finished by the time it is
collected costs a single read, so a healthy cluster waits for about the
time its slowest probe takes rather than a second per node. An abandoned
run does leave operations queued until the server's
deadline ends them: up to one per probed node, plus the `kubectl` one
on the control plane node. `api['error']` names the `kubectl`
operation, and a caller polling `health()` in a loop should know that a
later `expand_workers()` or `update_os()` waits for those alongside its
own commands.

That wait is a delay and not a failure, and the mechanism is worth
stating because the obvious implementation gets it wrong.
`await_idle()` waits for *every* agent operation on an instance,
because the next command must not race one still executing on the node,
but it judges only the operations it was handed --
`execute_and_await()` passes the ones it just submitted. An operation
nobody named is waited for while it can still progress and ignored once
it ends, whatever it ends as. Without that split, an abandoned probe
which the server later expires would abort an unrelated
`expand_workers()` with an `AgentOperationError` naming a `kubectl get
nodes` the caller never ran.

`remove_worker([])` is likewise an accepted no-op, so a caller computing
the removal list programmatically does not need to guard the call.

Every elapsed time this library measures -- the probe's timeout, the
stall notes, the progress reporting -- is taken from `time.monotonic()`,
so a wall clock adjustment during a long create cannot shorten a
timeout or invent a stall.

`ComponentNotInstalledError` is the one exception here which depends on
how the cluster was built rather than on what state it is in.
`create()` records `metallb_installed` and `longhorn_installed` in the
cluster metadata, and a verb that drives one of those components reads
it before it does anything. Metadata written before those keys existed
is read as having both components, which every such cluster does.

### What `signals` reports

Every node entry in `health()`'s report carries a `signals` dict, with
the same twelve keys in every outcome. Each reading is a fact about one
machine, read through that machine's agent by one short read-only
command, so it lives on that machine's entry.

| Key | What it is, and where it comes from |
|---|---|
| `probed` | The command ran at all. |
| `error` | Why it did not, or why it failed: the instance is gone, the node is not up, the probe was abandoned at the deadline, the operation failed, or the command exited non-zero. A node that was not probed says why in the words `api['error']` uses for a skipped probe. `None` when the probe ran cleanly. |
| `boot_id` | `/proc/sys/kernel/random/boot_id`, which changes at every boot. Only a UUID (8-4-4-4-12 hexadecimal) is reported, in lowercase; anything else the node prints is `None`, so that garbage never reads as a reboot. |
| `booted_at` | `btime` from `/proc/stat`, in Unix seconds. The same fact as `boot_id`, in a form a person can read. |
| `k3s_unit` | The systemd unit k3s runs as: `k3s` on control plane nodes and `k3s-agent` on workers. Reported so that a reader does not have to know the installer's naming. |
| `k3s_state` | That unit's systemd `ActiveState`, such as `active`, `activating` or `failed`. Only lowercase letters and hyphens, at most 32 characters, are reported; anything else is `None`. |
| `k3s_restarts` | That unit's systemd `NRestarts`. |
| `oom_kills` | `oom_kill` from `/proc/vmstat`: every kernel OOM kill since boot, including a pod exceeding its own memory limit. |
| `memory_total_bytes` | `MemTotal` from `/proc/meminfo`, converted from kB to bytes. |
| `memory_available_bytes` | `MemAvailable` from `/proc/meminfo`, in bytes. |
| `etcd_bytes` | Size of the embedded etcd data directory. Control plane nodes only. |
| `etcd_snapshot_bytes` | Size of the etcd snapshot directory: the `etcd-snapshot-dir` recorded in the cluster's `server_config` when it has one, otherwise k3s's default. A relative `etcd-snapshot-dir` reports `None`, because what k3s resolved it against cannot be known from the node's agent, and a size of the wrong directory would be worse than none. Control plane nodes only. |

A reading which could not be taken is `None` on its own and voids none
of the others; `error` is for the probe as a whole. `k3s_state` and
`k3s_restarts` are `None` when the unit is not loaded, because systemd
reports a restart count of zero for a unit that does not exist and zero
would be a claim. A directory that does not exist (k3s creates the
snapshot directory with the first snapshot) is `None`, not zero. The
etcd sizes are always `None` on a worker.

Every reading is raw, the counters among them are cumulative since
boot, and `health()` stores nothing. A health check that wrote would have new ways to fail, and
"since the last call" is meaningless when the command line, a daily
poll and an Ansible play all call it. The caller keeps the baseline,
usually yesterday's report, and diffs:

* A changed `boot_id` voids the baseline, because every reading here
  except the etcd sizes resets at boot. Treat the current value as the
  delta. An unexpected reboot is itself a finding the current report
  cannot show on its own.
* A counter lower than its baseline with an unchanged `boot_id` has
  been reset by some other route, and the same rule applies. systemd is
  understood to reset `NRestarts` when the unit is restarted by hand.

Signals never affect `healthy`, on a node or on the cluster. They are
facts, not judgements: whether three restarts or 200 MiB available is a
problem depends on a baseline and a workload that `health()` does not
have and the caller does. A probe that fails on a node which is
otherwise up reports it in `signals['error']` and leaves the node
healthy, so a slow `du` cannot flip the Ansible module's gate or
`--strict`.

## Worked example

This creates a one-node cluster, fetches its kubeconfig, and deletes
it again, with output collected rather than printed:

```python
from shakenfist_client_k3s.client import make_client
from shakenfist_client_k3s.cluster import Cluster
from shakenfist_client_k3s.progress import CollectingReporter

client = make_client()
reporter = CollectingReporter()
cluster = Cluster(client, 'mycluster', client.namespace, reporter=reporter)

cluster.create(control_plane_count=1, worker_count=1, metal_address_count=1)
kubeconfig = cluster.get_kubeconfig()
cluster.delete()

# Nothing above wrote to sys.stdout, or to the process's stdout
# behind its back. Everything either of them said is here.
print(reporter.getvalue())
```

No call here writes anything to `sys.stdout`; a caller that owns
stdout for its own output (an Ansible module's JSON result, in
particular) can run any of them and keep it that way. That includes
`delete()`, which used to be an exception: its kubectl calls ran
with no captured output, so the child process inherited file
descriptor 1 and kubectl's own lines reached the real stdout
directly, bypassing both `sys.stdout` and the reporter. They now
capture their output, and what kubectl says arrives through the
reporter (at debug level) or, on a failure, on the `KubeconfigError`
it raises.

`reporter.getvalue()` holds the same numbered-phase, per-node
progress text `sf-client k3s create` prints, for example:

```
[1/8] Creating node network
  created k3s-mycluster-node (uuid ...)
...
[8/8] Setting up longhorn version 1.6.0
Cluster mycluster is ready (... total)
```

The total follows what the call actually does, so the command line's
nine-phase create becomes eight here: the example above did not ask
for `write_kubeconfig`, so there is no `Updating local kubeconfig`
phase to count.

This exact call sequence -- `Cluster(...)`, `create()`,
`get_kubeconfig()`, `delete()`, with a `CollectingReporter` -- is
verified in `LibraryTestCase.setUp()` in
`shakenfist_client_k3s/tests/test_library_api.py`, against a
scripted fake client (`tests/fakes.py`) with `subprocess.run` and
`time.sleep` mocked and `primitives.get_k3s_release()`/
`get_longhorn_release()` patched directly. Mocking
`requests.request` alone is not enough to reproduce this: those two
release lookups are called as plain functions in the test, so
patching them is what keeps the lookup from reaching the real k3s
update API and the Longhorn chart index; `requests.request` itself is
never touched. Running the sequence against a real Shaken Fist
namespace needs nothing more than the real client `make_client()`
returns, as shown above; there is no other setup and no Click
context to fabricate.
