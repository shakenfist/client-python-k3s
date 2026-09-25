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
stable public surface. Phase 4 cuts `v0.1.0` to PyPI, so whatever
is public at that point becomes a compatibility surface for
external callers. Everything else `Cluster` exposes --
`create_instance()`, `await_boot()`, `await_idle()`,
`await_fetch()`, `await_execute()`, `reap_execute()`,
`execute_and_await()`, `instance_os_update()`,
`install_control_plane()`, `install_k3s_component()`,
`install_extra_control_plane()`, `install_workers()`,
`allocate_metallb_addresses()`, `configure_metallb_addresses()`,
`setup_metallb()`, `setup_longhorn()`,
`create_and_await_instances()`, `get_progress()`,
`interrupted_state()` and `require_usable()` -- is internal
orchestration the methods above are built from, not a supported
entry point, so treat it as unstable even though nothing today
stops a caller reaching it directly.

Phase 3 reshaped two of these rather than leaving them for later, and
the reshaping is worth naming because it corrects what this page used
to say about them. `install_workers()` now takes the instance uuids to
install, with no default -- a caller which means "every worker" has to
say so -- rather than gaining an incremental mode. That is deliberate:
`create_and_await_instances()` already knows exactly which instances
it just made, so passing that list along is the fix, and "incremental"
was rejected as the framing for it. `install_control_plane()` gained
an optional `manifests` argument, the same list of local paths
`create()` reads and forwards to it. Skipping MetalLB or Longhorn, by
contrast, is not a change to `setup_metallb()` or `setup_longhorn()`
themselves -- they are exactly as unconditional as before -- it is
`create()` deciding whether to call them at all.

`create()`'s remaining keyword arguments (`refresh_version_cache`,
`release_channel`, `sshkey`, `install_metallb`, `install_longhorn`,
`manifests`) mirror the command's options of the same name --
`manifests` takes a list of local paths, where `--manifest` is given
once per file; see its docstring in `cluster.py` for the full
signature. See `docs/usage.md` for what each command does; this page
does not restate it.

### Kubeconfig side effects default off in the library

`create(write_kubeconfig=...)` and `delete(update_kubeconfig=...)`
govern the only two things either call does to the machine it runs on
rather than to the cluster: writing and merging `~/.kube/config`, and
shelling out to `kubectl config unset`. Both default to `False` here,
which is the one place a `Cluster` method's default differs from what
`sf-client k3s` does -- the command line passes `True` unless
`--no-kubeconfig` was given, so `sf-client k3s` behaves as it always
has, while a library caller's `~/.kube/config` is left alone unless it
asks.

The asymmetry is deliberate (decision 6 of
`docs/plans/library-api-and-collection-phase-03-missing-verbs.md`).
Both side effects run on the calling machine, not on the cluster, and a
library whose default is to rewrite the caller's `~/.kube/config` is
surprising: an Ansible module or a conductor reconcile loop calling
`create()` from inside a process that manages its own kubectl
configuration should not find that file edited unless it said so.
Reversing the default costs nothing today because nothing has ever been
released -- there are no git tags and `shakenfist_client_k3s` is not on
PyPI, so `sf-client k3s` is the only caller in the tree. Phase 4 is the
first PyPI release, so this is the last point at which the default
could change for free; taking the other default "for symmetry with the
CLI" would have made the surprise permanent at the one moment avoiding
it cost nothing.

The cluster's kubeconfig is recorded in `md['kubeconfig']` regardless of
`write_kubeconfig`, and `get_kubeconfig()` serves it either way -- only
the local file write, the `kubectl config view --flatten` merge, and
the `kubectl config unset` cleanup are gated, never the credentials
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

## Exceptions

Every failure this library detects and reports is a
`K3sClusterException` subclass; `apiclient` exceptions, and
`OSError`/`yaml.YAMLError` from local file and subprocess work (an
unreadable `sshkey` path, a `~/.kube/config` write, a malformed
kubeconfig), propagate unchanged rather than being wrapped. Every
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
| `ManifestError` | `create(manifests=...)` is given a path that cannot be staged: wrong suffix, a basename that is not a plain filename, a duplicate basename, unreadable, not valid YAML, or a line colliding with the staging marker |
| `ComponentNotInstalledError` | a verb needs an optional component the cluster was built without -- `expand_addresses()` against a cluster created with `install_metallb=False` |
| `ReleaseLookupError` | the k3s or Longhorn release lookup fails or returns nothing usable |
| `AgentOperationError` | a Shaken Fist agent operation enters the `error` state |
| `CommandFailedError` | an agent command completes with a non-zero return code |
| `KubeconfigError` | a local `~/.kube/config` write, merge or `kubectl config unset` fails |

Each exception's docstring in `shakenfist_client_k3s/exceptions.py`
names the exact call site and the attributes it carries; several are
built through classmethods (`ClusterNotFoundError.not_found(name)`,
`KubeconfigError.merge_failed(...)`, and so on) rather than their
constructors, because one class covers several call sites whose
message text differs. `ClusterInterruptedError` carries the state the
cluster was left in (`state`) and, for its `not_usable()` form, which
verb refused to run (`verb`); both of its messages name `sf-client k3s
delete <name>` as the way out, because phase 3 deliberately built
detection and teardown rather than a way to resume a half built
cluster -- see decision 5 of
`docs/plans/library-api-and-collection-phase-03-missing-verbs.md`. One
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

`ComponentNotInstalledError` is the one exception here which depends on
how the cluster was built rather than on what state it is in.
`create()` records `metallb_installed` and `longhorn_installed` in the
cluster metadata, and a verb that drives one of those components reads
it before it does anything. Metadata written before those keys existed
is read as having both components, which every such cluster does.

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
`delete()`, which used to be an exception: its three `kubectl config
unset` calls ran with no captured output, so the child process
inherited file descriptor 1 and kubectl's `Property "..." unset.`
lines reached the real stdout directly, bypassing both `sys.stdout`
and the reporter. They now capture their output, and what kubectl
says arrives through the reporter (at debug level) or, on a failure,
on the `KubeconfigError` it raises.

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
update API and GitHub releases API; `requests.request` itself is
never touched. Running the sequence against a real Shaken Fist
namespace needs nothing more than the real client `make_client()`
returns, as shown above; there is no other setup and no Click
context to fabricate.
