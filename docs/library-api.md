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
from shakenfist_client import apiclient
from shakenfist_client_k3s.cluster import Cluster

client = apiclient.Client()
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

Each command's body is now a method taking that command's options,
minus `name` and `namespace`, and returning a value instead of
printing one:

| Method | Command it replaces |
|--------|---------------------|
| `create(control_plane_count, worker_count, metal_address_count, network=None, ...)` | `k3s create` |
| `get_kubeconfig()` | `k3s getconfig` |
| `show()` | `k3s show` |
| `delete()` | `k3s delete` |
| `expand_workers(worker_count)` | `k3s expand-workers` |
| `expand_addresses(address_count)` | `k3s expand-addresses` |
| `update_os()` | `k3s update-os` |

`create()`'s remaining keyword arguments (`refresh_version_cache`,
`release_channel`, `sshkey`) mirror the command's options of the same
name; see its docstring in `cluster.py` for the full signature.
`create()` and `delete()` still write and merge `~/.kube/config`, and
shell out to `kubectl config unset`, unconditionally -- that is
unchanged CLI behaviour, not new library behaviour, and making it
optional is future work. See `docs/usage.md` for what each command
does; this page does not restate it.

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

Every failure this library raises is a `K3sClusterException`
subclass, and every subclass's `__str__` renders exactly the text
`sf-client k3s ...` printed before this line existed as an exception
at all -- catching the base class and printing `str(e)` reproduces
the CLI's own error output. `K3sClusterException` deliberately does
not subclass anything in `shakenfist_client.apiclient`: catching one
hierarchy never catches the other, so a caller can tell "the cluster
API rejected this" apart from "Shaken Fist itself is unreachable".

| Exception | Raised when |
|-----------|-------------|
| `ClusterExistsError` | `create()` is called for a name already in use |
| `NetworkNotFoundError` | `create(network=...)` names a network that does not exist |
| `ClusterNotFoundError` | the named cluster does not exist -- see method docstrings for which |
| `ClusterIncompleteError` | `get_kubeconfig()` is called on a cluster that exists but has not finished `create()` |
| `ReleaseLookupError` | the k3s or Longhorn release lookup fails or returns nothing usable |
| `AgentOperationError` | a Shaken Fist agent operation enters the `error` state |
| `CommandFailedError` | an agent command completes with a non-zero return code |
| `KubeconfigError` | a local `~/.kube/config` write, merge or `kubectl config unset` fails |

Each exception's docstring in `shakenfist_client_k3s/exceptions.py`
names the exact call site and the attributes it carries; several are
built through classmethods (`ClusterNotFoundError.not_found(name)`,
`KubeconfigError.merge_failed(...)`, and so on) rather than their
constructors, because one class covers several call sites whose
message text differs.

## Worked example

This creates a one-node cluster, fetches its kubeconfig, and deletes
it again, with output collected rather than printed:

```python
from shakenfist_client import apiclient
from shakenfist_client_k3s.cluster import Cluster
from shakenfist_client_k3s.progress import CollectingReporter

client = apiclient.Client()
reporter = CollectingReporter()
cluster = Cluster(client, 'mycluster', client.namespace, reporter=reporter)

cluster.create(control_plane_count=1, worker_count=1, metal_address_count=1)
kubeconfig = cluster.get_kubeconfig()
cluster.delete()

# Nothing above wrote to sys.stdout -- everything is here instead.
print(reporter.getvalue())
```

Not one line reaches `sys.stdout`; a caller that owns stdout for its
own output (an Ansible module's JSON result, in particular) can run
this and keep it that way. `reporter.getvalue()` holds the same
numbered-phase, per-node progress text `sf-client k3s create` and
`delete` print, for example:

```
[1/9] Creating node network
  created k3s-mycluster-node (uuid ...)
...
[9/9] Updating local kubeconfig
Cluster mycluster is ready (... total)
```

This exact call sequence -- `Cluster(...)`, `create()`,
`get_kubeconfig()`, `delete()`, with a `CollectingReporter` -- is
verified against a scripted fake client with `subprocess.run` and
`requests.request` mocked, the same way
`shakenfist_client_k3s/tests/test_library_api.py` drives the
create-through-delete lifecycle test. Running it against a real
Shaken Fist namespace needs nothing more than the real
`apiclient.Client()` shown above; there is no other setup and no
Click context to fabricate.
