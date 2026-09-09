# Architecture

## Overview

This package is an `sf-client` plugin: it registers a `load` callable
in the `shakenfist_client.plugin` entry point group, which
`shakenfist_client.main` invokes at CLI startup to attach the `k3s`
Click command group. All communication with Shaken Fist happens
through the `apiclient.Client` instance that `sf-client` places in the
Click context.

Unit tests mock every external API, so they cannot tell whether a
cluster actually assembles; that is answered by the merge tier of CI,
which deploys a real cluster from an ephemeral runner. See
`docs/testing.md`.

## Module Structure

```
shakenfist_client_k3s/
├── __init__.py         # Click commands, the plugin entry point, and the group's error handling
├── cluster.py          # Cluster: one named cluster's state and orchestration
├── exceptions.py       # The K3sClusterException hierarchy this library raises
├── primitives.py       # Namespace scoped lookups and stateless helpers
├── progress.py         # Reporters, plus phase and wait-loop progress reporting
└── tests/              # Unit tests (testtools + stestr)
```

### Three layers, not two

Each Click command is argument parsing plus one call into a callable
layer, so the same orchestration is reachable with no Click context
at all -- a library caller (an Ansible module, a conductor reconcile
loop) uses it directly. `docs/library-api.md` is the reference for
that; this section is only the shape.

- **Commands (`__init__.py`)** -- `k3s list` / `show` / `delete`
  inspect and remove managed clusters; `k3s create` builds one:
  control plane nodes, workers, MetalLB address allocation, and
  Longhorn storage; `k3s getconfig` fetches a kubeconfig;
  `k3s expand-workers` / `expand-addresses` grow a cluster;
  `k3s update-os` updates every node's OS packages;
  `k3s query-k3s-version` / `query-longhorn-version` inspect the
  release version caches. `GroupCatchClusterExceptions`, a
  `click.Group` subclass, is the single place that catches a
  `K3sClusterException`, prints it and exits 1 -- every command
  raises rather than exiting directly.
- **`Cluster` (`cluster.py`)** -- everything scoped to one named
  cluster: its namespace metadata cache (`get_metadata()` /
  `set_metadata()` / `delete_metadata()`), the instance orchestration,
  and the seven methods each command body above moved onto
  (`create()`, `get_kubeconfig()`, `show()`, `delete()`,
  `expand_workers()`, `expand_addresses()`, `update_os()`). Methods
  return values instead of printing them, and raise
  `exceptions.K3sClusterException` subclasses instead of exiting.
- **Namespace scoped lookups and stateless helpers (`primitives.py`)**
  -- work with no cluster identity: the two release lookups, whose
  caches live in *namespace* metadata rather than any one cluster's,
  and `_describe_agent_op()`. `list`, `query-k3s-version` and
  `query-longhorn-version` call these directly rather than building a
  `Cluster`. Imports run one way only -- `cluster.py` imports
  `primitives`, never the reverse.

### Cluster state and orchestration (`cluster.py`)

- **Cluster state**: all cluster state is stored as Shaken Fist
  namespace metadata (`orchestrated_k3s_cluster_*` keys), so there is
  no local state file and any client can manage the cluster. A
  `Cluster` fetches its own document once and caches it for its
  lifetime, writing through to the API on every change, to keep the
  number of namespace reads the same regardless of how many methods
  are called against one instance
- **Instance orchestration**: helpers create instances from a
  `debian:12` base image, await boot and agent-idle state via the
  Shaken Fist agent, and run installation commands through agent
  execute operations. Instances are created with the `sf-agent2`
  side channel, which the current in-guest agent requires: without
  it the agent never connects, the instance's `agent_state` never
  reaches `ready`, and the `await_boot()` polling loop waits
  forever. Clusters therefore require `shakenfist_client` >= 0.7.7
  and a Shaken Fist server and guest image recent enough to speak
  `sf-agent2`; there is no fallback to the legacy `sf-agent`
  channel
- **Cluster assembly**: the first control plane node is installed
  with `k3s server`, additional control plane nodes and workers join
  using the node token, MetalLB is installed (from the official
  metallb helm chart -- the Bitnami chart references versioned
  docker.io/bitnami images which stopped being published in 2025)
  and configured with floating addresses routed to the node network,
  and Longhorn is installed for persistent volumes
- **Join address**: nodes register through the cluster's
  `join_address` (namespace metadata), initially the first control
  plane node's in-network address. It is mutable cluster state, not
  a property of a particular node: k3s agents only need it at
  registration time (they then maintain a client load balancer over
  every server they discover), so a future control plane replacement
  can join the new server via the old address, update
  `join_address`, and reap the old node. The join address must be an
  in-network address: the Shaken Fist network node neither hairpins
  floating addresses nor routes in-network traffic to the network's
  own routed addresses (shakenfist/shakenfist#3662), so neither is
  reachable from a joining node

### Release lookups (`primitives.py`)

The latest k3s release per channel is fetched from the k3s update
API, and the latest Longhorn release from the GitHub releases API.
Results are cached in namespace metadata and refreshed when stale.
Both parsers are defensive about upstream data: k3s channels without
a `latest` release (for example `v1.16-testing`) are skipped, and
Longhorn tags which are prereleases or not valid PEP 440 versions are
ignored (`packaging.version.Version` is used for comparison). These
are namespace scoped rather than cluster scoped -- the commands behind
them, `query-k3s-version` and `query-longhorn-version`, name no
cluster -- so they stay module level functions taking a client, a
namespace and a reporter.

### The exception hierarchy (`exceptions.py`)

Every failure this library detects and reports is a
`K3sClusterException` subclass, carrying the failure's details as
attributes and rendering the CLI's historic error text from
`__str__`. `apiclient` exceptions, and `OSError`/`yaml.YAMLError`
from local file and subprocess work, propagate unchanged rather than
being wrapped. `K3sClusterException` deliberately shares no base
class with `shakenfist_client.apiclient`'s exceptions, so catching
one hierarchy never catches the other -- a caller can tell "the
cluster API rejected this" apart from "Shaken Fist itself is
unreachable", though neither hierarchy catches the local
`OSError`/`YAMLError` cases just mentioned. See `docs/library-api.md`
for the exception list; this module's docstrings name the exact call
site and attributes for each one.

### Progress reporting (`progress.py`)

Every `Cluster` method that runs a long operation builds a `Progress`
on demand via `Cluster.get_progress()`, writing to the `Cluster`'s own
reporter (a `progress.Reporter` by default, or whatever a caller
passed in -- see `docs/library-api.md`). Work is announced as numbered
phases (`[3/9] Setting up metallb`), and the polling wait loops
(`await_boot`, `await_idle`, `await_fetch`) report per-item statuses
through `Progress.update()`. When stdout is a TTY the statuses are
rendered as one line per item, rewritten in place with ANSI cursor
movement and truncated to the terminal width. Otherwise (pipes, CI,
or `--verbose`, whose debug lines would interleave badly with cursor
movement) a status line is printed only when it changes, with a
heartbeat reprint every 60 seconds so logs still show liveness. Each
status shows how long the item has been in that status, so a stalled
command is visible as a growing elapsed time, and idle waits describe
the agent command currently executing rather than a bare operation
count. The module is dependency free.

The wait loops also detect failure: an agent operation which enters
the `error` state aborts the command immediately with the operation
uuid, the command it was on, and a pointer to `sf-client instance
events` for the server side detail (operations in the error state
never complete, so waiting on them would hang forever). Errored
operations which predate the current wait are ignored, so a historical
failure does not prevent later commands like `expand-workers` from
running. If a single agent command runs for more than five minutes a
one-off note flags that it may be stalled. Notes emitted mid-wait
leave the wait block's per-item timers intact (and, on a TTY, redraw
the status block below the note), so a stall note does not reset the
very elapsed counter it is drawing attention to.

## Python Version Compatibility

Like client-python, this plugin targets Python >= 3.7 to support the
widest range of client platforms. The runtime version lookup uses
`importlib.metadata` with the `importlib-metadata` backport on older
Pythons.

## Build and Packaging

- **Build system**: `setuptools` with `pyproject.toml`
- **Versioning**: `setuptools_scm` derives the version from git tags
  and writes `shakenfist_client_k3s/_version.py` at build time (that
  file is gitignored and must never be committed)
- **Distribution**: published to PyPI as `shakenfist_client_k3s`
- **Entry point**: `k3s = "shakenfist_client_k3s:load"` in the
  `shakenfist_client.plugin` group
