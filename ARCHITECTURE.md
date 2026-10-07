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
├── client.py           # make_client(): an API client for a caller with no Click context
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
  `k3s remove-worker` shrinks one; `k3s update-os` updates every
  node's OS packages; `k3s health` reports a cluster's health;
  `k3s query-k3s-version` / `query-longhorn-version` inspect the
  release version caches. `GroupCatchClusterExceptions`, a
  `click.Group` subclass, is the single place that catches a
  `K3sClusterException`, prints it to stderr and exits 1 -- every
  command raises rather than exiting directly.
- **`Cluster` (`cluster.py`)** -- everything scoped to one named
  cluster: its namespace metadata cache (`get_metadata()` /
  `set_metadata()` / `delete_metadata()`), the instance orchestration,
  and the nine methods each command body above moved onto
  (`create()`, `get_kubeconfig()`, `show()`, `delete()`,
  `expand_workers()`, `remove_worker()`, `expand_addresses()`,
  `update_os()`, `health()`). Methods return values instead of
  printing them, and raise `exceptions.K3sClusterException`
  subclasses instead of exiting.
- **Namespace scoped lookups and stateless helpers (`primitives.py`)**
  -- work with no cluster identity: the two release lookups, whose
  caches live in *namespace* metadata rather than any one cluster's.
  `list`, `query-k3s-version` and
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
- **Cluster assembly**: every node has its k3s configuration files
  written before its installer runs (see "k3s configuration" in
  `docs/usage.md`), the first control plane node is installed
  with `k3s server`, additional control plane nodes and workers join
  using the node token, and, unless a caller opts out, MetalLB is
  installed (from the official metallb helm chart -- the Bitnami
  chart references versioned docker.io/bitnami images which stopped
  being published in 2025) and configured with floating addresses
  routed to the node network, and Longhorn is installed for
  persistent volumes
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
API, and the latest Longhorn release from the index of the Helm
repository the Longhorn install uses (`charts.longhorn.io`). Neither
upstream has an API rate limit; the GitHub releases API, which the
Longhorn lookup once used, allows 60 anonymous requests an hour per
source address and failed creates from behind shared NAT (#97).
Results are cached in namespace metadata and refreshed when stale.
Both parsers are defensive about upstream data: a fetch failure, an
unparsable body or a document of the wrong shape is a
`ReleaseLookupError` rather than a traceback, k3s channels without a
`latest` release (for example `v1.16-testing`) are skipped, and
Longhorn chart versions which are deprecated, prereleases or not valid
PEP 440 versions are ignored (`packaging.version.Version` is used for
comparison). These
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

Everything this library tells its caller goes through `progress.py`,
which is dependency free. A `Cluster` method that knows how many phases
its operation has starts a `Progress` with that count
(`Cluster.start_progress()`); one called directly by a library caller
gets one lazily (`Cluster.get_progress()`). Both write to the
`Cluster`'s own reporter -- a `progress.Reporter` by default, or
whatever a caller passed in -- so there is one output channel rather
than two. Work is announced as numbered phases (`[3/9] Setting up
metallb`), and the polling wait loops (`await_boot`, `await_idle`,
`await_fetch`) report per-item statuses: in place on a terminal, one
line per change otherwise.

The wait loops are also where failure is detected, and the way they do
it is the part worth knowing. They enumerate the agent operation states
rather than naming the two endings they expect, because Shaken Fist
gives every operation a wall clock budget and moves one that overruns
it to `expired` rather than `error`, and because `deleted` is reachable
from any state -- so a loop waiting for `complete` or `error` by name
spun forever on either. The states themselves, what each means, and why
a wait for an instance to go idle treats an unrecognised state
differently from a wait for a command's output are documented on the
constants at the top of `cluster.py`, which is where they are used.

This section is only the shape. `docs/library-api.md` is the reference
for what a caller sees: the reporter interface, the two output modes,
the stall note, `health()`'s bounded probes (`kubectl` plus a per-node
signals probe, submitted together under one shared deadline), and which
exception each ending raises.

## Python Version Compatibility

Like client-python, this plugin targets Python >= 3.7 to support the
widest range of client platforms. The runtime version lookup uses
`importlib.metadata` with the `importlib-metadata` backport on older
Pythons.

On 3.7 and 3.8 that support is wheel-only. The published wheel is
`py3-none-any` and installs and runs there, which is what `pip install`
gets; building from the sdist does not work, because `[build-system]`
requires setuptools 77 or newer for the SPDX license metadata and
setuptools itself has needed Python 3.9 or newer since 76.0.0. Nothing
currently tests the declared floor either way --
shakenfist/client-python-k3s#82 tracks both halves of that.

## Build and Packaging

- **Build system**: `setuptools` with `pyproject.toml`
- **Versioning**: `setuptools_scm` derives the version from git tags
  into the distribution metadata, which `importlib.metadata.version()`
  reads in `Cluster.create()` to stamp `plugin_version` into the
  cluster's namespace metadata
- **Distribution**: published to PyPI as `shakenfist_client_k3s`
- **Entry point**: `k3s = "shakenfist_client_k3s:load"` in the
  `shakenfist_client.plugin` group

## The `shakenfist.k3s` Ansible collection

This repository ships a second artefact alongside the Python package:
an Ansible collection at `collection/`, `shakenfist.k3s`, carrying one
native module, `sf_k3s_cluster`. `docs/collection.md` is the full
reference -- its parameters, the connection rules, check mode, and why
it does not manage worker count; this section is only the shape and
where it sits relative to the rest of this inventory.

The module is a fourth caller of the same orchestration the "Three
layers, not two" section above describes for the Click commands: it
imports `shakenfist_client_k3s.client.make_client()` and
`shakenfist_client_k3s.cluster.Cluster` directly, exactly as a
conductor reconcile loop would, and contains no orchestration of its
own -- argument parsing in, `Cluster` method calls out, same as the
Click command bodies are for `__init__.py`. It carries no copy of
anything Ansible-connection-shaped from the `shakenfist.shakenfist`
collection; `shakenfist_client_k3s/client.py` (see `docs/library-api.md`)
already does what a `module_utils/sf_connection.py` would, which is
why none exists here.

The two artefacts are versioned and built together but published
separately. `tools/build-collection.py`, run by the `build-collection`
CI job, reads the same `setuptools_scm`-derived version described
above and rewrites `collection/galaxy.yml`'s version to match before
`ansible-galaxy collection build` runs, so collection and package never
carry two different version numbers for one commit. The dependency
runs the other way at install time: `collection/requirements.txt` pins
a static floor on `shakenfist_client_k3s` from PyPI (`>=0.3.0`, raised
only when the module starts calling a newer verb) rather than a
build-time match to the collection's own version, because a locally
built development collection demanding the plugin release with its own
unreleased version number would fail to install for a reason that has
nothing to do with compatibility. `publish-collection` in
`release.yml` publishes to Ansible Galaxy under the same release
workflow and the same tag that triggers `publish-pypi`, with its own
credential (`ANSIBLE_GALAXY_TOKEN`); see `RELEASE-SETUP.md` for both.
`ansible-galaxy collection install shakenfist.k3s` resolves against
Galaxy, and `docs/collection.md` covers both that and the tarball build
needed to work against an unreleased checkout.
