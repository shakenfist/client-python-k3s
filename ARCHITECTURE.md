# Architecture

## Overview

This package is an `sf-client` plugin: it registers a `load` callable
in the `shakenfist_client.plugin` entry point group, which
`shakenfist_client.main` invokes at CLI startup to attach the `k3s`
Click command group. All communication with Shaken Fist happens
through the `apiclient.Client` instance that `sf-client` places in the
Click context.

## Module Structure

```
shakenfist_client_k3s/
├── __init__.py         # Click commands and the plugin entry point
├── primitives.py       # Orchestration primitives
└── tests/              # Unit tests (testtools + stestr)
```

### Commands (`__init__.py`)

- `k3s list` / `show` / `delete` -- inspect and remove managed
  clusters
- `k3s create` -- create a cluster: control plane nodes, workers,
  MetalLB address allocation, and optionally Longhorn storage
- `k3s getconfig` -- fetch a kubeconfig for a cluster
- `k3s expand-workers` / `expand-addresses` -- grow a cluster
- `k3s update-os` -- update the operating system on all nodes
- `k3s query-k3s-version` / `query-longhorn-version` -- inspect the
  release version caches

### Primitives (`primitives.py`)

- **Cluster state**: all cluster state is stored as Shaken Fist
  namespace metadata (`orchestrated_k3s_cluster_*` keys), so there is
  no local state file and any client can manage the cluster
- **Release caches**: the latest k3s release per channel is fetched
  from the k3s update API, and the latest Longhorn release from the
  GitHub releases API. Results are cached in namespace metadata and
  refreshed when stale. Both parsers are defensive about upstream data:
  k3s channels without a `latest` release (for example `v1.16-testing`)
  are skipped, and Longhorn tags which are prereleases or not valid
  PEP 440 versions are ignored (`packaging.version.Version` is used
  for comparison)
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
  using the node token, MetalLB is installed and configured with
  floating addresses routed to the node network, and Longhorn is
  installed for persistent volumes

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
