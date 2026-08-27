# Shaken Fist k3s client plugin

This is a plugin for [sf-client](https://github.com/shakenfist/client-python),
the command line client for the
[Shaken Fist](https://github.com/shakenfist/shakenfist) minimal cloud. It adds
a `k3s` command group which orchestrates [k3s](https://k3s.io/) Kubernetes
clusters on Shaken Fist instances, including [MetalLB](https://metallb.io/)
for load balancer addresses and [Longhorn](https://longhorn.io/) for
persistent storage. Cluster state is stored in Shaken Fist namespace
metadata, so any authorized client can manage a cluster.

## Installation

```bash
pip install shakenfist_client_k3s
```

The plugin registers itself with `sf-client` via an entry point, so once
installed the commands appear automatically.

Cluster nodes communicate with Shaken Fist over the `sf-agent2` side
channel, so clusters require `shakenfist_client` >= 0.7.7 (installed
automatically) and a Shaken Fist server and guest image recent enough
to speak `sf-agent2`.

## Usage

```bash
# Create a cluster with two workers and a highly available control plane
sf-client k3s create mycluster --worker-count 2 --control-plane-count 3

# Fetch a kubeconfig for the new cluster
sf-client k3s getconfig mycluster

# Grow the cluster later
sf-client k3s expand-workers mycluster --worker-count 2

# And clean up when you are done
sf-client k3s delete mycluster
```

`sf-client k3s --help` lists the full command set.

## Documentation

* [Using the k3s command group](https://github.com/shakenfist/client-python-k3s/blob/develop/docs/usage.md)
  -- every command, its options, and where cluster state lives.
* [Testing and CI](https://github.com/shakenfist/client-python-k3s/blob/develop/docs/testing.md)
  -- how to run the tests, and what the two CI tiers cover.
