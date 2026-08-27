# Using the k3s command group

The plugin registers itself with `sf-client` through the
`shakenfist_client.plugin` entry point, so once it is installed the
`k3s` command group appears automatically. `sf-client k3s --help`
lists everything below.

Every command takes `--namespace`. Without it the client's own
namespace is used; an administrator can name a different one to work
on a cluster they do not own.

## Cluster lifecycle

### `create NAME`

Builds a cluster and, on success, leaves it in `~/.kube/config` as the
current context.

| Option | Default | Meaning |
|--------|---------|---------|
| `--control-plane-count` | 1 | Control plane nodes. More than one gives a highly available control plane. |
| `--worker-count` | 2 | Worker nodes. |
| `--metal-address-count` | 5 | Floating addresses routed into the cluster network for MetalLB to hand out. |
| `--network` | (a new one) | Join a pre-existing Shaken Fist network instead of creating one for this cluster. |
| `--release-channel` | `stable` | A k3s release channel. `stable`, `latest`, or a version-pinned channel such as `v1.26`. |
| `--refresh-version-cache` | off | Re-query the k3s and Longhorn release APIs instead of using the cached answers. |
| `--sshkey` | none | A public key to place on every node, for debugging. |

Each node is a Shaken Fist instance with 2 vCPUs, 2GB of RAM and a
50GB disk on a Debian 12 base image, with a floating address and the
`sf-agent2` side channel enabled. A full create is 15-25 minutes, and
reports numbered phases with per-phase elapsed times as it goes.

The local kubeconfig is written directly if `~/.kube/config` does not
exist. If it does, the merge shells out to `kubectl config view
--flatten`, so a local `kubectl` is required for that path; without
one the create stops after the cluster is built and tells you to fetch
the credentials with `getconfig`.

### `delete NAME`

Deletes every instance in the cluster, unroutes its floating
addresses, deletes the node network, and removes the cluster's
namespace metadata. It then removes the cluster's entries from the
local kubeconfig with `kubectl config unset`, so a local `kubectl` is
required.

Note that the node network is deleted whether the cluster created it
or it was named with `create --network`, so deleting a cluster built
on a shared pre-existing network takes that network with it.

### `expand-workers NAME [--worker-count N]`

Adds `N` more workers (default 2) to a running cluster. Existing
nodes are untouched.

### `expand-addresses NAME [--address-count N]`

Routes `N` more floating addresses (default 2) into the cluster
network and reconfigures MetalLB's pool to include them.

### `update-os NAME`

Runs an OS package update on every control plane node and worker.
This does not update k3s itself.

## Inspection

### `list`

Prints the names of the clusters recorded in the namespace.

### `show NAME`

Prints the cluster's namespace metadata: node UUIDs, the network, the
API addresses, the join address, the plugin version that created it,
and the release versions in use.

Note that the metadata includes the node token and the kubeconfig, so
the output is cluster-admin credentials. Do not paste it into a bug
report.

### `getconfig NAME`

Prints the cluster's kubeconfig on stdout, with the API server address
rewritten to the control plane's floating address and the cluster,
context and user all named `<cluster>.<namespace>`.

### `query-k3s-version CHANNEL` and `query-longhorn-version`

Print the version a release channel currently resolves to. Both read
the version cache held in namespace metadata; pass
`--refresh-version-cache` to re-query upstream. This is the same cache
`create` uses, so these are the commands to check what a create would
install.

## Where cluster state lives

There is no local state file. A cluster is described entirely by
Shaken Fist namespace metadata: one `orchestrated_k3s_cluster_<name>`
key per cluster, plus an `orchestrated_k3s_clusters` list and the two
release version caches. Any authorized client can therefore manage a
cluster somebody else created, and `~/.kube/config` is a convenience
rather than the record.
