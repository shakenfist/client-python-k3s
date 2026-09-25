# Using the k3s command group

The plugin registers itself with `sf-client` through the
`shakenfist_client.plugin` entry point, so once it is installed the
`k3s` command group appears automatically. `sf-client k3s --help`
lists everything below.

Every command takes `--namespace`. Without it the client's own
namespace is used; an administrator can name a different one to work
on a cluster they do not own.

The root `sf-client` options are a separate thing, and two of them
matter here. The connection options -- `--apiurl`, `--key` and
`--namespace` -- configure the client these commands use, so the cloud
they talk to and the namespace referred to above both follow what you
pass there. The root `--async` has no effect on `k3s` commands: each
one runs its own wait loops and reports progress as it goes, so the
client is always asked not to block on the command's behalf.

## Cluster lifecycle

### `create NAME`

Builds a cluster and, unless `--no-kubeconfig` is given, leaves it in
`~/.kube/config` as the current context on success.

| Option | Default | Meaning |
|--------|---------|---------|
| `--control-plane-count` | 1 | Control plane nodes. More than one gives a highly available control plane. |
| `--worker-count` | 2 | Worker nodes. |
| `--metal-address-count` | 5 | Floating addresses routed into the cluster network for MetalLB to hand out. Accepted but ignored when `--no-metallb` is given. |
| `--network` | (a new one) | Join a pre-existing Shaken Fist network instead of creating one for this cluster. |
| `--release-channel` | `stable` | A k3s release channel. `stable`, `latest`, or a version-pinned channel such as `v1.26`. |
| `--refresh-version-cache` | off | Re-query the k3s and Longhorn release APIs instead of using the cached answers. |
| `--sshkey` | none | A public key to place on every node, for debugging. |
| `--metallb` / `--no-metallb` | on | Install MetalLB for load balancer addresses. |
| `--longhorn` / `--no-longhorn` | on | Install Longhorn for persistent storage. |
| `--kubeconfig` / `--no-kubeconfig` | on | Merge the new cluster into `~/.kube/config`; see below. The cluster's own kubeconfig is recorded either way and is always available from `getconfig`. |
| `--manifest PATH` | none | Stage a local manifest into the cluster on first start. Repeatable; see below. |

Each node is a Shaken Fist instance with 2 vCPUs, 2GB of RAM and a
50GB disk on a Debian 12 base image, with a floating address and the
`sf-agent2` side channel enabled. A full create is 15-25 minutes, and
reports numbered phases with per-phase elapsed times as it goes.
Skipping MetalLB, Longhorn or the local kubeconfig update also skips
that phase's number, so a create that leaves all three out counts
fewer phases rather than reporting a phase it never runs.

With `--kubeconfig` (the default), the local kubeconfig is written
directly if `~/.kube/config` does not exist. If it does, the merge
shells out to `kubectl config view --flatten`, so a local `kubectl` is
required for that path; without one the create stops after the
cluster is built and tells you to fetch the credentials with
`getconfig`. With `--no-kubeconfig`, none of this runs and
`~/.kube/config` is left untouched.

`--manifest` stages a local file, unmodified, into the new cluster's
k3s auto-apply directory (`/var/lib/rancher/k3s/server/manifests/` on
the first control plane node) before k3s is installed there, so k3s
applies it itself the first time the server starts:

- Only files ending in `.yaml`, `.yml` or `.json` are accepted; k3s's
  own deploy controller only ever looks at those three suffixes, and
  anything else would be copied onto the node and then silently
  ignored.
- Nothing is templated, and the order manifests are applied in is
  k3s's business, not this command's. A payload that needs either
  belongs in a Helm chart installed afterwards instead.
- Two manifests sharing a filename are refused before anything is
  built (instances included), because the destination filename is the
  source basename and the second write would silently replace the
  first on the node.
- A file that cannot be read, is not valid YAML, or contains a line
  that collides with the internal transfer marker is refused the same
  way.
- k3s ships its own manifests in the same directory --
  `traefik.yaml`, `coredns.yaml`, `local-storage.yaml` and `ccm.yaml`
  as of the k3s versions current when this was written -- and
  reapplies them on every start. Naming a manifest one of those
  filenames is not refused, because the list is version specific and
  would go stale into false refusals, but k3s will overwrite it rather
  than the other way around. Pick a filename that does not collide.

A `create` for a name already holding a cluster is refused. If that
name belongs to a working cluster the error just says the name is
taken; if it belongs to a cluster an earlier `create` never finished
building, the error instead names the state it was left in and points
at `sf-client k3s delete NAME` as the way to clear it, because there is
no way to resume a half built cluster -- only to remove it and start
again. `show` and `health` (below) report a cluster in that state
rather than refusing, and `expand-workers`, `remove-worker` and
`expand-addresses` refuse to run against one for the same reason
`create` refuses to build over it: the operation needs a first control
plane node and a join token that an interrupted build may never have
recorded.

One gap in this is not yet closed: if `create` is killed in the narrow
window after it claims the name in the namespace's cluster list but
before it writes that cluster's own metadata document, the name is
left claimed with nothing to report its state -- `delete NAME` says the
cluster does not exist, because there is no metadata document for it to
read. There is currently no supported way to free such a name from
this plugin; see
[shakenfist/client-python-k3s#72](https://github.com/shakenfist/client-python-k3s/issues/72).

### `delete NAME`

Deletes every instance in the cluster, unroutes its floating
addresses, deletes the node network, and removes the cluster's
namespace metadata. It then removes the cluster's entries from the
local kubeconfig with `kubectl config unset`, unless `--no-kubeconfig`
is given, in which case that step is skipped and a local `kubectl` is
not needed.

This also works on a cluster that never finished being built --
indeed it is the supported way to clear one: whatever nodes, network
and metadata an interrupted `create` managed to leave behind are
removed the same way, and a note is printed first saying the cluster
was interrupted rather than complete.

Note that the node network is deleted whether the cluster created it
or it was named with `create --network`, so deleting a cluster built
on a shared pre-existing network takes that network with it.

### `expand-workers NAME [--worker-count N]`

Adds `N` more workers (default 2) to a running cluster. Existing
nodes are untouched. Refuses to run against a cluster that never
finished being built; see `create`, above.

### `remove-worker NAME --worker UUID [--worker UUID ...]`

Removes one or more workers from a running cluster, by the Shaken
Fist instance UUID of each (`expand-workers` reports the UUID it
created for each new worker). `--worker` is required and repeatable:

```
sf-client k3s remove-worker mycluster \
    --worker 3fa85f64-5717-4562-b3fc-2c963f66afa6 \
    --worker 7c9e6679-7425-40de-944b-e07fc1f90ae7
```

Every UUID given is checked against the cluster's worker list before
anything happens, so a typo in the last of three fails the whole
command rather than removing the first two and then failing. Each
surviving worker is then drained (`kubectl drain --ignore-daemonsets
--delete-emptydir-data`) and removed from k3s (`kubectl delete node`)
before its instance is deleted, so that pods running on it are
rescheduled rather than left orphaned on a node object nobody will
ever clean up.

Removing the last worker of a cluster is allowed -- a k3s server node
is schedulable, so a cluster with no workers still works -- but if
workloads on that worker have nowhere else to go, the drain blocks
until it times out, which surfaces as an agent operation error rather
than as a quick failure. Refuses to run against a cluster that never
finished being built; see `create`, above.

### `expand-addresses NAME [--address-count N]`

Routes `N` more floating addresses (default 2) into the cluster
network and reconfigures MetalLB's pool to include them. Refuses to
run against a cluster that never finished being built; see `create`,
above.

### `update-os NAME`

Runs an OS package update on every control plane node and worker.
This does not update k3s itself. Unlike the other expansion commands
above, this one also runs on a cluster that never finished being
built: it updates whichever nodes exist and does nothing if there are
none, which is a truthful answer rather than a refusal.

## Inspection

### `list`

Prints the names of the clusters recorded in the namespace.

### `show NAME`

Prints the cluster's namespace metadata: node UUIDs, the network, the
API addresses, the join address, the plugin version that created it,
and the release versions in use. A cluster that never finished being
built is shown rather than refused, with a note pointing out that its
`state` is not `created` and that `delete` is how to clear it.

Note that the metadata includes the node token and the kubeconfig, so
the output is cluster-admin credentials. Do not paste it into a bug
report.

### `health NAME`

Reports the state of the cluster and of every node in it: for each
control plane node and worker, whether the Shaken Fist instance still
exists and its instance and agent state; and whether the k3s API
answers, by running `kubectl get nodes` through the first control
plane node. It repairs nothing -- this is a report, not a fix -- and
always exits 0, because producing the report is what was asked for and
it succeeded regardless of what it found:

```
$ sf-client k3s health mycluster
Cluster mycluster in namespace default is healthy
  state: created
  nodes:
    [ok] k3s-mycluster-node-001 (3fa85f64-5717-4562-b3fc-2c963f66afa6, control plane): instance created, agent ready
    [ok] k3s-mycluster-node-002 (7c9e6679-7425-40de-944b-e07fc1f90ae7, worker): instance created, agent ready
  k3s API: answered on 3fa85f64-5717-4562-b3fc-2c963f66afa6
    NAME                     STATUS   ROLES                  AGE   VERSION
    k3s-mycluster-node-001   Ready    control-plane,master   10m   v1.30.2+k3s1
    k3s-mycluster-node-002   Ready    <none>                 9m    v1.30.2+k3s1
```

A cluster that never finished being built is reported here too, rather
than refused -- reporting on a broken cluster is what this command is
for -- so its `state` line names the state it was interrupted in
instead of `created`, and an instance the metadata names but which no
longer exists is reported as gone rather than failing the command.
`create -> health -> delete` is a reasonable way to check a cluster
came up correctly before handing it to something else.

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
