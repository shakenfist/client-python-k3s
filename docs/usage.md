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
| `--control-plane-count` | 1 | Control plane nodes, at least 1. More than one gives a highly available control plane. |
| `--worker-count` | 2 | Worker nodes, at least 0. |
| `--metal-address-count` | 5 | Floating addresses, at least 0, routed into the cluster network for MetalLB to hand out. Accepted but ignored when `--no-metallb` is given. |
| `--network` | (a new one) | Join a pre-existing Shaken Fist network instead of creating one for this cluster. |
| `--release-channel` | `stable` | A k3s release channel. `stable`, `latest`, or a version-pinned channel such as `v1.26`. |
| `--refresh-version-cache` | off | Re-query the k3s and Longhorn release APIs instead of using the cached answers. |
| `--sshkey` | none | A public key to place on every node, for debugging. |
| `--metallb` / `--no-metallb` | on | Install MetalLB for load balancer addresses. |
| `--longhorn` / `--no-longhorn` | on | Install Longhorn for persistent storage. |
| `--kubeconfig` / `--no-kubeconfig` | on | Merge the new cluster into `~/.kube/config`; see below. The cluster's own kubeconfig is recorded either way and is always available from `getconfig`. |
| `--manifest PATH` | none | Stage a local manifest into the cluster on first start. Repeatable; see below. |
| `--control-plane-cpus` | 2 | vCPUs for each control plane node. A positive integer. |
| `--control-plane-memory` | 2048 | RAM, in MB, for each control plane node. A positive integer; see "Sizing", below. |
| `--control-plane-disk` | 50 | Disk, in GB, for each control plane node. A positive integer. |
| `--worker-cpus` | 2 | vCPUs for each worker node. A positive integer. |
| `--worker-memory` | 2048 | RAM, in MB, for each worker node. A positive integer. |
| `--worker-disk` | 50 | Disk, in GB, for each worker node. A positive integer. |
| `--server-config PATH` | none | A YAML mapping of k3s configuration keys, applied to every control plane node. A few keys the plugin depends on are refused; see "k3s configuration", below. |
| `--agent-config PATH` | none | A YAML mapping of k3s configuration keys, applied to every worker, including workers added later by `expand-workers`. A smaller set of keys is refused for the agent role; see "k3s configuration", below. |

Each node is a Shaken Fist instance on a Debian 12 base image, with a
floating address and the `sf-agent2` side channel enabled. Nodes
default to 2 vCPUs, 2048 MB of RAM and a 50 GB disk in both roles, and
the six sizing options above change that per role. The sizes are
recorded when the cluster is created, and `expand-workers` builds new
workers at the size the cluster recorded. A full create is 15-25
minutes, and reports numbered phases with per-phase elapsed times as
it goes.
Skipping MetalLB, Longhorn or the local kubeconfig update also skips
that phase's number, so a create that leaves all three out counts
fewer phases rather than reporting a phase it never runs.

#### Cluster names and counts

`NAME` may contain only ASCII letters, digits and hyphens, must start
and end with a letter or digit, and may be at most 48 characters
long. Mixed case is accepted. The name becomes part of every node's
Shaken Fist instance name, `k3s-NAME-node-SERIAL`, which must be a DNS
host name of at most 63 characters; 48 leaves room for the prefix and
for serials up to five digits, so a cluster which was legal to create
can still grow. A name which breaks the rule is refused before
anything is built, rather than at the first instance create, which
would leave an interrupted cluster behind. Only `create` applies this
rule: a cluster that already exists under some other name keeps
working with every other command.

The names `k3s_version_cache` and `longhorn_version_cache` are refused
by every command, because they collide with the keys the release caches
use in the namespace's metadata.

The counts are checked the same way, and a boolean or a value that is
not an integer is refused. `expand-workers` and `expand-addresses`
require a count of at least 1, because 0 or less asks for nothing.

The size options below are checked by the library rather than by
Click, so an out-of-range size exits 1 with the library's message, like
every other refusal, rather than Click's exit 2 usage error. A refused
`create` does not create the `--namespace` namespace.

#### Sizing

A realistic floor for a control plane node is 4096 MB of RAM. On a
cluster built at exactly 2 vCPUs and 2048 MB, the k3s server process
alone held about 709 MB resident. A burst of pod creations then drove
the node into a global out-of-memory condition which killed Longhorn
and Traefik and took the API server down for about 30 seconds. That
is not reliably reproducible, because it depends on how recently k3s
restarted, so 2048 MB runs a control plane rather than one that holds
up under load.

The plugin deliberately validates only that each size is a positive
integer. A hard minimum would be a guess about workloads, and some
clusters will carry very little. The default stays at 2048 MB so that
existing invocations build what they built before; pass
`--control-plane-memory 4096` (or more) for a control plane you intend
to rely on.

#### k3s configuration

`--server-config` and `--agent-config` each take the path of a YAML
file holding a single mapping of k3s configuration keys, spelled as
k3s's own `config.yaml` spells them. The server file is applied to
every control plane node and the agent file to every worker. Neither
is interpreted: keys are not checked against k3s's flags, and k3s logs
and ignores a key it does not recognise for the role. An empty file
means no configuration. A file which uses a YAML alias (`*name`, which
`<<:` merge keys need too) is refused, because a few hundred bytes of
nested aliases expand to gigabytes. Both mappings are recorded in the
cluster's metadata, which is how `expand-workers` gives a new worker
the agent configuration the cluster was created with.

Each node gets up to three files in `/etc/rancher/k3s/`, all written
before the k3s installer first runs, and k3s reads them in this order:

1. `config.yaml` holds what the plugin sets or defaults for the node.
   On a server that is the kubeconfig mode, the floating API address as
   a SAN, `cluster-init` on the first server only, and the control
   plane taint (below). On a worker it is a single comment line.
2. `config.yaml.d/50-sf-client-k3s.yaml` holds your file, re-dumped as
   YAML. It is written only when the mapping is not empty.
3. `config.yaml.d/90-sf-client-k3s-enforced.yaml` holds what must
   survive your file. Today that is `disable+: [servicelb]`, on servers
   only, and only when MetalLB is installed.

k3s's merge rule is that a key in a later file replaces the same key
in an earlier one, unless the later key ends in `+`, in which case it
appends. So your file can replace any default the plugin wrote in
`config.yaml`, and a list key written with `+` adds to it. This is why
the servicelb disable lives in the last file: `disable: [traefik]` in
your file yields traefik and servicelb both disabled. The servicelb
disable is the one plugin default a caller cannot override; a caller
who wants servicelb wants `--no-metallb`. Disabling Traefik also
removes the `AdditionalAssignFailed ... PreferDualStack` log noise
from `metallb-controller`, which comes from Traefik's Service.

Keys the plugin sets, or depends on k3s leaving at its default, are
refused before anything is built. A trailing `+` does not get round
this, because on a string key `+` appends to the plugin's value.

| Role | Refused keys | Why |
|------|--------------|-----|
| server | `cluster-init` | The plugin sets it on the first server, which makes that node the embedded etcd cluster the others join. |
| server | `data-dir` | The plugin reads the join tokens and stages manifests under `/var/lib/rancher/k3s`. |
| server | `https-listen-port` | Extra servers and workers join through port 6443. |
| server | `node-name`, `with-node-id` | `remove-worker` finds a k3s node by its lowercased instance name, and a fixed name would be shared by every node. |
| server | `server`, `token`, `token-file` | The plugin joins extra servers to the first one itself. |
| server | `tls-san` (bare) | It would replace the floating API address the plugin put there. Write `tls-san+` to add SANs. |
| server | `write-kubeconfig`, `write-kubeconfig-mode` | Every `kubectl` and `helm` command, and the credential fetch, read `/etc/rancher/k3s/k3s.yaml`, and the plugin sets its mode. |
| agent | `data-dir`, `node-name`, `with-node-id`, `server`, `token`, `token-file` | As for servers: every node keeps the same layout, and the plugin joins and names workers itself. |

k3s's one-letter aliases for these keys are refused in the same way:
`d` (`data-dir`), `s` (`server`) and `t` (`token`) in both roles, and
`o` (`write-kubeconfig`) on servers. So is any key containing `=`,
because k3s reads only the part before the `=` as the key's name.

Other things to know:

- Control plane nodes are tainted
  `node-role.kubernetes.io/control-plane:NoSchedule` by default, in
  `config.yaml`, when the cluster has at least one worker. Put
  `node-taint: []` in `--server-config` to remove the taint, or
  `node-taint+: [key=value:NoSchedule]` to add another taint to it.
- A cluster created with `--worker-count 0` is not tainted. MetalLB's
  controller and Longhorn do not tolerate the taint, so tainting the
  only node would make MetalLB's rollout wait fail. That cluster stays
  untainted after `expand-workers` adds workers.
- The files are written only when a node is installed. Changing a
  cluster's recorded configuration afterwards is not supported, and
  nothing rewrites a running node.
- Every `create` refuses a k3s release older than `v1.21.1+k3s1`,
  whether or not a configuration was given, because older releases
  silently ignore the drop-in directory or the `+` suffix. The channels
  that still resolve to such a release (`v1.16` to `v1.20`, `testing`)
  are ones Longhorn's chart already refuses. `expand-workers` does not
  check.
- A bad file, a refused key, or a release that is too old is reported
  before anything is built or the cluster's name is registered.
- Both mappings are stored in the cluster's metadata, printed by
  `show`, and written to each node as a file whose mode follows the
  node's umask. k3s accepts credentials inline as keys
  (`etcd-s3-secret-key`, `agent-token`, a `datastore-endpoint` with a
  password in it), so keep them out of these files; `delete -v`
  redacts both mappings, but nothing else does.
- The refused keys protect the plugin's own operations. They are not a
  security boundary: whoever writes either file controls the security
  of every node in that role (`kube-apiserver-arg` alone can turn off
  the API server's authentication), which is no more than the
  cluster's owner can already do. A tool which accepts configuration
  from someone it trusts less than that must apply its own allowlist
  of keys.

An example for an OpenStack-Helm deployment. `servers.yaml` keeps
Traefik out of the way, labels the control plane nodes, and removes the
default taint:

```yaml
disable: [traefik]
node-label: [openstack-control-plane=enabled]
node-taint: []
```

and `agents.yaml` labels the workers as compute nodes:

```yaml
node-label: [openstack-compute-node=enabled, openvswitch=enabled]
```

```
sf-client k3s create mycluster \
    --server-config servers.yaml --agent-config agents.yaml
```

The servers end up with traefik and servicelb both disabled, and
untainted. Without `node-taint: []` they would keep the default taint,
and OpenStack-Helm's charts ship their control plane toleration
disabled, so every OpenStack service labelled for the control plane
would stay Pending. Enabling that toleration in each chart is the
alternative.

**Behaviour changes.** These apply to every new cluster, whether or not
the options above are used, and existing clusters are untouched:

- servicelb is disabled whenever MetalLB is installed.
- Control plane nodes are tainted when the cluster has workers, so
  ordinary workloads schedule only on workers.
- With the default one control plane node and two workers, Longhorn now
  has two storage nodes rather than three. Its default replica count
  is 3, so a new volume runs degraded with two replicas until a third
  node is added.
- k3s releases older than `v1.21.1+k3s1` are refused, as described
  above.

#### Local kubeconfig and manifests

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
- A file that cannot be read, cannot be decoded as UTF-8, does not
  parse, or contains a line that collides with the internal transfer
  marker is refused the same way. Manifests are read as UTF-8
  regardless of the locale the command runs under, which is what YAML
  and JSON both specify; a file in some other encoding is refused
  rather than mis-decoded into something the cluster would then apply.
- Whether a file is checked as JSON or as YAML is decided by its
  content, not its name, because that is how k3s decides: leading
  whitespace aside, a file starting with `{` goes to k3s's JSON
  decoder untouched and anything else through its YAML parser. So a
  tab indented `.json` manifest is accepted -- YAML forbids tabs for
  indentation and JSON does not -- and a `.yaml` file that is really
  JSON is accepted as JSON.
- The filename must be a plain one: letters, digits, dots, underscores
  and hyphens, starting with a letter or a digit. The filename is
  interpolated into the command that writes the file on the control
  plane node, so this is about the name rather than the content. The
  directory the file came from is not carried along -- the destination
  is the basename -- so `--manifest ~/work/net/policy.yaml` lands as
  `policy.yaml`.
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

`delete` has the same two writes in the other order, and deliberately
so: it releases the name from the cluster list first and removes the
metadata document second, so a `delete` killed between them leaves a
metadata document whose name is already free. Running `delete NAME`
again finishes the job -- the instances and the network it had already
removed stay removed -- which is why the window is recoverable on this
side and not on the create side.

### `delete NAME`

Deletes every instance in the cluster, unroutes its floating
addresses, deletes the node network if `create` made it, and removes
the cluster's namespace metadata. It then removes the cluster's
entries -- a user, context and cluster named `NAME.NAMESPACE` -- from
`~/.kube/config`, unless `--no-kubeconfig` is given, in which case
that step is skipped and a local `kubectl` is not needed.

The cleanup acts on `~/.kube/config` whatever `KUBECONFIG` says,
because that is the file `create` writes. If the file does not exist
there is nothing to remove, and `kubectl` is not run. Otherwise it
reads the entries present with `kubectl config view` and then runs
`kubectl config delete-context`, `delete-user` and `delete-cluster`
for each one by name, so it requires `kubectl` v1.20 or later
(`delete-user` arrived in v1.20.0). Any failure here -- no local
`kubectl`, or a `kubectl` call that fails -- comes after the cluster
itself has gone, so running `delete` again only reports that the
cluster does not exist. The error says which entries may remain and
gives the `kubectl --kubeconfig ~/.kube/config config delete-*`
commands that remove them by hand.

This also works on a cluster that never finished being built --
indeed it is the supported way to clear one: whatever nodes, network
and metadata an interrupted `create` managed to leave behind are
removed the same way, and a note is printed first saying the cluster
was interrupted rather than complete.

A network named with `create --network` is borrowed rather than
owned, so `delete` leaves it in place and only unroutes the addresses
this cluster routed into it. A cluster created before that
distinction was recorded is classified by its network's name: a
network `create` made is always named `k3s-NAME-node`, and only one
with that name is deleted.

### `expand-workers NAME [--worker-count N]`

Adds `N` more workers (default 2, at least 1) to a running cluster. Existing
nodes are untouched. Refuses to run against a cluster that never
finished being built; see `create`, above. New workers are built at
the worker size the cluster recorded when it was created, and are
given the `--agent-config` the cluster recorded, as a drop-in written
before k3s is installed on them. A cluster created before this
existed recorded none, so its new workers get no drop-in. The release
floor is not checked here.

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
command rather than removing the first two and then failing. The same
is true of the node names: every one is resolved from its instance up
front, so a worker whose instance record cannot supply one is refused
before any other worker has been touched.

Each worker being removed is then drained (`kubectl drain
--ignore-daemonsets --delete-emptydir-data --timeout=300s`) and
removed from k3s (`kubectl delete node`) before its instance is
deleted, so that pods running on it are rescheduled rather than left
orphaned on a node object nobody will ever clean up. The workers not
named are left alone entirely.

Removing the last worker of a cluster is allowed -- a k3s server node
is schedulable, so a cluster with no workers still works.

A drain that cannot finish is bounded and undone. If a pod on the
worker has nowhere else to go -- a PodDisruptionBudget refuses the
eviction, an unmanaged pod would need `--force`, or the cluster has no
other node with room -- `kubectl drain` gives up after its timeout and
says which pod it could not move. Draining cordons the node as its
first act, so the command then uncordons it before reporting the
failure: a refused removal leaves the cluster as it found it rather
than one node short of schedulable capacity. The same applies to the
`kubectl delete node` that follows a drain that did succeed -- by then
the node is not only cordoned but empty, so a failure there is the one
that most needs putting back. If the uncordon fails as well, the
output says so and names the `kubectl uncordon` to run by hand; the
original reason is still what the command reports, because that is the
part you can act on.

One failure is not undone, because by then there is nothing left to
undo: an instance delete that fails after the node object is already
gone from k3s. The worker stays in the cluster's records, so `delete`
still destroys the instance and `health` still reports it, and the
output names the instance to delete by hand before running
`remove-worker` for it again. Dropping the record instead would leave
an instance running that no command here could see.

A worker whose instance has already been deleted out of band is
removed from the cluster's records rather than refused. There is no
node to drain and no instance to destroy, so both are skipped. This is
the only command that can clear such an entry, which `health` reports
as a node that no longer exists.

Refuses to run against a cluster that never finished being built; see
`create`, above.

Workers are named by instance UUID rather than by node name, which is
also what makes a mixed case cluster name work here: Shaken Fist
accepts a capital letter in an instance name and Kubernetes does not
accept one in a node name, so on a cluster called `MyCluster` the
instance `k3s-MyCluster-node-002` is the k3s node
`k3s-mycluster-node-002`. The drain uses the name k3s registered.

### `expand-addresses NAME [--address-count N]`

Routes `N` more floating addresses (default 2, at least 1) into the cluster
network and reconfigures MetalLB's pool to include them. Refuses to
run against a cluster that never finished being built; see `create`,
above.

Also refuses a cluster created with `--no-metallb`, before it routes
anything. There is nothing to reconfigure on such a cluster, and the
refusal is checked up front because the alternative is the worst shape
of failure: the addresses get routed and charged for, and the command
then fails looking for MetalLB workloads in a namespace that does not
exist, leaving the caller paying for addresses nothing can hand out.
Clusters created before this was recorded are treated as having
MetalLB, which they do.

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
the release versions in use, `node_sizes` (the vCPUs, memory in MB
and disk in GB of each role), and `server_config` and `agent_config`
(the mappings given to `--server-config` and `--agent-config`, as
structure rather than text). A cluster created before sizing existed
reports the defaults it was built at, which is exact because it could
only have been built at the default, and one created before the k3s
configuration options reports both mappings as empty, for the same
reason; nothing is written back to the cluster's metadata. A cluster
that never finished being built is shown rather than refused, with a
note pointing out that its `state` is not `created` and that `delete`
is how to clear it.

Note that the metadata includes the node token and the kubeconfig, so
the output is cluster-admin credentials. Do not paste it into a bug
report.

### `health NAME`

Reports the state of the cluster and of every node in it: for each
control plane node and worker, whether the Shaken Fist instance still
exists and its instance and agent state; and whether the k3s API
answers, by running `kubectl get nodes` through the first control
plane node. It repairs nothing -- this is a report, not a fix -- and
by default exits 0 whatever it found, because producing the report is
what was asked for and it succeeded:

```
$ sf-client k3s health mycluster
Cluster mycluster in namespace default is healthy
  state: created
  nodes:
    [ok] k3s-mycluster-node-001 (3fa85f64-5717-4562-b3fc-2c963f66afa6, control plane): instance created, agent ready
        booted 2026-10-06T06:47:11Z, k3s active, 0 restarts, 0 OOM kills, 2702 of 3914 MiB available, etcd 138 MiB, snapshots 0 MiB
    [ok] k3s-mycluster-node-002 (7c9e6679-7425-40de-944b-e07fc1f90ae7, worker): instance created, agent ready
        booted 2026-10-06T06:50:41Z, k3s-agent active, 0 restarts, 0 OOM kills, 2173 of 2971 MiB available
  k3s API: answered on 3fa85f64-5717-4562-b3fc-2c963f66afa6
    NAME                     STATUS   ROLES                  AGE   VERSION
    k3s-mycluster-node-001   Ready    control-plane,master   10m   v1.30.2+k3s1
    k3s-mycluster-node-002   Ready    <none>                 9m    v1.30.2+k3s1
```

The indented line under each node is what that node reports about
itself: when it booted, whether k3s is running and how many times
systemd has restarted it, how many kernel OOM kills there have been
since boot, available memory, and on control plane nodes the size of
etcd and its snapshots. Nothing on it is judged -- there are no
markers and no thresholds, and it never changes whether the node or
the cluster is reported healthy -- because whether a number is a
problem depends on what the cluster is for. A node that could not be
read says `signals: not read` and why. The counts are cumulative
since boot, so compare against an earlier report to see what changed;
see [What `signals` reports](library-api.md#what-signals-reports) for
what each reading is and how to diff it.

A cluster that never finished being built is reported here too, rather
than refused -- reporting on a broken cluster is what this command is
for -- so its `state` line names the state it was interrupted in
instead of `created`, and an instance the metadata names but which no
longer exists is reported as gone rather than failing the command.

The command never hangs, which matters most on exactly the clusters
it is for. The `kubectl get nodes` probe, and the signals read on
each node, are only attempted when the node they would run on looks
able to answer -- the report has already read that node's instance and
agent state -- and they share a thirty second budget even then,
because a command queued against an instance whose agent is not
connected is accepted and then never runs. Each of those outcomes is
reported with the reason, on the `k3s API:` line or on the node's
signals line, rather than waited on. The budget is only spent when
something is wrong: the probes run on every node at once, so a
healthy cluster waits for about as long as its slowest probe takes,
whatever its size. The budget bounds the waiting only: on top of it
come one Shaken Fist API round trip per node to read its instance, one
per probe to submit it, and the reads of each probe, so on a slow Shaken
Fist API the command can take longer than thirty seconds.

An abandoned run leaves operations queued: up to one per probed node,
plus the `kubectl get nodes` against the control plane node. The
reason on the `k3s API:` line names that agent operation so it can be
recognised later: until Shaken Fist's own deadline ends it, a
subsequent `expand-workers` or `update-os` waits for it along with
everything else. That is a delay in a later command,
not a failure of it -- those commands wait for every agent operation on
a node, because the next command must not race one still running, but
they only fail on the ones they submitted themselves. It is still worth
knowing about if you poll `health` in a loop against a cluster whose
agent is intermittently slow.

Pass `--strict` to exit 1 when the cluster is not healthy, which is
what makes `health` usable from a shell:

```
sf-client k3s health mycluster --strict || exit 1
```

The report is printed identically either way -- only the exit code
changes -- so one run gives both the text and the branch, and nothing
has to parse the output.

Be precise about what "healthy" means here, because it is narrower than
it sounds: the cluster finished being built, every instance the metadata
names exists and has a ready agent, and `kubectl get nodes` on the first
control plane node exited zero. The node terms are Shaken Fist's view of
the machines, not Kubernetes' view of the kubelets, so a cluster whose
nodes are all `NotReady` still reports healthy -- the k3s API answered,
which is all the last term asks. `--strict` is therefore a good gate for
"did this cluster come up and is its control plane reachable" and not a
substitute for waiting on workload readiness; this repo's own functional
test uses both, `health --strict` and a separate `kubectl wait`. Folding
Kubernetes node readiness into the report is
[shakenfist/client-python-k3s#76](https://github.com/shakenfist/client-python-k3s/issues/76).

The default stays at "always 0" because "the cluster is unwell" and
"the health check could not run" are different answers, and a command
whose exit code conflates them is worse than one that reports neither.
A library caller reads `Cluster.health()`'s dictionary and does not
need either.

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

The k3s version comes from the k3s update API
(`https://update.k3s.io/v1-release/channels`), and the Longhorn version
from the index of Longhorn's Helm repository
(`https://charts.longhorn.io/index.yaml`), the same repository the
control plane node later installs the chart from. The lookups run where
`sf-client` runs, so that machine needs to reach both. Each gives up
after 30 seconds without an answer.

## Where cluster state lives

There is no local state file. A cluster is described entirely by
Shaken Fist namespace metadata: one `orchestrated_k3s_cluster_<name>`
key per cluster, plus an `orchestrated_k3s_clusters` list and the two
release version caches. Any authorized client can therefore manage a
cluster somebody else created, and `~/.kube/config` is a convenience
rather than the record.
