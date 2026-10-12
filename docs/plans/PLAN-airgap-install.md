# Air-gapped k3s install from labelled Shaken Fist artifacts

## Prompt

Before responding to questions or discussion points in this
document, explore the client-python-k3s codebase thoroughly. Read
relevant source files, understand existing patterns (the
`shakenfist_client.plugin` entry point and Click command group in
`__init__.py`, the orchestration primitives and namespace-metadata
cluster state in `primitives.py`, agent operation handling and its
wait loops, the two-mode progress reporting in `progress.py`, the
release version caches), and ground your answers in what the code
actually does today. Do not speculate about the codebase when you
could read it instead. Where a question touches on external
concepts (the Shaken Fist API and in-guest agent, k3s, MetalLB,
Longhorn, helm), research as needed to give a confident answer.
Flag any uncertainty explicitly rather than guessing.

Consult `ARCHITECTURE.md` for the plugin structure, cluster
assembly flow, and agent requirements (the `sf-agent2` side
channel). Consult `AGENTS.md` for build and test commands and
project conventions. Remember that orchestration behaviour can
only be fully validated against a live Shaken Fist cluster; unit
tests mock the API surface.

<!-- shared-block: plan-file-conventions v2 -->
Plan file conventions (shared block; do not edit -- the canonical
copy lives in shakenfist/development at
`templates/shared-blocks/plan-file-conventions.md`):

- All planning documents live in `docs/plans/`.
- Detailed planning gets one plan file per phase. Phase files are
  named for their master plan, sit in the same directory as it,
  and append `-phase-NN-descriptive` before the `.md` extension.
- The master plan tracks its phases in a table under its Execution
  section. `Merged` is last, and the push audit is the last row,
  for the reasons given in `plan-push-audit-phase`:

  | Phase | Plan | Status | Merged |
  |-------|------|--------|--------|
  | 1. Schema migration | PLAN-thing-phase-01-schema.md | Not started | |
  | 2. Public API | PLAN-thing-phase-02-api.md | Not started | |
  | 3. Push audit | - | Not started | |

- One commit per logical change, and at minimum one commit per
  phase. Unrelated changes are not batched into a single commit.
  Each commit is self-contained: it builds, passes tests, and has
  a message explaining what changed and why.
<!-- shared-block-end -->

## Situation

This plan is a proposal. Nobody has scheduled it yet. It was written
on 2026-10-12 against client-python-k3s `develop` at `094272c`,
Shaken Fist `develop` at `45a93056b`, client-python `develop` at
`0ab4f68` and agent-python `develop` at `5b87398`. Citations below
are to those trees unless they say otherwise. Phases are planned one
at a time, and each phase corrects the rows after it when its survey
finds them wrong.

### Why: update.k3s.io failed twice in about 36 hours

- From 2026-10-10 19:32 UTC, `update.k3s.io` served a self-signed
  `TRAEFIK DEFAULT CERT`. That is
  [#132](https://github.com/shakenfist/client-python-k3s/issues/132),
  still open, and it ejected #131 from the merge queue. `k3s create`
  failed in its version lookup within ten seconds, before it built
  anything.
- From roughly 06:00 to 14:00 UTC on 2026-10-11, `update.k3s.io` could
  not be reached from the homelab's egress. Zeek saw every SYN to all
  three of its AWS addresses answered with ICMP unreachable, and the
  CI proxy (squid on maui) logged `CONNECT` 503. That failed #135's
  unit tests. #134 separately stops the unit tests reaching the
  network.
- Before that, 252 of 252 squid `CONNECT`s to it over 2026-10-05..10
  succeeded.

The figures in the last two bullets come from the triage of #132 and
#135, as given to this plan. This plan restates them and did not
query the Zeek or squid logs again.

### What a create fetches from the internet today

The version lookup that failed is the smallest of these dependencies.

| # | What | Fetched by | Where in the code | When |
|---|------|------------|-------------------|------|
| 1 | `update.k3s.io/v1-release/channels`, the channel to version map | the client | `primitives.py:51`, `:109-174` | once per namespace per 24 hours (`:127`) |
| 2 | `update.k3s.io` again, by each node's installer | each node | `cluster.py:3165-3167`, `:3213-3220`; see "A latent dependency" below | every k3s install |
| 3 | `get.k3s.io`, the install script | each node | `cluster.py:3165`, `:3213` | every k3s install |
| 4 | GitHub release assets: `sha256sum-amd64.txt` and the `k3s` binary | each node's installer (`download_hash`, `download_binary` in install.sh) | via 3 | every k3s install |
| 5 | k3s's own system images (coredns, pause, local-path-provisioner, metrics-server, traefik, klipper) from docker.io | containerd on each node | implicit: no airgap images tarball is staged | first start of each node |
| 6 | Debian mirrors, and the extrepo helm repository | each node | `cluster.py:2934-2941` (`apt-get dist-upgrade` at create and `update-os`), `:3168-3171` (helm), `:3211` | every create |
| 7 | `metallb.github.io` and `charts.longhorn.io` charts, and the images those charts pull | first control plane node | `cluster.py:3428`, `:3455` | every create |
| 8 | `charts.longhorn.io/index.yaml`, the Longhorn version lookup | the client | `primitives.py:60`, `:177-261` | once per namespace per 24 hours |

The channel to version cache is stored in *namespace* metadata under
`orchestrated_k3s_cluster_k3s_version_cache` (`primitives.py:31`,
written at `:166`), so each namespace has its own. The merge tier of
CI deploys into the runner's own ephemeral namespace
(`tools/ci_deploy_test.sh:4-8`, `.github/workflows/functional-tests.yml:11`),
so its cache is always cold. Every merge run asks `update.k3s.io`, and
every merge run fails while that host is broken.

### Groundwork in flight

A separate, small pull request adds a stale-copy fallback to the cache:
[#140](https://github.com/shakenfist/client-python-k3s/pull/140), open
when this plan was written. It factors the fetch and parse out of
`get_k3s_release()` into `_fetch_k3s_releases()`, names the 24 hour
lifetime `RELEASE_CACHE_LIFETIME`, and when the upstream cannot answer
(`UPSTREAM_UNAVAILABLE_REASONS`, `_upstream_unavailable()`) it uses an
expired copy with a written warning. This plan treats that PR as
landed and builds on those names. Phase 1 rechecks them. The fallback
needs a copy to fall back to, so it does nothing for a namespace whose
cache was never filled. That is every CI run, and it is the gap phase 1
closes.

### A latent dependency: every node asks update.k3s.io as well

`install_control_plane()` and `install_k3s_component()` pass the
*resolved release*, such as `v1.33.4+k3s1`, as `INSTALL_K3S_CHANNEL`
(`cluster.py:3165-3167`, `:3213-3220`). This has been the case since
the initial commit. install.sh treats any `INSTALL_K3S_CHANNEL` as a
channel name: it requests `${INSTALL_K3S_CHANNEL_URL}/${INSTALL_K3S_CHANNEL}`
and takes the last path component of the URL it ends up at. That is
`get_release_version()`, lines 361-384 of the install.sh
`get.k3s.io` served on 2026-10-11 (sha256 `e5cc3b3d...`). Checked live on
2026-10-11:

```
stable       -> 302 to .../releases/tag/v1.36.5+k3s1   (a real channel)
v1.33.4+k3s1 -> 404 at .../channels/v1.33.4+k3s1       (what the plugin sends)
```

The 404's effective URL still ends in `v1.33.4+k3s1`, so the install
works by accident. It also works when the host is unreachable: curl
runs without `-f`, so `-w '%{url_effective}'` still prints the request
URL, and the command substitution's status is `sed`'s. So every node
install makes a request to `update.k3s.io` that always fails, and the
result is never used. When that host refuses connections or fails TLS,
the request costs nothing. When it blackholes them, each node waits on
curl's connect timeout before it carries on. `INSTALL_K3S_VERSION` is
the variable that skips the lookup. This is a one-line fix and does not
need to wait for this plan (see Bugs fixed). In the air-gapped path
the lookup is never reached, because `INSTALL_K3S_SKIP_DOWNLOAD`
returns from `download_and_verify()` before `get_release_version()` is
called (install.sh:751-757).

### Survey findings

**S1. Guest delivery uses the agent's put-blob operation.** The
operator settled this on 2026-10-12, and the code confirms it.

- *Client:* `instance_put_blob(instance_ref, blob_uuid, path, mode,
  deadline_seconds=None, progress_timeout_seconds=None,
  await_seconds=None)`, client-python `shakenfist_client/apiclient.py:1551-1564`.
  It is gated on the server capability `instance-put-blob`
  (`shakenfist/external_api/app.py:443`). The method has existed since
  client-python `v0.7.0` (`c54e8bc`, 2023-08), and the plugin's
  dependency floor, `shakenfist_client >= 0.7.7` (`pyproject.toml:58`),
  has the four argument form (`v0.7.7` `apiclient.py:1048`). The two
  timing keyword arguments arrived in `0b39248` (2026-09-01), which no
  tag contains yet. The newest tag is `v0.8.3`.
- *Server:* `POST /instances/<ref>/agent/put`, `InstanceAgentPutEndpoint`
  at `shakenfist/external_api/instance.py:2229-2326`. It requires
  instance ownership (`:2257`) and a ready agent. It refuses a mode
  that is not a string or a number (`:2283`), and otherwise accepts an
  integer or a symbolic mode. It looks the blob up by UUID alone
  (`:2294`), and queues two commands: `put-blob`, then `chmod`
  (`:2298-2309`). Preflight copies the blob onto the hypervisor that
  hosts the instance, and that time counts against the deadline
  (`shakenfist/operations/node_aop_op.py:90-134`).
- *Side channel:* `PutBlobCommand`, `shakenfist/daemons/sidechannel/main.py:475-503`.
  `_chunk_reader()` (`:887-937`) sends 100 KiB chunks (`MAX_CHUNK_SIZE`,
  `:48`), base64 encoded, with up to five unacknowledged
  (`MAX_OUTSTANDING`, `:49`). `ChmodCommand` (`:506-548`) parses the
  mode with `int(cmd['mode'])` (`:516`). A `CommandError` from the
  agent fails the operation (`:800-824`). The command list is
  fail-fast (`_abort_commands_if_terminal()`, `:1235`), so a failed
  put never runs its chmod.
- *Agent:* agent-python `shakenfist_agent/commandline/daemon.py`.
  `_ChunkConsumer` (`:77-92`) runs `open(path, 'wb')` (`:84`) and
  appends each chunk in turn. `_handle_put_file` is at `:282`, and
  `_handle_chmod` (`:327`) calls `symbolicmode.chmod()`. Any exception
  is sent back as a `CommandError`.

The limits that follow from this code, which the design has to
respect:

| Property | What the code does | Consequence for this plan |
|----------|--------------------|---------------------------|
| Size | No size cap anywhere on the path. The bound is time: `AGENT_OPERATION_DEFAULT_DEADLINE`, 600 s, which includes queueing and the preflight copy (`shakenfist/config.py:325`), and `AGENT_OPERATION_DEFAULT_PROGRESS_TIMEOUT`, 30 s (`:344`). | Nobody has measured throughput (open question 9). Asking for `deadline_seconds=0` with a progress timeout needs a client-python release that contains `0b39248`. |
| Parent directories | Not created. `open()` raises `FileNotFoundError`, which comes back as a `CommandError` and fails the operation. | Create the directories with an execute operation first. |
| Mode | The mode is set by the separate chmod after the transfer. Until then the file has the agent's umask mode. A string such as `'0755'` or `'755'` is parsed as *decimal* 755, which is octal 1363. | Pass an `int` (`0o755`, sent as 493) or a symbolic string (`'u=rwx,go=rx'`). Never pass an octal string. |
| Owner | Whoever the agent runs as. The plugin already assumes root ("Commands sent to a cluster node run as root", AGENTS.md). The API exposes no chown. | Root-owned files, which is what k3s expects. |
| Atomicity | None. The agent writes at the final path, truncating any file already there. A failed transfer leaves a partial file. A retried one (`agent/put` is retryable, operator guide `agent_operations.md:128-141`) starts again from offset 0. | Write to a staging path, verify the sha256, then `mv` into place. k3s watches its images directory and imports supported files as they appear (S7), so a partial file must never land there. |
| Access | The blob is looked up by UUID with no namespace check, both here and in preflight. `GET /blobs/<uuid>/data` is the same (`external_api/blob.py:166-199`). | Not relied on. The plugin reaches every blob through an artifact the caller can see, so a later rule of "blob must be reachable through a visible artifact" would still hold. Observed while planning, not filed. |
| Placement | Preflight's `ensure_local()` leaves a replica on every hypervisor that hosts a node. | Disk cost on the hypervisors, accounted for in S8. |

**S2. Fetch by URL is real, asynchronous, and always treats the
result as an image.** `cache_artifact(url, shared, namespace)`
(client `apiclient.py:961`) calls `POST /artifacts`
(`external_api/artifact.py:449-512`). That endpoint always creates
`TYPE_IMAGE` (`:460-463`, `:484`), resolves by ownership, and queues
`image_fetch` (`:501-509`). It returns the artifact in state `initial`.
Completion can be seen in the artifact's events, or with
`GET /artifacts/<ref>/outstanding` (`:1049-1080`), which client-python
has no method for.

The fetch runs on a Shaken Fist node using `requests` and follows
redirects. A proxy is configured only for `http://` URLs
(`shakenfist/images.py:396-405`, `HTTP_PROXY_SERVER`). Whether a node's
HTTPS fetches go through a proxy depends on that node's environment,
which this plan did not check on sfcbr.

- *Change detection:* `transfer_image()` (`images.py:152-251`) compares
  the response's `Last-Modified` and `Content-Length` with the blob
  already indexed. GitHub release assets send both after their
  redirect, so an unchanged asset is not downloaded again.
  `update.k3s.io` and `raw.githubusercontent.com` send no
  `Last-Modified` (checked 2026-10-11), so every request for them
  downloads the content again. `add_index()` then declines to add a
  version whose sha512 matches the last one (`artifact.py:663-690`).
  The blob it just fetched is left unreferenced, for a blob reaper that
  has its own leak (shakenfist#4440).
- *Upstream failure:* if the artifact already has a version and the
  source fails, whether by TLS error, connection error or a non-200
  during the freshness check, the cached version stays current
  (`operations/artifact_fetch_op.py:187-205`, Shaken Fist issue 3603;
  `images.py:188-193`). That is the stale-copy behaviour this plan
  wants, done server-side.
- *Transcode cost:* every `TYPE_IMAGE` fetch runs `transcode_image()`
  (`images.py:253-358`). A gzip file is decompressed. Anything else that
  is not already qcow2 is converted by qemu-img into a qcow2 in the
  fetching node's `image_cache` (`:332`), then archived as a second,
  replicated blob (`:337-350`, `operations/imgcache_op.py`). The
  version the artifact indexes is still the original blob, so reads
  return the right bytes. The cost is disk: a zstd tarball of 260 MB
  becomes another 260 MB or so of qcow2, stored
  `BLOB_REPLICATION_FACTOR` (default 2, `config.py:1020`) times.
- *The upload path does not transcode:* `upload_artifact(...,
  artifact_type='other')` (`external_api/artifact.py:553-720`,
  capability `artifact-upload-types`) skips this step. It needs the
  bytes on the client. The client's `blob_artifact()` takes no
  `artifact_type`.

**S3. Labels.** A label is a `TYPE_LABEL` artifact whose `source_url`
is `sf://label/<namespace>/<label>` (`artifact.py:38`, `external_api/label.py:30-47`).

- *Write:* `POST /label/<name>` with `blob_uuid` and an optional
  `max_versions` (`label.py:117-160`). On the client this is
  `update_label(label_name, blob_uuid)` (`apiclient.py:887`), which has
  no `max_versions` argument. A name is either bare, meaning the
  caller's own namespace, or `<namespace>/<label>`, and has at most one
  `/`. An existing label can be advanced only by its owner or by
  `system` (`label.py:143-147`). A new label can be created in another
  namespace only if that namespace trusts the caller (`:149-152`).
- *Read:* `GET /label/<name>` resolves by visibility (`label.py:172-190`).
  It returned a 500 in every release: both `v0.7.2` and `v0.8.0-rc4`
  pass a tuple to `url_filter`. `106c20523` (2026-08-06) fixed it on
  `develop`. sfcbr's build, `d49049d6f` (2026-10-09), contains the fix.
  client-python has no `get_label()`. The approach that works against
  every server is to call `get_artifacts()`, which lists everything
  visible to the caller (`external_api/artifact.py:411-420`), and keep
  the entry with `artifact_type == 'label'` and `source_url ==
  'sf://label/<ns>/<label>'`. Shaken Fist's image supply chain plan
  already selects this way. Its `blob_uuid` is the label's current
  version.

**S4. Visibility and trust.** The visibility test is
`namespace_or_shared_filter()` (`artifact.py:827-835`): `system` sees
everything, everyone sees a shared artifact, and otherwise a caller
sees an artifact whose owning namespace trusts the caller. Only `system` can
share an artifact (`external_api/artifact.py:494-499`, `:903-905`).

Conductor trusts each runner namespace from `ci-images`:
`add_namespace_trust('ci-images', 'sfcbr-<unique>')`
(private-ci `conductor/provisioner.py:2345-2347`). So anything in
`ci-images` is visible from a merge tier runner's namespace. A trust
grants sight and the right to *create* in the trusting namespace, but
not to update or delete there (Shaken Fist operator guide,
`authentication.md:459-497`). A runner can therefore gift a new label
to `ci-images`, but cannot move or remove one. Refreshing and reaping
need a credential that owns the cache namespace.

**S5. Versions.** An artifact keeps `max_versions` versions (default
3, `ARTIFACT_MAX_VERSIONS_DEFAULT`, `config.py:867`). `add_index()`
skips a version identical to the previous one and ends in
`delete_old_versions()` (`artifact.py:663-723`). A label's versions
are the history of what it has pointed at.

**S6. Checksums.** Shaken Fist records sha1, sha512 and xxh128 for a
blob (`external_api/blob.py:310-316`). It does not record sha256. k3s
publishes only sha256. So a sha256 check has to be computed either on
bytes the plugin streams with `get_blob_data()`, or in the guest with
`sha256sum`.

**S7. k3s air-gap mechanics.** These were checked against the
install.sh above, and against k3s source at `v1.36.5+k3s1` and at
`v1.21.1+k3s1`, the plugin's release floor (`cluster.py:362`).

- `INSTALL_K3S_SKIP_DOWNLOAD=true` skips the version lookup, the hash
  and the binary download (install.sh:280-284, `:751-757`). It requires
  an executable `${BIN_DIR}/k3s` (`:291-296`), and `BIN_DIR` defaults to
  `/usr/local/bin` (`:236`). Everything else install.sh does is
  unchanged: the systemd unit and its env file, the
  `kubectl`/`crictl`/`ctr` symlinks, and the killall and uninstall
  scripts. With download skipped, `INSTALL_K3S_CHANNEL` is read only
  for SELinux RPMs, which do not apply on Debian.
- Images go in `/var/lib/rancher/k3s/agent/images/`. Supported files
  are `.tar`, `.tar.lz4`, `.tar.bz2`/`.tbz`, `.tar.gz`/`.tgz` and
  `.tar.zst`/`.tzst` (wharfie `pkg/tarfile`, `SupportedExtensions`).
  Files whose names start with a dot are skipped. zstd is already
  supported at `v1.21.1+k3s1` (`pkg/agent/containerd/containerd.go:234`
  at that tag), and that release publishes a `.tar.zst`. Recent k3s
  watches the directory and imports supported files as they appear
  (`pkg/agent/containerd/watcher.go`). It imports again on every start
  unless a `.cache.json` exists there (`watcher.go:207-260`). Which
  release first shipped that cache was not checked.
- Each release publishes `k3s`, `k3s-arm64` and `k3s-armhf`;
  `k3s-airgap-images-{amd64,arm64,arm}.tar{,.gz,.zst}`;
  `sha256sum-{amd64,arm64,arm}.txt`, each covering that architecture's
  binary and all three tarball forms; and `k3s-images.txt`. The URL is
  `https://github.com/k3s-io/k3s/releases/download/<version, + as %2B>/<asset>`
  (install.sh:468, `:560`).
- install.sh can be fetched per tag from
  `raw.githubusercontent.com/k3s-io/k3s/<tag>/install.sh`, which
  existed for every tag checked. An `install.sh.sha256sum` sits beside
  it at `v1.30.14+k3s1` and `v1.36.5+k3s1`, but not at `v1.21.1+k3s1`.
  `get.k3s.io` serves `master`'s install.sh (its hash matched `master`
  on 2026-10-11), which differs from the tag's.

**S8. Sizes**, from `gh release view` on 2026-10-11:

| Release | `k3s` | `k3s-airgap-images-amd64.tar.zst` | `.tar.gz` |
|---------|-------|-----------------------------------|-----------|
| v1.36.5+k3s1 | 79.1 MB | 194.4 MB | 194.5 MB |
| v1.33.13+k3s2 | 80.6 MB | 263.3 MB | 263.2 MB |
| v1.30.14+k3s1 | 70.3 MB | 157.5 MB | 193.2 MB |

That comes to about 230-345 MB per release for amd64, and the install
script and checksum file add almost nothing. Fetched through the URL
path, the qcow2 transcode (S2) roughly doubles it, and replication
doubles it again. That is about 1-1.4 GB of Shaken Fist blob store per
cached release, before the per-hypervisor copies from S1. On sfcbr the
blob store is a live VM hot path, so this is not free.

**S9. What stays as it is.** The plugin sets no proxy for its nodes;
no `proxy` appears in `cluster.py`. The k3s configuration files are
written before the installer runs (`_k3s_config_commands()`,
`cluster.py:2998-3065`), and manifests are staged before it as well
(`:3116-3162`). An air-gapped install changes only where the installer
and its inputs come from, and not what happens around them.

**S10. Version selection, the release floor and the verbs.**
`create()` resolves the channel (`cluster.py:3672-3676`). Straight
after that, before anything is registered or built, it refuses any
release older than `K3S_RELEASE_FLOOR` (`check_k3s_release()`,
`:1885-1928`, called at `:3681`). The merge tier exercises the floor
with `--release-channel v1.20` (`tools/ci_deploy_test.sh:318`).
`query-k3s-version` (`__init__.py:274-295`) reads the same cache, and
`--refresh-version-cache` (`:169`, `:280`) forces a refresh.
`update-os` (`cluster.py:5144-5161`) runs apt and nothing else, so k3s
is untouched. `expand-workers` installs the `k3s_version` recorded at
create (`:3748`, used at `:3217`). A cached release therefore has to
stay available as long as any cluster built from it might grow.

### Downstream: 33fl's k3s CI runner plan

33fl's `docs/plans/PLAN-k3s-ci-runners.md` wants clusters it can
rebuild often. Its "Remaining blockers (2026-10-10)" are U1 (no
release carries the prerequisites), U2 (collection options; cleared by
#131), U3 (the collection has never run against a real API), U4 (the
payload is create-time only and there is no k3s upgrade, so "treat
the cluster as rebuildable"), U5 (#128, health probe output over
10 KiB), U6 (create is not resumable), and L1-L5 on its own side. None
of them is about internet access at install time, so **this plan is
not a prerequisite for that one**, and that one should not wait for
it.

The two plans support each other in three places:

- U4's "rebuild rather than upgrade" answer, and the weekly
  retire-and-rebuild habit, are cheaper and more predictable when a
  rebuild does not depend on `update.k3s.io`, `get.k3s.io` or GitHub,
  and when every rebuild of a pinned release installs the same bytes.
- Its phase 4 conductor scaler calls `expand-workers`, perhaps weeks
  after the cluster was built. With an air-gapped install, workers
  added later get the same bits as the originals. This plan's
  retention rule (decision 10) is what keeps those bits available.
- Its landmine 3, the DinD cold cache, is the pull-through registry
  problem. This plan leaves it alone (see the non-goals).

## Mission and problem statement

Let `create` and `expand-workers` install k3s without the internet.
The binary, k3s's system images and the install script come from
labelled artifacts in a Shaken Fist namespace. Shaken Fist fetches
them once with its own fetch-by-URL support, the plugin verifies them
against k3s's published sha256 sums, and the agent's put-blob
operation copies them onto each node. Every node of a cluster gets
byte-identical bits. On the way there, phase 1 gives the channel
lookup a copy of the channel map that the whole Shaken Fist cluster
can read, so that a fresh namespace (every CI run) does not depend on
`update.k3s.io` being up.

Non-goals:

- **Helm charts and container images stay online.** MetalLB and
  Longhorn come from `metallb.github.io` and `charts.longhorn.io`, and
  their images and workload images from their registries. Caching
  those is the pull-through registry problem, which 33fl's CI runner
  plan calls landmine 3, and it is a separate plan. The airgap tarball
  does cover k3s's own system images (row 5 of the table above), which
  takes docker.io's anonymous pull limits out of node bring-up.
- **OS packages stay online.** That covers `apt-get update` and
  `dist-upgrade` at create and in `update-os`, and extrepo and helm on
  the first server.
- **No upgrade verb.** Upgrading k3s on a running cluster needs a verb
  that does not exist (33fl U4). The cache would make one cheap to
  write later; see Future work.
- **amd64 only.** Shaken Fist's hypervisors are amd64.
- **A Shaken Fist cluster with no egress at all** is supported only
  because the read path looks at labels and nothing else. An operator
  can upload the files and label them by hand. The populate verb uses
  fetch-by-URL, which needs egress from Shaken Fist.

### Decisions

These are the planner's recommendations. The operator has confirmed
only decision 1. The others stand until the operator confirms or
changes them, and the open questions point at the ones still in
doubt.

1. **Guest delivery is `instance_put_blob()`, and this plan is
   plugin-only.** The operator settled this on 2026-10-12, and S1
   confirms it. No Shaken Fist server or agent change is required.
   Each file is put to a staging directory that the plugin creates
   first, because the agent does not create parent directories. Its
   sha256 is checked in the guest, and it is moved into place only if
   the check passes, because the agent writes in place and k3s imports
   whatever appears in its images directory. Modes are passed as
   integers. The final paths are those in S7: `/usr/local/bin/k3s` at
   `0o755`, and the tarball at `0o644` under
   `/var/lib/rancher/k3s/agent/images/`. The directories along that
   path are created with the modes k3s itself would use, for the
   reason given at `cluster.py:3123-3141` for the manifests directory.
   Phase 3 confirms what those modes are.
2. **Labels are how a create finds files, and URLs are how the cache
   is filled.** A label moves only after the plugin has checked the
   content: the channel map parses, or a release asset's sha256
   matches. A captive-portal page or a truncated download that Shaken
   Fist fetched therefore never becomes what a create installs. Labels
   also let an operator supply the files some other way, and they give
   each file a stable name scoped to a namespace.
3. **One label for each release and asset**, with no `/` or `+` in
   the name. For example: `k3s-channels`, `k3s-v1.36.5-k3s1-k3s`,
   `k3s-v1.36.5-k3s1-k3s-airgap-images-amd64.tar.zst`,
   `k3s-v1.36.5-k3s1-sha256sum-amd64.txt` and
   `k3s-v1.36.5-k3s1-install.sh`. A label's name is placed straight
   into a URL path by the client (`apiclient.py:889`), which does not
   quote it, so `+` is avoided rather than escaped. Phase 2 settles the
   exact scheme.
4. **The cache lives in one namespace, chosen by a
   `--cache-namespace` option** on `create`, `expand-workers` (which
   records it), `query-k3s-version` and the cache verbs, with a
   matching library parameter. It defaults to the caller's own
   namespace. The merge tier passes `ci-images`. Reading from an empty
   cache does nothing, and filling it is explicit (decision 6), so the
   default has no side effects.
5. **The channel map is read in this order:**
   1. the namespace cache, if fresh;
   2. the shared label, if fresh;
   3. upstream, whose answer is written to the namespace cache;
   4. if upstream cannot answer, the fresher of the two stale copies,
      with the groundwork's warning.

   A fresh shared copy means a CI run never contacts `update.k3s.io`
   at all. Only the refresh verb writes the shared label. Its age comes
   from label metadata the refresh writes, because a refresh that
   downloads identical content adds no version, so the blob's own
   `fetched_at` stops moving (S2, S5).
6. **Filling the cache is explicit, and done by the namespace's
   owner.** Phase 1 adds `sf-client k3s refresh-release-cache` and
   phase 2 adds `sf-client k3s cache-k3s-release <channel|release>`.
   `create` never fills the cache by default, for three reasons. A
   runner can create labels in a namespace that trusts it, but can
   never correct or remove them (S4). A fill is 300 MB or more of
   transfer, which should happen on a schedule rather than in the
   middle of somebody's create. And refreshing needs the owner's
   credential anyway. Open question 3 asks whether `create` should be
   allowed to opt in.
7. **Files are verified twice.** The populate verb streams each asset
   once with `get_blob_data()` and checks its sha256 before it labels
   the asset. Every node then checks its staged copy against the
   cached `sha256sum-amd64.txt` before moving it into place. The first
   check means nothing unverified is ever labelled. The second checks
   the bytes that will actually run. Both compare against a checksum
   file from the same GitHub release, just as install.sh's own check
   does today. So they protect against corruption, truncation and
   wrong content, but not against a compromised release.
8. **At install time the cache is preferred, and online install is
   the fallback.** When the resolved release's files are not all in
   the cache, `create` installs online, as today but with
   `INSTALL_K3S_VERSION`, and says so in written output.
   `--require-cached-k3s` refuses instead, before anything is built.
   Which path a create took is recorded in cluster metadata, so that
   `expand-workers` follows it and `show` reports it. Open question 2
   asks which of the two should be the default.
9. **Caching changes where the bits come from, not which release a
   channel means.** A channel still resolves to the latest release the
   channel map names, and that map is now cached for the whole Shaken
   Fist cluster. The floor check stays exactly where it is, before any
   cache lookup. `query-k3s-version` also reports whether the release
   is cached. An exact pin, `--k3s-version v1.33.4+k3s1`, skips channel
   resolution entirely. This is the first option #132 lists, and it is
   how a caller who wants the same bits every time gets them. Open
   question 7 records the alternative, "the newest cached release on
   this channel", and why it is not recommended.
10. **Retention is explicit and done by the owner.** A
    `prune-release-cache` verb deletes the labels and URL artifacts of
    releases that are no longer kept. It keeps the latest release of
    every channel in the cached map, every release recorded as
    `k3s_version` by a cluster in a namespace the caller can see, and
    the newest N releases of each minor version. Shaken Fist's
    reference counting frees the blobs, subject to shakenfist#4440.
    Open question 6 asks for N and for who runs it.
11. **Fetching goes through Shaken Fist's URL path, as proposed, and
    the transcode cost is accepted for now.** The upstream fix is to
    let `POST /artifacts` fetch an artifact of type `other` without
    transcoding it. The plan files that, and adopts it behind a
    capability check once it exists. The alternative, downloading on
    the client and uploading as `other`, avoids the transcode but
    moves the transfer onto the client. Open question 5 has the
    trade-off.
12. **The install script is cached per k3s release**, from the tag's
    install.sh, and checked against the tag's `install.sh.sha256sum`
    where one is published. Pinning the installer to the release it
    installs keeps the two consistent. The cost is that a fix to
    install.sh on `master` reaches only releases tagged after it. Open
    question 4.

## Open questions

1. ~~**Who refreshes and fills the shared cache on sfcbr?**~~
   **Decided 2026-10-12: conductor's nightly image build in
   private-ci.** It already publishes into `ci-images` with a
   credential that owns it, which a runner's namespace cannot do (S4).
   The wiring is private-ci work, not this plan's. The refresh and
   cache-fill commands must therefore be callable from the library, not
   only from the command line, and they must be safe to run nightly
   with nothing to do. Without the scheduled refresh, the shared
   channel map just gets older; the stale fallback keeps creates
   working, but with a release that is not the newest.
2. **Should the default be to prefer the cache, or to require it?**
   The recommendation is to prefer it, so that an empty cache behaves
   exactly as today, with `--require-cached-k3s` for callers who want
   a guarantee. The merge tier should run with that flag once the
   cache is filled.
3. **Should `create` be able to fill a missing release itself?** The
   recommendation is no, at least until phase 2's cost measurements
   are in (decision 6). If yes, it should be opt-in, and it creates
   labels in a trusting namespace that the creator cannot correct
   later.
4. **Where should the install script come from: the tag's install.sh
   or `get.k3s.io`?** The recommendation is the tag's (decision 12).
   The tag at the floor release has no `install.sh.sha256sum`, so for
   old releases the only check is a hash recorded at fill time.
5. **How should the transcode overhead (S2) be handled?** There are
   three options: accept it; ask Shaken Fist to fetch `other`
   artifacts without transcoding; or download on the client and
   upload as `other`. The recommendation is to accept it in phase 2,
   measure it, and file the upstream issue straight away. Moving to
   the upload path is worthwhile only if the measured cost hurts.
6. **What should the retention policy be, and who prunes?** The
   recommendation is N = 2 releases per minor version, with pruning
   run by the same job that fills the cache (decision 10).
7. **Should version selection fall back to "the newest cached release
   on this channel"?** The channel map gives only each channel's
   *latest* release, not which releases belong to it. "Newest cached"
   is only well-defined for minor channels such as `v1.33`, and for
   `stable` or `latest` it would need channel membership recorded at
   fill time. The recommendation is not by default (decision 9). It
   may be worth having as an explicit offline option if decision 8's
   online fallback proves too weak.
8. **Which tarball format, and should k3s keep it on the node?** The
   recommendation is `.tar.zst`: it is the smallest, and the floor
   release supports it (S7). Phase 3 checks whether to create
   `.cache.json`, so that k3s does not import the tarball again on
   every restart. That re-import costs CPU on a two vCPU node, and
   cumulative health's `NRestarts` would show it. Deleting the tarball
   after the first start is not recommended. k3s clears its pins on
   images at every start and pins only images whose tarball is still
   present (`containerd.go:153-157`, `clearLabels()` at `:228`), so after a restart
   kubelet's image garbage collection could remove an unpinned system
   image, and the node would then pull it from the internet again.
9. **How fast is put-blob, and is the 600 s default deadline enough
   for a 260 MB tarball** sent to several nodes at once? Nothing found
   records put-blob throughput. Phase 2 measures it on sfcbr. If the
   default is too short, the plugin needs a client-python release
   containing `0b39248` and a higher dependency floor, so that it can
   pass `deadline_seconds=0` with a progress timeout.

## Execution

Each phase gets its own plan file when it is planned, under the name
given in its row. None has been written yet. The recommended planning
effort is high for phases 1, 2, 3 and 5, because they involve cache
precedence and trust, agent operation wait loops, and assembly
ordering. It is medium for phase 4, which follows patterns the earlier
phases set, and high for the push audit.

| Phase | Plan | Status | Merged |
|-------|------|--------|--------|
| 1. Shared channel cache | `PLAN-airgap-install-phase-01-shared-channel-cache.md` (not yet written) -- `refresh-release-cache` runs `cache_artifact(K3S_CHANNELS_URL, namespace=<cache>)`, waits for the fetch (the wait mechanism is decided in the phase plan; see S2), parses the blob with the groundwork's parser, and only then advances `<cache>/k3s-channels` and writes `fetched_at` metadata. `get_k3s_release()` reads in decision 5's order. `--cache-namespace` is added on `create`, `query-k3s-version` and the library. The merge tier passes `--cache-namespace ci-images`. Docs. Unit tests cover the read order and the case where the label is missing or unreadable. A live check is needed for the trust visibility from a runner namespace. Shippable on its own, because it fixes the cold cache in CI. | Not started | |
| 2. Release artifact cache | `PLAN-airgap-install-phase-02-release-artifact-cache.md` (not yet written) -- `cache-k3s-release <channel|release>` resolves the release and applies the floor check. It then fetches the binary, the `.tar.zst`, `sha256sum-amd64.txt` and the tag's install.sh by URL into the cache namespace, streams each one to check its sha256 (decision 7), and labels it (decision 3). A lookup helper returns a release's blobs, or nothing. `query-k3s-version` reports whether a release is cached. Measures fill time, transcode and disk cost (open question 5) and put-blob throughput (open question 9). No change to installs yet. | Not started | |
| 3. Air-gapped install | `PLAN-airgap-install-phase-03-airgapped-install.md` (not yet written) -- In `install_control_plane()` and `install_k3s_component()`, when the release is cached: create the staging and k3s directories; put each file onto every target node in parallel through a new wait helper modelled on `await_fetch()`; check sha256 in the guest; `mv` into place; run the staged installer with `INSTALL_K3S_SKIP_DOWNLOAD=true` and today's `K3S_URL`, `K3S_TOKEN` and role. Otherwise fall back as decision 8 describes. Adds `--require-cached-k3s` and `--k3s-version`, and records the install source in metadata for `expand-workers` and `show`. Settles open question 8. | Not started | |
| 4. Retention, reporting and the collection | `PLAN-airgap-install-phase-04-retention-and-collection.md` (not yet written) -- `prune-release-cache` (decision 10); listing what is cached; `sf_k3s_cluster` options for the cache namespace, the requirement flag and the pin; `docs/usage.md`, `docs/library-api.md`, `docs/collection.md`, and the create flow in `ARCHITECTURE.md`. | Not started | |
| 5. Live validation | `PLAN-airgap-install-phase-05-live-validation.md` (not yet written) -- The merge tier creates its main cluster from a filled `ci-images` cache with `--require-cached-k3s`. It asserts that each node's installer logged `Skipping k3s download and verify`, that every node's `/usr/local/bin/k3s` has the published sha256, and that the system images were imported rather than pulled. A run in a namespace the runner owns fills, labels and prunes a release, because a runner cannot remove what it creates in `ci-images` (S4). Strict mode with a release that is not cached refuses before anything is built. | Not started | |
| 6. Push audit | - | Not started | |

<!-- shared-block: plan-push-audit-phase v3 -->
Push audit phase (shared block; do not edit -- the canonical
copy lives in shakenfist/development at
`templates/shared-blocks/plan-push-audit-phase.md`):

- Every master plan ends with a phase that runs the repository's
  `PUSH-AUDIT.md` over the whole plan's work. It is the last row of
  the Execution table and it is not optional. The rule binds every
  plan that carries the phase, which is decidable from the plan file
  alone: a plan that is already `Complete`, `Abandoned` or
  `Superseded` and does not carry the phase is not reopened to
  acquire one, and a plan that has the phase runs it even if it
  reaches `Complete` before the phase does.
- That phase audits the accumulated diff of every phase in the plan
  against the default branch, not the diff of the last phase alone.
  Auditing one phase at a time would miss what the phases did to
  each other -- the duplicated helper that only exists once phases
  three and six have both landed, the doc page that phase two made
  wrong and phase five never revisited.
- Once the plan's phases have merged, a diff against the default
  branch is empty and would read as a clean audit. The range is not
  reliably derivable after the fact either: unrelated work lands on
  the default branch between phases, so anything anchored on "since
  the plan file appeared" is far too wide. It has to be recorded. As
  each phase lands, what put it on the default branch goes into the
  plan: the merge commit of its pull request, whose diff against its
  first parent is the whole of what landed, or -- where the phase
  landed directly -- every commit of the phase, or its `first..last`
  range. A single commit is only ever enough when it is a merge
  commit.
- Where the Execution phases are a table, that record is a `Merged`
  column, added last so that a row which omits it still reaches
  `Status`; where they are prose sections it is a `Merged:` line in
  the phase's own section. The `Status` column keeps its single
  vocabulary term and nothing else (see `plan-status-vocabulary`).
  A phase that landed in another repository records `<repo> <sha>
  (#pr)` and is audited against that repository's default branch, as
  part of the pull request that lands it; the plan's own push-audit
  phase cites that audit rather than re-running it.
- Phases that landed before the plan started recording them are
  reconstructed rather than left blank. Recover what you can from
  `gh pr list --state merged` and `git rev-list --first-parent`, and
  say in the plan that the range was reconstructed. Do not trust a
  path-filtered `git log` on its own: it lists the commits that
  touched a path without saying which arrived directly and which
  arrived inside a pull request, and recording a commit that came in
  under a merge audits one commit of that pull request rather than
  the pull request. A reconstructed record may be a summary table in
  the audit phase's own section rather than a column or a line in
  the Execution table, which keeps retrospective archaeology out of
  a table that tracks live status. Where a phase accreted over
  months of unrelated commits and no range is recoverable, say that
  instead and name the paths the audit read -- an audit that says
  what it could not scope is a result; one that silently audits
  nothing is not.
- Findings land as their own pull request against the default
  branch, and the plan is not complete until they are resolved or
  explicitly declined in writing. A finding that is declined says
  why, in the plan, where the next reader will find it.
- Where the audit finds nothing, record that in the plan in one
  sentence. It is a real result, and a run of them is the evidence
  for making the phase conditional rather than mandatory.
- A repository with no `PUSH-AUDIT.md` still carries the phase, and
  the phase says that the runbook does not exist yet and what was
  done instead. Silently omitting it is what let the audit go
  untriggered for as long as it did.
<!-- shared-block-end -->

!!! note "In this project"

    The Execution phases are a table, so the record goes in a
    `Merged` column after `Status`. Phases here land as pull
    requests against `develop`, so the cell normally holds the
    single merge commit of that pull request.

<!-- shared-block: plan-phase-landing v1 -->
Phase landing (shared block; do not edit -- the canonical copy
lives in shakenfist/development at
`templates/shared-blocks/plan-phase-landing.md`):

A plan's status and a repository's review state both live in files
that every branch would otherwise rewrite. Left alone, that turns
each of them into a merge-conflict hot spot, and it spends a pull
request and a full CI run on a change that is entirely prose.
Three rules keep them out of the way.

- **A phase is closed out in the first commit of the next phase,
  not in a pull request of its own.** By the time the next phase
  branches, the previous one has merged, so its merge commit is
  known and its `Merged` cell can record the thing the push-audit
  phase actually needs. This is the only ordering that works: a
  phase cannot record its own merge commit, and a separate
  close-out pull request buys that record at the price of a round
  trip. The close-out sets the finished phase's `Status` and
  `Merged` cells and the plan's row in `docs/plans/index.md`, and
  it is committed before the next phase's own work, so that the
  branch never claims the plan is further along than the default
  branch is.

- **The last phase closes itself out.** The push-audit phase is
  the last row of every plan, so no next phase will carry its
  close-out. Where the audit raises findings, the plan is not
  complete until they are resolved or declined, and those land as
  their own pull request after the audit phase has merged -- so
  that pull request is the carrier, and it can record the audit
  phase's merge commit, which by then is known. Where the audit
  finds nothing there is no carrier, and no follow-up pull
  request is opened for the sake of one cell: the phase sets its
  own `Status`, and the plan's index row, to `Complete` in its
  own pull request, and records no `Merged` cell. It is the only
  row permitted to omit one. The column exists so that the
  push-audit phase can reconstruct what to audit; the audit phase
  is last, so nothing ever reads its own row.

- **`REVIEWS.md` is not pruned or regenerated in a pull request
  that changes code or documentation.** Editing a reviewed file
  stales its mark, and adding or removing an in-scope file moves
  the header count, but neither is the landing pull request's
  business. `prune` regenerates the file whether or not it dropped
  anything, so the `prune-reviews` workflow heals both on the next
  push to the default branch. Pruning from a branch is also wrong
  more often than it is right, though not for the reason it first
  appears: `prune` compares each stamp against `HEAD`, which on a
  branch is the branch tip, so it drops the marks for the files the
  pull request itself touched while keeping marks the default
  branch has already pruned. Committing that state merges a review
  file computed from a stale tree, and can resurrect marks
  `prune-reviews` has already removed. Accumulated staleness is
  reported by the `review-coverage` audit, which recomputes
  coverage against `HEAD` and raises an issue once the backlog is
  worth a review session.

  **A review session is the exception**, and it is not optional
  tidiness: `stamp` regenerates `REVIEWS.md` as well as writing the
  marks, and the rows, the sidecars and the marks are committed
  together (see `docs/code-review-tracking.md`). Where a repository
  requires a pull request to reach its default branch, that is how
  a review session lands, so "not in a pull request" is about the
  kind of change, not the mechanism.

These rules assume phases land one after another. Where two phase
branches are open at once, each closes out only the phase it
directly follows.
<!-- shared-block-end -->

!!! note "In this project"

    This repository does not deploy review tracking: there is no
    `REVIEWS.md` and no `prune-reviews` workflow, so the third rule
    has nothing to act on here. The first two apply as written.

<!-- shared-block: plan-status-vocabulary v1 -->
Plan status vocabulary (shared block; do not edit -- the canonical
copy lives in shakenfist/development at
`templates/shared-blocks/plan-status-vocabulary.md`):

A status cell -- in the master plan's own Execution phase table, and
in the row `docs/plans/index.md` carries for the plan -- holds
exactly one of these terms and nothing else:

- `Proposed` -- written down as a concept, not yet scheduled.
- `Not started` -- scheduled, but no work has begun.
- `In progress` -- work has begun and has not finished.
- `Blocked` -- cannot proceed until something outside the plan
  changes. Say what, in the plan.
- `Complete` -- the work is done.
- `Abandoned` -- deliberately dropped without being done.
- `Superseded` -- replaced by another plan, which the plan names.

The term is the whole cell. No dates, no phase arithmetic, no
parenthetical qualifiers, no summary of what happened: a status is
read to decide whether a plan still wants attention, and prose in
that column has repeatedly grown until it could no longer be read
either by a person scanning the table or by tooling. Detail belongs
in the plan file, and a one-line summary belongs in the index's own
Intent column.

Matching is case-insensitive, so `In Progress` is accepted, but the
spelling above is the one to write.
<!-- shared-block-end -->

## Agent guidance

### Execution model

<!-- shared-block: subagent-execution-model v1 -->
Sub-agent execution model (shared block; do not edit -- the
canonical copy lives in shakenfist/development at
`templates/shared-blocks/subagent-execution-model.md`):

All implementation work is done by sub-agents, never in the
management session. The management session is reserved for
planning, review, and decision-making. This keeps the management
context lean and avoids drowning it in implementation diffs.

The workflow is:

1. **Plan** at high effort in the management session.
2. **Spawn a sub-agent** for each implementation step with the
   brief from the plan, at the recommended effort level and model.
3. **Review** the sub-agent's output in the management session.
   Check the actual files -- the sub-agent's summary describes
   what it intended, not necessarily what it did.
4. **Fix or retry** if the output is wrong. Diagnose whether the
   brief was insufficient (improve it) or the model was too light
   (upgrade it), then re-run.
5. **Commit** once the management session is satisfied.

This applies to all steps, including high-effort ones. If a
sub-agent cannot succeed even with a detailed brief and the right
model, that is a signal the brief needs improving, not that the
management session should do the implementation itself.

Use `isolation: "worktree"` for sub-agents when the change is
risky or experimental; the worktree is discarded if the output is
unsatisfactory. For safe, well-understood changes, sub-agents can
work directly in the main tree.
<!-- shared-block-end -->

### Planning effort

<!-- shared-block: plan-planning-effort v1 -->
Planning effort (shared block; do not edit -- the canonical copy
lives in shakenfist/development at
`templates/shared-blocks/plan-planning-effort.md`):

The master plan itself is always created at **high effort** -- it
requires broad codebase understanding, cross-referencing several
source files, and judgment calls about scope and sequencing.

Each phase plan states the recommended effort level for planning
that phase. Phases that turn on design decisions, cross-component
coordination, protocol changes, or subtle correctness questions
should be planned at high effort. Phases that are mechanical, or
that follow a pattern already established elsewhere in the
codebase, can be planned at medium effort.
<!-- shared-block-end -->

!!! note "In this project"

    Phases involving cluster assembly ordering, agent operation
    wait loops, or the namespace-metadata representation of
    cluster state should be planned at high effort -- these are
    the places where a mistake only shows up against a live
    Shaken Fist cluster. Phases that mirror an already
    established pattern -- adding a Click subcommand alongside
    an existing one, for example -- can be planned at medium
    effort.

### Step-level guidance

<!-- shared-block: subagent-step-guidance v1 -->
Sub-agent step guidance (shared block; do not edit -- the
canonical copy lives in shakenfist/development at
`templates/shared-blocks/subagent-step-guidance.md`):

Each phase plan includes a table like this:

| Step | Effort | Model | Isolation | Brief for sub-agent |
|------|--------|-------|-----------|---------------------|
| 1a | medium | sonnet | none | One-sentence summary of what to do and which files to touch |
| 1b | high | opus | worktree | Why this needs high effort: requires understanding X to do Y |

**Effort levels**, from cheapest to most thorough:

- **low** -- Purely mechanical changes: rename, reformat, add a
  log line, regenerate generated code. The brief is a complete
  instruction.
- **medium** -- The plan provides enough context to follow a clear
  brief. The sub-agent may read a few files, but the approach is
  already decided.
- **high** -- Requires reading several files, making judgment
  calls, or understanding non-obvious invariants. The sub-agent
  needs to think about edge cases.
- **xhigh** -- The setting for hard coding and agentic steps:
  long-horizon changes, or steps where the sub-agent must both
  research and implement.
- **max** -- Correctness matters more than cost. Expect
  diminishing returns and occasional overthinking; reserve it for
  steps where a wrong answer would be expensive to detect.

**Brief for sub-agent:** this is the key field. Write it as if
briefing a colleague who has never seen the codebase. Include what
to change, which files to touch, what patterns to follow, and any
non-obvious constraints.

A good brief front-loads the research the planner already did, so
the implementing agent does not repeat it. Instead of "add storage
functions for the new object", name the functions to add, the file
they belong in, the existing equivalent to mirror (with line
numbers), and any registration the change also needs.

The better the brief, the lower the effort level needed and the
lighter the model that can succeed.
<!-- shared-block-end -->

!!! note "In this project"

    A brief should say explicitly whether a step can be verified
    by unit tests alone or needs a live cluster, because the
    sub-agent cannot reach one and should not pretend otherwise.

    A worked brief for this codebase: instead of "improve
    progress reporting for node creation", write "extend the
    progress reporter in `shakenfist_client_k3s/progress.py` to
    emit a per-node phase label, keeping both existing output
    modes working. Call it from the node creation path in
    `shakenfist_client_k3s/primitives.py`. Add coverage to
    `shakenfist_client_k3s/tests/test_progress.py` in the
    existing testtools/stestr style, with the Shaken Fist API
    mocked."

### Model choice

<!-- shared-block: subagent-model-roster v1 -->
Sub-agent model roster (shared block; do not edit -- the canonical
copy lives in shakenfist/development at
`templates/shared-blocks/subagent-model-roster.md`):

The planner recommends which model is best suited to each step.
This is a judgment call, not a rigid rule -- the right model
depends on what the step requires, not on whether it is "planning"
or "implementation". The models available to sub-agents are:

- **fable** -- The most capable model available, for the hardest
  reasoning and the longest-horizon work: multi-step changes a
  single sub-agent must carry end to end, or steps whose
  correctness depends on holding a whole subsystem in mind at
  once. It costs materially more than opus, so reserve it for
  steps that have already defeated opus or are expected to.
- **opus** -- The default for steps needing deep reasoning,
  architectural understanding, subtle correctness judgment
  (locking, state machines, migrations), or intricate
  implementation that would be costly to debug if it were wrong.
- **sonnet** -- A good default for well-briefed implementation
  work. Faster and cheaper than opus, and effective when the plan
  front-loads the research and the brief leaves no broad judgment
  calls to make.
- **haiku** -- Suitable for purely mechanical tasks:
  search-and-replace, regenerating generated code, adding log
  lines, running commands. The brief must be a near-complete
  instruction.

Model choice interacts with effort level and brief quality. A
detailed brief compensates for a lighter model -- sonnet at medium
effort with a thorough brief often matches opus at medium effort
with a vague brief. The planner's job is to write briefs good
enough that the recommended model can succeed.

The model also determines the context window: fable, opus and
sonnet have 1M tokens, haiku has 200K. A step that must hold many
files in context at once may need one of the larger-context models
for that reason alone, even when the reasoning itself is
straightforward.

**When in doubt, skew to the more capable model.** Saving money
only matters if the outcome is still acceptable. A failed or
low-quality implementation wastes more time -- and therefore more
money -- than the heavier model would have cost. Recommend a
lighter model only when you are confident the brief is detailed
enough for it to succeed.
<!-- shared-block-end -->

### Management session review checklist

<!-- shared-block: plan-review-checklist v1 -->
Management session review checklist (shared block; do not edit --
the canonical copy lives in shakenfist/development at
`templates/shared-blocks/plan-review-checklist.md`):

After a sub-agent completes, the management session verifies:

- [ ] The files that were supposed to change actually changed --
      read them, do not trust the summary.
- [ ] No unrelated files were modified.
- [ ] The changes match the intent of the brief: not merely
      syntactically correct, but semantically right.
- [ ] The project's own pre-merge checks pass, including any
      generated code that has to be regenerated and committed
      (see the project-specific checks below).
- [ ] The commit message follows project conventions, including
      the `Co-Authored-By` line recording model, context window,
      and effort level.
<!-- shared-block-end -->

!!! note "In this project"

    The project-specific checks referred to above are:

    - [ ] The code passes `tox -epy3`, `tox -eflake8` and
          `pre-commit run --all-files`.
    - [ ] The plugin still imports cleanly (`python3 -c 'import
          shakenfist_client_k3s'`) -- a broken import takes the
          whole `sf-client` CLI down.

## Administration and logistics

### Success criteria

We will know when this plan has been successfully implemented
because the following statements will be true:

* The code passes `tox -epy3`, `tox -eflake8` and
  `pre-commit run --all-files`.
* New code is compatible with Python >= 3.7 and the plugin still
  imports cleanly (`python3 -c 'import shakenfist_client_k3s'`) --
  a broken import takes the whole `sf-client` CLI down.
* There are unit tests for new parsing, error-handling and
  progress-reporting behaviour, in the existing testtools/stestr
  style with external APIs mocked.
* Lines are wrapped at 120 characters, single quotes for strings,
  double quotes for docstrings, no triple single quotes.
* Behaviour which can only be validated against a live Shaken
  Fist cluster has been exercised there (manually or via the
  functional CI) before merge.
* `ARCHITECTURE.md`, `README.md`, and `AGENTS.md` have been
  updated if the change adds or modifies modules or CLI commands.
* A merge tier run in a fresh namespace resolves its channel from the
  `ci-images` copy without contacting `update.k3s.io`, once phase 1
  has landed and the copy is fresh.
* A create from a filled cache makes no request to `update.k3s.io`,
  `get.k3s.io` or `github.com`, from the client or from any node. Every
  node's `/usr/local/bin/k3s` has the sha256 the release publishes, and
  so does every worker that `expand-workers` adds later.
* With the cache empty, a create behaves exactly as it did before this
  plan, apart from passing `INSTALL_K3S_VERSION` rather than
  `INSTALL_K3S_CHANNEL`.

!!! note "In this project"

    The close-out sections below apply as written, with one
    addition: when scanning the issue tracker for related bugs,
    remember that issues for this plugin sometimes belong
    upstream (for example `shakenfist/shakenfist` or
    `shakenfist/agent-python`). Reference cross-repository
    issues explicitly.

<!-- shared-block: plan-closeout-sections v1 -->
Plan close-out sections (shared block; do not edit -- the
canonical copy lives in shakenfist/development at
`templates/shared-blocks/plan-closeout-sections.md`):

### Future work

We should list obvious extensions, known issues, unrelated bugs we
encountered, and anything else we should one day do but have
chosen to defer to here, so that we do not forget them.

- **Helm charts through the same labels.** `helm install` can take a
  local chart `.tgz`, which put-blob can stage. That would remove
  `metallb.github.io` and `charts.longhorn.io` from create. The images
  those charts pull would still come from the internet.
- **The Longhorn version lookup** (`primitives.py:60`) has the same
  per-namespace cold cache as the k3s lookup. Phase 1's mechanism
  could carry its chart index too. #120, which changes how Longhorn
  versions are chosen, is open, so this waits for it.
- **A pull-through registry** for workload and chart images: 33fl's
  landmine 3, through k3s's `registries.yaml` mirrors. It needs a plan
  of its own.
- **helm itself** could come from a cached `get.helm.sh` tarball
  rather than extrepo and apt (`cluster.py:3168-3171`).
- **A k3s upgrade verb.** With releases cached, an in-place upgrade is
  a put-blob, a binary swap and a restart for each node (33fl U4).
- **arm64.** The assets exist (S7). Shaken Fist hypervisors would have
  to exist too.
- **Upstream, for Shaken Fist and client-python** (none of it is
  required by this plan):
  - fetching an `other` artifact by URL without transcoding it
    (decision 11);
  - sha256 among the blob hashes, which would let the populate verb
    check sha256 without streaming the blob (S6);
  - client methods for `GET /label/<name>` and
    `GET /artifacts/<ref>/outstanding` (S2, S3);
  - blobs can be read and put into an instance by UUID with no
    namespace check (S1). This was observed while planning and has not
    been filed.

### Bugs fixed during this work

This section should list any bugs we encounter during development
that we fixed. You should also scan the project's issue tracker,
where one exists, for directly related issues that we should
either resolve as part of this master plan or at least be aware of
while planning it.

None fixed yet. Found while planning, and worth fixing before this
plan starts, as a pull request of its own:

- **Nodes are given the resolved release as `INSTALL_K3S_CHANNEL`**
  (`cluster.py:3165-3167`, `:3213-3220`; see "A latent dependency"
  above). Every node install asks `update.k3s.io` for a channel that
  does not exist and works by accident. The fix is to pass
  `INSTALL_K3S_VERSION`. Being fixed ahead of this plan, in a pull
  request of its own (branch `install-k3s-version`).

Related issues:

- [#132](https://github.com/shakenfist/client-python-k3s/issues/132)
  (open): `update.k3s.io` served a self-signed certificate, and merge
  CI failed. Phase 1 answers the caveat at the end of its second
  option, that the stale fallback "only helps where a cache exists,
  which a fresh CI namespace may not have". Decision 9's
  `--k3s-version` is its first option.
- #140 (open): the stale-copy fallback this plan builds on, under
  "Groundwork in flight".
- #134 (open): stop the unit tests reaching the network. #135's unit
  tests failed during the 2026-10-11 outage because they had not yet
  stopped.
- #97 (fixed): the same class of problem for the Longhorn lookup, a
  shared-NAT rate limit on the GitHub API.
- #120 (open): Longhorn version selection. See Future work.
- shakenfist#4440: blob reaper leak. It decides how quickly pruned
  releases actually free disk (decision 10).

### Back brief

Before executing any step of this plan, please back brief the
operator as to your understanding of the plan and how the work you
intend to do aligns with that plan.
<!-- shared-block-end -->
