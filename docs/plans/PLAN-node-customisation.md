# Node customisation: per-role sizing and k3s configuration pass-through

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

<!-- shared-block: plan-file-conventions v1 -->
Plan file conventions (shared block; do not edit -- the canonical
copy lives in shakenfist/development at
`templates/shared-blocks/plan-file-conventions.md`):

- All planning documents live in `docs/plans/`.
- Detailed planning gets one plan file per phase. Phase files are
  named for their master plan, sit in the same directory as it,
  and append `-phase-NN-descriptive` before the `.md` extension.
- The master plan tracks its phases in a table under its Execution
  section:

  | Phase | Plan | Status |
  |-------|------|--------|
  | 1. Schema migration | PLAN-thing-phase-01-schema.md | Not started |
  | 2. Public API | PLAN-thing-phase-02-api.md | Not started |

- One commit per logical change, and at minimum one commit per
  phase. Unrelated changes are not batched into a single commit.
  Each commit is self-contained: it builds, passes tests, and has
  a message explaining what changed and why.
<!-- shared-block-end -->

## Situation

`sf-client k3s create` builds every node identically and gives the
caller almost no say in how k3s is configured. That was fine for the
workloads it was written for, but it rules out anything heavier. The
motivating case is a prototype OpenStack-Helm deployment on top of a
plugin-built cluster, in `homelab-deployments-lfs` (private), as the
first step towards a Kerbside deployment path that is not
kolla-ansible. The same limits would bite any other substantial
workload.

What the code does today:

* **Node sizing is hardcoded.** `Cluster.create_instance()`
  (`shakenfist_client_k3s/cluster.py:416-442`) creates every node,
  control plane or worker, with 2 vCPUs, 2048 MB of RAM and one 50 GB
  disk based on `BASE_OS_VERSION` (`cluster.py:51`, `debian:12`).
  `create_instance()` takes no arguments and does not know which role
  it is building for; the role is known only one frame up, in
  `create_and_await_instances(count, node_type)` (`cluster.py:668`).
  OpenStack-Helm's control services alone want something like 16-24 GB,
  and a compute node needs room for guests on top of libvirt and Open
  vSwitch.
* **k3s configuration is fixed.** The first control plane node gets an
  `/etc/rancher/k3s/config.yaml` with exactly three keys:
  `write-kubeconfig-mode`, `tls-san` (the floating API address) and
  `cluster-init` (`cluster.py:949-956`). Additional control plane nodes
  and workers are installed by `install_k3s_component()`
  (`cluster.py:1038-1065`) with no config file at all, just
  `INSTALL_K3S_CHANNEL`, `K3S_URL` and `K3S_TOKEN` in the installer
  environment. So there is no way to disable a packaged component
  (Traefik, servicelb), apply node labels or taints at join, change
  the cluster or service CIDRs, or pass kubelet arguments. The only
  payload hook is `--manifest` (phase 3 of the library API plan),
  which stages static manifests and cannot change how k3s itself
  runs.
* **Cluster state lives in namespace metadata**
  (`orchestrated_k3s_cluster_<name>`, via `get_metadata()` /
  `set_metadata()`, `cluster.py:293-316`). `expand_workers()`
  (`cluster.py:1944`) builds new workers from that state, so anything
  that should apply to later workers must be recorded there rather
  than only passed to `create()`.
* **A latent bug:** `install_k3s_component()` runs a bare
  `sudo apt-get install -y` with no package named (`cluster.py:1052`).
  apt treats that as a successful no-op, so it has gone unnoticed.

What OpenStack-Helm needs from the cluster, confirmed against
openstack-helm master at `a606f21` (2026-09-24):

* Node labels `openstack-control-plane=enabled`,
  `openstack-compute-node=enabled` and `openvswitch=enabled`
  (`doc/source/install/prerequisites.rst:284-298`).
* No Ingress controller. Charts no longer ship Ingress templates; the
  documented way in is Gateway API with Envoy Gateway on a MetalLB
  address (`doc/source/install/openstack.rst:7-60`). k3s's bundled
  Traefik is therefore dead weight at best.
* MetalLB, which the plugin already installs. k3s's own servicelb
  (klipper-lb) also claims `LoadBalancer` services, and the plugin
  does not disable it today. Both run at once -- verified on a live
  cluster on 2026-10-03, see open question 1.

**A second consumer arrived on 2026-10-03.** 33fl's
`docs/plans/PLAN-k3s-ci-runners.md` migrates the GitHub Actions static
runner pool onto a plugin-built k3s cluster running ARC, and its
phase 2 cannot start until per-role sizing exists: health-testing the
candidate host cluster `runners.static-ci` found the hardcoded
2 vCPU / 2048 MB is wrong for both roles, and that no amount of
configuration fixes it from outside the plugin. That plan records this
one as its blocking prerequisite 8, with `--disable` as 9 and control
plane tainting as 11. Its measurements are cited below where they
answer questions this plan had left open. The practical effect is that
phase 1 is now on two critical paths rather than one.

This plan is independent of the in-progress
[library API plan](PLAN-library-api-and-collection.md), but touches the
same code. Its phase 4 (first PyPI release) completed on 2026-10-03
and `shakenfist_client_k3s` 0.1.0 is on PyPI. Its phase 5 (the
`shakenfist.k3s` Ansible collection) merged as #90 before this plan's
options existed: phase 5's plan decided not to wait for this one, on
the grounds that every option here is a new *optional* module
parameter and so an additive change to a published argument spec. It
also records that 33fl cannot adopt the collection for CI runners
until this plan lands. See open question 2, and phase 5's decision 8.

## Mission and problem statement

Let the caller size control plane and worker nodes independently, and
pass arbitrary k3s configuration to server and agent nodes, through
both the CLI and the `Cluster` library API. Record both in cluster
metadata so that `expand-workers` builds new workers the same way as
the originals. Taint control plane nodes by default, so that the
scheduler cannot put a workload on the node holding etcd and the
apiserver.

Non-goals, deferred to Future work: choosing the base OS image,
attaching additional NICs or disks, and per-invocation sizing
overrides on `expand-workers`.

### Decisions

These were settled with the operator on 2026-09-27, before the plan
was written.

1. **k3s configuration is a generic YAML pass-through, not a set of
   curated flags.** The CLI gains `--server-config PATH` and
   `--agent-config PATH`; `Cluster.create()` gains matching
   `server_config` and `agent_config` parameters that take a mapping.
   This covers `disable`, `node-label`, `node-taint`, `cluster-cidr`,
   `service-cidr`, `flannel-backend`, `kubelet-arg` and anything else
   k3s grows, without a new flag each time. The cost is weaker
   validation, which the plugin partly recovers by rejecting keys it
   owns (decision 3).
2. **Sizing is per role.** The CLI gains
   `--control-plane-cpus`, `--control-plane-memory`,
   `--control-plane-disk`, `--worker-cpus`, `--worker-memory` and
   `--worker-disk`. Memory is in MB and disk in GB, matching the
   Shaken Fist API. The defaults stay at 2 / 2048 / 50, so existing
   invocations build exactly what they built before.

### Design

3. **User configuration goes in a drop-in file, not merged into
   `config.yaml`.** k3s reads `/etc/rancher/k3s/config.yaml` and then
   every file in `/etc/rancher/k3s/config.yaml.d/` in lexical order.
   Later files override scalar keys, and replace list keys unless the
   key is written with a `+` suffix, in which case they append. The
   plugin writes its own keys to `config.yaml` as today, and the
   caller's mapping, re-serialised with `yaml.safe_dump`, to
   `config.yaml.d/50-sf-client-k3s.yaml`. That leaves merging to k3s
   and keeps the plugin's keys visible on the node.

   Plugin-owned keys are rejected, with an error naming them, before
   any instance is created: `write-kubeconfig-mode`, `tls-san`,
   `cluster-init`, `server` and `token`. A caller who needs more SANs
   writes `tls-san+`, which k3s appends to the plugin's list; this
   should be documented. Validation also rejects a document that is
   not a single mapping. The whole check is a pure function, so it
   can be unit tested without mocks.

   ~~**Needs verifying in phase planning:** the k3s release that
   introduced `config.yaml.d` and the `+` suffix.~~ **Verified in
   phase 2 planning:** drop-ins arrived in v1.21.0+k3s1 and `+` in
   v1.21.1+k3s1. `--release-channel` can resolve to older releases,
   so phase 2 refuses them at create. The phase also extends the
   rejected keys to six more the plugin depends on (`data-dir`,
   `write-kubeconfig`, `https-listen-port`, `node-name`,
   `with-node-id`, `token-file`), and puts the servicelb disable in a
   later drop-in, because a caller's `disable` would otherwise replace
   it. See [the phase 2 plan](PLAN-node-customisation-phase-02-k3s-config.md), survey findings 3-5 and decisions
   3, 5 and 6. The enforced `disable+` appending to a caller's bare
   `disable`, and drop-ins loading on agents, were observed on a live
   cluster in phase 3 ([merge tier run](https://github.com/shakenfist/client-python-k3s/actions/runs/37246848512)).
4. **Server configuration applies to every control plane node, and
   agent configuration to every worker.** That includes additional
   control plane nodes (which currently get no config file) and
   workers added later by `expand-workers`. The drop-in has to exist
   before the installer runs, because the installer starts the
   service.
5. **Both are recorded in cluster metadata.** The new keys are
   `node_sizes`
   (`{'control_plane': {'cpus', 'memory', 'disk'}, 'worker': {...}}`),
   `server_config` and `agent_config`. Clusters created before this
   change have none of these keys, and the code reads them with
   today's values as defaults (`{}` for the two configs), in the same
   way `join_address` falls back to `api_address_inner`
   (`cluster.py:1046`). `show` should display the sizes.
6. **`create_instance()` takes the role.** Called as
   `create_instance(node_type)`, it looks up `md['node_sizes']`.
   `create_and_await_instances()` already has `node_type` and passes
   it down.

7. **Control plane nodes are tainted by default.** Added 2026-10-03.
   Decision 1 already makes `node-taint` reachable through the
   pass-through, but reachable is not the same as applied: on
   `runners.static-ci` all three nodes report `taints=NONE`, so a
   workload pod can be scheduled straight onto the single etcd and
   apiserver host. Combined with the OOM behaviour in open question 3,
   that is how one CI job takes the API server -- and therefore the
   whole cluster -- down. The 33fl sizing test had to work around it
   with an explicit `nodeAffinity` stanza, which every future manifest
   would otherwise have to repeat.

   So the plugin writes
   `node-taint: ['node-role.kubernetes.io/control-plane:NoSchedule']`
   into the control plane's own `config.yaml`, for every control plane
   node including the extras from `install_extra_control_plane()`.

   **It is plugin-defaulted, not plugin-owned.** Unlike the keys
   decision 3 rejects, a caller may set `node-taint` in
   `--server-config` and have it replace the plugin's value, because
   k3s's drop-in precedence replaces list keys outright. Writing
   `node-taint: []` is therefore the documented opt-out, and no new
   flag is needed.

   This is a behaviour change for clusters created after it lands, and
   the honest cost is that it is not free on small clusters: a
   three-node cluster gives up a third of its schedulable capacity.
   The OpenStack-Helm prototype may well want the opt-out, and phase 3
   should check whether it does rather than assume. **Answered in phase
   3 planning, from the chart source: it does, as written.** It labels
   only the control plane `openstack-control-plane=enabled`, and every
   chart it deploys ships its control-plane toleration with
   `enabled: false`, so every OpenStack control service would sit
   Pending. See the
   [phase 3 plan](PLAN-node-customisation-phase-03-live-validation.md),
   survey finding 3. The default is
   still the right way round -- a control plane that competes with
   workloads for memory is a correctness problem, and the opt-out is
   one line for the caller who has measured and decided otherwise.

   **One exception, found in phase 2 planning:** a cluster with no
   workers is not tainted. MetalLB's controller has no toleration for
   the taint, so tainting the only node would fail every zero-worker
   create with MetalLB at its `rollout status` wait. See the phase 2
   plan's survey finding 7 and decision 7.

   The default taint, and its `node-taint: []` opt-out, were observed
   on a live cluster in phase 3 ([merge tier run](https://github.com/shakenfist/client-python-k3s/actions/runs/37246848512)).

## Open questions

1. ~~Should the plugin disable servicelb whenever it installs
   MetalLB?~~ **Answered on 2026-10-03 against the live cluster
   `runners.static-ci`: yes.** Both controllers run at once.
   `svclb-traefik-*` pods are present on all three nodes, so klipper
   is active, while the address traefik actually holds was assigned by
   MetalLB -- the Service carries
   `metallb.io/ip-allocated-from-pool: empty`. So MetalLB wins the
   assignment and servicelb runs one pod per node accomplishing
   nothing. The recommendation this question carried stands: add
   `servicelb` to the plugin-owned server configuration whenever
   `install_metallb` is true, and note it in the release notes.

   Two details for whoever implements it. The pods are BestEffort
   with no resource requests, which on a small control plane makes
   them OOM-kill candidates ahead of anything that matters. And
   `metallb-controller` was logging
   `AdditionalAssignFailed ... cannot assign additional IP in
   PreferDualStack` every few minutes -- 184 occurrences over 25 days
   -- because the Service asks for dual-stack while the pool is IPv4
   only. Harmless, since the IPv4 address is assigned, but it is
   permanent error noise that nothing noticed, and it goes away with
   traefik. Worth confirming it is traefik's Service and not
   something the plugin configures. **Confirmed in phase 2 planning:**
   k3s's bundled `manifests/traefik.yaml` sets
   `ipFamilyPolicy: "PreferDualStack"`, and the plugin sets no IP
   family anywhere.
2. ~~Should this plan land before or after the library API plan's
   phase 4 (first PyPI release)?~~ **Answered by events, 2026-10-04:
   phase 4 landed first and `v0.1.0` is released; phase 5 merged as
   #90 without these options, so they are a follow-on there.** The
   original recommendation: this work is small and does not
   change existing behaviour, so it does not need to hold up the
   release. The collection in phase 5 exposes whatever `create()`
   parameters exist when it is written. Recommendation: do not block
   phase 4 on this plan; if this plan lands first, phase 5 picks the
   new options up for free, and if not, they are a follow-on release.
3. ~~Is there a sensible floor on sizing?~~ **Answered on
   2026-10-03: validate positive integers only, but document a
   realistic floor.** The recommendation this question carried was
   right about validation and too relaxed about the default. Measured
   on `runners.static-ci`, whose nodes are exactly today's hardcoded
   2 vCPU / 2048 MB:

   - `k3s-server` -- one process holding apiserver, controllers,
     scheduler and etcd -- is **709 MB RSS** on its own, 36% of the
     node, and it grows from ~190 MB after a restart as its caches
     warm. That leaves roughly 400 MB for containerd, every system
     pod and the kernel.
   - A burst of 20 to 60 pod creations drove the node into *global*
     OOM. The kernel killed `longhorn-manager` and `traefik` (both
     BestEffort), systemd restarted k3s, and the API server refused
     connections for ~30s. Those were the only OOM kills in that
     node's 25 day life.
   - It is not a clean threshold: an identical burst succeeded
     minutes earlier. Whether 2 GB survives depends on how recently
     k3s restarted, which makes it a random production failure rather
     than a reproducible one.

   So k3s's documented 2 GB server minimum is a floor at which a
   control plane runs and does not work. The plugin should still
   reject only non-positive integers -- a hard minimum would be
   guesswork about workloads it cannot see -- but 2048 MB should not
   remain a silent default that appears fine until the first busy
   day. Phase 1 should say so in `docs/usage.md`, and the CI runner
   plan's figure of 4 GB minimum for a control plane is a reasonable
   number to document.

## Execution

Each phase is small enough that its detail lives in this table.
`/next-phase` can split out a phase file if planning one reveals more
than expected.

| Phase | Plan | Status | Merged |
|-------|------|--------|--------|
| 1. Per-role sizing | [PLAN-node-customisation-phase-01-sizing.md](PLAN-node-customisation-phase-01-sizing.md) -- `create_instance(node_type)`; `node_sizes` in metadata with fallback defaults for existing clusters; six CLI flags and matching `create()` parameters; `show` displays sizes, including the 2 / 2048 / 50 fallback for clusters created before them (it already prints every recorded key); the documented sizing floor from open question 3; drop the bare `apt-get install -y` at `cluster.py:1052`; unit tests for the metadata fallback and for the sizes reaching `client.create_instance`; regenerate the `tests/cli_contract/` snapshots; update `docs/usage.md` and `docs/library-api.md`. Unit tests can verify everything except the live build. | Complete | `ddb1f3b` (#92) |
| 2. k3s configuration pass-through | [PLAN-node-customisation-phase-02-k3s-config.md](PLAN-node-customisation-phase-02-k3s-config.md) -- Open question 1 and the `config.yaml.d` version floor are resolved in the phase plan; a pure validation function for the caller's mapping; `--server-config` / `--agent-config` (loaded with `yaml.safe_load`, UTF-8) and `server_config` / `agent_config` on `create()`; write the drop-in on every server and agent before its installer runs, including in `install_k3s_component()` and on `expand-workers`; record both in metadata; write the default control plane `node-taint` per design 7 (only when the cluster has workers) and disable `servicelb` through a later, plugin-enforced drop-in whenever `install_metallb` is true, per open question 1; refuse k3s releases older than v1.21.1+k3s1; unit tests for validation, the file content, `expand-workers` reusing the recorded config, the default taint being present, and a caller's `node-taint: []` replacing it; docs with an OpenStack-Helm-flavoured example (`disable: [traefik]`, role labels). (The sizing floor moved to phase 1, where open question 3 put it; this row used to repeat it.) The drop-in content must reach the node through a quoted heredoc, following rule 2 at the top of `cluster.py`. | Complete | `b791364` (#98) |
| 3. Live validation | [PLAN-node-customisation-phase-03-live-validation.md](PLAN-node-customisation-phase-03-live-validation.md) -- Extend `tools/ci_deploy_test.sh` to create the main cluster with non-default sizes on both roles and both config files (disable Traefik, label control plane and workers differently), then assert with `kubectl`: no Traefik, no `svclb-*` pods, the expected labels on each node, the default `NoSchedule` taint on every control plane node, the recorded sizes in `show` and in Shaken Fist's own `instance show`, and a worker added by `expand-workers` carrying the agent label and worker sizes. The minimal cluster gains one worker and carries the `node-taint: []` opt-out, because a zero-worker cluster is never tainted and could not show it; it is also the positive control for the absence checks. Run the merge tier against the branch through `workflow_dispatch`. (This row used to ask for a run of the homelab OpenStack-Helm prototype's provision stage; that stage has not been built, and design 7's question to it was answered from the chart source instead.) | Complete | `51ff6f4` (#108) |
| 4. Push audit | [PLAN-node-customisation-phase-04-push-audit.md](PLAN-node-customisation-phase-04-push-audit.md) -- Run `PUSH-AUDIT.md` over the union of the three recorded merges (`ddb1f3b`, `b791364`, `51ff6f4`), each diffed against its first parent, judging that code as it stands after #107, which has since swept the library API audit's rules over part of it. (This row used to say "against `develop`", which is empty once the phases have merged.) | In progress | |

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

* Choosing the base image (`BASE_OS_VERSION` is a module constant).
  Debian 13 is the obvious next target. The apt-specific commands
  throughout `cluster.py` mean non-Debian bases are a much larger
  change.
* Additional NICs per role, for example a second interface for a
  Neutron provider bridge. The OpenStack-Helm prototype can manage
  without one by using a dummy interface.
* Additional disks per role, for example a dedicated Longhorn or
  Ceph disk.
* Per-invocation sizing and config overrides on `expand-workers`, for
  heterogeneous worker pools.
* Matching Longhorn's `defaultReplicaCount` to the number of
  schedulable nodes. Once phase 2 taints the control plane, the default
  one control plane plus two workers has two Longhorn storage nodes
  against a default of three replicas, so new volumes run degraded.
  They work, but it is the wrong default. See the phase 2 plan, risk 3.
* Surfacing the new options in the `shakenfist.k3s` collection.
  Phase 5 of the library API plan landed first (#90), so its
  `sf_k3s_cluster` module needs the sizing and config options added
  once both have landed.

### Bugs fixed during this work

This section should list any bugs we encounter during development
that we fixed. You should also scan the project's issue tracker,
where one exists, for directly related issues that we should
either resolve as part of this master plan or at least be aware of
while planning it.

* `install_k3s_component()` runs a bare `sudo apt-get install -y`
  with no package named (`cluster.py:1052`). Fixed in phase 1, step 1a.

### Back brief

Before executing any step of this plan, please back brief the
operator as to your understanding of the plan and how the work you
intend to do aligns with that plan.
<!-- shared-block-end -->
