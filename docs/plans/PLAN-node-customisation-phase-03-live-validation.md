# Node customisation phase 3: live validation

## Prompt

Before responding to questions or discussion points in this document,
read `tools/ci_deploy_test.sh` end to end, the `cluster_deploy` job in
`.github/workflows/functional-tests.yml`, `docs/testing.md`, the
`#### Sizing` and `#### k3s configuration` sections of `docs/usage.md`,
and `Cluster._k3s_config_commands()` and `Cluster.create_instance()` in
`shakenfist_client_k3s/cluster.py`. Ground answers in what that code
does today. The master plan is
[PLAN-node-customisation.md](PLAN-node-customisation.md). This phase
exists to observe on a real cluster what phases 1 and 2 could only pin
with unit tests. That means risk 1 of the
[phase 2 plan](PLAN-node-customisation-phase-02-k3s-config.md) in
particular, and design 7's open question about the OpenStack-Helm
prototype.

## Planning effort

Medium. The code under test is already written, and this phase adds
assertions to an existing shell script in a pattern that script
already uses. The judgement is about which assertions can pass
vacuously and how to give each one a positive control. Decisions 2
and 4 carry that judgement.

Review effort: medium. The reviewer reads each new assertion and asks
what would make it pass with the feature broken.

## Scope

In:

* Extend `tools/ci_deploy_test.sh` so the main cluster is created with
  non-default sizes on both roles and with `--server-config` and
  `--agent-config`. Then assert, on the live cluster:
  * Traefik is absent.
  * No `svclb-*` pods exist.
  * Each node carries its role's label.
  * Every control plane node has the default `NoSchedule` taint.
  * The sizes reached both the cluster metadata and the Shaken Fist
    API.
  * A worker added by `expand-workers` carries the agent label and the
    worker sizes.
* Extend the minimal cluster so it can show the `node-taint: []`
  opt-out (decision 2). It also becomes the positive control for the
  absence checks (decision 4).
* Run the merge tier against the branch through `workflow_dispatch`
  before opening the pull request.
* Record what the live run showed: in the master plan, against phase 2's
  risk 1, and in `docs/testing.md`.
* Answer design 7's question about the OpenStack-Helm prototype. That
  is done in this planning commit, from the chart source (survey
  finding 3).

Out:

* Running the OpenStack-Helm prototype. It does not exist yet (survey
  finding 3, decision 5).
* The coverage gaps in #101 (`update-os` and `health --strict` in the
  failing direction) and #102 (root options). They are in the same
  script but are separate issues. Adding them here would make this
  phase's runtime and failure surface about something else.
* #97, the Longhorn release lookup hitting the GitHub rate limit. It
  can fail this phase's dispatch runs (risk 2), but fixing it is its
  own change.
* Plugin behaviour changes. If a live assertion shows that k3s does not
  merge the way phase 2 assumed, this phase stops and reports
  (decision 7).

## What the survey found

The master plan's phase 3 row was written before phase 2 executed.
Findings 2 and 3 change what the row asked for. Both were corrected at
source in this planning commit:

* the phase 3 row of the master plan's Execution table;
* design 7's paragraph on the prototype;
* the master plan's row in `docs/plans/index.md`.

Nothing later needs to redo that.

### 1. Phase 2 is live-tested only by accident, and not asserted

The merge queue run for #98
(<https://github.com/shakenfist/client-python-k3s/actions/runs/37233448610>)
passed. Its `Cluster deployment` job took 19 minutes, from 21:41 to
22:00 UTC. That run built the default 1 + 2 cluster with the enforced
servicelb drop-in and the control plane taint. So a default create
still works under both. Nothing in it checked that either took effect,
though. `tools/ci_deploy_test.sh` creates the main cluster at
`:138-140` with no sizing or config flags. Its only node assertions are
the count and readiness in `wait_for_nodes()` (`:61-79`). The three k3s
merge behaviours in phase 2's risk 1 are still unobserved:

* the enforced `disable+` appending to a caller's bare `disable`;
* a caller's `node-taint` replacing `config.yaml`'s;
* drop-ins loading on agents.

At 19 of the job's 80 minutes, the budget has room for what follows.

### 2. The opt-out cannot be observed on the minimal cluster as it is

The master plan asks for "a second create passing `node-taint: []`".
The obvious second create is the minimal cluster at `:274-276`. It has
`--worker-count 0`, though, and phase 2's decision 7 skips the taint
when there are no workers. So its control plane is untainted whatever
the caller passes, and an opt-out assertion there would pass with the
opt-out broken. Observing the replacement needs a cluster that would
otherwise be tainted, which means one with at least one worker.

### 3. There is no prototype provision stage to run

The master plan says to "run the homelab OpenStack-Helm prototype's
provision stage against the branch". The prototype lives in the private
`homelab-deployments-lfs`, worktree `-wt-osh`, commit `f123701`. It is
`notes/openstack-helm-k3s-plan.md`, a plan marked "STATUS: PROPOSED
... Nothing below has been built yet". Neither
`playbooks/openstack-helm-k3s.yml` nor `roles/openstack_helm/` exists.

Design 7's actual question, whether the prototype wants the taint
opt-out, can be answered from the chart source. The answer is yes, as
the prototype is currently written. This was read from a local
openstack-helm checkout at `99d96acba` (2026-06-26). That is older
than the `a606f21` the master plan cites, so a toleration block added
or removed since then would change it:

* The prototype puts `openstack-control-plane=enabled` only on the k3s
  control plane node (its stage 1).
* Every chart it deploys selects that label: rabbitmq, mariadb,
  memcached, keystone, glance, placement, nova, neutron. libvirt and
  openvswitch target the compute and openvswitch labels instead.
* Every one of those charts carries a `pod.tolerations.<chart>` block
  holding a `node-role.kubernetes.io/control-plane` toleration, for
  example `nova/values.yaml:2357-2366`. Every one of them ships it with
  `enabled: false`.
* So under the default taint, every OpenStack control service would
  sit Pending.
* The prototype also assumes "the control plane node is schedulable in
  k3s, so Longhorn's three default replicas fit on three nodes", which
  phase 2 made false (phase 2 risk 3).

The prototype has three ways round this:

* `node-taint: []` in its `server.yaml`;
* enabling the toleration in each chart's values;
* labelling the workers for the control plane as well.

Its 24 GB control plane was sized to host the OpenStack control
services. That is the "measured and decided otherwise" case design 7
wrote the opt-out for, so `node-taint: []` is the natural fit. That
choice belongs to the prototype, not this plan. The prototype's notes
are also stale in two places: they still treat servicelb as a choice
for the caller, and they rely on the control plane for Longhorn
replicas. Both are reported to the operator rather than edited, since
the notes live in another repository.

### 4. The facts the new assertions rely on

* `k3s show` prints `node_sizes = {...}` as a Python `repr`
  (`__init__.py:342-344`). `ast.literal_eval` can read it back. The
  same output carries the node token and the admin kubeconfig. So the
  existing rule at `dump_state()` (`:39-49`) applies: capture it, never
  print it.
* `sf-client --simple instance show UUID` prints `cpus:N`,
  `memory:N` and a `disk_spec,TYPE,BUS,SIZE,BASE` line per disk
  (`client-python`, `shakenfist_client/commandline/instance.py:192-193`
  and `:224-227`). That is Shaken Fist's own view of the instance, not
  the plugin's record of what it asked for.
* The control plane and worker UUIDs are in `k3s show` as
  `control_plane_nodes` and `worker_nodes`. `worker_uuids()` (`:81-87`)
  already parses the second.
* k3s labels every server node `node-role.kubernetes.io/control-plane`,
  so `kubectl get nodes -l node-role.kubernetes.io/control-plane`
  selects the control plane without knowing node names. That matters
  because this cluster's name is deliberately mixed case (`:14-20`).
* Traefik is a k3s `HelmChart` object, `kube-system/traefik`. It
  creates a `LoadBalancer` Service, so on a cluster with servicelb
  active it produces `svclb-traefik-*` pods.

## Decisions

1. **No third cluster.** Every assertion goes on one of the two
   clusters the script already builds. A third cluster would be
   another 10-15 minutes and another set of VMs on an under-cloud
   whose capacity is the usual cause of merge-queue failures. Decision
   2 shows the two existing clusters can carry everything.

2. **The minimal cluster gains one worker and carries the opt-out.**
   It is created with `--worker-count 1` and a `--server-config`
   holding `node-taint: []`. It keeps `--no-metallb --no-longhorn
   --no-kubeconfig` and `--metal-address-count 0`. It then asserts that
   the control plane node has no taints and that `wait_for_nodes 2`
   succeeds.

   The main cluster carries the default taint, so it cannot also carry
   the opt-out (server config is uniform across servers). The minimal
   cluster is the only other candidate, and survey finding 2 means it
   needs a worker. The cost is one VM and a join, roughly 3-5 minutes.
   It also means the zero-worker exception is no longer exercised on
   any cluster. That is acceptable: the exception is a plugin-side
   branch, fully unit tested, while the opt-out is a k3s merge
   behaviour that only a live node can show. The comment above
   `MINIMAL_CLUSTER` (`:21-27`) is updated to say why it now has a
   worker.

3. **Sizes are non-default on every field, and differ between roles.**
   * Main cluster control plane: `--control-plane-cpus 4
     --control-plane-memory 4096 --control-plane-disk 30`.
   * Main cluster workers: `--worker-cpus 3 --worker-memory 3072
     --worker-disk 40`.

   4096 MB is the control plane floor `docs/usage.md` already
   recommends. 3072 MB on the workers leaves headroom for Longhorn and
   the test deployment, now that the control plane no longer takes
   workloads. Every value differs from the 2 / 2048 / 50 default and
   from the other role's, so a dropped flag or a role swap shows up.
   The disks are smaller than the defaults, which offsets some of the
   extra memory on the under-cloud. The minimal cluster keeps the
   defaults.

   Sizes are asserted in two places. `k3s show`'s `node_sizes` proves
   the metadata. `instance show` on one control plane and one worker
   proves Shaken Fist built what was asked. The first alone would pass
   if `create_instance()` ignored its role.

4. **Every absence check has a positive control on the other cluster.**
   * The main cluster's `--server-config` is `disable: [traefik]` plus
     `node-label: [ci-role=server]`. Its `--agent-config` is
     `node-label: [ci-role=agent]`.
   * The bare `disable` is chosen on purpose. It is the case where the
     plugin's enforced `disable+: [servicelb]` has to append rather
     than be replaced, which is phase 2's risk 1, first bullet.

   On the main cluster, after the LoadBalancer test (which takes
   minutes, so Traefik's install job has long since run if it was
   going to), assert:
   * there is no `kube-system/traefik` HelmChart;
   * no pod name contains `traefik`;
   * no pod name starts with `svclb-`. `ci-web` is itself a
     LoadBalancer Service, so live servicelb would have made
     `svclb-ci-web-*` pods by then.
   * `ci-role=server` selects exactly one node and `ci-role=agent`
     exactly two.
   * every node selected by `node-role.kubernetes.io/control-plane`
     has a taint with key `node-role.kubernetes.io/control-plane` and
     effect `NoSchedule`.

   On the minimal cluster, which disables nothing and labels nothing,
   poll for up to five minutes until the `kube-system/traefik`
   HelmChart and at least one `svclb-traefik-` pod exist. That proves
   the main cluster's absence checks are looking for the right names,
   and that servicelb stays on without MetalLB. Also assert that no
   node carries a `ci-role` label.

5. **The prototype run is replaced by survey finding 3.** There is no
   provision stage to run. Building one means writing a playbook in a
   private repository against a lab cluster that needs 24 vCPU and 56
   GB free, which is the prototype's own stage 1, not a validation
   step for this plugin. The question design 7 asked of it is answered
   above from the charts. The live check of the plugin's behaviour is
   decision 4, in CI, where it runs on every merge and not once. This
   is the decision a reviewer is most likely to disagree with. The
   argument for it is that a heavier consumer adds load, not
   coverage: everything the prototype would exercise in the plugin
   (sizes, both configs, labels, the taint and its opt-out) is
   asserted here. The prototype's own failures would be about
   OpenStack-Helm.

6. **`expand-workers` is checked between the expand and the
   removal.** The existing flow at `:213-235` adds a worker and
   removes it. Between `wait_for_nodes 4` and the removal:
   * `ci-role=agent` selects three nodes;
   * `instance show` on `new_worker` reports the worker sizes.

   That is the master plan's design 4 and 5 promise: later workers
   are built from recorded metadata.

7. **A failed k3s-semantics assertion stops the phase.** The phase stops
   if any of the following fails because k3s behaved differently from
   what phase 2 assumed, rather than because of a script mistake:
   * the taint is not replaced;
   * servicelb survives the caller's `disable`;
   * the agent labels are missing.

   That is a plugin bug with a design choice in it, so the management
   session reports it with the run URL and asks. It does not get a
   fix inside this phase.

8. **`dump_state()` shows labels and taints.** Add `kubectl get nodes
   --show-labels` and a custom-columns listing of each node's taints.
   Several new assertions fail on exactly that state, and the
   namespace is gone by the time anyone reads the log. Both are safe
   to print.

## Step plan

| Step | Effort | Model | Isolation | Brief for sub-agent |
|------|--------|-------|-----------|---------------------|
| 3a | medium | opus | none | Extend `tools/ci_deploy_test.sh` per decisions 1-4, 6 and 8 of `docs/plans/PLAN-node-customisation-phase-03-live-validation.md`; read survey finding 4 there for the output formats. (1) Before the main create (`:119-140`), write a server config (`disable: [traefik]`, `node-label: [ci-role=server]`) and an agent config (`node-label: [ci-role=agent]`) into the existing `manifest_dir` temp directory with quoted heredocs, the way `ci-staged.yaml` is written, and pass `--server-config`, `--agent-config` and the six sizing flags from decision 3. Put the sizes in shell variables near the top so the create and the assertions share them. (2) Add a helper that reads `node_sizes` from `sf-client k3s show` without printing the output (it carries the token and kubeconfig; see the comment in `dump_state()`): capture, `sed -n` the `node_sizes = ` line, and compare with `python3 -c` using `ast.literal_eval` against the expected dict. The venv's python is on PATH. Add a helper that takes an instance UUID and expected cpus/memory/disk and checks `sf-client --simple instance show UUID` for `cpus:`, `memory:` and the size field (4th, comma-separated) of the `disk_spec,` line. On mismatch, print only those three values. (3) After the LoadBalancer test (`:185-211`), add a status block with decision 4's main-cluster assertions. Use `kubectl get helmchart -n kube-system traefik` failing as "absent", and `kubectl get pods -A --no-headers` piped through grep. Use `-l node-role.kubernetes.io/control-plane` and jsonpath over `.spec.taints` for the taint. Then check sizes: `node_sizes` from show, plus `instance show` on the first `control_plane_nodes` UUID and the first `worker_uuids` entry. (4) Between `wait_for_nodes 4` and the remove (`:216-219`), assert three `ci-role=agent` nodes and the worker sizes on `new_worker` (decision 6); `new_worker` is computed at `:220`, so move that computation up. (5) Minimal cluster (`:261-297`): write a server config with `node-taint: []`, create with `--worker-count 1` and `--server-config`, `wait_for_nodes 2`, assert the control plane node's `.spec.taints` is empty, then poll up to five minutes (30 x 10s, like the LoadBalancer loop) for the traefik HelmChart and an `svclb-traefik-` pod, and assert no node has a `ci-role` label. Update the `MINIMAL_CLUSTER` comment (`:21-27`) to say why it has a worker and carries the opt-out. (6) In `dump_state()`, add `kubectl get nodes --show-labels \|\| true` and `kubectl get nodes -o custom-columns=NAME:.metadata.name,TAINTS:.spec.taints \|\| true`. (7) In `docs/testing.md`, extend the merge-tier paragraph (`:26-32`) with one or two sentences on what the clusters now assert, and keep "A full run is 15-25 minutes" honest: say 20-30. Constraints: every assertion fails with an `echo` naming what was expected and what was found, then `exit 1`. Follow the script's existing comment style, which explains why an assertion exists and what would make it pass vacuously. `set -e` is on, so guard greps that may legitimately match nothing with `\|\| true` only where a zero count is the expected answer, as `count_routed_addresses()` does. This step can only be verified by shellcheck (`pre-commit run --all-files`) and `bash -n`; it cannot reach a cluster. Do not claim the assertions pass. Commit subject: `Assert node customisation on a live cluster.` |
| 3b | — | management session | none | Not a sub-agent step. Push the branch (no pull request) and run `gh workflow run functional-tests.yml --ref node-customisation-phase-03-live-validation`. Watch it with `ci-status`. Triage a failure with the `merge-ci-triage` approach. If it is #97 or another systemic signature, comment there and re-dispatch. If it is a script mistake, brief a sub-agent with the log excerpt and repeat 3a's verification. If it is a k3s-semantics failure (decision 7), stop and ask. Record the URL of the passing run for 3c. |
| 3c | low | sonnet | none | Record the live result. In `docs/plans/PLAN-node-customisation.md`, add one sentence to design 3 and one to design 7 saying the behaviour was observed on a live cluster in phase 3, with the passing run's URL. The behaviours are: the enforced `disable+` surviving a caller's `disable` and drop-ins loading on agents (design 3); the default taint and its `node-taint: []` opt-out (design 7). Do not edit the phase 2 plan, which is a record of what was planned. If the run showed anything `docs/usage.md` states differently, correct `docs/usage.md`. Otherwise leave it alone. Commit subject: `Record node customisation's live validation.` |

The management session reviews 3a against the script rather than the
sub-agent's summary. For each new assertion it names the broken
behaviour the assertion catches and the positive control that keeps it
from passing vacuously (decision 4). An assertion with no answer to
both goes back. It runs `pre-commit run --all-files` (which shellchecks
`tools/`), `tox -epy3` and `tox -eflake8`. The last two are unchanged
by this phase but are still the project's gate.

## Risks and mitigations

1. **Bigger nodes meet a full under-cloud.** Decision 3 adds about
   5 GB of memory to the main cluster and a VM to the minimal one, on
   capacity every merge queue shares.

   Mitigation: smaller disks offset part of it. The 3b dispatch run
   shows whether the runner's namespace can hold it before anything
   reaches the queue. If instance creation fails on capacity, the
   management session lowers the worker memory to 2560 rather than
   dropping the assertion.
2. **#97 ejects the dispatch run.** The Longhorn release lookup is
   unauthenticated and shares the under-cloud's egress address.

   Mitigation: 3b treats it as systemic, records the occurrence on #97
   and re-dispatches. It is not a reason to change this phase.
3. **An absence check is placed too early and passes before the thing
   it checks for could exist.**

   Mitigation: decision 4 orders the main cluster's checks after the
   LoadBalancer test and gives each a positive control on the minimal
   cluster. The 3a review checks both for every absence assertion.
4. **The positive-control poll on the minimal cluster flakes.**
   Traefik's install pulls an image from the internet.

   Mitigation: five minutes is generous for one chart. If it does
   flake, the failure message names the poll, so it cannot be mistaken
   for a node customisation fault. Lengthening the poll is the fix,
   not removing it.
5. **The OpenStack-Helm answer is read from a June checkout.**

   Mitigation: survey finding 3 states the commit. The answer only
   matters to the prototype, whose stage 1 check ("every node is Ready
   and carries its labels") plus a first chart install will show it
   live.

## Definition of done

* `grep -c -e '--server-config' -e '--agent-config' tools/ci_deploy_test.sh`
  is at least 3: both on the main create, and the server config on the
  minimal one.
* `grep -n -e "node-taint: \[\]" -e 'svclb-' -e 'ci-role=' -e 'helmchart' tools/ci_deploy_test.sh`
  shows each pattern at least once.
* `grep -n -e 'control-plane-cpus' -e 'worker-disk' tools/ci_deploy_test.sh`
  shows the sizing flags on the main create.
* `grep -n 'show-labels' tools/ci_deploy_test.sh` finds the line in
  `dump_state()`.
* `pre-commit run --all-files`, `tox -epy3` and `tox -eflake8` pass.
* A `workflow_dispatch` run of `functional-tests.yml` on this branch
  passes. Its URL appears in the master plan's design 3 and design 7.
* The merge-queue run for this phase's pull request passes.
* `docs/testing.md` says what the merge tier asserts about node
  customisation, and its run-time figure matches the dispatch run's
  `Cluster deployment` duration to within five minutes.
* The master plan's Execution table and `docs/plans/index.md` agree on
  this phase's status.

## Back brief

Before executing any step of this plan, back brief the operator on
your understanding of the plan and how the work you intend to do
aligns with it. In particular, confirm decisions 2 and 5. Decision 2
changes the shape of the minimal cluster that CI has always built.
Decision 5 drops a step the master plan asked for.
