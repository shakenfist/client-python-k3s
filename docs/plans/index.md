# Plans index

This page registers every planning document in `docs/plans/`, oldest
first. New plans start from `PLAN-TEMPLATE.md` at the repository root,
and a plan that is not listed here is invisible: registering it is part
of writing it, not a tidy-up afterwards.

Master plans decompose their work into numbered phases. Where a phase
is small enough its detail lives in the master plan's own Execution
table, and the Phases column below records where to look; where a
phase needs a plan of its own it gets a file named for its master plan
with `-phase-NN-descriptive` appended, linked from both the master
plan's Execution table and the Phases column here.

The `Status` column holds exactly one term from the shared vocabulary
in `PLAN-TEMPLATE.md`: `Proposed`, `Not started`, `In progress`,
`Blocked`, `Complete`, `Abandoned` or `Superseded`. Anything a reader
needs beyond that term belongs in the plan file, with a one line
summary in `Intent`.

## Master plans

| Date | Plan | Intent | Status | Phases |
|------|------|--------|--------|--------|
| 2026-08-08 | [Startup progress tracking UI cleanup](PLAN-progress-ui-cleanup.md) | Replace the repeated per-poll output of `k3s create` with a phase-numbered progress reporter, and fail fast on errored agent operations | Complete | Steps 1-6 in the plan, plus four addenda from live runs |
| 2026-08-08 | [Functional CI: two tier testing with real cluster deployments](PLAN-functional-ci.md) | Add a merge-queue tier that deploys, expands and deletes a real k3s cluster in the runner's own Shaken Fist namespace | Complete | 1. Workflow restructure, 2. Deployment test, 3. Queue enablement, 4. Live validation |
| 2026-09-03 | [Library API, missing verbs, and the shakenfist.k3s collection](PLAN-library-api-and-collection.md) | Make the orchestration callable from Python, add the verbs a daemon needs, and ship an optional Ansible collection for cluster bringup. All six phases are complete. `v0.2.0` published the plugin to PyPI and `shakenfist.k3s` 0.2.0 to Galaxy, and phase 6 audited the union of the ten merge commits that landed phases 1-5, fixing a node-token leak into error output and seven other defects. Two success criteria remain unmet and are tracked: nothing has run the collection against a real API (#89), and phase 2's client construction has no functional coverage (#102) | Complete | 1. [Library API](PLAN-library-api-and-collection-phase-01-library-api.md), 2. [Client construction](PLAN-library-api-and-collection-phase-02-client-construction.md), 3. [Missing verbs](PLAN-library-api-and-collection-phase-03-missing-verbs.md), 4. [First release](PLAN-library-api-and-collection-phase-04-first-release.md), 5. [The collection](PLAN-library-api-and-collection-phase-05-collection.md), 6. [Push audit](PLAN-library-api-and-collection-phase-06-push-audit.md) |
| 2026-09-27 | [Node customisation: per-role sizing and k3s configuration pass-through](PLAN-node-customisation.md) | Let callers size control plane and worker nodes separately, pass arbitrary k3s server/agent configuration recorded so expand-workers matches, and taint control plane nodes by default; motivated by an OpenStack-Helm prototype and blocking 33fl's CI runner migration | Complete | 1. [Per-role sizing](PLAN-node-customisation-phase-01-sizing.md), 2. [k3s configuration pass-through](PLAN-node-customisation-phase-02-k3s-config.md), 3. [Live validation](PLAN-node-customisation-phase-03-live-validation.md), 4. [Push audit](PLAN-node-customisation-phase-04-push-audit.md) |
| 2026-10-04 | [Cumulative health signals: what the health verb cannot see](PLAN-cumulative-health-signals.md) | Report what has happened to a cluster since it was last looked at -- k3s restart count, OOM kills, memory headroom, etcd growth -- so a daily poll can tell "fine" from "fine right now"; phases planned one at a time | Complete | 1. [Agent-read signals](PLAN-cumulative-health-signals-phase-01-agent-signals.md), 2. [Kubernetes-read signals](PLAN-cumulative-health-signals-phase-02-kubernetes-signals.md), 3. [Live validation](PLAN-cumulative-health-signals-phase-03-live-validation.md), 4. [Push audit](PLAN-cumulative-health-signals-phase-04-push-audit.md) |
| 2026-10-07 | [Argument validation: cluster names and counts](PLAN-argument-validation.md) | Validate cluster names and counts once, in the library, before anything is built, and remove kubeconfig entries by name so delete works for every cluster and namespace name (#96) | Complete | 1. Argument validation, 2. Push audit |
| 2026-10-08 | [Recoverable create: no name without state](PLAN-recoverable-create.md) | Write a cluster's metadata before claiming its name, run every check that can fail before the first write, and let delete free names stuck by earlier versions, so every way a create can stop is recoverable with `delete NAME` (#72) | Complete | 1. Recoverable create, 2. Push audit |
| 2026-10-12 | [Air-gapped k3s install from labelled Shaken Fist artifacts](PLAN-airgap-install.md) | Install k3s with no internet access for k3s itself. Shaken Fist fetches the binary, system images tarball and install script by URL into labelled artifacts, the plugin verifies them against the published sha256 sums, and put-blob stages them on each node for `INSTALL_K3S_SKIP_DOWNLOAD`. Phase 1 alone shares the channel map across namespaces, which fixes the cold version cache in CI after the update.k3s.io outages (#132). Plugin-only; Helm charts and container images stay online | Proposed | 1. Shared channel cache, 2. Release artifact cache, 3. Air-gapped install, 4. Retention, reporting and the collection, 5. Live validation, 6. Push audit |
| 2026-11-16 | [remove-worker: refuse a worker which still has work on it](PLAN-remove-worker-only-if-empty.md) | Add `remove_worker(only_if_empty=True)`, which cordons a worker, lists its pods and refuses to remove it if anything a drain would evict is still there. An unforced `kubectl drain` evicts a pod owned by any controller, so the drain was never the refusal the docstring implied, and a caller scaling a pool of workers cannot close the window between looking and acting from outside | Complete | 1. The emptiness option, 2. Push audit |

## Phases of plans in other repositories

A phase whose plan of record lives elsewhere, but whose code lives
here, so its plan file does too. The `plan-index` audit exempts phase
plans from registration; these are listed anyway, because a plan file
the index never links is invisible to anyone reading this page to find
out what has been planned.

| Date | Plan | Intent | Status |
|------|------|--------|--------|
| 2026-11-26 | [Agent operation dependencies phase 5](PLAN-agent-operation-dependencies-phase-05-adoption.md) | The first example application for shakenfist's `PLAN-agent-operation-dependencies.md`: `Cluster.execute_and_await()` declares a `depends_on` edge from each command to the previous command submitted to the *same* instance, so the rest of a node's sequence is abandoned when the operation before it never completed. Six of the eleven call sites gain fate sharing from the one change. Survey finding 11, found while executing, is the phase's most useful output and narrows all of that: an edge is satisfied by a dependency in `complete`, and a command which runs and exits non-zero leaves a `complete` operation, so a failed `apt-get update` does not stop the `dist-upgrade` behind it. What the edge catches is `error`, `abort`, `expired` and `deleted` -- the wedged side channel, the fired deadline, the cascade -- which is the #3516 failure family rather than the worked example everybody reasoned from, and the gap is filed as shakenfist#5091 as a per-edge opt-in rather than by making a non-zero exit an error. Edges are never declared across instances, because `aops` order is cross-instance and a cross-instance edge spends the server's dependency wait budget while serialising a deliberately concurrent fan-out. A cluster which does not advertise `agentoperation-dependencies` gets today's unchained submission and a written note rather than the `IncapableException` client-python raises, because this plugin declares the edge on behalf of a caller who asked for a k3s cluster and never mentioned dependencies. The master plan's worked example, a rolling `update_os()`, is declined: nothing here rolls. `v0.8.5` carries phase 3's surface and is on PyPI, so raising this repository's `shakenfist_client` floor to it is step 5b; that floor matters because `tools/ci_deploy_test.sh` installs the client from PyPI, which makes the cluster-deploy tier the live proof. | In progress |
