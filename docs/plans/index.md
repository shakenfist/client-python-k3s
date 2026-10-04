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
| 2026-09-27 | [Node customisation: per-role sizing and k3s configuration pass-through](PLAN-node-customisation.md) | Let callers size control plane and worker nodes separately, pass arbitrary k3s server/agent configuration recorded so expand-workers matches, and taint control plane nodes by default; motivated by an OpenStack-Helm prototype and blocking 33fl's CI runner migration | In progress | 1. [Per-role sizing](PLAN-node-customisation-phase-01-sizing.md), 2. k3s configuration pass-through, 3. Live validation, 4. Push audit |
| 2026-10-04 | [Cumulative health signals: what the health verb cannot see](PLAN-cumulative-health-signals.md) | Report what has happened to a cluster since it was last looked at -- k3s restart count, OOM kills, memory headroom, etcd growth -- so a daily poll can tell "fine" from "fine right now"; placeholder, phases unplanned | Proposed | 1. Agent-read signals, 2. API-read signals, 3. Live validation, 4. Push audit |
