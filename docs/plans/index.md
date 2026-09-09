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
| 2026-08-08 | [Startup progress tracking UI cleanup](progress-ui-cleanup.md) | Replace the repeated per-poll output of `k3s create` with a phase-numbered progress reporter, and fail fast on errored agent operations | Complete | Steps 1-6 in the plan, plus four addenda from live runs |
| 2026-08-08 | [Functional CI: two tier testing with real cluster deployments](functional-ci.md) | Add a merge-queue tier that deploys, expands and deletes a real k3s cluster in the runner's own Shaken Fist namespace | Complete | 1. Workflow restructure, 2. Deployment test, 3. Queue enablement, 4. Live validation |
| 2026-09-03 | [Library API, missing verbs, and the shakenfist.k3s collection](library-api-and-collection.md) | Make the orchestration callable from Python, add the verbs a daemon needs, and ship an optional Ansible collection for cluster bringup | In progress | 1. [Library API](library-api-and-collection-phase-01-library-api.md), 2. Client construction, 3. Missing verbs, 4. First release, 5. The collection, 6. Push audit |
