# Plans index

This page registers every planning document in `docs/plans/`, oldest
first. New plans start from `PLAN-TEMPLATE.md` at the repository root,
and a plan that is not listed here is invisible: registering it is part
of writing it, not a tidy-up afterwards.

Master plans decompose their work into numbered phases. Neither plan
here needed separate phase files -- both are small enough that the
phases live in the master plan's own Execution table -- so the Phases
column records where to look rather than linking out.

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
