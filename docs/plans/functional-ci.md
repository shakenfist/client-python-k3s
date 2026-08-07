# Functional CI: two tier testing with real cluster deployments

## Situation

The repository's CI is currently a single tier. Despite its name,
`.github/workflows/functional-tests.yml` runs only smoke checks on
pull requests: flake8, the unit tests, pre-commit, a requirements
install, an import check, and the automated reviewer. CodeQL and
supply chain checks run in their own workflows. Nothing exercises
the orchestration path -- and both substantial recent bugs (the
`sf-agent2` side channel requirement, and the in-guest agent
rejecting `KUBECONFIG=... helm ...` command lines, see
shakenfist/agent-python#120) were only discoverable against a live
Shaken Fist cluster. The unit tests mock the API surface and
cannot catch this class of failure.

The infrastructure to do better already exists:

* The CI fleet has ephemeral VM runners (`runs-on: [self-hosted,
  vm, debian-12]`). Each runner VM is created in its own per-job
  Shaken Fist namespace on the under-cloud (the namespace name is
  the runner's hostname, which is how
  `shakenfist/actions/setup-test-environment` derives
  `SHAKENFIST_NAMESPACE`), and carries under-cloud credentials in
  `~/.shakenfist`, which `sf-client` reads automatically.
* Because the k3s plugin builds clusters from ordinary Shaken
  Fist instances, the runner can deploy a k3s cluster *directly
  into its own namespace* on the under-cloud. No nested Shaken
  Fist cloud is required (unlike shakenfist/shakenfist's smoke
  cluster jobs), and no namespace lifecycle management is needed:
  the namespace and everything the test created die with the
  runner.
* shakenfist/shakenfist already models the two tier shape: smoke
  jobs run on `pull_request`, expensive jobs are gated on
  `github.event_name == 'merge_group' || github.event_name ==
  'workflow_dispatch'`, and merge queue required checks hang off
  collection jobs.

A full `k3s create` is 15-25 minutes. That is acceptable for a
merge queue tier, and the recent fail-fast work (agent operations
in the error state abort the command rather than hanging the wait
loops) makes CI-driven deployments viable: a genuine failure now
exits within seconds rather than consuming the job timeout.

## Mission and problem statement

Roll out shakenfist/shakenfist style two tier CI:

1. A **smoke tier** on every pull request: the existing sanity
   checks (lint, unit tests, pre-commit, import verification),
   CodeQL, and the automated reviewer. This tier already exists
   and needs at most renaming and a collection job.
2. A **merge tier** driven by a merge queue: on `merge_group`
   (and `workflow_dispatch` for manual runs), an ephemeral VM
   runner deploys a real k3s cluster into its own namespace with
   `sf-client k3s create`, asserts the cluster is functional,
   exercises the expansion commands, and deletes it.

## Open questions

* **Load balancer reachability.** The strongest assertion is an
  HTTP request to a MetalLB `LoadBalancer` service address from
  the runner. MetalLB addresses are under-cloud floating
  addresses routed to the cluster's node network; whether they
  are reachable from the runner VM's own job network needs a live
  run to confirm. Fallback: perform the curl from a cluster node
  via an agent execute operation, which stays within the node
  network.
* **Merge queue enablement.** Turning on the merge queue and
  making the merge-tier collection job a required status check is
  a repository settings change (possibly via export-repo-config
  in shakenfist/development), not something this repository's
  workflow files can do. Operator step.
* **Scope of command coverage.** `create`, `getconfig`,
  `expand-workers`, `expand-addresses`, `show` and `delete` are
  cheap to chain on one cluster. `update-os` adds an apt
  dist-upgrade across all nodes (~5-10 minutes) for little new
  coverage -- proposed: leave it out initially.
* **kubectl on the runner.** The assertions need a kubectl;
  pinning a static binary download in the test script is simplest
  and matches the cluster's k3s version only loosely. k3s ships a
  kubectl on the control plane node, so an alternative is running
  all kubectl assertions through agent execute operations and
  keeping the runner dependency-free.

## Execution

| Phase | Work | Status |
|-------|------|--------|
| 1. Workflow restructure | Add `merge_group` trigger, name the tiers, add collection jobs mirroring shakenfist/shakenfist | Not started |
| 2. Deployment test | `tools/ci_deploy_test.sh` plus the merge-gated job on `[self-hosted, vm, debian-12]` | Not started |
| 3. Queue enablement | Merge queue + required checks (operator, repo settings) | Not started |
| 4. Live validation | `workflow_dispatch` runs until green, fix what they find | Not started |

Phase 2 sketch, all logic in `tools/ci_deploy_test.sh` per the
no-large-scripts-in-workflow-steps convention:

1. Create a venv; install `shakenfist-client` from PyPI and the
   plugin from the checkout under test.
2. `sf-client k3s create ci --control-plane-count 1
   --worker-count 2 --metal-address-count 2` in the runner's own
   namespace (the default from `~/.shakenfist`). Line mode
   progress output (not a TTY) gives change-only logs with
   heartbeats.
3. `sf-client k3s getconfig ci` and assert: all nodes reach
   `Ready` within a deadline; a test deployment with a
   `LoadBalancer` service is assigned a MetalLB address; the
   service answers HTTP (see the reachability open question).
4. `sf-client k3s expand-workers ci --worker-count 1` and assert
   the node count grows; `expand-addresses` and assert the pool
   grows.
5. `sf-client k3s delete ci` and assert the instances and
   metadata are gone (teardown correctness is part of the test,
   even though the namespace would die with the runner anyway).

Step timeouts throughout, a job timeout of 90 minutes as
backstop, and a concurrency group per ref. The job must not
require secrets beyond what the runner already carries.

**Ordering dependency**: the first merge-tier run can only pass
once the `progress-ui-cleanup` branch has merged -- it carries
the `helm --kubeconfig` fix for the agent command validation
failure that currently breaks `setup_metallb()` (and the
fail-fast error handling that makes CI hangs impossible). Land
that branch first.

## Administration and logistics

### Success criteria

We will know when this plan has been successfully implemented
because the following statements will be true:

* Pull requests run the smoke tier only; merge queue entries run
  the smoke tier plus a full cluster deployment on an ephemeral
  VM runner.
* The deployment job creates, verifies, expands and deletes a
  k3s cluster in the runner's own namespace with no manual
  cleanup required, whether it passes or fails.
* A deliberate orchestration regression (for example
  reintroducing the `KUBECONFIG=` helm prefix) is caught by the
  merge tier within one poll interval of the failing phase, not
  by a job timeout.
* All workflow lint (actionlint via pre-commit) passes, and
  shell logic lives in `tools/`, not inline in workflow steps.

### Future work

* Consider a scheduled (weekly) run of the merge tier against
  released `shakenfist-client` + plugin from PyPI, to catch
  server-side drift breaking released versions.
* Consider asserting Longhorn volume provisioning (create a PVC,
  bind it) once the basic tier is stable.
* `update-os` coverage if it earns its runtime.
* Streaming agent operation output
  (shakenfist/shakenfist#3661) would make deployment CI logs
  far more diagnosable.

### Bugs fixed during this work

None yet. Related context: shakenfist/agent-python#120 (agent
rejects environment variable prefix command lines) is the bug
class this CI exists to catch.

### Back brief

Before executing any step of this plan, please back brief the
operator as to your understanding of the plan and how the work
you intend to do aligns with that plan.
