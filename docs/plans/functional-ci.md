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

* **Load balancer reachability.** *(Resolved.)* Live testing on
  a local cluster confirmed MetalLB addresses are reachable from
  hosts outside the under-cloud (an external host got HTTP 200
  from a `LoadBalancer` service in 2ms) and that only clients
  *inside the cluster's own node network* are blocked (no u-turn
  routing at the network node, shakenfist/shakenfist#3662). The
  first live CI run then confirmed the remaining case: the runner
  VM, on a different virtual network to the cluster, fetched HTTP
  from the MetalLB address directly. The agent-execute fallback
  considered for this case is not needed.
* **Merge queue enablement.** *(Resolved.)* The operator created
  a "Develop branch" repository ruleset (id 20595671) mirroring
  the shakenfist/shakenfist one: merge queue on develop (ALLGREEN
  grouping, merge commits, up to 5 entries merged together) with
  `Can enqueue` and `Can merge` as the required status checks.
  The parent repository's `Can see status` check was deliberately
  omitted because no such job exists in this workflow.
* **Scope of command coverage.** `create`, `getconfig`,
  `expand-workers`, `expand-addresses`, `show` and `delete` are
  cheap to chain on one cluster. `update-os` adds an apt
  dist-upgrade across all nodes (~5-10 minutes) for little new
  coverage -- proposed: leave it out initially.
* **kubectl on the runner.** *(Resolved.)* The test script
  downloads a pinned static kubectl from dl.k8s.io with sha256
  verification. The pin tracks the cluster's k3s channel only
  loosely, which is fine for the simple operations the assertions
  use. Note that `k3s delete` itself also needs a kubectl on the
  runner (it unsets the cluster's local kubeconfig entries), and
  `kubectl config unset` was verified to handle the dotted
  `<cluster>.<namespace>` entry names correctly.

## Execution

| Phase | Work | Status |
|-------|------|--------|
| 1. Workflow restructure | Add `merge_group` trigger, name the tiers, add collection jobs mirroring shakenfist/shakenfist | Validated live |
| 2. Deployment test | `tools/ci_deploy_test.sh` plus the merge-gated job on `[self-hosted, vm, debian-12]` | Validated live |
| 3. Queue enablement | Merge queue + required checks (operator, repo settings) | Complete |
| 4. Live validation | `workflow_dispatch` runs until green, fix what they find | Complete |

The first `workflow_dispatch` run after the implementation merged
(actions run 31281565829, 2026-08-08) was green end to end with no
fixes required: the deployment job took 13.5 minutes, all create
phases completed, the LoadBalancer service answered HTTP from the
runner, and expand-workers, expand-addresses and delete all passed
their assertions. Job dispositions matched the design: the
automated reviewer and `can_merge` were skipped, `can_enqueue`
succeeded.

The queue itself was then validated by the pull request carrying
this plan's status update (pull request 23). Its first
`merge_group` run (actions run 31285741554) failed systemically:
a ~12 second MariaDB outage on the under-cloud surfaced as an
auth 503 mid-deployment, which the client treats as fatal
(filed as shakenfist/client-python#360). Triage confirmed the
failure was unrelated to the queued change and the entry was
re-queued as-is; the second run (actions run 31288759648) was
green end to end in 14.5 minutes and merged the pull request
through the queue. The failure was itself useful validation:
the queue correctly rejected the entry, the deployment script's
EXIT trap ran its diagnostics, and fail-fast error handling
ended the run within seconds of the API error rather than
consuming the job timeout.

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
once the `progress-ui-cleanup` branch (pull request 17) has
merged -- it carries the `helm --kubeconfig` fix for the agent
command validation failure that breaks `setup_metallb()`, the
switch to the official metallb chart (the Bitnami images are no
longer published), and the fail-fast error handling that makes
CI hangs impossible. The same ordering applied to the process
documents: `PLAN-TEMPLATE.md` and `PUSH-AUDIT.md` on this branch
describe the `progress.py` module and `Progress` reporter that
pull request 17 introduces. This resolved as planned: pull
request 17 merged first, then this work, and the first
`workflow_dispatch` run was green.

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
* Teach `k3s show` to honour the client's `--json` output flag,
  redacting secret metadata keys (`node_token`, `kubeconfig`,
  `ssh_key`). That gives the CI assertions a stable format to
  parse instead of grepping the human output, and stops
  interactive `show` printing cluster admin credentials.
* Redact the node token from the command line
  `primitives.reap_execute()` echoes when an agent operation
  fails; today a failed worker install prints `K3S_TOKEN=...` to
  the log.

### Bugs fixed during this work

* Every `k3s` subcommand except `list` passed the raw
  `--namespace` option value (None when not given) into namespace
  metadata API calls, which raise `TypeError` on None. All local
  testing had passed `--namespace` explicitly, so `sf-client k3s
  create ci` as documented in the README had never worked. The
  commands now default the namespace to the client's own (found by
  the automated reviewer on the pull request for this plan).

Related context: shakenfist/agent-python#120 (agent rejects
environment variable prefix command lines) is the bug class this
CI exists to catch.

### Back brief

Before executing any step of this plan, please back brief the
operator as to your understanding of the plan and how the work
you intend to do aligns with that plan.
