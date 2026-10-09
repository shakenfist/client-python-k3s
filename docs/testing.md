# Testing and CI

## Running the tests locally

```bash
tox -epy3      # unit tests (testtools + stestr)
tox -eflake8   # lint; it diffs against HEAD~, so stage or commit first
pre-commit run --all-files
```

Unit tests live in `shakenfist_client_k3s/tests/` and mock the Shaken
Fist API, the k3s update API and the Longhorn chart index. They cannot
reach the orchestration path -- whether a cluster actually assembles is
only answerable against a live Shaken Fist cloud, which is what the
merge tier of CI is for.

## Checking that the tests would fail

A green suite says the tests pass. It does not say that any of them
would have failed, and for the properties nobody exercises by hand --
a secret that must not reach stderr, a heredoc body that must not be
able to end itself, a file that must be created `0600` -- that is the
only question worth asking. Reading a test cannot distinguish "this
holds" from "this cannot fail".

```bash
python3 tools/mutation-check.py          # all of them
python3 tools/mutation-check.py --only kubeconfig
python3 tools/mutation-check.py -v       # show each test run
```

Each entry in that script makes one stated property false with a
minimal edit, runs the test that is supposed to notice, and requires a
failure. The file is restored from a copy taken immediately before the
edit, so uncommitted work survives -- but do not interrupt a run. It is
not in CI: it rewrites the working tree and costs a test run per
mutation. Run it when the defended properties change, and when
answering a review, so the set visibly grows instead of being
reinvented each round.

**A mutation that survives is the interesting outcome.** It means
either that the property has no test or that the test cannot see the
code you changed. The second really happens: the Ansible module harness
runs the module as a script, so `sys.path[0]` is the tests directory,
the repository root is never on the path, and those tests import the
*installed* package. A bare `stestr run` therefore checks whatever was
last installed, which is why those entries go through `tox -epy3`
instead. That is
[#106](https://github.com/shakenfist/client-python-k3s/issues/106), and
it was found by exactly this script reporting a survivor.

## The two CI tiers

`.github/workflows/functional-tests.yml` runs two tiers.

The **smoke tier** runs on every pull request: `sanity_checks` does
flake8, the unit tests, `pre-commit run --all-files`, a requirements
install and an import check, and `automated_reviewer` calls the shared
reviewer workflow.

The **merge tier** runs on `merge_group` events from the develop
branch's merge queue, and on a manual `workflow_dispatch`.
`cluster_deploy` runs `tools/ci_deploy_test.sh`, which first checks
that a create on the `v1.20` release channel is refused as older than
the release floor without registering the name, then creates a real
k3s cluster, verifies it serves a LoadBalancer service, expands its
workers and addresses, and deletes it. `health --strict` runs after
each create and straight after `expand-workers`, and because `healthy`
requires every node `Ready`, the minimal cluster's run (which nothing
has waited before) and the `expand-workers` one (before the script's
own node wait) also check that those verbs waited for their nodes.
That cluster is built with
non-default node sizes and with `--server-config` and `--agent-config`,
and the script asserts that the sizes reached both the cluster metadata
and Shaken Fist, that Traefik and servicelb are absent, that each role's
nodes carry its label (a worker added by `expand-workers` included), and
that the control plane carries the default `NoSchedule` taint. A second,
minimal cluster with one worker asserts the `node-taint: []` opt-out,
and is the positive control for the absence checks: Traefik and its
`svclb-traefik-*` pods have to appear there. It is built with
`--network` on a network the script makes, which its delete has to
leave in place. Some of the node
customisation behaviour is covered by unit tests only: the refusal of
keys the plugin owns, the configuration files on a node and the order
k3s reads them in, and the zero-worker cluster that is never tainted.
Last, immediately before its delete, the minimal cluster is damaged on
purpose by `tools/ci_health_signals.py`: a pod OOM-killed at its memory
limit, a SIGKILLed k3s-agent, a stopped kubelet, an etcd snapshot, and a
disk filled to 3% free, which it leaves full for the delete to remove.
It asserts that `health()` reports each, that `health --strict` exits 1
while the kubelet is stopped and 0 otherwise, and that a fresh cluster's
readings all have the shape they should.
The script runs on an ephemeral VM runner, in that runner's own
per-job Shaken Fist namespace on the under-cloud, so everything the
test creates dies with the runner. A full run is 20-30 minutes.

`can_enqueue` and `can_merge` are the required status checks.
`can_enqueue` reports on pull requests and `can_merge` on merge queue
entries; each is skipped for the other event, which GitHub treats as
success. Both pass when every job they depend on either succeeded or
was skipped.

## Path filtering

Both tiers are gated on a `check_paths` job which uses
`dorny/paths-filter` to decide whether anything outside `docs/`
changed. A documentation-only pull request or queue entry skips
`sanity_checks` and `cluster_deploy` -- and, through `sanity_checks`,
the automated reviewer -- so it does not spend ephemeral VM capacity
the whole fleet shares on lanes that exercise none of what it touched.

Two details are load bearing:

* It is a filter job, not trigger-level `paths-ignore`. A required
  status check inside a `paths-ignore`'d workflow never reports on a
  filtered pull request, and a required check that never reports
  blocks the merge forever. A skipped one satisfies it.
* `predicate-quantifier: 'every'` is set. `dorny/paths-filter`
  defaults to ANY-match semantics, under which the `'**'` pattern
  matches everything and silently defeats the `'!docs/**'` exclusion.

`check_paths` is in the `needs:` of both collection jobs. Without
that, a failure of the filter itself would skip the lanes and leave
the required check green having tested nothing.

## Content scanning is deliberately unfiltered

`.github/workflows/supply-chain.yml` runs gitleaks over the history
and lints the agent context with skillsaw, and is not path filtered.
A credential pasted into a documentation code sample is still a
credential, and prose is exactly where an instruction aimed at an
agent would be hidden -- so these two checks have to run on the
changes every other lane skips.

## Re-running CI on a pull request

Comment `@shakenfist-bot please retest` to dispatch the functional
tests against the pull request's branch, or `@shakenfist-bot please
re-review` to request another automated review. Both are restricted
to collaborators with write access, and neither works on a pull
request from a fork.
