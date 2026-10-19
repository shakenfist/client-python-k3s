# Testing and CI

## Running the tests locally

```bash
tox -epy3      # unit tests (testtools + stestr)
tox -eflake8   # lint; it diffs against HEAD~, so stage or commit first
pre-commit run --all-files
```

Unit tests live in `shakenfist_client_k3s/tests/` and mock the Shaken
Fist API, the k3s update API and the Longhorn chart index. The test
package refuses every internet socket connection and every lookup of a
name (see `shakenfist_client_k3s/tests/network_guard.py`), and so does
the separate interpreter the Ansible module tests run in. A test that
forgets a stub therefore fails straight away with
`NetworkAccessRefused`, naming the host it tried to reach, and it fails
the same way whether or not the machine has working DNS. Without the
guard it would pass until that site went down (#133). Loopback is
refused too, on purpose: a test which needs a server of its own should
listen on a Unix domain socket. A test which
calls `create()` must stub both release lookups, which
`fakes.stub_release_lookups(self)` does.

The unit tests cannot reach the orchestration path -- whether a
cluster actually assembles is only answerable against a live Shaken
Fist cloud, which is what the merge tier of CI is for.

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

## Merging and releasing

Pull requests merge to `develop` through the merge queue, not with the
merge button directly. A pull request can be queued once `can_enqueue`
passes; the queue then runs `functional-tests.yml` again, merge tier
included, on the commit it is about to merge, and merges only when
`can_merge` passes. That queue run is the one a release counts, so a
change merged around the queue, by an administrator bypassing it,
cannot be released until a later commit has gone through the queue.

The queue builds one entry at a time (`max_entries_to_build: 1` in the
`develop` ruleset). The concurrency groups of `sanity_checks` and
`cluster_deploy` depend on that: a queue run shares a group with
whatever the queue builds next, so that a superseded run is cancelled,
while a re-run of an old queue run keeps a group of its own. Raising
the setting would make each entry cancel the one ahead of it, so
revisit those groups first.

Pushing a `v*` tag runs `release.yml`. Its first job, `ci-gate`,
checks that `functional-tests.yml` succeeded on the tagged commit, in
its merge queue run. How the gate decides is documented with the
action, as
[`require-ci-success`](https://github.com/shakenfist/actions/blob/main/docs/actions.md#require-ci-success),
and it has no override. A run dispatched by the retest comment (see
"Re-running CI on a pull request" below) does not count.

The queue merges an entry only once `can_merge` passes, so a commit
that merged through the queue already has a successful run. A queue
run that fails, flaky or not, gets its entry ejected, and the commit
it built never reaches `develop`: re-running that run cannot make any
tag pass. Re-queue the pull request instead, and tag the commit that
merges. Re-running a queue run matters only when the tagged commit's
own run was re-run after it merged and that later attempt failed,
because the gate judges each run at its latest attempt. Then use
"Re-run failed jobs" on it. "Re-run all jobs" also works, because
`check_paths` compares against the `develop` commit recorded in the
run's event payload rather than the branch, which by then already
contains the queued commit. A queue entry that
touched only `docs/` still counts: its lanes are skipped by
`check_paths`, but the run concludes successfully.

`build` and `build-collection` run only if the gate passed. `build`
checks the wheel twice before uploading it: with the fleet's shared
[`check-dist`](https://github.com/shakenfist/actions/blob/main/docs/actions.md#check-dist)
action, and with `tools/check-dist.sh`, which also holds the wheel to
its member ceiling. The publishing jobs then wait for approval in the
`release` environment.

So tag a commit on `develop` that went through the merge queue, and
wait for its run to finish first. A gate failure means nothing was
signed or published, so every failure is recoverable: the template's
[recovery table](https://github.com/shakenfist/development/blob/main/templates/release-automation/README.md#recovering-from-a-gate-failure)
says what to do for each cause, which is usually moving the tag, or
re-running CI and then the `ci-gate` job. Running `release.yml` by
hand with `workflow_dispatch` skips the gate and the publishing jobs,
and only builds and checks the packages. The one-time setup is in
`RELEASE-SETUP.md`.

## Path filtering

Both tiers are gated on a `check_paths` job which uses
`dorny/paths-filter` to decide whether anything outside `docs/`
changed. A documentation-only pull request or queue entry skips
`sanity_checks` and `cluster_deploy` -- and, through `sanity_checks`,
the automated reviewer -- so it does not spend ephemeral VM capacity
the whole fleet shares on lanes that exercise none of what it touched.

Three details are load bearing:

* It is a filter job, not trigger-level `paths-ignore`. A required
  status check inside a `paths-ignore`'d workflow never reports on a
  filtered pull request, and a required check that never reports
  blocks the merge forever. A skipped one satisfies it.
* `predicate-quantifier: 'every'` is set. `dorny/paths-filter`
  defaults to ANY-match semantics, under which the `'**'` pattern
  matches everything and silently defeats the `'!docs/**'` exclusion.
* On `merge_group`, `base` is the event payload's `base_sha`, not the
  `develop` branch. Once the queue merges an entry, `develop` contains
  it, so a re-run diffing against the branch would find no changes,
  skip both lanes, and pass having tested nothing -- a run the release
  gate would then accept, as described above.

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
