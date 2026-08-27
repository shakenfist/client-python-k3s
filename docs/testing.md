# Testing and CI

## Running the tests locally

```bash
tox -epy3      # unit tests (testtools + stestr)
tox -eflake8   # lint; it diffs against HEAD~, so stage or commit first
pre-commit run --all-files
```

Unit tests live in `shakenfist_client_k3s/tests/` and mock the Shaken
Fist API, the k3s update API and the GitHub releases API. They cannot
reach the orchestration path -- whether a cluster actually assembles is
only answerable against a live Shaken Fist cloud, which is what the
merge tier of CI is for.

## The two CI tiers

`.github/workflows/functional-tests.yml` runs two tiers.

The **smoke tier** runs on every pull request: `sanity_checks` does
flake8, the unit tests, `pre-commit run --all-files`, a requirements
install and an import check, and `automated_reviewer` calls the shared
reviewer workflow.

The **merge tier** runs on `merge_group` events from the develop
branch's merge queue, and on a manual `workflow_dispatch`.
`cluster_deploy` runs `tools/ci_deploy_test.sh`, which creates a real
k3s cluster, verifies it serves a LoadBalancer service, expands its
workers and addresses, and deletes it. It runs on an ephemeral VM
runner, in that runner's own per-job Shaken Fist namespace on the
under-cloud, so everything the test creates dies with the runner. A
full run is 15-25 minutes.

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
