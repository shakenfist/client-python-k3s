# Audit findings: tests

Step 6d of
`PLAN-library-api-and-collection-phase-06-push-audit.md`: the *Tests*
section of `PUSH-AUDIT.md` and the `functional-test-coverage v1`
shared block, over the 74 files in `scope-files.txt`.

Read-only. Nothing in this file was fixed; the suite results are step
6a's, in `verification.md`, and were not re-run.

## Summary

| Action | Count |
|---|---|
| fix | 0 |
| document | 0 |
| consider | 6 |
| none | 9 |
| **Total** | **15** |

No finding gates the phase. That is the honest result rather than a
courtesy: the suite passes with 461 tests and no skips, every one of
the twelve subcommands has unit coverage, every exception class and
every one of the twenty named factories in `exceptions.py` is
constructed by a test, and -- the question the shared block cares
most about -- **both verbs phase 3 added and all four flags it added
have a functional assertion in `tools/ci_deploy_test.sh` that would
have failed before the change and passes after**. The six `consider`
items are cheap additions to coverage that already exists, not
repairs.

## Functional coverage census

The whole of the functional question, per the "In this project" note:
`tools/ci_deploy_test.sh` is the only place a `k3s` subcommand runs
for real, from the merge queue and `workflow_dispatch`
(`.github/workflows/functional-tests.yml:208-256`). Line numbers
below are in that script.

### The twelve subcommands

| Subcommand | Exercised by `ci_deploy_test.sh`? | Unit coverage? |
|---|---|---|
| `create` | Yes -- twice. Full cluster at `:138` (1 CP, 2 workers, 2 addresses, `--manifest`), minimal at `:274` (`--no-metallb --no-longhorn --no-kubeconfig`) | Yes. `test_progress.K3sCreateSmokeTestCase` (11 tests, real create against `fakes.FakeClusterClient`), `test_library_api` (5 classes), `test_commands.CreateNodeSizingOptionsTestCase`, `test_cli_errors` |
| `delete` | Yes -- `:247` (asserts delisted and no instances left, `--all` so error-state nodes count) and `:317` (`--no-kubeconfig`, sha256 before/after) | Yes. `test_cluster.DeleteInterruptedClusterTestCase`, `DeleteReleasesTheNameBeforeTheMetadataTestCase`, `test_library_api.OptionalKubeconfigTestCase` |
| `expand-addresses` | Yes -- `:239` (count before/after via `k3s show`) **and the refusal path** at `:303` on a cluster really built without metallb | Yes. `test_cluster.ExpandAddressesWithoutMetallbTestCase`, `test_commands.CommandWiringTestCase` |
| `expand-workers` | Yes -- `:215`, then `wait_for_nodes 4` | Yes. `test_cluster.ExpandWorkersTestCase` (5 tests) |
| `getconfig` | Yes -- `:159` and `:295`, output redirected to a file `kubectl` then uses | **Error paths only.** `test_cli_errors` covers unknown-cluster and no-kubeconfig; the happy path is covered at library level (`test_library_api.ClusterAccessorTestCase`) and functionally, not at the CLI. See F2 |
| `health` | Yes -- `:183` and `:293`, both `--strict`, which is what makes it an assertion | Yes, extensively. `test_cluster.HealthTestCase` (10), `HealthProbeIsSkippedTestCase` (8), `test_commands.HealthCommandTestCase` (10) |
| `list` | Yes -- `:248` and `:318`, as the post-delete assertion | Yes. `test_commands.ListOutputTestCase` |
| `query-k3s-version` | **No** | Yes. `test_commands`, `test_cli_errors` (HTTP error, unknown channel) |
| `query-longhorn-version` | **No** | Yes. `test_cli_errors` (HTTP error, no parsable release) |
| `remove-worker` | Yes -- `:226`, on a deliberately mixed-case cluster name (`:20`) so the node name and the instance name differ, then asserts the node object is gone and the uuid left the metadata | Yes. `test_cluster.RemoveWorkerTestCase` (10), `RemoveWorkerEdgeCaseTestCase` (6), `RemoveWorkerNodeNameTestCase`, `DrainFailureTestCase` (7), `test_commands.RemoveWorkerCommandTestCase` |
| `show` | Yes -- `:85`, `:95`, `:232`; the script parses its `worker_nodes` and `routed_addresses` lines, so the output format is asserted | Partly. `test_cluster.ShowReportsStateTestCase`, `ShowReportsNodeSizesTestCase` cover `Cluster.show()`; the CLI's own `'    %s = %s'` rendering has no unit test and is covered only by the greps above |
| `update-os` | **No** | Yes. `test_commands.CommandWiringTestCase.test_update_os_updates_every_node`, `test_cluster.InterruptedClusterVerbsTestCase.test_update_os_is_allowed`, `test_cli_errors` |

Nine of twelve exercised; three not. Of the three,
`query-k3s-version` and `query-longhorn-version` call
`primitives.get_k3s_release()` and `get_longhorn_release()`, both of
which **do** run for real in CI because `Cluster.create()` calls them
(`cluster.py:1481`, `cluster.py:1297`) -- including the live
`update.k3s.io` and `api.github.com` fetches and the namespace
metadata cache write. Only the Click wiring and the `print()` are
uncovered. `update-os` is uncovered entirely; see F1.

### Create's flags, which is where phase 3's user-visible work landed

| Flag | Exercised? |
|---|---|
| `--manifest` | Yes, `:126-140`, with a manifest carrying `$HOME`, backticks, `$(...)` and escaped quotes, compared byte for byte after k3s applies it (`:165-178`) |
| `--kubeconfig` (default on) | Yes, positive half asserted at `:152` |
| `--no-kubeconfig` | Yes, `:276`, by sha256 before/after plus a name grep, on both `create` and `delete` (`:317`) |
| `--no-metallb` | Yes, `:276`, and its consequence asserted by the `expand-addresses` refusal at `:303` |
| `--no-longhorn` | Yes, `:276`, with the cluster then proved healthy and answering `kubectl` (`:293-297`) |
| `--control-plane-count`, `--worker-count`, `--metal-address-count` | Yes (1/2/2 and 1/0/0) |
| `--namespace`, `--network`, `--release-channel`, `--refresh-version-cache`, `--sshkey` | No. All pre-date phase 1; all unit tested |
| node sizing (`--control-plane-cpus` etc.) | No -- and **out of scope**: added by `b6c4b7b` under the `node-customisation` plan, not by these ten merges |

### Library verbs

`docs/library-api.md:86-96` names nine `Cluster` methods as the
library API. Each is the body of the CLI verb beside it, so each is
functionally exercised exactly as far as that verb is:

| Verb | Functional route | Unit coverage |
|---|---|---|
| `create()` | via `k3s create` | Yes (above) |
| `get_kubeconfig()` | via `k3s getconfig` | Yes |
| `show()` | via `k3s show` | Yes |
| `health()` | via `k3s health --strict` | Yes |
| `delete()` | via `k3s delete` | Yes |
| `expand_workers()` | via `k3s expand-workers` | Yes |
| `remove_worker()` | via `k3s remove-worker` | Yes |
| `expand_addresses()` | via `k3s expand-addresses` | Yes |
| `update_os()` | **none** | Yes |
| `primitives.list_clusters()` | via `k3s list` | Yes |
| `primitives.get_k3s_release()` | inside `create` | Yes, 8 tests |
| `primitives.get_longhorn_release()` | inside `create` | Yes, 5 tests |
| `client.make_client()` | **none** | Yes, 6 tests (`test_client.py`) |

`make_client()` is the one library entry point no functional tier
reaches, and it is phase 2's deliverable. See F3.

### The Ansible module and the build tooling

| Thing | Functional? | Unit |
|---|---|---|
| `collection/plugins/modules/sf_k3s_cluster.py` | **No.** `ansible-lint` in pre-commit is static; `release.yml` builds and publishes the collection but never runs a play | Yes, heavily: `test_ansible_module.py`, 29 tests in 12 classes, each a real subprocess |
| `tools/build-collection.py` | Only on a tag push, in `release.yml:84` | `semver_from()` only, 6 tests. `main()` and `collection_version()` untested. See F10 |
| `tools/check-dist.sh` | Yes -- `tools/check-wheel-build.sh` runs in `sanity_checks` on every PR | Yes, 11 tests |

The module's missing integration coverage is **already
shakenfist/client-python-k3s#89**; per decision 6 it is a comment on
that issue, not a new finding. Confirmed here rather than
rediscovered.

## Checklist questions answered

### Is there unit test coverage for the changes, normal and adversarial, especially around external API responses?

Yes for normal cases, and largely yes for adversarial ones. The three
external shapes the checklist names, taken one at a time:

**The k3s update API** (`primitives.get_k3s_release`,
`primitives.py:46-110`). Covered well.
`test_primitives.GetK3sReleaseTestCase` has 8 tests: a channel with
no `latest` key, an entry with no `name` key, an unresolvable
channel, an unknown channel, a fresh cache avoiding the fetch, a
cache that is not a dict, a cache dict missing `releases`, a response
with no `data` key at all, and a 500. The "no `data` key" test also
asserts the empty parse is *not* persisted, which is the right
assertion -- a transient upstream error poisoning a shared namespace
cache for 24 hours is the expensive failure here. Not covered: a
non-JSON body (F7).

**The GitHub releases API** (`primitives.get_longhorn_release`,
`primitives.py:113-181`). Covered more thinly.
`GetLonghornReleaseTestCase` has 4 tests: prereleases and unparsable
tags skipped, an empty list, a cache missing `latest`, and a 500. Not
covered: any malformed or unexpected *shape* -- see F6, which is a
real asymmetry with the k3s lookup rather than a style point, because
the two functions index their payloads differently.

**Agent operation payloads.** Covered best of the three, and
deliberately: `test_cluster.AgentOperationEndingsTestCase` and
`AwaitExecuteTimeoutTestCase` together cover `expired`, `deleted`,
`error`, a state this version has never heard of (`'reticulating'`,
asserted to be waited for rather than treated as finished, and warned
about once), a stalled command, a bystander's failed operation not
aborting our wait, our own operation's failure aborting it, and a
wall-clock step not moving a monotonic deadline.
`AGENT_OP_KNOWN_STATES` is treated as open everywhere it matters. The
gap is one asymmetry on the success path: F8.

### All tests should pass -- run `tox -epy3`

Step 6a: exit 0, 461 tests, 0 failures, 0 skipped. Not re-run here.

### What tests are skipped? Could we reduce that number?

**Nothing is skipped.** Step 6a reports 0 skips. There are five
`skipTest()` call sites, all conditional on an environment fact that
holds in CI and in a development checkout:

| Site | Condition | Reducible? |
|---|---|---|
| `test_build_collection.py:47` | `tools/build-collection.py` absent | No. Fires only in an installed copy of the package, which ships `tests/` but not `tools/` |
| `test_check_dist.py:42` | `tools/check-dist.sh` absent | No, same reason |
| `test_ansible_module.py:142` | `collection/` absent | No, same reason |
| `test_cluster.py:1557` | the subprocess could not be forced out of UTF-8 mode | No. The test spawns a child with `LC_ALL=C`, `PYTHONUTF8=0`, `PYTHONCOERCECLOCALE=0` and skips only if that still yields a UTF-8 preferred encoding; on Debian it does not, so the test runs |
| `test_cluster.py:1785` | no `/bin/sh` | No, and it is correct to guard it |

So the number cannot usefully be reduced: three of the five are the
price of the suite also working from an installed wheel, which is a
property worth more than three skip lines it never takes.

### Run `tox -eflake8` and confirm clean output

Step 6a: exit 0, but it diffs against `HEAD~1`, which on this branch
touches only plan files, so the run proves nothing. 6a therefore also
ran flake8 directly over all 23 scope `.py` files at
`--max-line-length=120`: exit 0, no output. That is the meaningful
result. Recorded there, not re-run here.

### Run `pre-commit run --all-files`

Step 6a: exit 0, all four hooks passed (skillsaw, actionlint,
shellcheck, ansible-lint).

### Does the change alter orchestration behaviour that unit tests cannot reach? If so, has it been exercised against a live cluster?

Yes, and yes for everything phase 3 added. Three behaviours in the
scope are genuinely unreachable from a unit test, and all three have
a live-cluster assertion:

1. **Whether a quoted heredoc survives the agent's command
   transport.** The unit tests
   (`test_cluster.ManifestHeredocTestCase`,
   `HeredocDelimiterTestCase`) run the generated command through
   `/bin/sh`, which pins the quoting but not the transport.
   `ci_deploy_test.sh:126-178` stages a manifest full of
   metacharacters and compares what k3s parsed, byte for byte. This
   is the single best functional assertion in the script.
2. **Whether the k3s node name `remove-worker` computes is the one
   k3s registered.** Unit tests pin the lowercasing
   (`RemoveWorkerNodeNameTestCase`); only a real cluster can say the
   lowercased name matches. `CLUSTER=ciMixed` at `:20` exists purely
   to make the two names differ.
3. **Whether skipping metallb, longhorn or the kubeconfig leaves a
   working cluster.** `:274-330`.

### Which functional test would have failed before this change and passes after?

The shared block's central question, answered per phase:

- **Phase 3 (`d51cf59`, the big one):** named, for every new
  behaviour. `git diff d51cf59^1 d51cf59 -- tools/ci_deploy_test.sh`
  is +170/-2 and adds the manifest round trip, both `health --strict`
  calls, the `remove-worker` sequence, the whole minimal-cluster
  section, the `--no-kubeconfig` sha256 comparisons on create and
  delete, and the `expand-addresses` refusal. Each would have failed
  before `d51cf59`, because the flag or verb did not exist. This is
  the model answer and it is worth saying plainly.
- **Phase 1 (`7fb29e5`):** none added, and correctly so. Phase 1 is a
  refactor whose stated contract is that user-visible behaviour does
  not change, pinned by thirteen golden `--help` fixtures
  (`tests/cli_contract/`) and `test_cli_errors.py`. The pre-existing
  functional tier re-ran `create`/`delete`/`expand-*`/`getconfig`/
  `show`/`list` through the refactored code, which is the coverage.
  One user-visible change it did make -- errors moving from stdout to
  stderr -- is unit tested only, though `getconfig > file` at `:159`
  would break if an error reached stdout.
- **Phase 2 (`1c32d12` and four others):** **none, and that is a
  finding.** F3.
- **Phase 4 (`2506c19`, `d2e43d1`):** packaging. Covered by
  `tools/check-wheel-build.sh` in `sanity_checks` and by
  `release.yml` -- the right tier for the change.
- **Phase 5 (`eb248bd`):** the collection. Its only change to
  `ci_deploy_test.sh` is a doc-path typo fix. The collection has no
  functional tier at all, which is #89.

### Is any error path or argument-validation branch reachable from outside the process untested?

Mostly no. Every exception class in `exceptions.py` (15) and every
classmethod factory (20) is constructed by a test, and
`test_exceptions.TotalAttributesTestCase` additionally asserts that
each multi-reason exception's `FIELDS` tuple is fully populated from
both directions, which is a stronger guard than per-factory tests.
`test_cli_errors.py` walks fifteen failure paths twice: once through
the subcommand object (to see the exception and assert **both**
streams are empty) and once through the group (to see the text and
exit code on stderr).

The branches found with no test:

- `_probe_k3s_api`'s "completed but recorded no result"
  (`cluster.py:918-921`) -- F9.
- `_bind_new_cluster_context`'s "the named namespace already exists"
  branch (`__init__.py:79-82`). Only the create-it half is tested
  (`test_commands.CreateNamespaceNoticeTestCase`). Informational; the
  branch is three lines and correct by inspection.
- `k3s show`'s CLI rendering loop (`__init__.py:321-324`) and
  `k3s getconfig`'s `print()` (`__init__.py:307`) -- F2.
- `expand-workers --worker-count 0`/negative and
  `expand-addresses --address-count 0`/negative -- F14, an occurrence
  for #96.
- `separated_runner()`'s click 8.0/8.1 branch
  (`test_cli_errors.py:92-94`) -- F13, an occurrence for #82.

### Does the test suite mock the system under test, or the boundary?

The boundary, correctly, with no exception I would call wrong.

The 93 `mock.patch` targets are: `time.sleep` (37),
`requests.request` (18), `sys.stdout` (15), `subprocess.run` (7),
`shutil.which` (5), `os.environ` (4), two clocks, and three
`apiclient.Client` patches -- every one of which is a clock, the
network, the local filesystem, or the HTTP client. `tests/fakes.py`
is a hand-written stand-in at the `apiclient` surface, written (per
its own docstring) because a `MagicMock` makes every wait loop spin
forever -- so the orchestration really runs.

For the Ansible module, the harness is the shape the brief describes,
verified: `module_harness.py:359` patches only `apiclient.Client`, and
`make_client`, `Cluster`, `Progress` and `CollectingReporter` all run
for real in a real subprocess whose fd 1 belongs to the module. The
second replacement, `install_fake_create()`
(`module_harness.py:273-295`), stands in for `Cluster.create()` --
but `create()` is a collaborator of the module under test, not the
module itself, the stand-in drives the *real* `Progress` through the
*real* reporter so the property under test (create emits a lot of
output, and none of it may reach fd 1) is preserved, and tests that
do not need create to succeed leave the real one in place. That is
the correct call, and the docstring says why.

Elsewhere, `mock.patch.object(Cluster, ...)` appears eighteen times.
Each is a sibling method, used either as a spy on a branch
(`setup_metallb`/`setup_longhorn`, to see which of the two a
`--no-longhorn` create skipped) or as a failure injector at a precise
point (`install_control_plane` raising, to prove the metadata was
written first). In every case a companion test runs the same path
with nothing patched -- `test_progress.py:773-791` creates with
`--no-metallb`, `--no-longhorn` and both, for real, against the fake
client. Spy plus real-run is the right pair, and it is present.

## Findings

### F1: `update-os` has no functional coverage, and runs `apt-get dist-upgrade` on real nodes

- **File**: `tools/ci_deploy_test.sh` (absent); the verb is
  `shakenfist_client_k3s/cluster.py:2499` and
  `shakenfist_client_k3s/__init__.py:503`
- **Action**: consider
- **Claim**: `update-os` is the only one of the twelve subcommands
  whose body is never run against a real cluster, and its body is
  `apt-get update && apt-get dist-upgrade -y` on every node -- the
  kind of work a unit test can only assert was *requested*.
- **Evidence**: `ci_deploy_test.sh` contains no `update-os`
  invocation. `Cluster.update_os()` (`cluster.py:2499-2518`) calls
  `instance_os_update()` (`cluster.py:988-996`), which hands those two
  commands to `execute_and_await()`. The unit tests
  (`test_commands.py:351`, `test_cluster.py:1062`) assert the right
  instance uuids are passed and that the verb is permitted on an
  interrupted cluster; nothing asserts the commands succeed on a
  Debian guest, that `dist-upgrade` does not need a tty, or that the
  agent's command transport tolerates a multi-minute command. Phase 1
  moved this verb from a module function into a `Cluster` method
  (`7fb29e5`), so the refactor rests on unit tests alone.
- **Proposed change**: one invocation on the minimal cluster between
  `:297` and `:299`, where a one-node cluster is already up and
  healthy -- `sf-client k3s update-os "${MINIMAL_CLUSTER}"` followed
  by `sf-client k3s health "${MINIMAL_CLUSTER}" --strict` to prove the
  node survived it. No new tier, no new cluster. The honest cost is
  time: a `dist-upgrade` on a fresh Debian image is a few minutes
  added to an 80-minute budget, and it introduces a dependency on the
  guest's apt mirror, which is a new flake source. If that trade is
  not wanted, the alternative is to say so in a tracking issue rather
  than leave the gap unstated.

### F2: `getconfig` and `show` have no unit test for what they print

- **File**: `shakenfist_client_k3s/__init__.py:307`,
  `shakenfist_client_k3s/__init__.py:321-324`
- **Action**: none
- **Claim**: Both commands' only job at the CLI layer is formatting,
  and neither has a unit test that reads the formatted output; both
  are covered functionally instead.
- **Evidence**: `grep getconfig` across `tests/` finds only
  `test_cli_errors.py` (unknown cluster, unfinished cluster) and the
  golden `--help` fixture. `show`'s `'    %s = %s'` loop has no test
  either -- `test_commands.NamespaceDefaultingTestCase:40` invokes
  `show` but asserts on which namespace was fetched. Both *are*
  asserted functionally: `ci_deploy_test.sh:159` pipes `getconfig`
  into a file `kubectl` then uses, and `:86`/`:96` grep `show`'s
  output for `worker_nodes` and `routed_addresses`, so the format is
  load-bearing in the merge tier. `list`, by contrast, has
  `ListOutputTestCase` for exactly this.
- **Proposed change**: a `ListOutputTestCase` equivalent for each: one
  test asserting `getconfig` prints the kubeconfig and nothing else,
  one asserting `show` prints `Cluster metadata:` then one
  `    key = value` line per key. Cheap, but the behaviour is already
  guarded where it matters, so this is informational.

### F3: phase 2's deliverable has no functional coverage at all

- **File**: `shakenfist_client_k3s/client.py:37`,
  `shakenfist_client_k3s/__init__.py:33` (the `ctx.obj['CLIENT']`
  read)
- **Action**: consider
- **Claim**: Phase 2 exists to make client construction correct --
  `make_client()` for library callers, and the removal of the plugin's
  own `apiclient.Client` so that `sf-client`'s `--apiurl`, `--key` and
  `--namespace` are honoured. Asked "which functional test would have
  failed before this change and passes after", the answer is none: no
  tier runs `make_client()`, and no tier passes the root options.
- **Evidence**: `git diff 1c32d12^1 1c32d12 --
  tools/ci_deploy_test.sh` is empty, as are the other four phase 2
  merges. `ci_deploy_test.sh` invokes `sf-client` with no root options
  anywhere, relying on `~/.shakenfist` discovery through
  `shakenfist_client.main`'s own callback, so the bug phase 2 fixed --
  a second client built from discovered configuration -- would be
  **invisible** to this tier, which is precisely the failure mode
  `test_root_options.py:1-21` describes ("an operator who configures
  by environment variable got the right credentials by accident").
  `make_client()` is reached only by the Ansible module, whose own
  coverage is #89, and in `test_client.py` with `apiclient.Client`
  patched out -- so no code has ever called
  `apiclient.Client(suppress_configuration_lookup=True, ...)` against
  a live API in CI.
- **Proposed change**: two additions to `ci_deploy_test.sh`, both
  read-only and both against credentials the runner already carries.
  First, after the install at `:104`, resolve the runner's own
  `~/.shakenfist` values and re-run one harmless command with them
  passed explicitly -- `sf-client --apiurl "$u" --key "$k" --namespace
  "$n" k3s list` -- which fails if the plugin ever starts building its
  own client again. Second, a `python3 -c` one-liner calling
  `make_client()` with no arguments and printing `client.namespace`,
  which is the only way to prove auto-discovery resolves a real config
  file into a working client. Together that is under ten lines and no
  new infrastructure. It is the largest real gap this lens found.

### F4: nothing keeps `ci_deploy_test.sh` in sync with the command list

- **File**: `tools/ci_deploy_test.sh`,
  `shakenfist_client_k3s/__init__.py:131`
- **Action**: consider
- **Claim**: A subcommand added later gets functional coverage only if
  its author remembers to add it, and nothing fails if they do not --
  which is how the three gaps in the census above came to exist
  unrecorded.
- **Evidence**: no test in `tests/` reads `tools/ci_deploy_test.sh`
  (`grep -rn ci_deploy_test shakenfist_client_k3s/` is empty). The
  census in this file had to be assembled by hand, and the only reason
  it is checkable is that it now exists in writing.
  `test_check_dist.py` shows the project is already willing to unit
  test a shell script's behaviour, so the pattern is established.
- **Proposed change**: a test that reads `tools/ci_deploy_test.sh`,
  and for each name in `shakenfist_client_k3s.k3s.commands` asserts
  the string `k3s <name>` appears in it, unless the name is in an
  explicit `NO_FUNCTIONAL_COVERAGE` frozenset whose entries each carry
  a comment saying why. That turns this census into an enforced
  invariant and makes the next gap a failing test rather than an audit
  finding. Skip the way `test_check_dist.py` does when `tools/` is
  absent.

### F5: `test_cli_contract.SUBCOMMANDS` is hand-maintained

- **File**: `shakenfist_client_k3s/tests/test_cli_contract.py:23-36`
- **Action**: consider
- **Claim**: The golden-fixture list is a literal, so a new
  subcommand's own `--help` fixture is not required by anything.
- **Evidence**: `test_subcommand_help` loops over the literal
  `SUBCOMMANDS`. Nothing compares it to
  `shakenfist_client_k3s.k3s.commands`. The gap is *partial* rather
  than total: adding a command changes the group's own `--help`, so
  `test_group_help` fails against `cli_contract/group.txt` and forces
  the author to the docstring at `:50-54`, which tells them to add the
  name. So this is a belt-and-braces finding, not a hole.
- **Proposed change**: one assertion in a new test --
  `self.assertEqual(set(SUBCOMMANDS), set(shakenfist_client_k3s.k3s.commands))`
  -- which makes the omission name itself instead of arriving as a
  `group.txt` diff the author has to interpret.

### F6: the GitHub releases lookup has no unexpected-shape test, and indexes its payload where the k3s lookup guards

- **File**: `shakenfist_client_k3s/primitives.py:151-155`
- **Action**: consider
- **Claim**: `get_longhorn_release()` reads `reldata['prerelease']`
  and `reldata['tag_name']` with hard subscripts while iterating
  whatever `r.json()` returned, so a response that is not a list of
  release objects raises `TypeError` or `KeyError` rather than
  `ReleaseLookupError` -- and unlike the k3s lookup, nothing tests
  that case.
- **Evidence**: `get_k3s_release()` guards the equivalent read --
  `if 'name' not in reldata or 'latest' not in reldata: continue`
  (`primitives.py:86-89`) -- and raises
  `ReleaseLookupError.no_usable_k3s_channels` for a payload with no
  `data` key (`primitives.py:96-98`), covered by
  `test_primitives.py:128 test_response_missing_data_raises`.
  `get_longhorn_release()` has neither. GitHub answers some conditions
  with a JSON *object* (`{"message": ..., "documentation_url": ...}`)
  rather than an array; iterating that yields key strings, and
  `'message'['prerelease']` is `TypeError: string indices must be
  integers`. `GetLonghornReleaseTestCase` tests an empty list
  (`:212`) but no malformed one. The checklist names this API as one
  that changes shape over time, so this is the case it was asking
  about.
- **Proposed change**: skip any `reldata` that is not a dict or that
  lacks `prerelease`/`tag_name`, then let the existing
  `latest is None` guard (`primitives.py:172`) raise
  `no_parsable_longhorn_release`, with two tests: a response that is a
  dict, and a list containing one well-formed and one malformed entry.
  Honest cost: a guard plus a test, not a one-liner -- so it is a
  legitimate thing for 6g to decline and file, as long as it says so.

### F7: neither release lookup survives a non-JSON body

- **File**: `shakenfist_client_k3s/primitives.py:79`,
  `shakenfist_client_k3s/primitives.py:148`
- **Action**: none
- **Claim**: `r.json()` is called with no guard on both paths, so an
  HTTP 200 carrying HTML -- a captive portal, a proxy error page, a
  CDN interstitial -- produces a `JSONDecodeError` traceback rather
  than a `ReleaseLookupError`.
- **Evidence**: both call sites check `r.status_code` first
  (`primitives.py:75`, `:144`) and then call `r.json()` unguarded.
  `_fake_response()` in `test_primitives.py:27` always returns a
  `json()` that succeeds, so no test can reach this. The status check
  is a real and correct defence against the common case; this is the
  residue.
- **Proposed change**: wrap each `r.json()` and raise a new
  `ReleaseLookupError` reason naming the product, the URL and the
  first 512 bytes of the body -- the shape
  `no_usable_k3s_channels` already uses (`exceptions.py:587-596`) --
  with one test per lookup. Lower value than F6 because the failure is
  a loud traceback on a read-only command rather than a wrong answer,
  so this belongs in an issue rather than in this phase.

### F8: the agent-operation success path indexes `results['0']` where three sibling readers guard it

- **File**: `shakenfist_client_k3s/cluster.py:763`,
  `shakenfist_client_k3s/cluster.py:716`
- **Action**: consider
- **Claim**: `reap_execute()` and `await_fetch()` read
  `aop['results']['0'][...]` with hard subscripts, while
  `_probe_k3s_api()`, `_agent_op_error()` and `_describe_agent_op()`
  all treat the same field as possibly absent or `None`. Agent
  operation payloads are one of the three shapes the checklist names,
  and this is an internal inconsistency about whether that shape is
  trusted.
- **Evidence**: `_agent_op_error()` reads
  `aop.get('results', {}) or {}` (`cluster.py:561`);
  `_describe_agent_op()` reads `aop.get('results', {}) or {}`
  (`primitives.py:187`); `_probe_k3s_api()` reads
  `(aop.get('results') or {}).get('0')` and has a dedicated branch for
  the empty case, with a comment saying "An operation which completed
  without recording a result for its only command should not happen,
  and health() is the one method which must not turn 'should not
  happen' into a traceback" (`cluster.py:915-921`). `reap_execute()`
  then does `aop['results']['0']['return-code']` (`cluster.py:763`)
  and `await_fetch()` does `aop['results']['0']['content_blob']`
  (`cluster.py:716`). No test covers either unguarded read against a
  `complete` operation with empty results -- `fakes.py:171` and `:177`
  always populate `'0'`.
- **Proposed change**: the same guard the health path already has,
  raising `AgentOperationError` (which already carries a `results`
  field and renders it, per `test_exceptions.py:111`) with a reason
  saying the operation completed without recording a result, plus one
  test per call site. This is a guard and a message, roughly ten lines
  -- not a one-liner. The consequence today is a traceback mid-create
  rather than a wrong cluster, so it is reasonable for 6g to file it;
  what is not reasonable is leaving the inconsistency unnamed.

### F9: `_probe_k3s_api`'s "recorded no result" branch has no direct test

- **File**: `shakenfist_client_k3s/cluster.py:918-921`
- **Action**: none
- **Claim**: The branch is reachable from outside the process -- it
  needs only an agent operation reported `complete` with no results --
  and no test enters it.
- **Evidence**: `grep 'recorded no result'` across `tests/` finds one
  hit, `test_cluster.py:2809`, and it is an `assertNotIn`: the test
  `test_an_unrecognised_ending_is_named_rather_than_mislabelled`
  asserts this message is *not* what an unrecognised state produces.
  So the branch's existence is asserted by a test that must not reach
  it, and nothing asserts what it says when it does.
- **Proposed change**: one test using `fakes.HealthClient` with the
  probe operation's `results` set to `{}`, asserting
  `report['api']['probed']` is True, `answered` is False, and `error`
  names the missing result. Eight lines, no code change. Named here
  because the block asks for untested outside-reachable branches to be
  named; taking it is 6g's call.

### F10: `tools/build-collection.py`'s `main()` runs only on a tag push

- **File**: `tools/build-collection.py:89-125`
- **Action**: consider
- **Claim**: The only part of the collection build that is tested is
  the pure version conversion. The part that rewrites `galaxy.yml`,
  invokes `ansible-galaxy collection build` and reverts the rewrite
  runs for the first time in the job that publishes a release.
- **Evidence**: `test_build_collection.py` has 6 tests, all against
  `semver_from()`; its own docstring says it is "driven by importing
  the script rather than running it". `main()` and
  `collection_version()` have no test. The script is invoked only at
  `.github/workflows/release.yml:84`, inside the tag-triggered
  `build-collection` job; `sanity_checks` does not run it. Phase 5's
  own history is the argument for caring: survey finding 3 records a
  release that failed in `publish-collection` and had to be recovered
  by hand, and the `finally` revert at `build-collection.py:124-125`
  exists because an earlier version of this branch committed a
  machine-chosen version by accident. A regression in the `re.sub` at
  `:116`, in the `galaxy_bin` resolution at `:96-98`, or in the
  revert, is discovered at the moment it is most expensive.
- **Proposed change**: a step in `sanity_checks` that installs
  `ansible-core` and runs `tools/build-collection.py`, then asserts
  `dist-collection/` holds exactly one tarball and that
  `git diff --exit-code collection/galaxy.yml` is clean -- the second
  half being the assertion that the revert worked. About five lines
  plus the `ansible-core` install, in a job that already builds two
  venvs. Alternatively a unit test that runs `main()` in a temporary
  copy of `collection/` with a stub `ansible-galaxy`, which needs no
  CI change but proves less.

### F11: the Ansible module has no integration coverage -- #89

- **File**: `collection/plugins/modules/sf_k3s_cluster.py`
- **Action**: none
- **Claim**: Confirmed, not rediscovered. No tier runs the module
  through `ansible-playbook` against anything.
- **Evidence**: `ansible-lint` in `.pre-commit-config.yaml` is static;
  `release.yml` builds and publishes the collection; there is no
  `molecule/`, no `tests/integration/`, and `ci_deploy_test.sh` never
  installs the collection. The 29 unit tests in
  `test_ansible_module.py` run the module in a real subprocess against
  a faked `apiclient.Client`, which is the correct shape and is not a
  substitute. Per decision 6 this is a comment on
  shakenfist/client-python-k3s#89, which already records it.
- **Proposed change**: none here. It needs a live cluster and an
  Ansible lane in the merge tier -- a new CI tier, which this phase
  cannot add. #89 is the right home.

### F12: the `requires_ansible '>=2.15.0'` floor is never exercised -- #91

- **File**: `collection/meta/runtime.yml`, `tox.ini:24-25`
- **Action**: none
- **Claim**: Confirmed. `tox.ini` installs `ansible-core` unbounded,
  so the resolver always picks the newest.
- **Evidence**: `tox.ini:15-23` says so itself, at length, and names
  #91 -- including that two behaviour changes inside the advertised
  range have already been found (2.21 made both `invocation` and
  traceback capture opt-in) and that closing it needs an older Python
  than this environment has. Nothing to add beyond confirming it still
  holds.
- **Proposed change**: none here; #91.

### F13: the declared `click >= 8.0.0` floor is never resolved, so a test helper's compatibility branch is dead

- **File**: `shakenfist_client_k3s/tests/test_cli_errors.py:74-94`,
  `pyproject.toml` (`click >= 8.0.0`)
- **Action**: none
- **Claim**: `separated_runner()` exists to work on click 8.0/8.1,
  where `CliRunner` merges the streams unless told not to, but CI only
  ever resolves the newest click, so the `mix_stderr=False` branch
  never executes and the floor it defends is never tested.
- **Evidence**: `separated_runner()` inspects `CliRunner`'s signature
  and takes the `mix_stderr=False` path only on click < 8.2; its
  docstring explains that an install resolving 8.0 or 8.1 is "entirely
  valid per the declared dependency, and not what the tox environment
  happens to build". `tox.ini` pins nothing, so the suite proves the
  file works on one click version out of a declared range of three
  majors' worth of minors.
- **Proposed change**: nothing new. This is the same defect class as
  shakenfist/client-python-k3s#82 ("nothing tests the declared
  floor", in `pyproject.toml`'s own words at the `requires` table) and
  belongs there as an occurrence: the project declares lower bounds on
  `click`, `shakenfist_client`, `setuptools` and Python, and resolves
  the newest of each. Closing it means a lower-bound tox environment,
  which is a plan of its own.

### F14: `expand-workers` and `expand-addresses` range-check nothing -- #96

- **File**: `shakenfist_client_k3s/__init__.py:447-448`,
  `shakenfist_client_k3s/__init__.py:492-493`
- **Action**: none
- **Claim**: Both counts are `click.INT` with no `IntRange`, so zero
  and negative values are accepted, do nothing, and report success in
  terms the operator did not ask for.
- **Evidence**: `--worker-count` and `--address-count` are
  `type=click.INT`, while the node sizing options added by another
  plan use `type=click.IntRange(min=1)`. `expand_workers(0)` reaches
  `create_and_await_instances(0, 'worker')` and then prints
  `Added 0 workers to cluster banana` (`cluster.py:2167`);
  `expand_addresses(-1)` reaches `allocate_metallb_addresses(-1)`,
  where `range(-1)` is empty and the note reads `no routed addresses
  were available (requested -1)` (`cluster.py:1199-1200`). No test
  passes zero or a negative to either verb. The Ansible module *does*
  validate its equivalents
  (`test_ansible_module.ShapeRangeTestCase`, three tests), so the
  library's two front doors disagree.
- **Proposed change**: nothing new -- shakenfist/client-python-k3s#96
  already records that `Cluster.create()` does not range-check its
  counts, and this is the same defect on two more verbs plus the
  CLI/module asymmetry. Add it there as an occurrence. If #96 is ever
  taken, `click.IntRange(min=0)` on these two options and a positive
  check in the `Cluster` methods closes both halves.

### F15: `health --strict` is only ever functionally exercised in the direction that passes

- **File**: `tools/ci_deploy_test.sh:183`,
  `tools/ci_deploy_test.sh:293`
- **Action**: none
- **Claim**: Both functional `health --strict` calls are on healthy
  clusters, so the tier proves the flag exits 0 when it should and
  never proves it exits 1 when it should.
- **Evidence**: `:183` and `:293` are both bare `sf-client k3s health
  ... --strict` under `set -e`, which asserts exit 0. The exit-1
  direction is unit tested
  (`test_commands.HealthCommandTestCase.test_strict_exits_one_for_an_unhealthy_cluster`,
  plus `test_strict_renders_the_same_report_it_would_have_anyway`),
  and the whole of `HealthProbeIsSkippedTestCase` covers the unhealthy
  shapes. So the logic is well covered; what is uncovered is whether a
  *real* broken cluster produces an unhealthy report.
- **Proposed change**: before deleting the minimal cluster at `:317`,
  `sf-client instance delete` its control plane node and assert
  `sf-client k3s health "${MINIMAL_CLUSTER}" --strict` exits non-zero
  and says so. That would also be the only functional exercise of
  `_node_health`'s `ResourceNotFoundException` branch
  (`cluster.py:977-979`). Marked `none` rather than `consider` because
  deliberately wrecking a cluster mid-script changes what the
  subsequent `k3s delete` is being asked to do, so it needs more
  thought than its line count suggests -- and because the logic is
  already the best-unit-tested part of the scope.
