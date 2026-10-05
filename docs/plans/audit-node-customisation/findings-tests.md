# Findings: tests lens (step 4c)

Returned as text by the sub-agent and saved here by the management
session, because the harness does not let sub-agents write report files.
The management session spot-checked T-2, T-3 and T-4 against the
worktree before saving.

## What was read, and at which revision

* The brief: step 4c of `PLAN-node-customisation-phase-04-push-audit.md`,
  and its survey findings 1-2 and decisions 1, 5 and 7.
* `README.md`, `scope-files.txt` and `verification.md` in this directory.
* `PUSH-AUDIT.md`: the *Tests* section, and the `functional-test-coverage
  v1` block with its "In this project" note.
* `PLAN-node-customisation-phase-03-live-validation.md`, all of its
  decisions, and decision 2 in particular.
* `tools/ci_deploy_test.sh`, end to end. Its version before phase 3 was
  read with `git show 51ff6f4^1:tools/ci_deploy_test.sh`. Phases 1 and 2
  did not touch `tools/`.
* `shakenfist_client_k3s/cluster.py`, the parts these phases added: the
  owned-key sets and the release floor (`:189-274`);
  `validate_node_sizes()`, `validate_k3s_config()`, `read_k3s_config()`
  and `check_k3s_release()` (`:455-657`); `_node_size()` and
  `create_instance()`; `_k3s_config_commands()` and
  `install_k3s_component()`; `create()` and `expand_workers()`.
* From `__init__.py`: `k3s_create` and `_bind_new_cluster_context()`.
* From `exceptions.py`: `NodeSizeError`, `K3sConfigError` and
  `UnsupportedReleaseError`.
* `primitives.get_k3s_release()`.
* The tests these phases added or changed: in `test_cluster.py`,
  `ValidateNodeSizesTestCase`, `ValidateK3sConfigTestCase`,
  `ReadK3sConfigTestCase`, `CheckK3sReleaseTestCase`,
  `K3sConfigCommandsTestCase`, `ExpandWorkersTestCase`, and
  `ShowReports{NodeSizes,K3sConfig}TestCase`; in `test_library_api.py`,
  `NodeSizingTestCase` and `K3sConfigTestCase`; in `test_commands.py`,
  `CreateNodeSizingOptionsTestCase`; `test_exceptions.py`; `fakes.py`.

**Revision.** The code was judged in this worktree. Its HEAD is
`7f4efb1`, which is `3e84907` plus plan-only commits, so the code is the
code at `3e84907`. `git diff <m>^1 <m>` for `ddb1f3b`, `b791364` and
`51ff6f4` was used only to locate what each phase added.

**Method, and what was not run.** No suite was re-run. Suite results come
from `verification.md`: 565 passed, 0 skipped. Claims about what the
tests catch were checked by mutating functions *in memory*: recompiling
one function from its source with a one-line change, swapping it into
the imported module, running the named test classes with `unittest`, and
restoring the original. The worktree on `sys.path` was confirmed by the
module's `__file__`. No file was edited. Only `test_cluster`,
`test_library_api` and `test_commands` were run this way, never the
Ansible module tests, so #106 does not apply.

The k3s channel API was fetched once, on 2026-10-06, to check what
`--release-channel` resolves to today.

## The central question: one behaviour at a time

For each user-visible behaviour these phases added: which functional test
in `tools/ci_deploy_test.sh` would have failed before it and passes
after. Line numbers are in the script at this revision.

| Behaviour | Functional test | Fails before, passes after? |
|---|---|---|
| Six sizing flags | Main create passes all six (`:266-276`). `assert_node_sizes` (`:423`) checks the metadata. `assert_instance_size` checks Shaken Fist's own view of one control plane node and one worker (`:424-435`). | Yes. Before phase 1 the flags did not exist, so click refuses the create. Every value differs from the defaults and from the other role's, so a dropped flag or a swapped role fails. |
| `--server-config` / `--agent-config` | `ci-role=server` must select exactly 1 node and `ci-role=agent` exactly 2 (`:392-401`). The minimal cluster must carry no `ci-role` label (`:598-602`). | Yes. |
| `read_k3s_config()` | It is the CLI's read path for both files on the main create, and for the server file on the minimal create. | Yes, through the rows above. Its error paths have no live run (see T-9). |
| Owned-key refusal | None. | No. See T-9. |
| Release-floor refusal (`UnsupportedReleaseError`) | None. | No. See T-1. |
| servicelb disabled under MetalLB | No `svclb-` pods after the LoadBalancer test (`:379-386`). The positive control is `svclb-traefik-*` on the minimal cluster, which has no MetalLB (`:573-593`). | Yes. Before phase 2, servicelb ran beside MetalLB. The caller's bare `disable` also exercises the `disable+` append. |
| Default taint | Every control plane node must carry `node-role.kubernetes.io/control-plane:NoSchedule`, with an empty-list guard (`:403-420`). The opt-out (`:548-563`) is checked against it. | Yes. |
| `node-taint: []` opt-out | The minimal cluster has a worker and the opt-out, and its control plane node must have no taints (`:548-563`). | Yes. |
| Zero-worker exception | None. | No. See T-7: decision 2 still holds. |
| `expand-workers` reuses recorded config | After the expand, `ci-role=agent` must select 3 nodes, and `instance show` on the new worker must report the worker sizes (`:448-458`). | Yes. |

Each new assertion was reviewed for vacuity, as phase 3 did: `pod_names`
returns 1 on a kubectl failure; the Traefik HelmChart check requires
`NotFound`, not just any failure; the empty control plane selection is
guarded; `new_worker` must be exactly one UUID; a missing `node_sizes`
line is an explicit failure. None of them passes with the feature broken.
Each absence check has its positive control on the other cluster.

## Findings

### T-1. The release-floor refusal has no functional test, and a cheap one exists

* **Where:** `tools/ci_deploy_test.sh` (absent). The check is
  `cluster.py:2049` (`check_k3s_release(target_release,
  release_channel)`), before the name is registered.
* **Claim:** nothing in the merge tier runs a create that the release
  floor refuses. That is the block's finding, about this change. A check
  would cost seconds and build nothing.
* **Rating:** `consider`.
* **Evidence:**
  * The k3s channel API, fetched 2026-10-06, still resolves `v1.20` to
    `v1.20.15+k3s1`, `v1.16`-`v1.19` to older releases, and `testing` to
    `v1.18.2-rc3+k3s1`. All of these are below `K3S_RELEASE_FLOOR`
    (1, 21, 1).
  * `--release-channel` is a free `click.STRING` (`__init__.py:169-172`).
    `get_k3s_release()` returns whatever `latest` the named channel has
    (`primitives.py:116-121`).
  * In `create()`, `read_manifests`, size and config validation,
    `start_progress` and the release lookup (which at most refreshes the
    namespace's version cache) all run before the refusal
    (`cluster.py:2006-2049`). The cluster list write and the network
    allocation come after it.
  * `GroupCatchClusterExceptions` prints `str(e)` and exits 1.
  * Something like this, placed before the main create, would need no
    VM: `sf-client k3s create ciTooOld --release-channel v1.20
    --worker-count 0 > /tmp/k3s-ci-floor-refusal 2>&1` must fail; its
    output must contain `older than` and `v1.21.1`; `sf-client k3s list`
    must not list `ciTooOld`.
  * It is also the only check that would notice the live channel API's
    release strings stop parsing. The unit test
    `test_a_release_below_the_floor_registers_nothing`
    (`test_library_api.py:620`) patches `get_k3s_release` to return a
    literal.
  * One fragility should be designed for. If upstream ever drops the
    `v1.20` channel, the create fails with `ReleaseLookupError` (`unknown
    channel`) instead. The message assertion then fails plainly rather
    than passing.
  * A change to the script needs a merge-tier dispatch (risk 4 of the
    phase plan). That is why this is `consider` and not `fix`.

### T-2. The CLI's "a bad config file leaves no namespace behind" assertion is vacuous

* **Where:** `shakenfist_client_k3s/tests/test_commands.py:236-245`
  (`test_a_refused_key_exits_before_create_is_called`, last line).
* **Claim:** the test asserts `create_namespace.assert_not_called()`, but
  `create_namespace` could not have been called whatever the ordering.
  The test passes no `--namespace`, and `_bind_new_cluster_context()`
  only looks a namespace up when that option is given (`__init__.py:78`).
  The client is a bare `MagicMock`, so even with `--namespace` its
  `get_namespace()` returns a truthy mock and the namespace counts as
  existing. So nothing tests the ordering that the comment at
  `__init__.py:230-232` states and phase 2 chose deliberately: read the
  files before binding the context.
* **Rating:** `consider`. This is a real defect in a test phase 2 added,
  and the fix is two lines: pass `--namespace newns` and set
  `self.client.get_namespace.return_value = None`, as
  `CreateNamespaceNoticeTestCase` (`:96`) already does.
* **Evidence:** an in-memory mutation replaced `k3s_create`'s callback
  with one that calls `_bind_new_cluster_context()` before the original
  body, so the bind happens before the file read. The test still passed
  (0 failures). The corrected invocation (`--namespace newns` with
  `get_namespace` returning None), run under the same mutation, recorded
  `create_namespace.called == True`. So the corrected test kills it.

### T-3. Nothing pins that a caller's own `+` key is accepted

* **Where:** `cluster.py:556`, and the absence of a test in
  `ValidateK3sConfigTestCase`.
* **Claim:** `docs/usage.md:136` tells callers to write `node-taint+:
  [key=value:NoSchedule]` to add a taint. `node-label+` and `disable+` are
  equally legitimate. No test asserts that a non-owned key ending in `+`
  is accepted. `REALISTIC` contains only `tls-san+`, which is
  special-cased, and the CI configs use no `+` key.
* **Rating:** `consider`. This is a one-assertion test, for example
  `validate_k3s_config({'node-taint+': [...], 'disable+':
  ['local-storage']}, 'server')` returns text that round-trips.
* **Evidence:** mutation M19 changed the condition to `key.rstrip('+') in
  owned or key.endswith('+')`. That refuses every `+` key except
  `tls-san+`. It **survived** all 355 tests in `test_cluster`,
  `test_library_api` and `test_commands`.

### T-4. ±Infinity passes the JSON-representability check

* **Where:** `cluster.py:565`, and its comment at `:559-563`.
* **Claim:** the comment says a value JSON cannot give back "comes back
  different and fails the comparison", with NaN as the example. NaN fails
  only because `nan != nan`. `float('inf')` (YAML `.inf`) and `-.inf`
  compare equal after the round trip, so they are accepted. `json.dumps`
  emits them as the non-standard token `Infinity`, and so does
  `shakenfist_client`'s `_actual_request_url()`, whose `json.dumps(data,
  ...)` leaves `allow_nan` at its default. Whether the Shaken Fist API
  accepts that body was **not** verified. If it does not, this is exactly
  the failure after registration that `not_representable` exists to
  prevent. If it does, the only harm is non-standard JSON in the
  metadata.
* **Rating:** `consider`. `json.dumps(value, allow_nan=False)` refuses NaN
  and both infinities through the same branch, and needs one test.
  Practically, no k3s flag wants an infinite value.
* **Evidence:** `validate_k3s_config(yaml.safe_load('x: .inf\n'),
  'server')` returns `'x: .inf\n'`. Mutation M20 (adding
  `allow_nan=False`) survived all 355 tests, so nothing pins either
  behaviour.

### T-5. Adversarial caller-YAML cases: what is pinned and what is not

* **Where:** `ValidateK3sConfigTestCase` and `ReadK3sConfigTestCase`
  (`test_cluster.py:1973-2205`).
* **Claim:** the refusals are well covered. Several accepted shapes, and
  several YAML constructs reached through a real file, have no test. Each
  row below was checked by running `yaml.safe_load` and
  `validate_k3s_config` on the input.
* **Rating:** `consider`. A small table-driven test through
  `read_k3s_config()` would pin the current behaviour. None of these rows
  is a defect in itself.

| Input | Behaviour today | Pinned? |
|---|---|---|
| Non-mapping: list, string | `not_a_mapping` | Yes |
| Non-mapping: `true`, `42` | `not_a_mapping` | No (same branch as the list case) |
| Multi-document file | `unreadable` | Yes |
| Integer key; `~` (null) key; `yes` (bool) key | `non_string_key` | Integer only |
| Nested mapping value with string keys | Accepted, re-dumped in block style | No (only nested lists, and a nested *integer* key refused) |
| Unicode key or value (`café`) | Accepted; `safe_dump` writes ASCII escapes (`"caf\xE9"`) | No (manifests have one, `test_cluster.py:1753`; config does not) |
| NEL / U+2028 in a value | Accepted, written escaped (`\N`, `\L`); never a raw line break | No |
| `!!binary`, `!!set`, `!!timestamp` | `not_representable` | Only `datetime.date` passed directly, not via YAML |
| `!!python/object:...` | `unreadable` (safe_load ConstructorError) | No (the invalid-YAML test covers the same branch) |
| Anchors, aliases, `<<` merge keys | Expanded and accepted; the file on the node holds the expansion | No |
| Recursive alias (`a: &x [*x]`) | `not_representable` (json ValueError) | No (only mentioned in a comment) |
| Key with trailing `+` | Owned ones refused (with `++` too); `tls-san+` allowed | Yes, except the non-owned case (T-3) |
| `tls-san` vs `tls-san+` | Refused with a hint, versus allowed | Yes |
| `'token '`, `TOKEN`, `token_file`, `--token`, `''` | All accepted | No. Whether k3s treats any of these as an alias of an owned key is the security lens's question. If it does, a refusal test belongs with that fix. |
| Very large input | No size limit (T-6) | No |

### T-6. Alias expansion makes validation exponential in the input

* **Where:** `cluster.py:565` and `:570-571` (`json.dumps` and
  `yaml.safe_dump` of the round trip).
* **Claim:** `yaml.safe_load` builds aliases as shared references, so
  parsing a "billion laughs" file is cheap. `validate_k3s_config()` then
  serialises the expansion.
* **Rating:** `none` for this lens, deferred to the security lens. The
  file is the caller's own and the cost lands on the caller's own
  process, so there is no privilege boundary. Worth remembering if the
  collection later surfaces these options (survey finding 3).
* **Evidence:** nine-way nesting, measured:

| Levels | Input | Output | Time |
|---|---|---|---|
| 4 | 167 bytes | 86,736 bytes | 0.04 s |
| 5 | 202 bytes | 913,425 bytes | 0.38 s |
| 6 | 237 bytes | 9,416,484 bytes | 3.75 s |

Both size and time grow about tenfold per level, so nine levels in under
350 bytes is roughly 9 GB. That size was not run.

### T-7. Zero-worker exception: phase 3's decision 2 still holds, and more strongly than it said

* **Where:** `cluster.py:1462` (`if md.get('worker_nodes'):`).
* **Claim:** decision 2 said moving the minimal cluster to one worker
  meant "the zero-worker exception is no longer exercised on any
  cluster". It was never meaningfully exercised. Before phase 3, the
  minimal cluster was `--worker-count 0 --no-metallb --no-longhorn`
  (`git show 51ff6f4^1:tools/ci_deploy_test.sh`). The exception's failure
  mode is MetalLB's rollout wait; the only `kubectl rollout status` the
  plugin runs is in the MetalLB setup (`cluster.py:1769-1771`). Nothing on
  that cluster waited on a pod: `wait_for_nodes` checks node Ready,
  `health --strict` probes `kubectl get nodes` (`cluster.py:1224`), and a
  NoSchedule taint affects neither. So a broken exception would have
  passed that cluster too, and decision 2 lost no coverage. A live check
  needs a zero-worker cluster *with* MetalLB: a third cluster, which
  decision 1 rejects, or giving up the opt-out and svclb positive control
  the minimal cluster now carries. The exception is a plugin-side branch,
  and it fails loudly (every such create would time out) rather than
  silently. The decision stands.
* **Rating:** `none`.
* **Evidence:**
  * `test_the_first_server_config_without_workers_has_no_taint`
    (`test_cluster.py:3040`) pins the branch.
  * Mutations M6 (`if True:`) and M6b (`if False:`) were both killed by
    `K3sConfigCommandsTestCase`. M6b would also fail the main cluster's
    live taint check.
  * The documented "stays untainted after `expand-workers`"
    (`docs/usage.md:139-140`) is pinned by
    `test_only_the_new_worker_has_k3s_installed` (`test_cluster.py:303`):
    `install_k3s_component` is called once, with only the new worker and
    `'agent'`.
  * The branch is driven by crafted metadata rather than by `create(1, 0,
    ...)`. That is equivalent, since `create()` builds the workers before
    `install_control_plane()`. If that order were reversed, every cluster
    would lose its taint, and the main cluster's live check would catch
    it.

### T-8. HA clusters run the extra-server config path with no live coverage

* **Where:** `tools/ci_deploy_test.sh:267` and `:520`. Both creates use
  `--control-plane-count 1`.
* **Claim:** phase 2 gave extra servers their own config. It lacks
  `cluster-init`, and they also get the caller's server drop-in, the
  enforced drop-in and the taint, all written before an installer that
  joins through `K3S_URL`. No merge-tier cluster has an extra server. The
  k3s merge rules involved are the same ones the single server proves
  live; the new part is only that the files are written by
  `install_k3s_component()` with role `server`, a method exercised live
  for agents. HA was never built by the merge tier before these phases
  either.
* **Rating:** `none`. A pre-existing gap these phases widened slightly.
  Worth an issue if HA coverage is not already planned; no open issue
  was found for it.
* **Evidence:** `test_an_extra_server_config_has_no_cluster_init` and
  `test_the_caller_drop_in_round_trips_for_each_role` (`inst-cp2`) pin the
  commands. `test_every_config_command_precedes_the_installer` pins the
  ordering on `inst-cp2`.

### T-9. Owned-key refusal and `read_k3s_config()` error paths: no functional test, and none needed

* **Where:** `__init__.py:233-237`, and `cluster.py:593-625`.
* **Claim:** no live test feeds the CLI a refused key or an unreadable
  file. The unit tier already runs the real thing, short of the plugin's
  entry point: `test_a_refused_key_exits_before_create_is_called` invokes
  the real `GroupCatchClusterExceptions` group, with a real file on disk,
  through the real `read_k3s_config()`; `ReadK3sConfigTestCase` uses real
  files for every refusal. A merge-tier run would add only sf-client's
  plugin loading, which every live create already exercises.
* **Rating:** `none`. If T-1 is taken, a refused key costs about four
  more lines in the same block, and it also covers T-2's ordering through
  the real binary.
* **Evidence:** all killed: M14 (`read_k3s_config` skips validation); M9
  and M9b (`create()` skips either role's validation); M8 (the write path
  skips revalidation).

### T-10. The `tls-san+` route is documented but not exercised live

* **Where:** `exceptions.py` `owned_key()` steers callers to `tls-san+`.
  CI's `ci-server.yaml` (`tools/ci_deploy_test.sh:258-261`) has no
  `tls-san+`.
* **Claim:** the k3s rule it relies on is already observed live: a `+`
  key in a later file appends to a bare key in an earlier one, and the
  main cluster proves that through `disable` (50 file) plus `disable+`
  (90 file). So `tls-san+` adds little coverage.
* **Rating:** `none`. If wanted, adding `tls-san+: [ci-extra.invalid]` to
  `ci-server.yaml` would make the script's existing kubectl calls check
  that the plugin's own SAN survived, since they go through the floating
  address and kubectl verifies the serving certificate against it.
  Showing the extra SAN was added would need a separate check, and how
  k3s exposes its serving SANs would have to be confirmed on a live node
  first.
* **Evidence:** script `:379-386` (svclb absent with a caller's bare
  `disable`). `test_bare_tls_san_is_refused_and_the_message_says_what_to_write`
  and `test_tls_san_plus_is_allowed_on_a_server` cover the unit side.

### T-11. `tools/mutation-check.py` has no entries for these phases' defended properties

* **Where:** `tools/mutation-check.py`, `MUTATIONS`. It has 12 entries,
  none about sizes, k3s config or the release floor.
* **Claim:** `docs/testing.md` says to run the script, and grow its set,
  "when the defended properties change". These phases added several such
  properties. The script was added by #107 (`2c90813`) after phase 2
  merged, so this is not an omission by the phases. It is still the
  obvious place to record what was checked by hand here.
* **Rating:** `consider`. The entries are mechanical, and every one below
  is known to be killed today.
* **Evidence:** in-memory mutations, each against the named test classes.
  All **killed**: M1 (`+` not stripped before the owned-key check); M2
  (`tls-san+` not excused); M3 (floor off by one, `(1, 21, 0)`); M3b
  (floor check disabled); M6 and M6b (taint always, taint never); M7
  (enforced drop-in ignores `metallb_installed`); M8 (no revalidation at
  write time); M9 and M9b (`create()` skips server or agent config
  validation); M10 (`create()` skips the release check); M11
  (`expand-workers` ignores the recorded `agent_config`); M12 (delimiter
  check on the config text off); M13 (representability check off); M14
  (`read_k3s_config()` skips validation); M15 (every node built at worker
  size); M16 (`True` accepted as a size); M17 (`create()` skips size
  validation); M18 (`node_sizes` not recorded). Two **survived**: M19
  (T-3) and M20 (T-4).

### T-12. With `--namespace` naming a new namespace, a release-floor refusal leaves that namespace behind

* **Where:** `__init__.py:239` (binding, which creates the namespace) runs
  before `cluster.py:2049`.
* **Claim:** the config files are read before binding precisely so that a
  bad file leaves no namespace (`__init__.py:230-232`). The release check
  cannot move there, because the lookup reads the namespace's version
  cache. So `create --namespace new --release-channel v1.20` creates `new`
  and then refuses. `ReleaseLookupError.unknown_channel` behaves the same
  way and predates these phases.
* **Rating:** `none`. Consistent with existing behaviour, and the docs say
  "before the name is registered", which stays true.
* **Evidence:** the code order is `__init__.py:78-82`, then
  `cluster.py:2041-2049`.

## Existing issues (rediscoveries, not re-raised)

* **#93:** `test_commands.py:211`, `tempfile.NamedTemporaryFile('w',
  ...)`, has no `encoding=`. It was added by phase 2 (`b791364`, per `git
  blame`) and is not in #93's list of twenty. Its content is ASCII, so it
  is harmless. Worth a comment on #93 so the site is swept with the rest.
* **#96:** a negative `--worker-count` reaches
  `create_and_await_instances()` as `range(-1)` and builds no worker, so
  `md['worker_nodes']` stays empty and the zero-worker taint branch
  applies. The missing count validation is #96's. Nothing new.
* **#104:** `check_k3s_release()` refuses a non-string or unparseable
  `latest` as `unparseable` rather than crashing, which narrows #104 for
  the k3s lookup's consumer. Nothing to add.
* **#101 / #102:** none of phase 3's assertions touch `update-os`,
  `health --strict` in the failing direction, or root options. Not
  re-raised.
* **#106:** avoided by method. The in-memory mutations ran
  `test_cluster`, `test_library_api` and `test_commands` with the
  worktree first on `sys.path`, and never the Ansible module tests.

## Checked and clean

* **Functional coverage per behaviour:** see the table above. Every
  behaviour except the owned-key refusal (T-9), the release floor (T-1)
  and the zero-worker exception (T-7) has a live assertion that would have
  failed before its phase.
* **No vacuous assertions in phase 3's additions.** Each absence check
  runs after the LoadBalancer test and has a positive control on the
  other cluster: Traefik HelmChart and pods, and `svclb-`, against
  `svclb-traefik-*` on the minimal cluster; `ci-role` labels, against
  their absence on the minimal cluster; the default taint, against the
  opt-out. Both size checks compare against values that differ by role
  and from the defaults. `assert_instance_size` reads Shaken Fist, not the
  plugin's record. Failure handling inside `$(...)` was checked in
  `pod_names`, `count_labelled_nodes` (pipefail; it is the function's last
  command), `control_plane_uuids` and `worker_uuids`. Every `|| true` is
  on a grep whose zero match is the expected answer.
* **Unit tests mock the boundary, not the code under test.** The Shaken
  Fist client is `fakes.FakeClusterClient`, or a `MagicMock` in the CLI
  tests. The k3s and Longhorn lookups are patched in `LibraryTestCase`,
  and `get_k3s_release` has its own tests against a mocked `requests`.
  `subprocess.run` and `time.sleep` are patched. `validate_node_sizes`,
  `validate_k3s_config`, `read_k3s_config` and `check_k3s_release` run
  unmocked, as pure functions on real input and real files.
  `K3sConfigCommandsTestCase` drives the real `install_*` methods against
  the fake client, and
  `test_the_files_which_land_are_the_files_which_were_sent` runs the
  heredocs through a real `/bin/sh`. `ExpandWorkersTestCase._expand`
  patches `await_boot`, `instance_os_update` and `install_k3s_component`
  on the class under test, but only in tests whose question is the
  `create_instance` call or which instances are handed on;
  `_expand_for_real` covers what is actually sent.
  `CreateNodeSizingOptionsTestCase` patches `Cluster.create`, which is
  right for a click-wiring test, and `NodeSizingTestCase` /
  `K3sConfigTestCase` cover `create()` itself.
  `test_text_with_a_delimiter_line_is_refused` patches the delimiter
  *constant*, not the logic, and runs the real dumper.
* **Refusals before registration:** the library tests assert
  `client.calls == []`, empty metadata, no network, no instances and no
  agent commands for a bad size, a bool size, an owned server key, an
  owned agent key, and a release below the floor. M9, M9b, M10 and M17
  confirm these would fail.
* **Skips:** `verification.md` reports 0 skipped of 565. The only
  conditional skip these phases added is `test_cluster.py:3175`, which
  skips when `/bin/sh` is absent: an environmental guard, and correct.
* **Exceptions:** `K3sConfigError` and `UnsupportedReleaseError` are
  covered by `TotalAttributesTestCase`, which walks
  `_ReasonedK3sException`'s subclasses. `NodeSizeError` has its own test
  case. Every `K3sConfigError` and `UnsupportedReleaseError` classmethod
  has its `reason`, fields and message asserted somewhere in the unit
  tests.
* **External API shapes:** the k3s channel API's `latest` strings today
  (`v1.21` resolves to `v1.21.14+k3s1`, `testing` to `v1.18.2-rc3+k3s1`)
  match what `check_k3s_release()`'s docstring and its tests assume.
  Non-string and unparseable releases are refused, not crashed on.
* **Multi-document files, non-mapping files, integer keys, dates, doubled
  `+`, `tls-san` vs `tls-san+`, delimiter collisions, and undecodable
  bytes** are all pinned. T-5 lists what is not.
* **`tox -epy3`, `pre-commit run --all-files`, and direct flake8** pass,
  per `verification.md`. `tox -eflake8` was vacuous there, which that
  file already records.

## Summary

Twelve findings: 0 `fix`, 5 `consider` (T-1 to T-5), 7 `none` (T-6 to
T-12). Six of the nine behaviours have a live assertion that would have
failed before its phase. The gaps are the release-floor refusal (T-1,
cheap to close: `--release-channel v1.20` is refused before anything is
built), the owned-key refusal (T-9, not needed), and the zero-worker
exception (T-7, phase 3's decision stands and was never live-covered).
Two genuine test defects: T-2's vacuous `create_namespace` assertion, and
T-3's unpinned acceptance of a caller's own `+` key. 18 of 20 in-memory
mutations were killed.
