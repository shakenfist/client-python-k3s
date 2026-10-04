# Node customisation phase 2: k3s configuration pass-through

## Prompt

Before responding to questions or discussion points in this document,
read `install_control_plane()`, `install_k3s_component()`,
`install_extra_control_plane()`, `install_workers()`, `create()`,
`show()` and `expand_workers()` in `shakenfist_client_k3s/cluster.py`,
the two shell rules and the manifest constants above `read_manifests()`
in the same file, the `k3s create` command in
`shakenfist_client_k3s/__init__.py`, and `HeredocDelimiterTestCase` and
`ShellQuotingTestCase` in `shakenfist_client_k3s/tests/test_cluster.py`.
Ground answers in what that code does today. The master plan is
[PLAN-node-customisation.md](PLAN-node-customisation.md); its decisions
1, 3, 4, 5 and 7 and open question 1 are what this phase implements,
and they are not restated here except where the survey changed them.

## Planning effort

High. This phase changes what is written onto every node before k3s
starts, adds two metadata keys, and changes the default behaviour of
`create` in two ways (servicelb disabled under MetalLB; control plane
tainted). Most of it can be pinned by unit tests, but the k3s merge
semantics it relies on can only be confirmed live, and some of them
were not what the master plan assumed (survey findings 3 and 4).

Review effort: high for step 2b, which writes the node-side files; medium
for the rest.

## Scope

In:

* `server_config` and `agent_config` mappings on `Cluster.create()`,
  and `--server-config PATH` / `--agent-config PATH` on `k3s create`.
* A pure validation function for those mappings, and a library helper
  that reads and parses a config file, both run before anything is
  built.
* A k3s release floor, checked before anything is built.
* Writing the plugin's own `config.yaml` and the caller's drop-in onto
  every server and every agent before its installer runs, including
  the additional control plane nodes (which get no config file today)
  and workers added later by `expand-workers`.
* Disabling servicelb whenever MetalLB is installed (open question 1).
* The default control plane taint (design 7), with the zero-worker
  exception in decision 7 below.
* Recording both mappings in cluster metadata, and `show()` reporting
  them for older clusters too.
* Unit tests, the regenerated `create.txt` CLI contract fixture, and
  `docs/usage.md` / `docs/library-api.md`.

Out:

* Live validation: phase 3. The merge-tier functional CI on this
  phase's pull request does build a default cluster, which exercises
  the two default changes (decision 10).
* Surfacing the options in the `shakenfist.k3s` collection: master plan
  Future work, unchanged.
* Changing Longhorn's replica count to suit a tainted control plane:
  see risk 3. Recorded in Future work.
* Per-invocation config overrides on `expand-workers`: master plan
  Future work.

## What the survey found

The master plan's line references predate phase 1 and #90, and have
moved; the current ones are below. The findings that change the plan
are 3, 4, 5 and 7.

Findings 3 to 7 were corrected at source in the master plan as part of
this planning commit: in design 3, design 7, open question 1, the
phase 2 row and Future work. Nothing later needs to redo that. Finding
1's line numbers were left alone in the master plan's Situation, which
describes the tree that plan was written against.

### 1. The code the master plan describes is still as it describes, at new lines

* The first control plane's `config.yaml` heredoc, with exactly
  `write-kubeconfig-mode`, `tls-san` and `cluster-init`, is now at
  `cluster.py:1048-1055`, inside `install_control_plane()`
  (`cluster.py:997`).
* `install_k3s_component()` is at `cluster.py:1137-1163` and still
  writes no config file: it runs `apt-get update` and the installer
  with `INSTALL_K3S_CHANNEL`, `K3S_URL` and `K3S_TOKEN`. Phase 1 removed
  the bare `apt-get install -y`.
* `install_extra_control_plane()` (`cluster.py:1165`) and
  `install_workers()` (`cluster.py:1172`) both call it.
  `expand_workers()` (`cluster.py:2148`) reaches it through
  `install_workers()`, so a change there covers `expand-workers` with no
  separate edit.
* `create()` checks its inputs at `cluster.py:1440-1453`
  (`read_manifests()`, then `validate_node_sizes()`), resolves the k3s
  release at `cluster.py:1481` and registers the name at
  `cluster.py:1507`. The initial metadata dict ends with `node_sizes` at
  `cluster.py:1595`.

### 2. Rule 2's guard test does not see `install_k3s_component()`

`HeredocDelimiterTestCase` (`tests/test_cluster.py:2092`) asserts that
every generated heredoc has a quoted delimiter, but it scans
`_control_plane_and_metallb_commands()` (`tests/test_cluster.py:2058`)
only. `install_k3s_component()` has never generated a heredoc, so this
did not matter. It will after this phase, so step 2b extends the
helper to drive `install_k3s_component()` for both roles. Otherwise
the new heredocs are unguarded by the test that exists to catch an
unguarded heredoc.

### 3. The `config.yaml.d` and `+` floor is v1.21.1, and the plugin can resolve older

Design 3 asked for this to be verified. From the k3s source history of
`pkg/configfilearg/parser.go`, checked with `gh api .../compare`:

* Drop-in directory support is `a0a1071aa5` (k3s#3162, 2021-04-15),
  first released in **v1.21.0+k3s1**. v1.20.15+k3s1 (the last 1.20) and
  v1.19.16+k3s1 do not contain `dotDFiles` at all.
* The `+` append suffix is `8f1a20c0d3` (2021-04-25), first released in
  **v1.21.1+k3s1**.

So a v1.20 node would ignore the drop-in silently, and a v1.21.0 node
would read `tls-san+` as a different key. Both fail silently.
`--release-channel` resolves any channel `update.k3s.io` lists, and on
2026-10-05 that list still includes `v1.16` to `v1.20` and `testing`
(which resolves to `v1.18.2-rc3+k3s1`). No channel resolves to
v1.21.0: `v1.21` resolves to v1.21.14.

The old channels are already unusable by default. The Longhorn chart's
`kubeVersion` is `>=1.21.0-0`, so a default create on any of them
already fails at the Longhorn install. Only `--no-longhorn` creates on
those channels lose anything from a floor (decision 6).

A later fix, `e514940020` (2024-07-29), made drop-ins load when
`config.yaml` itself is missing. The main argument parser already did
that: `readConfigFile()` tolerates a missing main file when drop-ins
exist. The fix was to `FindString()`, the narrow lookup k3s uses for a
handful of keys before parsing. Writing a `config.yaml` on every node
(decision 4) sidesteps the question on every version the floor allows.

### 4. A caller's `disable` replaces the plugin's

Open question 1 says to add `servicelb` to the plugin-owned server
configuration whenever MetalLB is installed. Design 3 puts the
plugin's keys in `config.yaml` and the caller's in a drop-in read
after it. k3s's merge rule is that a later list key replaces an
earlier one unless written with `+`. So the headline example from the
master plan, `disable: [traefik]` in `--server-config`, would silently
re-enable servicelb. The master plan does not address this; decision 5
does.

### 5. The plugin depends on more k3s settings than design 3 lists

Design 3 rejects five plugin-owned keys: `write-kubeconfig-mode`,
`tls-san`, `cluster-init`, `server` and `token`. The code also depends
on these, each of which a caller could change through the
pass-through:

* `data-dir`. The token reads (`cluster.py:1123`, `1129`) and
  `K3S_MANIFEST_DIR` (`cluster.py:135`) hardcode
  `/var/lib/rancher/k3s`.
* `write-kubeconfig`. Every `kubectl` and `helm` command, and the
  credential fetch at `cluster.py:1647`, reads `/etc/rancher/k3s/k3s.yaml`.
* `https-listen-port`. `K3S_URL` hardcodes `:6443` (`cluster.py:1154`).
* `node-name` and `with-node-id`. `remove_worker()` addresses the k3s
  node by the lowercased instance name (`cluster.py:2316-2319`). A
  `node-name` in `--agent-config` would also give every worker the same
  name.
* `token-file`. It is the same thing as `token`, by another route.

Decision 3 extends the rejected set to cover them.

### 6. Open question 1's error noise is Traefik's

The master plan asked whether the `AdditionalAssignFailed ... PreferDualStack`
noise comes from Traefik's Service or from something the plugin
configures. It is Traefik's: k3s's bundled `manifests/traefik.yaml`
sets `service.spec.ipFamilyPolicy: "PreferDualStack"`. Nothing in
`shakenfist_client_k3s/` mentions an IP family. Disabling Traefik
through `--server-config` removes it. Disabling servicelb does not.

### 7. A tainted control plane strands MetalLB on a zero-worker cluster

k3s's bundled CoreDNS, metrics-server, local-path-provisioner and
Traefik all tolerate `node-role.kubernetes.io/control-plane:NoSchedule`
(checked in k3s `manifests/` on master). MetalLB's speaker does too,
through the chart's `tolerateMaster` default. MetalLB's **controller**
does not: the chart's `controller.tolerations` is `[]`.
`configure_metallb_addresses()` waits on
`kubectl rollout status deployment/metallb-controller --timeout=300s`
(`cluster.py:1250`), so with `--worker-count 0`, MetalLB on and the
default taint, every create would fail after five minutes. Longhorn's
workloads have no tolerations either. Design 7 did not consider zero
workers. Decision 7 does, and the master plan's design 7 now notes the
exception.

## Decisions

1. **Two new module-level pure functions in `cluster.py`, beside
   `validate_node_sizes()`.**
   * `read_k3s_config(path, role)` reads a file as UTF-8 and parses it with
     `yaml.safe_load`. It turns `OSError`, `UnicodeDecodeError` and
     `yaml.YAMLError` (including the error for a stream holding more
     than one document) into the new exception, then returns the
     result of the next function.
   * `validate_k3s_config(config, role)` takes the mapping and
     `'server'` or `'agent'`, raises on anything unusable, and returns
     the YAML text the plugin will write.

   The CLI calls `read_k3s_config()`, so file errors stay inside the
   `K3sClusterException` hierarchy for the command line, as they do for
   `--sshkey` and `--manifest`. `create()` calls `validate_k3s_config()`
   on the mappings it was given, next to `validate_node_sizes()`,
   before the name is registered.

   `None`, from an empty file or a library caller's default, is treated
   as `{}`. An empty config is a legitimate request for nothing, and the
   CLI already means "no file" by omitting the option.

2. **What `validate_k3s_config()` refuses.** Each refusal is a
   classmethod on a new `K3sConfigError(K3sClusterException)` in
   `exceptions.py`, after `NodeSizeError`, naming the role and, where
   relevant, the offending key:
   * Anything that is not a mapping (a list, a scalar).
   * A key that is not a string.
   * A value that does not survive a JSON round trip unchanged. The
     mapping is stored in namespace metadata, which is JSON. Otherwise
     `yaml.safe_load` happily produces `datetime.date`, `bytes` and
     integer keys, and `set_metadata()` would fail after the name is
     registered. Compare `json.loads(json.dumps(config)) == config`, and
     catch `TypeError`/`ValueError` from `json.dumps`.
   * A plugin-owned key (decision 3).
   * A line of the dumped text that equals the heredoc delimiter, as
     `read_manifests()` does for manifests.

   It does not check that keys are real k3s flags. That would be a copy
   of k3s's flag list that goes stale. k3s strips a flag it does not
   recognise for the role and logs it, which is the documented cost of
   master plan decision 1.

3. **Plugin-owned keys, per role.** The comparison strips a trailing
   `+`, because k3s's `+` on a string key appends to the string. So
   `write-kubeconfig-mode+` is as much a collision as
   `write-kubeconfig-mode`. The one exception is `tls-san+`, which is
   allowed and documented as the way to add SANs (design 3).
   * Server: `cluster-init`, `data-dir`, `https-listen-port`,
     `node-name`, `server`, `tls-san` (bare), `token`, `token-file`,
     `with-node-id`, `write-kubeconfig`, `write-kubeconfig-mode`.
   * Agent: `data-dir`, `node-name`, `server`, `token`, `token-file`,
     `with-node-id`.

   Two module-level frozensets, with a comment per key naming the line
   of the plugin that depends on it (survey finding 5). The comments
   matter: the next person to wonder whether `data-dir` really has to
   be refused should not have to re-derive it.

   `node-taint` and `disable` are deliberately absent. Decisions 5 and 7
   explain why.

4. **Three files per node, written before its installer runs.**
   * `/etc/rancher/k3s/config.yaml` holds the keys the plugin owns or
     defaults for that node. It is written on **every** node, servers
     and agents alike; a worker's is a single YAML comment line naming
     the plugin. Writing a main file everywhere sidesteps the pre-2024
     `FindString()` behaviour (survey finding 3), and means anyone
     looking at a node can find where its configuration comes from.
   * `/etc/rancher/k3s/config.yaml.d/50-sf-client-k3s.yaml` holds the
     caller's mapping, re-serialised with
     `yaml.safe_dump(config, default_flow_style=False, sort_keys=True)`.
     It is written only when the mapping is non-empty.
   * `/etc/rancher/k3s/config.yaml.d/90-sf-client-k3s-enforced.yaml`
     holds plugin keys that must survive the caller's file. Today that
     is only `disable+: [servicelb]` (decision 5). It is written only
     when there is something to put in it.

   One helper, `Cluster._k3s_config_commands(md, role, first_server=False)`,
   returns the shell commands for all three. Both
   `install_control_plane()` and `install_k3s_component()` call it, so
   the first server, the extra servers and the agents cannot drift
   apart. Every body goes through a quoted heredoc (rule 2) with a new
   delimiter constant, `K3S_CONFIG_DELIMITER = 'SFK3SCONFIG'`, beside
   `K3S_MANIFEST_DELIMITER`. The paths are module literals, so rule 1
   does not apply to them.

   What each role's `config.yaml` holds:
   * First server: `write-kubeconfig-mode`, `tls-san` (floating
     address) and `cluster-init: true`, as today, plus the default
     `node-taint` (decision 7).
   * Extra servers: the same, without `cluster-init`. They join through
     `K3S_URL`, and `tls-san` carries the floating address so their
     serving certificate matches the first server's.
   * Agents: the comment line only.

5. **servicelb is disabled through the enforced drop-in, not
   `config.yaml`.** Survey finding 4 rules out `config.yaml`, because
   the caller's `disable: [traefik]` would replace it. Refusing a bare
   `disable` would make the master plan's own headline example an
   error. The installer's command line is ruled out too: k3s's
   documented precedence is that "for repeatable arguments ... the CLI
   arguments will overwrite all values in the list", so it would
   discard the caller's `disable` instead.

   A `+` key in a file read after the caller's appends whatever the
   caller wrote. So `disable: [traefik]` yields `[traefik, servicelb]`,
   and no `disable` yields `[servicelb]`. It is written on servers only,
   and only when `md.get('metallb_installed', True)`.

   This is the decision a reviewer is most likely to question. It adds
   a third file, and it means the plugin overrides the caller on one
   key. It is still right: servicelb under MetalLB is pure waste (open
   question 1), and the caller who wants klipper wants `--no-metallb`,
   which turns this off.

6. **k3s releases older than v1.21.1+k3s1 are refused at create.** A
   pure `check_k3s_release(version)` parses `vMAJOR.MINOR.PATCH` from
   the start of the string and raises a new
   `UnsupportedReleaseError(K3sClusterException)`. That error names the
   release, the channel that resolved to it, and the floor. It also
   raises on a version it cannot parse, rather than guessing: a version
   we cannot read is not one we can promise drop-ins on. `create()`
   calls it immediately after `get_k3s_release()` at `cluster.py:1481`,
   which is still before the name is registered.

   The floor applies to every create, not only to ones passing a
   config. Every create with MetalLB now writes the enforced drop-in,
   and a rule that depends on the arguments is one more thing to
   document. The only creates that lose anything are `--no-longhorn`
   creates on channels whose Kubernetes went end-of-life in 2022 (survey
   finding 3).

   `expand-workers` does not check. Old clusters have no
   `agent_config`, so no drop-in is written for them, and refusing to
   expand a running cluster for a file it will not receive would be a
   regression.

7. **The default control plane taint is written only when the cluster
   has at least one worker.** Survey finding 7 shows that tainting the
   only node strands MetalLB's controller and fails the create. The
   condition is computed from `md['worker_nodes']` at install time.
   `create()` builds the workers before `install_control_plane()` runs,
   so the list is already populated, and no new metadata key is needed.

   Design 7's opt-out is unchanged. The taint lives in `config.yaml`, so
   a caller's `node-taint` in the drop-in replaces it, and
   `node-taint: []` removes it. `node-taint+:` adds to it, which is
   worth one line in the docs.

   The cost is that a cluster created with no workers stays untainted
   after `expand-workers` adds some. There is no verb that adds control
   plane nodes, and re-tainting a running node is a different
   operation, so this is documented and left.

8. **Both mappings are recorded in the initial metadata dict**, as
   `server_config` and `agent_config`, next to `node_sizes`. They are
   recorded as given, not as dumped text, so `show` displays structure.
   `install_k3s_component()` reads `md.get('agent_config', {})` or
   `md.get('server_config', {})` by role, so `expand-workers` uses the
   recorded agent config with no change to `expand_workers()` itself.

   `show()` fills both with `{}` for clusters created before them, on
   the same deep copy as `node_sizes`. Like the sizes, the fill is
   exact rather than a guess: those clusters had no way to receive a
   config.

9. **CLI options `--server-config PATH` and `--agent-config PATH`** on
   `k3s create`, `click.Path(exists=True, dir_okay=False)`, each read
   through `read_k3s_config()` and passed to `create()` by keyword. The
   CLI contract fixture changes by additions only, as in phase 1.

10. **Live coverage in this phase is the merge queue's default create.**
    `tools/ci_deploy_test.sh` builds a 1 + 2 cluster with MetalLB, so
    the merge-tier run on this pull request exercises the enforced
    servicelb drop-in, the default taint and the new `config.yaml` on
    every node. The minimal cluster (`--worker-count 0 --no-metallb`)
    exercises the untainted, no-drop-in path. Asserting on those with
    `kubectl` is phase 3's job. This phase is live-checked only in the
    sense that a broken file would fail the build.

## Step plan

| Step | Effort | Model | Isolation | Brief for sub-agent |
|------|--------|-------|-----------|---------------------|
| 2a | high | opus | none | Library validation and metadata, no node-side writes yet. In `shakenfist_client_k3s/exceptions.py`, after `NodeSizeError` (line 483), add `K3sConfigError(K3sClusterException)` with a docstring enumerating every refusal and classmethods `not_a_mapping(role, value)`, `non_string_key(role, key)`, `not_representable(role, key, value)` (the value does not survive a JSON round trip; say why the metadata needs it to), `owned_key(role, key)` (message names the key and, for `tls-san`, says to write `tls-san+`), `delimiter_collision(role, delimiter)` and `unreadable(path, reason)`; and `UnsupportedReleaseError(K3sClusterException)` with `too_old(release, channel, floor)` and `unparseable(release, channel)`. Follow `NodeSizeError`'s shape (attributes plus a classmethod constructor). In `shakenfist_client_k3s/cluster.py`: add `K3S_CONFIG_DELIMITER = 'SFK3SCONFIG'` beside `K3S_MANIFEST_DELIMITER` with a comment; add `K3S_SERVER_OWNED_KEYS` and `K3S_AGENT_OWNED_KEYS` frozensets with exactly the members in decision 3 of `docs/plans/PLAN-node-customisation-phase-02-k3s-config.md`, each key commented with the plugin line that depends on it (survey finding 5 has them); add `K3S_RELEASE_FLOOR = (1, 21, 1)` with a comment citing k3s commits `a0a1071aa5` (drop-ins, v1.21.0+k3s1) and `8f1a20c0d3` (`+`, v1.21.1+k3s1). Add pure module functions after `validate_node_sizes()` (line 280): `validate_k3s_config(config, role)` per decisions 1-3 (`None` -> `{}`; returns the `yaml.safe_dump(config, default_flow_style=False, sort_keys=True)` text, or `''` for an empty mapping; a key is owned if `key.rstrip('+')` is in the role's set, except exactly `tls-san+`); `read_k3s_config(path, role)` per decision 1 (`open(path, encoding='utf-8')`, catch `OSError`/`UnicodeDecodeError`/`yaml.YAMLError` into `K3sConfigError.unreadable`, then validate, then return the mapping, not the text); `check_k3s_release(release, channel)` per decision 6 (regex `^v(\d+)\.(\d+)\.(\d+)` on the release string; rc versions such as `v1.18.2-rc3+k3s1` parse as their numbers). In `Cluster.create()` (line 1334) add keyword arguments `server_config=None, agent_config=None` after the six size arguments, document them in the docstring (where they end up on the node, what is refused and why, that they are recorded so expand-workers reuses the agent one), call `validate_k3s_config()` for both immediately after `validate_node_sizes(node_sizes)` (line 1453) and extend the comment above it, call `check_k3s_release(target_release, release_channel)` immediately after `get_k3s_release()` (line 1481) -- both before the name is registered at line 1507, which is the point -- and record `'server_config': server_config or {}` and `'agent_config': agent_config or {}` in the initial metadata dict after `node_sizes` (line 1595), with a comment in the style of the `node_sizes` one. In `show()` (line 1743) fill `server_config` and `agent_config` with `{}` when absent, on the same deep copy as `node_sizes` (copy once, not three times), and extend its docstring. Tests in `tests/test_cluster.py` (testtools, no mocks needed for the pure functions): `validate_k3s_config` accepts `{}`, `None` and a realistic mapping (`disable`, `node-label`, `tls-san+`, `node-taint: []`, `kubelet-arg`); refuses a list, a string, an int key, a `datetime.date` value, each owned key per role with and without `+`, bare `tls-san` but not `tls-san+` on a server, and a string value whose dumped form has a line equal to `SFK3SCONFIG`; `read_k3s_config` refuses a missing file, a non-UTF-8 file, invalid YAML and a two-document file, and returns `{}` for an empty file; `check_k3s_release` accepts `v1.21.1+k3s1` and `v1.36.5+k3s1`, refuses `v1.21.0+k3s1`, `v1.20.15+k3s1`, `v1.18.2-rc3+k3s1` and `stable`. Tests in `tests/test_library_api.py`, mirroring `test_an_invalid_size_registers_nothing` and the `NodeSizingTestCase` there: an owned key in either config, and a too-old release (patch `primitives.get_k3s_release` to return `v1.20.15+k3s1`), each raise and register no name, network or instance; a valid create records both mappings; a default create records `{}` for both. A `show()` test for metadata without the keys. Unit tests verify all of this. Commit subject: `Validate and record k3s configuration.` |
| 2b | high | opus | worktree | Node-side writes, per decisions 4, 5 and 7 of `docs/plans/PLAN-node-customisation-phase-02-k3s-config.md`; 2a has landed and `validate_k3s_config()` returns the dumped text. Add `Cluster._k3s_config_commands(self, md, role, first_server=False)` in `cluster.py` returning a list of shell commands: `mkdir -p /etc/rancher/k3s/config.yaml.d`; then `config.yaml` via `"cat - > /etc/rancher/k3s/config.yaml << 'SFK3SCONFIG'\n...\nSFK3SCONFIG\n"` (use the constant, never a literal), built with `yaml.safe_dump` of a dict rather than string formatting -- for a server `{'write-kubeconfig-mode': '0644', 'tls-san': [md['api_address_floating']]}` plus `'cluster-init': True` when `first_server`, plus `'node-taint': ['node-role.kubernetes.io/control-plane:NoSchedule']` when `md['worker_nodes']` is non-empty; for an agent a single comment line `# Written by shakenfist_client_k3s; caller configuration is in config.yaml.d/.`; then, if `validate_k3s_config(md.get(role + '_config', {}), role)` returns non-empty text, `config.yaml.d/50-sf-client-k3s.yaml` with that text; then, for a server when `md.get('metallb_installed', True)`, `config.yaml.d/90-sf-client-k3s-enforced.yaml` with `disable+:\n- servicelb\n`. Re-validating at write time is deliberate: a direct library caller can reach the install methods without `create()`, and the delimiter check must hold for what is actually written. Replace the hand-built heredoc in `install_control_plane()` (lines 1047-1055, keep the `mkdir -p /etc/rancher/k3s/`'s intent) with `cmds.extend(self._k3s_config_commands(md, 'server', first_server=True))`, keeping the comment about why the SAN matters and updating it. In `install_k3s_component()` (line 1137) map `node_role` (`'server'`/`'agent'`) to the role and prepend the helper's commands before `apt-get update`, so the files exist before the installer starts the service. Write a comment block above the helper explaining the three files and why servicelb is in the enforced drop-in rather than `config.yaml` (k3s replaces a list key in a later file unless written with `+`; see survey finding 4) and why the taint is conditional (survey finding 7). Extend `_control_plane_and_metallb_commands()` (`tests/test_cluster.py:2058`) so it also collects `install_k3s_component()` commands for a server and an agent with non-empty configs in the metadata -- `HeredocDelimiterTestCase` must then see the new heredocs. New tests, unit only: the first server's `config.yaml` parses (strip the heredoc framing, `yaml.safe_load`) to exactly the expected keys with and without workers; an extra server gets the same without `cluster-init`; an agent gets the comment-only file; the 50 drop-in is present only when the role's config is non-empty and round-trips to the recorded mapping; the 90 drop-in is present on servers only when `metallb_installed` is true or absent, and never on agents; a `node-taint: []` server config produces a 50 file whose parse has `node-taint == []` (the opt-out is the caller's file replacing the default); every config command precedes the `curl ... get.k3s.io` command in its list; `expand_workers()` on metadata with an `agent_config` writes it to the new workers; `expand_workers()` on metadata without one writes no 50 file. Verify by mutation as well as by running the tests: move the drop-in after the installer, drop the `worker_nodes` condition, and write the enforced file on agents, confirming a test fails for each. Unit tests verify the commands; whether k3s merges them as intended is phase 3's live check. Commit subject: `Write k3s configuration onto every node.` |
| 2c | medium | sonnet | none | Add `--server-config` and `--agent-config` to `k3s create` in `shakenfist_client_k3s/__init__.py` after the six sizing options, each `type=click.Path(exists=True, dir_okay=False)` with help text saying it is a YAML mapping of k3s configuration keys applied to every control plane node (respectively every worker, including workers added later by expand-workers), that a few keys the plugin depends on are refused, and pointing at `docs/usage.md`. In `k3s_create()` read each given path with `read_k3s_config(path, 'server')` / `read_k3s_config(path, 'agent')` (import beside `Cluster, DEFAULT_NODE_SIZE` from `shakenfist_client_k3s.cluster`) and pass `server_config=` / `agent_config=` to `c.create()`; pass `None` when the option was not given. Regenerate `tests/cli_contract/create.txt` with `CliRunner` at `terminal_width=80` exactly as `_assert_help_matches` does, never by hand; the diff must be additions only. Add tests to `tests/test_commands.py` beside `CreateNodeSizingOptionsTestCase`: both options reach `Cluster.create()` as parsed mappings; a file containing `token: x` exits non-zero with the `K3sConfigError` message and `create()` is never called; omitting both passes `None`. Unit tests suffice. Commit subject: `Add k3s configuration options to k3s create.` |
| 2d | medium | sonnet | none | Document the pass-through. In `docs/usage.md`: add the two options to the `create` options table; add a `#### k3s configuration` subsection after `#### Sizing` covering the three files and their order on the node (decision 4 of `docs/plans/PLAN-node-customisation-phase-02-k3s-config.md`), k3s's merge rule (later files replace list keys unless the key ends in `+`), the refused keys per role with one clause each on why, `tls-san+` for extra SANs, the default control plane taint and its `node-taint: []` opt-out and `node-taint+` addition, the zero-worker exception, and the v1.21.1+k3s1 release floor; an OpenStack-Helm-flavoured example pair of files (`disable: [traefik]` and `node-label: [openstack-control-plane=enabled]` for servers; `node-label: [openstack-compute-node=enabled, openvswitch=enabled]` for agents); and a short **Behaviour changes** note: servicelb is disabled whenever MetalLB is installed, control plane nodes are tainted when the cluster has workers, and with the default one control plane and two workers Longhorn now has two storage nodes rather than three. Extend the `show` and `expand-workers` sections for the recorded configs. In `docs/library-api.md`: add `server_config` / `agent_config` to `create()`'s keyword list, add `K3sConfigError` and `UnsupportedReleaseError` rows to the exceptions table in the voice of `NodeSizeError`, and list `read_k3s_config()` beside `read_manifests()` only if the library docs already list `read_manifests()` as public. Wrap prose at the width the surrounding text uses, and check no line exceeds it. Do not touch README.md, ARCHITECTURE.md or AGENTS.md. Commit subject: `Document k3s configuration pass-through.` |

The management session reviews each step against the files rather than
the sub-agent's summary. For every step it runs `tox -epy3`,
`tox -eflake8`, `pre-commit run --all-files` and
`python3 -c 'import shakenfist_client_k3s'`, then commits. Step 2b runs
in a worktree because it rewrites the one heredoc every cluster
depends on; its output is merged back only after review.

## Risks and mitigations

1. **The k3s merge semantics are read from source, not observed.**
   Three things in decisions 4, 5 and 7 have not been seen on a node:
   the enforced `disable+` appending to a caller's bare `disable`, the
   caller's `node-taint` replacing `config.yaml`'s, and drop-ins loading
   on agents. Mitigation: phase 3 asserts all three with `kubectl`. If
   the management session can spare a cluster before then, a manual
   create with `--server-config` containing `disable: [traefik]` and
   one `kubectl get pods -A` settles the first.
2. **Default behaviour changes reach existing users on upgrade.**
   servicelb disappears, and control plane nodes become unschedulable
   for workloads. Release notes are generated from pull request titles
   (`release.yml:311`), so neither would appear there unprompted.
   Mitigation: the pull request title and description state both, and
   `docs/usage.md` carries the note from step 2d. Existing clusters are
   untouched: nothing rewrites a running node's config.
3. **Longhorn loses the control plane as a storage node.** Its default
   replica count is 3. On the default 1 + 2 shape there are now two
   schedulable nodes, so a new volume runs degraded with two replicas.
   It works, but it looks wrong in the Longhorn UI. Mitigation: documented
   in 2d. Matching Longhorn's `defaultReplicaCount` to the worker count is
   a separate change with its own trade-offs, recorded in master plan
   Future work and not done here.
4. **A plugin-owned key list goes stale** as k3s grows flags. Mitigation:
   the per-key comments say which plugin line depends on each, so the
   list is re-derivable. An unknown key is k3s's to ignore, not the
   plugin's to refuse.
5. **The heredoc guard silently skips the new code** (survey finding 2).
   Mitigation: 2b extends `_control_plane_and_metallb_commands()`, and
   the reviewer checks that `HeredocDelimiterTestCase` fails when one new
   delimiter is unquoted.

## Definition of done

* `grep -n "cat - > /etc/rancher/k3s/config.yaml << 'EOF'" shakenfist_client_k3s/cluster.py`
  finds nothing: the hand-built heredoc is gone.
* `grep -n 'self._k3s_config_commands(' shakenfist_client_k3s/cluster.py`
  shows a call in `install_control_plane()` and one in
  `install_k3s_component()`, and no other writer of `config.yaml`
  exists (`grep -n 'rancher/k3s/config' shakenfist_client_k3s/cluster.py`
  finds only the helper and comments).
* Each of `cluster-init`, `data-dir`, `https-listen-port`, `node-name`,
  `server`, `tls-san`, `token`, `token-file`, `with-node-id`,
  `write-kubeconfig`, `write-kubeconfig-mode` has a test refusing it
  in a server config.
* With one new heredoc delimiter unquoted by hand,
  `tox -epy3 -- HeredocDelimiterTestCase` fails.
* `git diff develop -- shakenfist_client_k3s/tests/cli_contract/`
  contains only `+` lines, all in `create.txt`.
* `tox -epy3`, `tox -eflake8` and `pre-commit run --all-files` pass;
  `python3 -c 'import shakenfist_client_k3s'` succeeds.
* `K3sConfigError` and `UnsupportedReleaseError` each appear in
  `exceptions.py` and in the `docs/library-api.md` exceptions table.
* `docs/usage.md` states the release floor as `v1.21.1+k3s1`, and no
  other page states a different one (`grep -rn '1\.21' docs/`).
* The merge-tier functional CI passes on this phase's merge queue run.
  That is the live check that a default create still builds with the
  new files, the enforced drop-in and the taint.
* The master plan's Execution table and `docs/plans/index.md` agree on
  this phase's status.

## Back brief

Before executing any step, back brief the operator on how the work
aligns with this plan.

There is one gate. Decision 5 (servicelb via an enforced drop-in) and
decision 7 (no taint without workers) both depart from the master plan
as written. If the operator disagrees with either, change it here
before 2b starts, because 2b is the step that builds the node files
around them. 2a does not depend on either and can proceed regardless.
