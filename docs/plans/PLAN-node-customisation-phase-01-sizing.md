# Node customisation phase 1: per-role sizing

## Prompt

Before responding to questions or discussion points in this document,
read `Cluster.create()`, `create_instance()`,
`create_and_await_instances()`, `show()` and `expand_workers()` in
`shakenfist_client_k3s/cluster.py`, the `k3s create` and `k3s show`
commands in `shakenfist_client_k3s/__init__.py`, and
`FakeClusterClient` in `shakenfist_client_k3s/tests/fakes.py`. Ground
answers in what that code does today. The master plan is
[PLAN-node-customisation.md](PLAN-node-customisation.md); its decisions 2, 5 and
6 and open question 3 are what this phase implements, and they are not
restated here except where the survey changed them.

This phase changes the namespace-metadata representation of cluster
state, which `PLAN-TEMPLATE.md` says to plan at high effort, and it
was. Nothing in it can only be validated live except that Shaken Fist
honours the sizes it is handed; see decision 7.

## Planning effort

High, for the reason above. Review effort: medium. The diff is small
and the tests carry most of the weight; the review's job is to check
the metadata fallback and the validation ordering, not to rethink the
design.

## Scope

In:

* `create_instance(node_type)` reading per-role sizes from metadata.
* `node_sizes` recorded in metadata at create time, with a read-time
  fallback to today's 2 / 2048 / 50 for clusters created before it.
* Six CLI options on `k3s create` and six matching keyword arguments
  on `Cluster.create()`, validated before anything is built.
* `show` reporting sizes for every cluster, old ones included.
* Removing the bare `sudo apt-get install -y` in
  `install_k3s_component()`.
* Unit tests, regenerated `create.txt` CLI contract fixture, and
  `docs/usage.md` / `docs/library-api.md`, including the documented
  sizing floor from open question 3.

Out:

* k3s configuration pass-through, the default control plane taint and
  disabling servicelb: phase 2.
* Live validation with non-default sizes: phase 3 (but see decision 7).
* Sizing overrides on `expand-workers`: master plan Future work.
* Exposing the options through the `shakenfist.k3s` Ansible
  collection. That collection is not on `develop` yet (PR #90); see
  survey finding 4.

## What the survey found

### 1. Every code reference in the master plan is accurate

Checked against `develop` at `65b4791`: `create_instance()` at
`cluster.py:416-442` with the hardcoded `2, 2048` at 420-421 and
`'size': 50` at 432; `BASE_OS_VERSION` at 51;
`create_and_await_instances(count, node_type)` at 668, calling
`self.create_instance()` with no arguments at 684; the first control
plane's `config.yaml` at 949-956; `install_k3s_component()` at
1038-1065 with the bare `sudo apt-get install -y` at 1052;
`get_metadata()` / `set_metadata()` at 297-316; `join_address`'s
fallback at 1046; `expand_workers()` at 1944. Nothing needed
correcting there, which is worth saying because the plan was written
before phase 4 and phase 5 of the library API plan moved code around.

`create_instance()` has exactly one caller (`cluster.py:684`), so
making `node_type` a required argument breaks nothing in the tree.
`docs/library-api.md:98-114` lists `create_instance()` and
`create_and_await_instances()` as internal and unstable, so it breaks
no promise to callers of the released `v0.1.0` either. `create()` and
`show()` are on the stable side of that line, and every change to them
here is additive.

### 2. `show` already prints every metadata key

The master plan says "`show` displays sizes" as if it were work. The
CLI's `k3s_show` (`__init__.py:288-296`) prints every key `Cluster.show()`
returns, so a cluster created after this phase shows `node_sizes` with
no CLI change. The real work is clusters created before it, whose
metadata has no such key; see decision 4.

### 3. The master plan disagrees with itself about the sizing floor

Open question 3 says "Phase 1 should say so in `docs/usage.md`"; the
Execution table puts "the documented sizing floor from open question
3" in phase 2's row. Phase 1 is right: the floor belongs beside the
flags that let a caller go above it, and those arrive here. Corrected
at source -- the phase 2 row no longer carries it.

### 4. The library API plan has moved on since this plan was written

The master plan's Situation says the library API plan's phase 4 "is
planned on the unpushed branch `library-api-phase-04`". It merged as
`2506c19` (#81) and was released as `v0.1.0`. Phase 5, the Ansible
collection, is open as PR #90. Open question 2 (land before or after
phase 4?) is therefore answered by events, and the Future work bullet
"if phase 5 of the library API plan is written before this plan lands"
has come true: `sf_k3s_cluster` will need these options in a follow-on.
All three corrected at source in the master plan.

PR #90 also renames every plan file to `PLAN-*.md`
(`0b3fe70 Name the plan files the way the convention says.`), and
touches `cluster.py` -- but only docstring paths, at lines 6, 1608 and
1974, none of which this phase edits. See risk 1.

### 5. `FakeClusterClient.create_instance()` discards the sizes

`tests/fakes.py:101` accepts `cpus`, `memory` and `disks` and records
none of them, so no existing test can assert what size anything was
built at. It records `sshkey` beside the instance in
`instance_sshkeys` for the reason its comment gives; sizes need the
same treatment.

### 6. The CLI contract test's docstring forbids what this phase does

`test_cli_contract.py:40-53` says a changed `--help` output is a bug,
with one exception for a phase that adds a *command*. This phase adds
*options* to an existing command, which changes `create.txt`. Phase 3
of the library API plan did the same for `--manifest` and regenerated
the fixture without widening the exception. Step 1c widens it, so the
next phase is not told its correct change is a bug.

### 7. Removing the bare install is safe

`sudo apt-get install -y` with no package names exits 0 and does
nothing. It has been there since the initial commit (`e8ecd31`). Its
neighbour, `sudo apt-get update`, is redundant after
`instance_os_update()` but harmless, and is left alone. `curl`, which
the next command needs, is not installed anywhere in the plugin for the
first control plane node either: it is in the Debian 12 base image.

## Decisions

1. **The library takes six flat keyword arguments, not a mapping.**
   `create(..., control_plane_cpus=2, control_plane_memory=2048,
   control_plane_disk=50, worker_cpus=2, worker_memory=2048,
   worker_disk=50)`. `docs/library-api.md:140-146` promises that
   `create()`'s keyword arguments mirror the command's options of the
   same name, and the Ansible module in PR #90 maps flat options
   one-to-one. The nested shape is the metadata's business, not the
   caller's.

2. **One source of truth for the defaults.** A module constant in
   `cluster.py`, `DEFAULT_NODE_SIZE = {'cpus': 2, 'memory': 2048,
   'disk': 50}`, is read by `create()`'s defaults, the CLI's defaults
   and the metadata fallback. Three copies of 2048 is how the docs and
   the code disagree in a year.

3. **Validation is a pure module-level function, run first.**
   `validate_node_sizes(sizes)` takes the nested mapping and raises a
   new `exceptions.NodeSizeError` naming the role, the field and the
   value for anything that is not a positive `int`. `bool` is rejected
   explicitly, because `True` is an `int` in Python and `cpus=True`
   would otherwise build a one-vCPU node. `create()` calls it beside
   `read_manifests()`, before the name is registered in the cluster
   list -- an invalid size discovered after that point leaves a claimed
   name and a metadata document stuck in `initial`. The CLI also uses
   `click.IntRange(min=1)` so the command line fails in click's own
   style before reaching the library, but the library check is the one
   that matters, because a library caller has no click.

4. **Old clusters fall back at read time, and `show()` reports the
   fallback.** A `Cluster._node_size(md, node_type)` helper returns
   `md.get('node_sizes', {}).get(node_type, DEFAULT_NODE_SIZE)` (a
   copy). `create_instance()` uses it, so `expand-workers` on a
   pre-change cluster builds exactly what it built before. `show()`
   returns its metadata with `node_sizes` filled in when absent. This
   is the decision a reviewer is most likely to question, since `show`
   otherwise reports what is stored. It is right here because the
   filled-in values are not a guess: before this phase there was no
   way to build a node at any other size, so 2 / 2048 / 50 is a
   statement of fact about every such cluster. Nothing is written
   back; `show()` stays read-only.

5. **Sizes are recorded with the rest of the initial metadata**, in
   the dict `create()` builds at `cluster.py:1391-1426`, next to
   `metallb_installed`. That is before any instance exists, so an
   interrupted create still describes what it was building.

6. **`create_instance(node_type)` takes the role as a required
   argument.** No default: a default of `'worker'` would silently size
   a control plane node as a worker the day someone adds a caller and
   forgets it. The method is internal (survey finding 1).

7. **Live validation stays in phase 3, but the merge queue already
   covers the default path.** The functional CI's merge-tier job
   builds a real cluster with default sizes, which proves the
   refactor did not change what a default create builds. That
   Shaken Fist honours non-default integers is the one live-only
   claim, and phase 3 checks it. 33fl is waiting on this phase; if it
   uses the flags before phase 3 lands, that is an early live check,
   and anything it finds is a phase 3 finding.

8. **Documented floor: 4096 MB for a control plane node**, with the
   measurements from open question 3 summarised and the 2048 MB
   default explicitly described as one that runs but does not hold up
   under load. The default itself does not change (master plan
   decision 2); validation still accepts any positive integer (open
   question 3).

9. **The phase file is named for `develop` as it is today**, i.e.
   without the `PLAN-` prefix PR #90 introduces. Whichever of the two
   pull requests merges second renames to match; see risk 1.

## Step plan

| Step | Effort | Model | Isolation | Brief for sub-agent |
|------|--------|-------|-----------|---------------------|
| 1a | low | sonnet | none | In `shakenfist_client_k3s/cluster.py`, `install_k3s_component()` (around line 1052), delete the list element `'sudo apt-get install -y',` -- it names no package and is a no-op. Leave `'sudo apt-get update'` and the curl command alone. No test asserts on this list (grep `apt-get install` under `tests/` to confirm). Update the "Bugs fixed during this work" bullet in `docs/plans/PLAN-node-customisation.md` to say it was fixed in this commit rather than "in phase 1". Unit tests suffice. Commit subject: `Drop an apt-get install that names nothing.` |
| 1b | high | opus | none | Implement per-role sizing in the library. In `shakenfist_client_k3s/cluster.py`: (1) add `DEFAULT_NODE_SIZE = {'cpus': 2, 'memory': 2048, 'disk': 50}` beside `BASE_OS_VERSION` (line 51), with a comment that memory is MB and disk GB as the Shaken Fist API takes them; (2) add module-level `validate_node_sizes(sizes)` beside `read_manifests()`, taking `{'control_plane': {...}, 'worker': {...}}` and raising a new `exceptions.NodeSizeError` (add it in `exceptions.py` after `SshKeyError`, following that class's shape: a `K3sClusterException` subclass with a docstring and a classmethod constructor, e.g. `not_positive_integer(role, field, value)`) for any value that is not an `int`, is a `bool`, or is < 1; the message names the role with underscores replaced by spaces, the field, and `repr(value)`; (3) add keyword arguments `control_plane_cpus`, `control_plane_memory`, `control_plane_disk`, `worker_cpus`, `worker_memory`, `worker_disk` to `Cluster.create()` (line 1236) with defaults taken from `DEFAULT_NODE_SIZE`, document them in its docstring, build the nested mapping, and call `validate_node_sizes()` immediately after `read_manifests(manifests)` (around line 1298) -- before the name is registered in the cluster list, which is the point; (4) record the mapping as `'node_sizes'` in the initial metadata dict (line 1391-1426), with a comment in the style of the `metallb_installed` one saying readers must fall back to `DEFAULT_NODE_SIZE` for clusters created before the key existed; (5) add `Cluster._node_size(self, md, node_type)` returning a copy of `md.get('node_sizes', {}).get(node_type, DEFAULT_NODE_SIZE)`; (6) change `create_instance(self)` to `create_instance(self, node_type)` with no default, using `_node_size()` for cpus, memory and the disk `size`, and pass `node_type` from `create_and_await_instances()` (line 684); (7) in `show()`, return a copy of the metadata with `node_sizes` filled from `DEFAULT_NODE_SIZE` for both roles when absent, and say in the docstring why that is fact rather than guess (before this key existed every node was built at the default). Do not write the fallback back to metadata. In `tests/fakes.py`, make `FakeClusterClient.create_instance()` record `(name, cpus, memory, disks[0]['size'])` in a new `instance_sizes` list, beside `instance_sshkeys`, with a comment. Tests, in the existing testtools style: `validate_node_sizes` accepts the defaults and rejects 0, -1, `True`, `2.0`, `'2'` and `None`, naming role and field; `create()` with an invalid size raises `NodeSizeError` and registers no name and creates no network or instance (mirror `test_create_registers_the_name_before_building_anything` in `tests/test_library_api.py:190`); `create()` with distinct sizes per role reaches `client.create_instance` with the right values for each role and records `node_sizes`; a default `create()` builds 2 / 2048 / 50 everywhere; `expand_workers()` on a cluster with recorded worker sizes uses them; `expand_workers()` on metadata with no `node_sizes` key builds 2 / 2048 / 50; `show()` on such metadata reports the defaults and does not call `set_metadata`. Unit tests verify all of this; nothing here needs a live cluster. Commit subject: `Size control plane and worker nodes separately.` |
| 1c | medium | sonnet | none | Add six options to `k3s create` in `shakenfist_client_k3s/__init__.py` (after `--manifest`, line 187): `--control-plane-cpus`, `--control-plane-memory`, `--control-plane-disk`, `--worker-cpus`, `--worker-memory`, `--worker-disk`, each `type=click.IntRange(min=1)` with its default read from `DEFAULT_NODE_SIZE`, imported beside `Cluster` in the existing `from shakenfist_client_k3s.cluster import Cluster` line and help text stating the unit (vCPUs, MB, GB). Pass them through to `c.create()` by keyword. Regenerate `tests/cli_contract/create.txt` by invoking `create --help` through `CliRunner` with `terminal_width=80` exactly as `_assert_help_matches` does, never by hand; the fixture diff must be additions only. Widen the exception in `CliContractTestCase`'s docstring (`tests/test_cli_contract.py:40-53`) from "a phase which deliberately adds a command" to "adds a command or an option", with the same rule that nothing existing may change. Add a CLI test (in `tests/test_commands.py`, following how it drives `k3s create`) that the options reach `Cluster.create()` and that `--worker-memory 0` is refused by click without calling `create()`. Unit tests suffice. Commit subject: `Add per-role sizing options to k3s create.` |
| 1d | medium | sonnet | none | Document sizing. `docs/usage.md`: add the six options to the `create` table (line 27-39) with defaults and units; rewrite the paragraph at line 41-42 so it says nodes default to 2 vCPUs, 2048 MB and 50 GB per role and are sized by those options, and that `expand-workers` builds new workers at the size the cluster recorded; add a short "Sizing" paragraph documenting a realistic floor of 4096 MB for a control plane node, summarising the measurement in `docs/plans/PLAN-node-customisation.md` open question 3 (k3s-server ~709 MB RSS on a 2048 MB node, a burst of pod creations drove it into global OOM and the API server down for ~30s, and it is not reproducible because it depends on how recently k3s restarted) and saying the plugin deliberately validates only positive integers; extend the `show` section (line 254) to mention `node_sizes`, and that clusters created before sizing existed report the defaults they were built at. `docs/library-api.md`: add the six keyword arguments to the list at line 140-146, add `NodeSizeError` to the exceptions table (line 237-245) in the same voice as `SshKeyError`, and add `validate_node_sizes()` beside `read_manifests()` as a stable module-level name only if 1b made it one -- otherwise leave it unlisted (it is internal). Do not touch README.md, ARCHITECTURE.md or AGENTS.md; nothing here changes the pitch, the shape of the system or a convention. Commit subject: `Document per-role node sizing.` |

The management session reviews each step, runs `tox -epy3`,
`tox -eflake8`, `pre-commit run --all-files` and
`python3 -c 'import shakenfist_client_k3s'`, and commits.

## Risks and mitigations

1. **PR #90 renames every plan file.** If it merges first, this branch
   rebases and `git mv`s `node-customisation-phase-01-sizing.md` (and
   the master plan, which #90 will already have renamed) to the
   `PLAN-` names, fixing the links in the master plan and
   `docs/plans/index.md`. If this merges first, #90 needs the same on
   its side. The management session checks `gh pr view 90 --json
   state` before opening this phase's PR and before merging it.
2. **A size check placed one line too late** turns a typo into a
   claimed name and a stuck `initial` document. Mitigation: the test
   in 1b asserting no name, network or instance after a `NodeSizeError`,
   which the reviewer reads rather than trusts.
3. **The fallback hides a bad metadata document.** A `node_sizes` key
   present but missing a role would fall back silently. Only `create()`
   writes the key, always with both roles, so this cannot happen from
   the plugin; it is accepted rather than validated, and the reviewer
   confirms no other writer exists (`grep -n node_sizes`).
4. **Shaken Fist refuses a size** (quota, flavour limits, a hypervisor
   without the memory). That surfaces as the API client's exception
   from `create_instance()`, mid-create, exactly as a bad network does
   today. Not new, and phase 3 will see it if it happens.

   **This happened.** #90 merged first (`eb248bd`), and this branch was
   rebased onto it: the phase file was renamed to
   `PLAN-node-customisation-phase-01-sizing.md` and every link to it and
   to the master plan updated. Survey finding 4 and decision 9 are left
   as written, since they describe the tree this plan was drafted
   against.

## Definition of done

* `grep -n "2, 2048\|'size': 50" shakenfist_client_k3s/cluster.py`
  finds nothing: the hardcoded sizes are gone.
* `grep -rn 'apt-get install -y'"'"',' shakenfist_client_k3s/cluster.py`
  finds nothing.
* `grep -n 'create_instance(' shakenfist_client_k3s/cluster.py` shows
  every call passing a role.
* `git diff develop -- shakenfist_client_k3s/tests/cli_contract/`
  contains only `+` lines in `create.txt` and no other file.
* `sf-client k3s create --help` lists all six options with units.
* The tests named in 1b and 1c exist and pass under `tox -epy3`;
  `tox -eflake8` and `pre-commit run --all-files` pass;
  `python3 -c 'import shakenfist_client_k3s'` succeeds.
* `docs/usage.md` no longer says every node is 2 vCPUs / 2GB / 50GB
  as a fixed fact, and states the 4096 MB control plane floor.
* `NodeSizeError` appears in both `exceptions.py` and the
  `docs/library-api.md` exceptions table.
* The merge-tier functional CI passes on the pull request's merge
  queue run, which is the live check that a default create still
  builds.
* Master plan Execution table and `docs/plans/index.md` agree on this
  phase's status.

## Back brief

Before executing any step, back brief the operator on how the work
aligns with this plan. There is no shape gate in this phase -- every
step is cheap to redo -- but step 1b is the one to read closely,
because decision 4 (`show()` reporting the fallback) is the one
judgement call the master plan did not already make.
