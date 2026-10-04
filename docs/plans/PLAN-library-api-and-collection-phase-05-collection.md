# Collection phase 5: `shakenfist.k3s` on Galaxy

## Prompt

Before responding to questions or discussion points in this document,
read `shakenfist_client_k3s/client.py` in full, then
`shakenfist/deploy/collection/plugins/module_utils/sf_connection.py`
and one module beside it (`sf_namespace.py` is the closest shape) in
the Shaken Fist server repository, then
`shakenfist/tools/build-collection.py` and the `build-collection` and
`publish-collection` jobs in that repository's `release.yml`. Ground
your answers in what those files do rather than in what this plan says
they do.

Two things about this phase are unlike the four before it. It publishes
to a second package index with a second credential, so it repeats
phase 4's shape -- irreversible publication behind a credential no
agent can create -- on machinery that, unlike `release.yml`'s Python
half, has never executed anywhere. And its deliverable is an Ansible
module argument spec, which is a public interface in the same way a
published sdist's `[build-system]` table is: additive growth is cheap,
renaming or retyping a parameter is not.

## Planning effort

**High**, which departs from the master plan's "In this project" note.
That note puts "the collection build machinery copied from
`shakenfist/tools/build-collection.py`" at medium effort, on the
premise that it is an established pattern. Survey finding 1 shows the
premise is half false: the machinery exists and is readable, but it has
never run -- the repository it lives in has no releases at all -- so
"copied from a working example" is not what this is. The module's
argument spec needs the other half of the high rating on its own
merits.

## Scope

In scope:

1. The collection skeleton: `galaxy.yml`, `meta/runtime.yml`,
   `README.md`, `requirements.txt`, `.ansible-lint`.
2. One module, `sf_k3s_cluster`, with check mode and structured
   returns.
3. Unit tests for the module, in this repository's existing test
   layout.
4. `tools/build-collection.py` and a `build-collection` job, dry-run
   before anything is tagged.
5. `ansible-lint` in pre-commit and in the pull request tier of CI.
6. The Ansible Galaxy credential, which is a human gate.
7. A `publish-collection` job, and the first `shakenfist.k3s` release.
8. Operator documentation in `docs/`.

Explicitly out of scope:

- **Per-role node sizing, k3s configuration pass-through and the
  control plane taint.** These are survey finding 5: four prerequisites
  33fl added on 2026-10-03, planned separately as the
  `node-customisation` master plan, whose pull request #87 is open and
  whose status is `Not started`. Decision 8 explains why this phase does
  not wait for them.
- **Worker count.** The module never manages it; that is the master
  plan's second decision and `PLAN-k3s-ci-runners.md` design decision
  5, and it is the whole reason the module is not a declarative
  reconciler.
- **Roles.** This collection ships a module and nothing else. The core
  collection's roles deploy a cloud; a k3s cluster inside one needs no
  equivalent.
- **Raising `requires-python`.** Still
  shakenfist/client-python-k3s#82, untouched here.
- **Correcting 33fl's plan.** Survey finding 6 records what is stale
  there; it is another repository and another plan's business.

## What the survey found

The master plan's phase 5 row and its first decision were written in
early September, before phases 1-4 ran. Nine findings, of which four
change what this phase does.

### 1. The build machinery has never run

`shakenfist/tools/build-collection.py` exists (76 lines), and
`release.yml` in that repository has `build-collection` at line 63 and
`publish-collection` at line 202, so the master plan's "copyable almost
verbatim" is true as a statement about text. It is misleading as a
statement about risk:

```
$ gh run list -R shakenfist/shakenfist --workflow release.yml --limit 6
(no output)
$ gh release list -R shakenfist/shakenfist --limit 3
(no output)
```

That repository has never run its release workflow and has never cut a
GitHub release, so neither job has ever executed. This phase is the
first exercise of both, in either repository.

### 2. The Galaxy credential does not exist anywhere visible

`publish-collection` authenticates with
`--api-key "${{ secrets.ANSIBLE_GALAXY_TOKEN }}"`. That secret is not
set in this repository, which has no Actions secrets at all, and is not
set in the server repository either:

```
$ gh api repos/shakenfist/client-python-k3s/actions/secrets --jq '.secrets[].name'
(nothing)
$ gh api repos/shakenfist/shakenfist/actions/secrets --jq '.secrets[].name'
DEPENDENCIES_TOKEN
```

An organisation-level secret would also satisfy the reference, and
whether one exists could not be established -- listing them needs the
`admin:org` scope and returns 403. **This is an open question for
Michael, not a finding**, and it is on the critical path: step 5e
cannot be written as "create a token" if one already exists at the
organisation level.

Either way this is phase 4's shape again. A Galaxy token is created
through a logged-in browser and no agent can make one.

### 3. The Galaxy namespace is claimed and still empty

```
$ curl -s -o /dev/null -w '%{http_code}' https://galaxy.ansible.com/api/v3/namespaces/shakenfist/
200
$ curl -s '...search/collection-versions/?namespace=shakenfist' | jq .meta.count
0
```

So the master plan's "already claimed and currently empty" still holds.
But the master plan's **open question 1** recommended letting a core
release go first, "so the token and namespace-permission path is
validated on the component we understand best". Finding 1 makes that
recommendation unachievable on any timeline this phase controls: the
core repository has no release process that has ever run. Waiting is
unbounded. Settled by decision 1.

### 4. The connection story already exists, built for this module

`shakenfist_client_k3s/client.py` (82 lines) was added by phase 2 and
its docstring names `sf_k3s_cluster` as a caller. It already does the
three things the module needs:

- all three connection parameters verbatim with
  `suppress_configuration_lookup=True`, or full discovery when none are
  given;
- `ValueError` on a partial set, rather than the server collection's
  fall-back-to-discovery, with a message naming what was supplied;
- `apiclient.UnconfiguredException` deliberately *not* caught, with a
  comment saying the collection translates it into `fail_json()`.

So the module does not need a copy of the server collection's
`sf_connection.py`, and should not have one -- that file's own header
records issue 4314, where per-module copies diverged. What the module
needs from it is the argument spec fragment (three parameters, `no_log`
on `key`) and the `fail_json()` translation, which is a dozen lines.
This reduces the phase's scope rather than growing it.

### 5. Four upstream prerequisites appeared on 2026-10-03

`33fl/docs/plans/PLAN-k3s-ci-runners.md` added prerequisites 8-11 the
day before this plan was written: per-role node sizing (`cluster.py`
hardcodes 2 vCPU / 2048 MB / 50 GB for every node), a `--disable`
passthrough for the k3s installer, cumulative health signals, and a
`node-role.kubernetes.io/control-plane:NoSchedule` taint. Three are
blocking for 33fl's phase 2.

They are not in this master plan's phase list. They are the
`node-customisation` master plan, registered in `docs/plans/index.md`
on 2026-09-27, `Not started`, four phases, with pull request #87 open
carrying the plan file and two commits. Decision 8 covers the
interaction.

### 6. 33fl's plan carries two claims this project just falsified

That plan says "The plugin **has never been released**. There are no
git tags and `shakenfist-client-k3s` 404s on PyPI, so 'install the
collection, pip install the plugin' is blocked until a first release is
cut." Both halves became false on 2026-10-03: `v0.1.0` is tagged and
PyPI serves it. Recorded here rather than fixed, because it is another
repository's plan; the person updating `PLAN-k3s-ci-runners.md` next
should know.

### 7. The wheel cannot accidentally ship the collection

Worth stating because a top-level `collection/` directory is exactly
the kind of addition that broke the wheel before.
`[tool.setuptools.packages.find]` has `include = ["shakenfist_client_k3s*"]`,
which is anchored, so no top-level directory can become a package; and
phase 4's review added `tools/check-wheel-build.sh` to the pull request
tier, which builds through the sdist and asserts twelve entries. The
sdist will grow, which is correct and harmless.

### 8. `ansible-lint` is absent, and no scaffolding exists

`.pre-commit-config.yaml` has skillsaw, actionlint and shellcheck and
nothing Ansible-aware; there is no `galaxy.yml`, `plugins/`, `meta/` or
`collection/` anywhere in the tree. Both as the master plan says.

### 9. Pull request #87 collides with this phase's first commit

This phase's first commit renames every plan file to the `PLAN-` prefix
the shared block prescribes. #87 adds
`docs/plans/node-customisation.md` without it, and adds a row to
`docs/plans/index.md` immediately below the rows the rename edits.
Whichever lands second leaves the tree half-converted. **This needs
coordinating rather than deciding**, and it is raised with Michael
rather than resolved here.

### Corrections made at source

The master plan's phase 5 row, its first decision's "copyable almost
verbatim" sentence, and its open question 1 are corrected in the same
commit as this plan, so the next reader does not have to rediscover
findings 1, 3 and 4. `docs/plans/index.md`'s row for the plan is
updated to describe phase 5 as it is actually scoped.

## Decisions this plan already takes

1. **`shakenfist.k3s` is the first collection published into the
   `shakenfist` namespace.** This settles the master plan's open
   question 1 against its own recommendation, because finding 1 shows
   the recommendation waits on something with no schedule. The
   consequence is accepted deliberately: this phase validates the token
   and namespace-permission path, and the core collection inherits a
   path someone has already walked. That is the same trade phase 4
   made with the PyPI pending publisher and it worked.

2. **The collection lives at `collection/` in the repository root.**
   The server repository uses `shakenfist/deploy/collection` because it
   sits inside a deployer tree; there is no equivalent here, and a
   top-level directory is what `ansible-galaxy collection build` wants
   pointed at. Finding 7 is why this is safe.

3. **The module reuses `shakenfist_client_k3s.client.make_client` and
   carries no copy of `sf_connection.py`.** It declares the three
   connection parameters itself (`key` with `no_log: True`) and
   translates two exceptions into `fail_json()`: `ValueError` for a
   partial connection and `apiclient.UnconfiguredException` for
   discovery finding nothing. Finding 4 is the reasoning, and issue
   4314 in the server repository is the precedent for not copying.

4. **`requirements.txt` pins `shakenfist_client_k3s>=0.1.0` as a static
   floor.** Not injected at build time alongside the version, even
   though `build-collection.py` is already rewriting that file's
   neighbour. The floor is a fact about what the module's source needs
   -- every verb it calls shipped in `0.1.0` -- not a fact about the
   build, and a build-time floor equal to the collection version would
   make a locally built development collection demand a plugin version
   PyPI does not have. Raise it in the commit that first uses a newer
   verb; the done criteria include a check that the floor is not
   ahead of the released plugin.

5. **`state: present` and `state: absent`, and no `worker_count`
   parameter at all.** Inherited rather than decided, from the master
   plan's second decision and `PLAN-k3s-ci-runners.md` design decision
   5: conductor owns worker membership and the module owns "exists at
   minimum shape", which is what keeps two writers off one
   read-modify-write metadata document. A `worker_count` parameter
   would be the entire race, politely spelled.

   **The creation-time count is `initial_workers`, and is never
   reconciled.** Refined in step 5b, because the decision above and
   `create()`'s signature pull in opposite directions: `create()`
   requires a `worker_count` argument, so a module which creates
   clusters has to say *something* about workers even though it must
   never manage them. `initial_workers` (integer, default 0) is the
   resolution. It is passed to `create()` only on the path which
   creates a cluster, and is read nowhere else: an existing cluster
   whose worker count differs from `initial_workers` is **not**
   `changed`, is not reported as a difference, and is never resized.

   The name carries that semantics on purpose, and is not negotiable
   for the same reason the parameter it replaces is forbidden. A
   parameter called `worker_count` invites the one-line "and if it
   differs, expand" change that reintroduces the race; a parameter
   called `initial_workers` makes that change read as the
   contradiction it is. The default of 0 rather than the command
   line's 2 follows from the same place: a play handing the cluster
   straight to a scaler wants control plane nodes and nothing else.

   The same never-reconciled rule applies to every other shape
   parameter the module takes -- `control_plane_count`,
   `metal_address_count`, `network`, `release_channel`, `sshkey`,
   `install_metallb`, `install_longhorn` and `manifests` are all
   creation-time only. Workers are the one singled out here because
   they are the one with a competing writer; the rest are simply verbs
   this library does not have.

6. **Check mode is supported, and is how idempotency is tested.** A
   module that cannot say "nothing to do" without doing it cannot be
   trusted in a playbook that runs twice, and the cluster state this
   reads is already in namespace metadata, so the check is cheap.

7. **`build-collection` lands and is dry-run before any tag;
   `publish-collection` cannot be dry-run, and its first exercise is
   the first tagged release.** Identical in shape to phase 4's
   `publish-pypi`, with the same event guard
   (`github.event_name == 'push' && startsWith(github.ref, 'refs/tags/v')`)
   and the same recovery: if it fails, burn the version and go to the
   next one rather than re-pushing a signed tag. Galaxy, like PyPI,
   does not allow a version to be replaced.

8. **This phase does not wait for `node-customisation`.** The argument
   spec grows additively -- `control_plane_size`, `worker_size` and a
   k3s configuration mapping are new optional parameters when they
   exist, which breaks no playbook written against this version. The
   cost is honest and worth stating: 33fl cannot use the published
   collection for CI runners until those land, so this phase does not
   unblock 33fl by itself. It was never going to, because 33fl's own
   plan puts those prerequisites in its phase 2 and the collection in
   its phase 3.

   This is the decision most likely to be argued with, and the argument
   against it is real: shipping a module that the one known consumer
   cannot yet use invites a second argument-spec revision immediately
   after the first publish, and every revision of a published interface
   costs more than the one before. The reason to go anyway is that the
   alternative is worse. Holding phase 5 behind a `Not started` master
   plan with four phases of its own makes the library-api plan's
   completion depend on work that has not been scoped, and the
   additive-growth property means the second revision is cheap in
   exactly the way the first would not be.

9. **One module, no roles.** Finding 8 and the scope section; stated as
   a decision because "a role that wraps the module" is the obvious
   next suggestion and the answer is that the module is the interface.

## Step plan

| Step | Effort | Model | Isolation | Brief for sub-agent |
|------|--------|-------|-----------|---------------------|
| 5a | medium | sonnet | none | **Done 2026-10-04, `fed33e5`.** **Commit subject: `Scaffold the shakenfist.k3s collection.`** Create `collection/` with `galaxy.yml` (namespace `shakenfist`, name `k3s`, `version: 0.0.0` with a comment saying `build-collection.py` injects the real one, `dependencies: {}` per the master plan's first decision, Apache-2.0, repository/homepage/issues pointing at this repository), `meta/runtime.yml` with `requires_ansible: '>=2.15.0'` to match the core collection, `README.md`, `requirements.txt` naming `shakenfist_client_k3s>=0.1.0` with decision 4's reasoning as a comment, and `.ansible-lint`. Model all five on `shakenfist/deploy/collection/` in the server repository, which you must read first -- including its `.ansible-lint` skip list, of which only `galaxy[no-changelog]` is likely to apply here (there are no roles, so `var-naming[no-role-prefix]` and `package-latest` have nothing to act on; do not copy skips that skip nothing). No module yet. Verify `ansible-galaxy collection build collection/ --output-path /tmp/...` succeeds and that `tools/check-wheel-build.sh` still reports twelve entries, and say both in the commit message. |
| 5b | high | opus | none | **Done 2026-10-04, `452954c`.** **Commit subject: `Add the sf_k3s_cluster Ansible module.`** Write `collection/plugins/modules/sf_k3s_cluster.py`. Read `shakenfist_client_k3s/client.py` and `shakenfist_client_k3s/cluster.py`'s `Cluster` constructor and `create()`/`delete()`/health verb signatures first, and `shakenfist/deploy/collection/plugins/modules/sf_namespace.py` for the house style of `DOCUMENTATION`/`EXAMPLES`/`RETURN` and `run_module()`. Parameters: `name` (required), `state` (`present`/`absent`, default `present`), the cluster's own namespace, the shape parameters `create()` takes today, and the three connection parameters with `no_log: True` on `key`. **No `worker_count`** -- decision 5. `supports_check_mode=True`. Build the client with `shakenfist_client_k3s.client.make_client()` and translate `ValueError` and `apiclient.UnconfiguredException` into `fail_json()` per decision 3; do not copy `sf_connection.py`. Route the reporter so progress does not go to stdout -- a module writing to stdout corrupts its own JSON, which is the single most important constraint in this step, and `Cluster`'s reporter is injectable precisely because phase 1 made it so. Return `changed` truthfully: existence-and-shape only, so an existing cluster at the requested shape is `changed: False` in both check mode and real mode. |
| 5c | high | opus | none | **Done 2026-10-04, `603c11a`** -- 29 tests, 20 mutations, suite 379 -> 408. **Commit subject: `Test the sf_k3s_cluster module.`** Unit tests under `shakenfist_client_k3s/tests/`, matching `test_cluster.py`'s `testtools.TestCase` style with `mock`, not pytest. Cover: the argument spec rejects a partial connection with a message naming what was supplied; `UnconfiguredException` becomes `fail_json` rather than a traceback; check mode never calls a mutating `Cluster` method; an existing cluster at the requested shape reports `changed: False`; `state: absent` on an absent cluster is `changed: False`; the module writes nothing to stdout (assert it, do not assume -- capture it). Importing the module requires `ansible-core` on the test path, so add it to `tox.ini`'s deps and say in the commit message what that does to the environment's install time. Then mutate the module on purpose and confirm each test fails for the right reason, carrying forward phase 4's mutation script in the scratchpad and stating the running count. |
| 5d | medium | sonnet | none | **Done 2026-10-04, `6af36ee`. Verified by dispatch [37161725657](https://github.com/shakenfist/client-python-k3s/actions/runs/37161725657): `build-collection` and `build` succeeded, the three publishing jobs skipped, and the uploaded artifact was a ten entry tarball at collection version `0.1.1-dev12+g813d144` with no `ansible_collections` path in it.** **Commit subject: `Build the collection in CI.`** Adapt `shakenfist/tools/build-collection.py` to `tools/build-collection.py`: `COLLECTION_DIR = pathlib.Path('collection')`, same semver decomposition (read the original's docstring on why `packaging` is used rather than string surgery -- `0.1.0rc1` must become `0.1.0-rc1`), same `galaxy.yml` version rewrite, output to `dist-collection/`. Add a `build-collection` job to `.github/workflows/release.yml` modelled on the server repository's at line 63, **unguarded** so `workflow_dispatch` exercises it. Add `ansible-lint` to `.pre-commit-config.yaml` and a step running it over `collection/` to the `sanity_checks` job in `functional-tests.yml`, beside phase 4's wheel check. Add `dist-collection/` to `.gitignore`. Then dispatch `release.yml` against this phase's branch and confirm `build-collection` succeeds while all four publishing jobs skip; record the run id in the commit message. Note the version the dispatch produces will be a development version -- that is expected and is why `publish-collection` is not added yet. |
| 5e | low | -- | none | **BLOCKED on open question 1 as of 2026-10-04.** **Management session, no commit. HUMAN GATE.** Resolve open question 1 below: establish whether an organisation-level `ANSIBLE_GALAXY_TOKEN` already exists. If not, Michael creates a Galaxy API token at <https://galaxy.ansible.com/ui/token/> under an account with `shakenfist` namespace permission, and adds it as a repository or organisation Actions secret named `ANSIBLE_GALAXY_TOKEN`. Verify by command afterwards (`gh api repos/shakenfist/client-python-k3s/actions/secrets --jq '.secrets[].name'` must list it, or confirm the organisation-level one covers this repository) and record the output. No agent can create the token; do not attempt a workaround. |
| 5f | medium | sonnet | none | **Done 2026-10-04, `8f718ed`. Verified by dispatch [37162847894](https://github.com/shakenfist/client-python-k3s/actions/runs/37162847894): both builds succeeded and all four publishing jobs skipped, `publish-collection` among them. That is the only check here which matters, since a guard that fired on a dispatch would have put a development version on Galaxy permanently.** **Commit subject: `Publish the collection on release.`** Add `publish-collection` to `release.yml`, modelled on the server repository's at line 202: `needs: [build-collection, sign-tag]`, the same event guard as the other publishing jobs, `environment: release`, download the `collection` artifact into `runner.temp`, install `ansible-core` into its own venv, `ansible-galaxy collection publish` with `--api-key "${{ secrets.ANSIBLE_GALAXY_TOKEN }}"`. Read the comment above the server repository's job about why it downloads into `runner.temp` rather than the workspace and preserve the reasoning. Then dispatch `release.yml` once and confirm `publish-collection` **skips** -- if it runs on a dispatch the guard is wrong, and that is the one failure mode which cannot be undone later. Update `RELEASE-SETUP.md` with a Galaxy section covering the token and the namespace permission, in the style of its PyPI section. |
| 5g | low | -- | none | **BLOCKED behind 5e.** **Management session, no commit.** Tag the next version from `develop` so the collection publishes for real, approve the `release` environment, and confirm all six jobs succeed. Remember the ruleset added in phase 4f: `creation` applies to the human pushing the tag, and `current_user_can_bypass` reads `always`. If `publish-collection` fails, do **not** re-push the tag -- go to the next patch version, per phase 4's decision 8, which Galaxy's refusal to replace a version makes mandatory here too. |
| 5h | medium | sonnet | none | **Done 2026-10-04, `614b9ac`.** **Commit subject: `Document the collection.`** `docs/collection.md`: install with `ansible-galaxy collection install shakenfist.k3s`, the `pip install shakenfist_client_k3s` the control node also needs and why `requirements.txt` names it, a worked playbook using `shakenfist.k3s.sf_k3s_cluster`, the connection-parameter rule (all three or none, and what a partial set does), check mode, and an explicit statement that the module does not manage worker count and conductor does. Link it from `README.md`'s documentation links and summarise in `ARCHITECTURE.md` -- the component inventory genuinely changes here, which is what that file is for. Verify the playbook against a real cluster if one is available; if not, say so rather than implying it was run. Then set the master plan's phase 5 row to `Complete` with the merge and the collection version, and update `docs/plans/index.md`. |

## Risks and mitigations

| Risk | Mitigation |
|------|------------|
| The module writes progress to stdout and corrupts its own JSON return. This is the likeliest way to ship something that passes tests and fails in a playbook. | 5b routes the reporter explicitly and 5c asserts stdout is empty by capturing it, rather than reasoning that it should be. Phase 3 already fixed a case of exactly this (`kubectl config unset` writing past the reporter, pinned by `KubectlUnsetLeakTestCase`), so the failure mode is known to be real in this codebase. |
| `publish-collection` fails on the first real release, after `sign-tag` has pushed a signed tag -- the same exposure phase 4 carried. | Decision 7's recovery: burn the version. 5f's dispatch proves the guard skips, which is the half that can be tested; the credential half cannot be, which is why 5e verifies the secret exists by command before 5g tags anything. |
| The semver conversion in `build-collection.py` is wrong for a version shape this repository produces but the server repository never did, and `ansible-galaxy` rejects `galaxy.yml` at build time. | 5d's dispatch builds a real development version (`0.1.1.devN+g<sha>`) through the real script, which is the shape most likely to break and is exercised before any tag. |
| The argument spec needs a second revision as soon as `node-customisation` lands, and a published interface is expensive to revise. | Decision 8 accepts this with reasoning. The mitigation is structural: every parameter those phases add is optional, so the revision is additive. 5b must not add a parameter *in anticipation* of them -- an optional parameter that does nothing is worse than an absent one. |
| An organisation-level `ANSIBLE_GALAXY_TOKEN` exists, 5e creates a second one, and two tokens with the same name in different scopes make a later rotation miss one. | Open question 1 is answered before 5e acts, not during it. |
| Nothing exercises `sf_k3s_cluster` against a real cluster. Found while writing 5h: the merge tier's `cluster_deploy` job drives `sf-client k3s` directly and deliberately installs neither Ansible nor the collection, to stay lighter than a full Ansible run. So the module's 29 tests are all unit tests against a faked `apiclient.Client`, and the first time it meets a real Shaken Fist API is in someone's playbook. | Not mitigated, and deliberately not fixed here -- adding an Ansible tier to the merge queue is a change to CI's shape rather than a step of this phase. The unit tests are stronger than they sound (`make_client`, `Cluster`, `Progress` and `CollectingReporter` are all real; only the API client is faked, which is the same seam the rest of this package's tests use) but they cannot catch a wrong API call. Tracked as shakenfist/client-python-k3s#89, filed before 5g publishes so the gap is tracked rather than remembered; that issue argues for extending `cluster_deploy` to run a playbook against the cluster it has already built, since the expensive part is the create. |
| `ansible-lint` in pre-commit slows every commit in a repository where it has one directory to lint. | Scope it to `collection/` with a `files:` pattern rather than running it repository-wide, and confirm in 5d that an unrelated commit does not invoke it. |

## Open questions

1. **Does an organisation-level `ANSIBLE_GALAXY_TOKEN` already exist?**
   Unresolvable with the credentials available: listing organisation
   secrets returns 403 without the `admin:org` scope. Blocks 5e and
   nothing earlier. Michael can answer it from the organisation
   settings page in a few seconds, or
   `gh auth refresh -h github.com -s admin:org` makes it answerable by
   command.

2. **How is pull request #87's unprefixed plan file reconciled with
   this phase's rename?** Survey finding 9. *Settled 2026-10-04:* #87
   renames its own file before merging, which costs it one `git mv` of
   `docs/plans/node-customisation.md` to
   `docs/plans/PLAN-node-customisation.md` plus the matching link in
   its new `docs/plans/index.md` row, and means `develop` is never
   half-converted. Nothing on this branch has been changed in that
   repository's `node-customisation` worktree -- it is another
   session's branch, and this plan does not edit it.

3. **Should `shakenfist.k3s`'s version track the Python package's, as
   `build-collection.py` makes it do by construction?** Inherited from
   the server repository's design, where collection and server ship
   from one repository and one `setuptools_scm` version. It is probably
   right here too, and it is certainly simplest. The question is
   whether a collection whose module changed not at all should get a
   new version because the plugin did. Not blocking: decided by
   adopting the script as-is in 5d, and revisitable before 5g makes any
   version public.

## What the review of #90 changed

One round, 3 `fix` / 1 `document` / 5 `consider` / 1 `none`. All four of
the first two were taken, three `consider` items were taken because each
was a defect in code this phase added, and two were declined with their
reasoning recorded below.

Two findings were worth more than their labels suggested.

**The module only caught `K3sClusterException`.** `Cluster` wraps
cluster-shaped problems, but the API client raises its own exceptions and
`Cluster` passes most of them through -- `cluster.py` catches
`apiclient.APIException` at `:761`, `:2203` and `:2250` and nowhere else,
which is itself the evidence. So an unauthorised namespace, a dropped
connection or a transport error reached Ansible as MODULE FAILURE with a
traceback, discarding the collected log, which for a create that ran
twenty minutes is the only record of how far it got.

Sweeping for the same shape found a **second site the review did not
report**, and the likelier of the two: `apiclient.Client.__init__` calls
`_collect_capabilities()`, which GETs `base_url` before the constructor
returns, so a typo in `api_url` raises inside `make_client()` where only
`ValueError` and `UnconfiguredException` were caught. A first run against
a misconfigured inventory hits that site, not the orchestration one.

**Two test assertions could not fail.** `assertIn('namespace')` and
`assertIn('auth_namespace')` were both satisfied by the parenthetical the
module appends to every connection failure, so they held whatever the
message said about the parameters -- the one thing they existed to check.
The test now compares against what `make_client()` actually raises,
obtained by calling it. Demonstrated rather than asserted: a mutation
which names only `api_url` and silently drops `namespace` is **passed** by
the old assertions and **failed** by the new ones.

Declined, with reasons:

- **Attaching the collection tarball to the GitHub release.** 5f
  considered this and matched the shared template, which treats the two
  publishing jobs as independent. The window in which it helps is the one
  before Galaxy publishing works, which 5g closes, and `release.yml`
  staying comparable to the server repository's is worth more than
  closing a gap that is about to shut on its own.
- **Pinning `ansible-core` in `tox.ini`.** Pinning a range makes the
  untested floor less visible rather than less true, and testing the floor
  properly needs an interpreter this environment does not have:
  ansible-core 2.15 does not run on Python 3.13. Filed as
  shakenfist/client-python-k3s#91, and noted at both the `tox.ini`
  dependency and in `collection/meta/runtime.yml` so the gap is derivable
  from the files that make the claim.

The `none` item -- no end-to-end coverage against a real cluster -- was
already #89, filed before the review raised it.

Mutations stand at **29** (20 from 5c, 9 from this round), all killed,
one incidentally. Tests stand at 415.

### Round two

1 `fix` / 1 `document` / 5 `consider` / 2 `none`, down from 3 `fix`. Both
`none` items were #89 and #91, already filed -- the reviewer agreeing they
are tracked rather than raising them. All seven actionable items taken.

The `fix` was mine from round one: `build-collection.py`'s
`read_text()`/`write_text()` did not state `encoding='utf-8'`, which
`AGENTS.md` requires. Latent rather than live, since `galaxy.yml` is
ASCII -- and that is the kind that survives. The sharper half was the
accompanying `consider`: `FileEncodingIsStatedTestCase` existed and could
not have caught it, because it scanned the package directory with a
non-recursive `listdir` and matched only `open()`. It now walks `tools/`
and `collection/` as well, and has a second test for
`Path.read_text`/`write_text`. Broadening it surfaced twenty pre-existing
unencoded calls in `tests/`, which are #93 -- out of scope here because
they predate this branch and are in files unrelated to the collection.

The most serious item was labelled `consider`: a `health()` failure after
a **successful** create was reported through the outer handler's "the
cluster may be partly built; `state: absent` removes whatever exists"
message, and without `changed=True`. An operator following that advice
would have destroyed a healthy cluster they did not know they had built.
Both `health()` call sites now go through `_probe()`, which downgrades a
probe failure after a create to a warning with `changed=True`, and fails
without the destructive advice when nothing was changed.

Running `ansible-test sanity --test validate-modules` -- which the review
suggested and which had apparently never been run against either
collection in the fleet -- found three more. The `author` field is fixed.
The other two are declined and filed as #94: `missing-gplv3-license` asks
for a header that would misstate an Apache-2.0 project's licence, and
`import-before-documentation` conflicts directly with flake8's E402, which
*does* run in CI. Moving the imports was tried and reverted; it also
cannot fully succeed, because `from __future__ import annotations` must be
the first statement in the file.

One mutation **survived** on the first pass: the post-release guard added
this round was unreachable, because the conversion only ran via
setuptools_scm. That is why `semver_from()` is now a pure function with
its own test file -- the guard is covered, and so are the rc, dev+local
and epoch shapes.

Mutations stand at **35**, all killed. Tests stand at 425.

### Round three, and why it is the last

1 `fix` / 2 `document` / 5 `consider` / 1 `none`. The `fix` count went
3 -> 1 -> 1, and this round's `fix` was in code round two added, which is
the signal to stop: the reviewer is now reviewing the consequences of its
own previous suggestions rather than the change this phase set out to
make. Every item was taken; no fourth round was requested.

The `fix` and the `consider` beside it shared one cause, and fixing the
reported instance would have left the others. `_probe()` had taught two
call sites that advice depends on how far a run got, and the three outer
handlers still gave create advice unconditionally: a failure on the first
metadata read told an operator to delete a cluster this run never created,
and a failure during a *delete* advised `state: absent`, which is what
they had already asked for. The same handlers called `fail_json()` without
`changed`, so a create that built instances and then failed reported the
task as unchanged -- which is what handlers and callbacks key on. One
`_Mutation` object now records what was attempted, and both the advice and
the `changed` flag are read off it, including on the
`K3sClusterException` path the review did not mention.

One of this round's findings was that **a test from round one pinned the
bug**: `test_an_unreachable_api_says_so_and_says_what_to_do` asserted
`state: absent` appeared in the message for a scenario where nothing had
been touched. It failed the moment the advice became conditional, which is
the correct outcome and a reminder that a test asserting current behaviour
is not the same as a test asserting intended behaviour.

Also taken: the counts are range-checked, so a templated
`control_plane_count: 0` is refused before a client is built rather than
producing a cluster with no control plane tens of minutes later -- the
library-level version of that check is #96, deliberately left out because
it changes a signature's contract for every caller. The two publishing
jobs are now ordered, `publish-collection` needing `publish-pypi`, so a
half-published release always means "on PyPI, not on Galaxy" -- the half
that can be finished rather than the half that has consumed a version
number. And the `'Got only'` split survived round two's fix by being
relocated rather than deleted; it is gone.

Mutations stand at **40**, all killed. Tests stand at 432.

## Definition of done

Each of these is checkable, and most are one command:

- `ansible-galaxy collection build collection/` succeeds, and the
  tarball it produces contains `plugins/modules/sf_k3s_cluster.py`.
- `tools/build-collection.py` run from a clean checkout rewrites
  `collection/galaxy.yml`'s version to a semver string `ansible-galaxy`
  accepts, for a tagged version *and* for a
  `0.1.1.devN+g<sha>` development version.
- `ansible-lint collection/` passes, and runs in both
  `pre-commit run --all-files` and the pull request tier of CI.
- `stestr run sf_k3s_cluster` passes, and the module's test for empty
  stdout fails when a `print()` is added to the module.
- A `workflow_dispatch` run of `release.yml` shows `build-collection`
  succeeding and `sign-tag`, `publish-pypi`, `publish-collection` and
  `github-release` all skipped.
- `gh api repos/shakenfist/client-python-k3s/actions/secrets` lists
  `ANSIBLE_GALAXY_TOKEN`, or an organisation-level secret of that name
  is confirmed to cover this repository.
- `curl -s '.../search/collection-versions/?namespace=shakenfist'`
  reports a count of at least 1, and the version it reports matches the
  tag pushed in 5g.
- `ansible-galaxy collection install shakenfist.k3s` succeeds in a
  throwaway `ANSIBLE_COLLECTIONS_PATH`, and
  `ansible-doc -t module shakenfist.k3s.sf_k3s_cluster` renders.
- `collection/requirements.txt`'s floor is not ahead of the version
  PyPI serves for `shakenfist_client_k3s`.
- `tools/check-wheel-build.sh` still reports twelve entries, so the
  wheel never grew the collection.
- `git grep -c worker_count -- collection/` reports exactly one match:
  the `worker_count=module.params['initial_workers'],` argument in
  `sf_k3s_cluster.py`'s single `create()` call. Anything else is a
  regression.

  This was `grep -rn 'worker_count' collection/` returns nothing until 5b,
  which cannot hold once the module exists: `create()`'s second parameter
  *is* called `worker_count`, and a module that creates clusters has to
  pass it. Passing `create()`'s first three arguments positionally to keep
  the bare grep clean was considered and rejected -- three unlabelled
  integers into a ten-parameter call is worse code than the grep is a
  check. Two other changes keep the new wording meaningful: `git grep`
  rather than `grep -rn`, because `ansible-lint` leaves a gitignored copy
  of the whole collection in `collection/.ansible/` which `grep -rn`
  counts and CI's checkout order makes intermittent; and no comment under
  `collection/` may spell the name either, so the count stays at one. What
  is being asserted has not changed: no `worker_count` in the argument
  spec, in the documentation, in the tests, or on any path comparing one
  against a cluster that already exists.
- `pre-commit run --all-files` and `tox -epy3` pass.
- The master plan's phase 5 row is `Complete` with the collection
  version recorded, and `docs/plans/index.md` links this file.

## Back brief

Restate, before starting 5a: why this phase publishes to Galaxy before
the core collection does, when the master plan recommended the
opposite; what the module must never do to its own stdout and why;
which parameter the module deliberately does not take, and what race
taking it would create; and what happens if `publish-collection` fails
after `sign-tag` has pushed the tag. If any of those four comes back
wrong, the phase is not ready to start and the brief needs rewriting
rather than the implementer needing correcting.
