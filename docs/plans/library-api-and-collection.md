# Library API, missing verbs, and the shakenfist.k3s collection

## Situation

This package is CLI-shaped. Every command body in `__init__.py` holds
its own orchestration inline -- `k3s_create()` is roughly 190 lines at
`__init__.py:93-286` -- and every helper in `primitives.py` takes a
Click `ctx` as its first argument. There is no callable API, no
structured output, and no way to drive a cluster from anything that is
not `sf-client`.

Two consumers now want one:

- **An Ansible module.** `33fl/docs/plans/PLAN-k3s-ci-runners.md`
  design decision 5 wants cluster bringup driven from
  `static_runner.yml`, ensuring a cluster exists at a minimum shape.
- **Conductor.** The same plan's design decision 2 has conductor
  polling the cluster and adding or removing workers. Conductor is a
  Python daemon; it wants the library directly, not Ansible.

The coupling is shallower than it looks. `ctx.obj` only ever carries
five keys -- `CLIENT`, `PROGRESS`, `VERBOSE`, `name`, `namespace` --
so those become explicit arguments rather than a context object. Two
things are genuinely load-bearing, and both are fatal to a module that
must own stdout for its JSON result and fail structurally:

- **31 bare `print()` calls** in `primitives.py`.
- **`sys.exit(1)` on every failure path** (`primitives.py:91`, `:114`,
  `:124`, `:170`, `:198`, `:283`, `:407`, and others).

`progress.Progress` already accepts a `stream` argument, so a
collecting reporter drops straight into the `log` list that all four
`sf_*` modules in `shakenfist.shakenfist` return.

Beyond the refactor, the plugin is missing verbs and carries
behaviours a daemon cannot tolerate. Re-verified against `develop` on
2026-09-03:

- **No `remove-worker`.** No cordon, no k8s Node object deletion, no
  removal from `worker_nodes` metadata, no VM delete. This is the
  entire conductor scale-down verb.
- **`install_workers()` is not incremental.** It runs the k3s agent
  install across *all* of `md['worker_nodes']`
  (`primitives.py:537-542`), so `expand-workers` reinstalls k3s on
  every already-joined worker.
- **No crash recovery.** `create` registers the cluster name in
  namespace metadata and then builds for minutes; a mid-create failure
  leaves a name-squatting cluster stuck in `state: initial` with
  `delete` as the only way out. `state` is only `initial` ->
  `created` -> `deleted` (`__init__.py:177`, `:279`, `:442`).
- **No health verb.** Nothing answers "are all nodes Ready" without
  hand-rolled kubectl.
- **MetalLB and Longhorn are unconditional** (`__init__.py:234-235`).
  A CI cluster of ephemeral pods does not want Longhorn.
- **Kubeconfig side effects are mandatory.** `create` writes
  `~/.kube/config` and needs a local `kubectl` to merge into an
  existing one, exiting 1 if absent (`__init__.py:249-253`). `delete`
  unconditionally shells `kubectl config unset` three times and exits
  1 on failure (`__init__.py:459-465`) -- *after* the VMs are already
  gone.
- **No manifest payload hook.** No way to install extra manifests or
  `HelmChart` resources at bootstrap.

Finally, the packaging story has a hole nobody had noticed: **this
package has never been released.** There are no git tags, and both
`shakenfist-client-k3s` and `shakenfist_client_k3s` 404 on PyPI.

## Mission and problem statement

Turn this package into something a daemon and an Ansible module can
drive, and ship an optional Ansible collection that exposes cluster
bringup -- without making the k3s feature mandatory for anyone
deploying Shaken Fist.

Done means: `primitives.py` and a new orchestration layer are callable
with a client and a cluster name, return values, and raise exceptions;
the missing verbs exist; `shakenfist.k3s` is published to Ansible
Galaxy; and `shakenfist-client-k3s` is on PyPI so the collection's
install instructions are true.

Constraints:

- **Optionality is the whole point.** `shakenfist.shakenfist` must not
  change, and must not learn about k3s. A deployment that does not
  want k3s installs neither the plugin nor the collection and is
  unaffected.
- **The CLI must keep working exactly as it does today.** The Click
  commands become thin wrappers; their output and exit codes are
  observable behaviour that CI and humans depend on.
- **Python >= 3.7**, single quotes, 120 columns, per `AGENTS.md`.
- **The plugin must never break `sf-client` startup.** It is imported
  unconditionally by the entry-point loader
  (`client-python/shakenfist_client/main.py:138-148`, unguarded), so
  top-level imports stay cheap and reliable.

## Decisions

- **The Ansible module ships as its own collection,
  `shakenfist.k3s`, built and published from this repository.** Not as
  a module inside `shakenfist.shakenfist`.

  Ansible's native unit of optional distribution is the collection,
  which mirrors exactly how this package is already an optional CLI
  plugin: installed or not, discovered by presence via the
  `shakenfist_client.plugin` entry point. The operator story becomes
  symmetric --
  `pip install shakenfist-client` plus
  `ansible-galaxy collection install shakenfist.shakenfist` for the
  core, and the same two commands with the k3s names for this. The
  `shakenfist` Galaxy namespace is already claimed and currently
  empty, so a second collection in it costs nothing
  administratively.

  `galaxy.yml` keeps `dependencies: {}`. The module talks to the API
  through `shakenfist_client` directly and uses nothing from
  `shakenfist.shakenfist`, so it stays usable against a cloud that was
  not deployed with the collection.

  The build machinery is copyable almost verbatim from
  `shakenfist/tools/build-collection.py` and the `build-collection` /
  `publish-collection` job pair in that repository's `release.yml`;
  both repositories already share the same release template.

- **The module ensures existence and shape; it never manages worker
  count.** Cluster state is read-modify-write namespace metadata with
  no locking, and conductor is the other writer. Splitting by
  convention -- module owns "exists at minimum shape", conductor owns
  worker membership -- avoids a two-writer race. This is
  `PLAN-k3s-ci-runners.md` design decision 5, and it is why the module
  is deliberately *not* a full declarative reconciler.

- **Failures raise, they do not exit.** A local exception hierarchy,
  caught and turned into `sys.exit(1)` by the Click layer and into
  `fail_json()` by the module layer.

## Alternatives considered and rejected

- **A module inside `shakenfist.shakenfist` that imports this package
  and fails gracefully when it is absent.** There is precedent for
  tolerating an absent optional dependency -- `roles/node/tasks/
  config.yml` wraps the `hashivault` lookup in `block`/`rescue` -- but
  that is for a third-party plugin, not first-party content. This
  option puts a module for an optional component in the mandatory
  collection, forces its `requirements.txt` to either over-declare or
  leave a confusing runtime error, turns collection-versus-plugin
  version skew into a support matrix, and makes the server repository
  carry tests for code it does not ship. It was genuinely better at
  one thing: a single collection to install.

- **A role that shells out to `sf-client k3s create` via
  `ansible.builtin.command`.** Cheapest to write, and it needs none of
  the refactor. Rejected: no check mode, no structured return, no
  idempotency, and `print()` plus `sys.exit(1)` is precisely the
  interface a module must not have.

## Open questions

1. **Should the collection be the first thing published into the
   `shakenfist` Galaxy namespace?** `shakenfist.shakenfist` has the
   publish job wired but has never shipped a version -- a
   collection-version search for the namespace returns zero.
   Recommendation: let a core release go first, so the token and
   namespace-permission path is validated on the component we
   understand best. Awaiting an answer; it only affects ordering.

2. **What does `remove-worker` do about the k8s Node object?**
   Deleting the VM without deleting the Node leaves NotReady
   tombstones (`PLAN-k3s-ci-runners.md` decision 2 names this). Doing
   it from the plugin means the plugin needs cluster credentials and a
   kubectl-equivalent, which cuts against making kubeconfig side
   effects opt-out. Recommendation: the plugin cordons and deletes the
   Node using the kubeconfig already in namespace metadata, via the
   API rather than a `kubectl` subprocess.

3. **Does `create` grow a `--no-metallb` as well as `--no-longhorn`?**
   The CI cluster needs MetalLB for the federation float
   (`PLAN-k3s-ci-runners.md` decision 4), so nothing needs it off
   today. Recommendation: implement both flags anyway, since the code
   paths are adjacent, but do not spend design effort on the MetalLB
   case.

## Execution

| Phase | Work | Status | Merged |
|-------|------|--------|--------|
| 1. Library API | Extract orchestration from the Click command bodies into a callable layer taking a client, cluster name and namespace; replace `sys.exit(1)` with an exception hierarchy; route `print()` through a reporter; make the Click commands thin wrappers | Not started | |
| 2. Client construction | Honour the root `--apiurl`/`--key`/`--namespace` in `_bind_cluster_context()` (`__init__.py:36-40` currently discards them), and add the `api_url`/`namespace`/`key` plus `suppress_configuration_lookup=True` path that `sf_namespace._make_client()` uses | Not started | |
| 3. Missing verbs | `remove-worker`; make `install_workers()` incremental; reconcile/crash recovery for `state: initial`; a health verb; `--no-longhorn`/`--no-metallb`; kubeconfig side effects opt-out; manifest payload hook | Not started | |
| 4. First release | Cut `v0.1.0` so `shakenfist-client-k3s` exists on PyPI and `setuptools_scm` has a real version to stamp | Not started | |
| 5. The collection | `shakenfist.k3s` with `sf_k3s_cluster`; `tools/build-collection.py`; `build-collection` and `publish-collection` jobs in `release.yml`; ansible-lint in pre-commit and CI; docs | Not started | |
| 6. Push audit | Run `PUSH-AUDIT.md` over the accumulated diff of phases 1-5 against `develop` | Not started | |

Phase ordering is forced by dependency rather than convenience.
Phase 1 is the keystone: phases 3 and 5 are both much easier after it,
and phase 5 is impossible before it. Phase 4 must precede phase 5
because the collection's `requirements.txt` names a PyPI package that
must exist. Phase 2 is separable but belongs before phase 5, because
the module's connection parameters are the same code path.

Phases 1 and 3 are independently useful to conductor even if the CI
runner migration never happens.

## Administration and logistics

### Success criteria

We will know this plan has been implemented because the following are
true:

* `tox -epy3` passes, with new coverage for the exception paths that
  replaced `sys.exit(1)` and for the reconcile logic.
* `pre-commit run --all-files` passes, including ansible-lint over the
  new collection.
* `python -c 'import shakenfist_client_k3s'` still succeeds, and
  `sf-client k3s --help` lists the same commands with the same output
  as before the refactor.
* A cluster can be created, expanded, health-checked, shrunk and
  deleted entirely from Python, with no Click context and no
  `~/.kube/config` side effects.
* `ansible-galaxy collection install shakenfist.k3s` followed by a
  playbook using `shakenfist.k3s.sf_k3s_cluster` brings up a cluster,
  is idempotent on a second run, and supports `--check`.
* `pip install shakenfist-client-k3s` works from PyPI.
* Installing neither the plugin nor the collection leaves a Shaken
  Fist deployment unchanged: `shakenfist.shakenfist` is not modified
  by this plan.
* User-visible changes are documented in `docs/`. `AGENTS.md` changes
  only if a *convention* changed; `ARCHITECTURE.md` only if the
  *shape of the system* changed; `README.md` only if the pitch,
  install story or documentation links changed.

### Future work

- Teaching conductor to use the new library API is tracked in
  `33fl/docs/plans/PLAN-k3s-ci-runners.md`, not here.
- A k3s version upgrade story. `update-os` covers the OS only, and
  open question 4 of the CI runners plan asks whether the control
  plane is rebuilt or upgraded in place.
- Shrinking control plane count, and any declarative reconciliation of
  worker count, are deliberately out of scope while conductor owns
  worker membership.

### Bugs fixed during this work

<!-- Record bugs found and fixed while executing this plan. -->

Nothing yet. Note that
[#41](https://github.com/shakenfist/client-python-k3s/issues/41) --
`k3s delete` destroying a pre-existing network passed to
`create --network` -- is adjacent to phase 3 but is tracked as its own
issue, not as part of this plan.
