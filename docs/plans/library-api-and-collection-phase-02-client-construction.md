# Client construction phase 2: one client, honouring the root options

## Prompt

This phase gives the plugin one client instead of two. Today every
`k3s` command throws away the `apiclient.Client` that `sf-client`'s
root callback built from `--apiurl`, `--key` and `--namespace`, and
builds its own; this phase uses the root's, and adds the client
factory a library caller needs so that an Ansible module and
conductor construct a client the way the rest of Shaken Fist does.

Ground every change in the tree rather than in this document. The
files that matter are `shakenfist_client_k3s/__init__.py` (the Click
group, the two binding helpers and the one construction site),
`shakenfist_client_k3s/cluster.py` (which takes a client and never
builds one), and the six tests which patch `apiclient.Client` today.
Two files in sibling repositories are the constraints this phase is
written against: `shakenfist_client/apiclient.py` in `client-python`,
whose `Client.__init__()` and `_calculate_async_deadline()` decide
what an async strategy costs, and
`shakenfist/deploy/collection/plugins/modules/sf_namespace.py` in the
server repository, whose `_make_client()` is the pattern the factory
mirrors.

**Planning effort:** high. The phase is small in lines and subtle in
consequence: the async strategy it forces is read by five call sites
inside `apiclient` and decides whether the orchestration's own wait
loops run at all, and the six tests which mask the bug today have to
be migrated in the direction that makes the fix testable rather than
the direction that makes them pass.

**Review effort:** medium. The diff is small and the argument is
local, but decision 2 mutates an object this package does not own and
a reviewer should weigh that deliberately rather than by inspection.

## Scope

In: `_bind_namespace_context()` taking the client from
`ctx.obj['CLIENT']` rather than constructing one; forcing
`ASYNC_CONTINUE` on it; a new `shakenfist_client_k3s/client.py` with a
`make_client()` for library callers; migrating the six tests which
patch `apiclient.Client`; a test which drives the real root group so
that "honours `--apiurl`" is asserted rather than asserted-about; and
correcting `docs/library-api.md`'s two client-construction examples.

Out, and deliberately so:

- **Every new verb.** No `remove-worker`, no health verb, no
  incremental `install_workers()`, no reconcile. Phase 3.
- **The kubeconfig side effects, and the `kubectl config unset` leak
  phase 1 found.** Phase 3 owns both, and the phase 3 row of the
  master plan already names them.
- **Routing `GroupCatchClusterExceptions`'s `print(str(e))` to
  stderr.** Also phase 3's, also already recorded there. It is
  adjacent to this phase's file but it is a user-visible output
  change, and this phase makes none.
- **Changing `apiclient` upstream.** The properly clean way to get a
  per-operation async strategy is a `client-python` change, and it is
  rejected in decision 2 and recorded under Future work: it would
  couple this phase to a `client-python` release and a
  `shakenfist_client >=` floor bump in `pyproject.toml:33` to buy an
  isolation nothing in this process consumes.
- **The k3s `--namespace` option.** See decision 6; nothing about it
  changes, and that is worth stating because "honour the root
  `--namespace`" reads as though it might collapse the two meanings.
- **`shakenfist.shakenfist`**, which this plan never modifies.

## What the survey found

The survey checked the master plan's phase 2 row against `develop` at
551f569, and against `client-python` at 186a298 and `shakenfist` at
1aa663c2e for the two cross-repository claims. The substance of the
row held: the root callback does honour the three connection options
and store the client in `ctx.obj['CLIENT']`, the plugin does discard
it, the root `--async` default really is `pause` where this code needs
`ASYNC_CONTINUE`, and `sf_namespace._make_client()` really does use
the `suppress_configuration_lookup=True` path the row describes.

Nine things were wrong, stale, or missing. Four of them change what
this phase does.

1. **There is one construction site now, not the two the row cites.**
   The row says `__init__.py:36` and `:98`; phase 1 collapsed both
   into `_bind_namespace_context()`, which builds
   `apiclient.Client(async_strategy=apiclient.ASYNC_CONTINUE)` at
   `__init__.py:24`. All ten subcommands reach it, three of them
   directly (`:106`, `:167`, `:191`) and seven through
   `_bind_cluster_context()` (`:38`) or `_bind_new_cluster_context()`
   (`:54`). One site to change, not two. Corrected in the master plan
   as part of this planning commit.

2. **The `client-python` citation is one line out.** The row says
   `main.py:228-237`; the construction is at `:229-237`, with the
   client built at `:230-235` and stored at `:236`. The root's four
   connection options are at `:196-199`, and `--async`'s `pause`
   default is `:199`. Corrected in the master plan.

3. **The bug is narrower than "the root options are ignored", and the
   narrowing is load-bearing for how we describe it.** `Client`'s own
   configuration lookup reads `SHAKENFIST_API_URL`,
   `SHAKENFIST_NAMESPACE` and `SHAKENFIST_KEY` from the environment
   (`apiclient.py:237-255`), so an operator who configures by
   environment variable is unaffected -- both clients resolve the same
   values, which is why this has survived unnoticed. What is lost is
   an explicitly passed `--apiurl`, `--key` or `--namespace`: the
   plugin's client falls back to the environment, `~/.shakenfist` or
   `/etc/sf/shakenfist.json` and can authenticate as a different
   namespace, against a different API server, than the one the
   operator named. It also loses the root client's 301 redirect
   rewrite of `base_url` (`apiclient.py:419-423`), which is per
   client.

4. **The resolved target namespace is wrong too, not just the
   credentials.** `__init__.py:25-26` defaults the k3s `--namespace`
   option from `client.namespace`, so a `--namespace` passed to the
   root selects neither the identity nor the namespace operated on.

5. **Forcing `ASYNC_CONTINUE` is measurable, and this is why the root
   client cannot simply be used as it stands.** Four of the client
   calls `cluster.py` makes read `self.async_strategy` to compute
   their own blocking deadline -- `create_instance`
   (`apiclient.py:675-676`), `delete_instance` (`:830`),
   `allocate_network` (`:1104`) and `instance_execute` through
   `_await_agentop` (`:1298`) -- as does `_request_url`'s dependency
   retry (`:428-432`). `_calculate_async_deadline()` (`:181-188`) maps
   `continue` to -1, `pause` to 60 and `block` to 3600. Under the
   root's `pause` default each of those calls would block for up to a
   minute inside `apiclient`, emitting nothing but `LOG.debug`, before
   the reporter-driven wait loop in `cluster.py` ever ran. This is
   phase 1's finding 5, now with the call sites attached.

6. **The risk of the wrong strategy is silence, not killed
   operations.** `_add_agentop_timing()` (`apiclient.py:1327-1340`)
   deliberately refuses to derive a server side deadline from the
   async strategy, its comment naming the 60 second kill that would
   otherwise follow from the CLI's `ASYNC_PAUSE` default. Worth
   recording so that a reviewer weighs decision 2 against the real
   cost rather than the frightening one.

7. **`sf_namespace._make_client()` is at
   `shakenfist/deploy/collection/plugins/modules/sf_namespace.py:92-117`,
   and two of its choices must not be copied.** It sets
   `verbose=False`, `sync_request_timeout=1800` and
   `async_strategy=apiclient.ASYNC_BLOCK`, and adds `base_url`,
   `namespace`, `key` and `suppress_configuration_lookup=True` only
   when all three of `api_url`, `namespace` and `key` were supplied,
   letting the client auto-discover otherwise. It converts
   `UnconfiguredException` into `module.fail_json()`. The
   all-three-or-auto-discover rule is exactly what to mirror;
   `ASYNC_BLOCK` is wrong here (finding 5) and `fail_json()` belongs
   to phase 5's module, not to this package.

8. **`ARCHITECTURE.md:8-10` already documents this phase's behaviour
   as though it were true**: "All communication with Shaken Fist
   happens through the `apiclient.Client` instance that `sf-client`
   places in the Click context." It is false today and true when this
   phase lands. Deliberately *not* corrected in this planning commit:
   rewriting it to describe the bug and then rewriting it back one
   commit later is churn, and the sentence is a Definition of done
   item instead, so the phase is what makes the document honest.

9. **`docs/library-api.md` teaches library callers the wrong client,
   and that is a phase 1 defect this phase must fix.** Both its
   examples, at `:17` and `:156`, say `client = apiclient.Client()`,
   whose `async_strategy` defaults to `ASYNC_BLOCK`
   (`apiclient.py:220`). A caller following the page verbatim gets the
   blocking behaviour of finding 5 -- up to an hour inside
   `create_instance` with the reporter silent -- which is the opposite
   of what the page's own worked example claims to demonstrate. In
   scope here because the fix is the factory this phase adds.

One further finding shapes the work without changing it. Six tests
patch `shakenfist_client_k3s.apiclient.Client` and hand back a fake --
`test_commands.py:33`, `:108`, `:140` and `:230`,
`test_cli_errors.py:71`, and `test_progress.py:651` -- and all of them
invoke the `k3s` group directly with `obj={'VERBOSE': False}` (for
example `test_commands.py:41-43`), never through the root `cli`. So no
test has a `CLIENT` in `ctx.obj` today, and the reason the bug is
invisible is that every test made the *constructed* client the fake.
How those six migrate is therefore the test of this fix, not
incidental churn: see decision 1.

## Decisions this plan already takes

1. **The CLI reads `ctx.obj['CLIENT']` directly, with no fallback to
   constructing one.** A fallback would be a second code path that
   production never takes, and it is exactly the shape of thing that
   let this bug live unnoticed since the plugin was written. A
   `KeyError` on `'CLIENT'` names the missing key and can only be
   reached by a caller which is not `sf-client`, because the root
   callback sets it unconditionally at `main.py:236` before any
   subcommand runs. The six tests gain the client through `obj=`
   instead, which is what makes the fix testable.

2. **The async strategy is forced by setting it on the client the root
   built, not by building a second client.** `Client.async_strategy`
   is read per call at the five sites in finding 5 and never again at
   construction, so setting it is within the class's own contract --
   `snapshot()` (`apiclient.py:715-717`) already overrides the
   strategy per call, by the same reasoning. The alternative,
   constructing a fresh client from the root client's `base_url`,
   `namespace` and `key` with `suppress_configuration_lookup=True`,
   costs a second capabilities `GET` (`_collect_capabilities()`,
   `apiclient.py:305-308`) and a second `_authenticate()` on every
   invocation, to buy an isolation nobody consumes: within one
   `sf-client k3s ...` process nothing reads `main.CLIENT` or
   `ctx.obj['CLIENT']` after the plugin has it, and the process then
   exits.

   **This is the decision a reviewer is most likely to argue with**,
   because it mutates an object this package does not own. The honest
   counter-argument is that the properly clean fix is upstream -- a
   per-call `async_strategy` argument, or a copy-returning
   `Client.with_async_strategy()`, in `client-python` -- and that it
   would couple this phase to a `client-python` release and a
   dependency floor bump for a behaviour the one-line assignment
   already gets. Recorded under Future work rather than done.

   A consequence to state plainly: the root `--async` is ignored for
   every `k3s` command. That is correct rather than regrettable.
   `--async` bounds how long a single CLI call blocks, and these
   commands block for minutes by design while their own wait loops
   report progress. It is documented in `docs/usage.md`, not warned
   about at runtime, because a warning would be output this phase has
   promised not to add.

3. **The library's factory is a new module,
   `shakenfist_client_k3s/client.py`, holding one public function
   `make_client(api_url=None, namespace=None, key=None)`.** It
   mirrors `sf_namespace._make_client()`'s all-three-or-auto-discover
   rule exactly (finding 7), sets `async_strategy=ASYNC_CONTINUE`
   rather than that function's `ASYNC_BLOCK`, leaves
   `sync_request_timeout` at `apiclient`'s default of 300 rather than
   the collection's 1800 -- with `ASYNC_CONTINUE` no single HTTP call
   waits for orchestration, so the collection's reason for widening it
   does not apply -- and lets `UnconfiguredException` propagate, since
   translating it into `fail_json()` is phase 5's job and into an exit
   code is nobody's.

   A module of its own, rather than a function in `cluster.py` or
   `primitives.py`, because it is neither cluster scoped nor namespace
   scoped, and because phase 5's module and conductor both want it
   without wanting the orchestration. It imports only
   `shakenfist_client.apiclient`, which `__init__.py` imports already,
   so it adds nothing to the cost of the unguarded plugin load.

4. **`make_client()` is not called by the CLI.** Under decision 1 the
   CLI already has a client, and giving it a `make_client()` call
   would resurrect precisely the construction path decision 1
   deletes. The factory therefore needs unit coverage of its own
   rather than inheriting the CLI's, which is why step 2b carries
   tests and not just code.

5. **`docs/library-api.md`'s two examples become `make_client()`**,
   and the page gains a short paragraph on why the strategy matters
   (finding 9). This is the only user-visible change in the phase and
   it is a documentation one.

6. **Nothing about the k3s `--namespace` option changes.** The root
   `--namespace` is an authentication identity; the k3s option selects
   which namespace to operate on, for an administrator working on
   someone else's cluster (`__init__.py:12-23`, `docs/usage.md:8-10`).
   They stay two different things, and the resolution order -- the
   option if given, else `client.namespace` -- is unchanged. What
   changes is only that `client.namespace` now comes from the client
   the operator's own flags configured.

## Step plan

Three steps. 2a is the fix and its tests, 2b is the factory and its
tests, 2c is documentation. 2a and 2b touch disjoint files and could
run in either order; 2c must be last because it describes both.

| Step | Effort | Model | Isolation | Brief for sub-agent |
|------|--------|-------|-----------|---------------------|
| 2a | medium | opus | none | Make the CLI use the client `sf-client` already built. In `shakenfist_client_k3s/__init__.py`, `_bind_namespace_context()` (`:11-27`) currently calls `apiclient.Client(async_strategy=apiclient.ASYNC_CONTINUE)` at `:24`. Replace that with `client = ctx.obj['CLIENT']` followed by `client.async_strategy = apiclient.ASYNC_CONTINUE`, and comment the assignment with why: the strategy is read per call inside `apiclient` (`create_instance` at `apiclient.py:675`, `delete_instance` at `:830`, `allocate_network` at `:1104`, `_await_agentop` at `:1298`, `_request_url` at `:428`), the root's default is `pause`, and under `pause` each of those blocks for up to 60 seconds inside the client with no reporter output before this package's own wait loops run. No fallback construction if `'CLIENT'` is absent -- see decision 1 -- and do not add a try/except around the subscript. Update the function's docstring, which currently explains why it constructs a client. Then migrate the six tests which patch `shakenfist_client_k3s.apiclient.Client`: `tests/test_commands.py:33`, `:108`, `:140` and `:230`, `tests/test_cli_errors.py:71`, and `tests/test_progress.py:651`. In each, delete the patcher and pass the fake in the Click object instead -- `obj={'VERBOSE': False, 'CLIENT': self.client}` -- at every `runner.invoke()` in that test case, including `test_cli_errors.ClientTestCase._invoke()` (`:77-82`) and `test_progress`'s `_create()` (`:665-671`). Do not weaken any existing assertion to make something pass. Then add the two regression tests, in a new `tests/test_root_options.py`: first, that `apiclient.Client` is never constructed while a k3s command runs (patch `shakenfist_client_k3s.apiclient.Client` with a mock whose `side_effect` raises, invoke a cheap command such as `list`, assert exit code 0 and that the mock was not called); second, an end-to-end check through the real root group -- import `shakenfist_client.main`, patch `shakenfist_client.main.apiclient.Client` to return a `mock.MagicMock()` with `namespace` set, invoke `main.cli` with `['--apiurl', 'https://api.example.com', '--key', 'k', '--namespace', 'ns', 'k3s', 'list']`, and assert both that the patched constructor received those values as `base_url`, `key` and `namespace`, and that the k3s command used that same client object and left its `async_strategy` equal to `apiclient.ASYNC_CONTINUE`. That second test only works when this package is installed, because `main.py:259-269` attaches the group through the `shakenfist_client.plugin` entry point; `tox -epy3` installs it, so note that dependency in the test's docstring rather than working around it. Verification is unit tests only. Do not touch `cluster.py`, `primitives.py` or any golden `--help` fixture. |
| 2b | medium | sonnet | none | Add `shakenfist_client_k3s/client.py`, a new module whose only public name is `make_client(api_url=None, namespace=None, key=None)`, returning an `apiclient.Client`. Mirror `_make_client()` in the server repository at `shakenfist/deploy/collection/plugins/modules/sf_namespace.py:92-117`: build a kwargs dict with `verbose=False` and `async_strategy=apiclient.ASYNC_CONTINUE`, and add `base_url=api_url`, `namespace=namespace`, `key=key` and `suppress_configuration_lookup=True` only when all three arguments are truthy, so that a caller supplying none of them gets the same environment, `~/.shakenfist` and `/etc/sf/shakenfist.json` discovery the CLI gets. Three deliberate differences from that function, each of which needs a comment saying so: `ASYNC_CONTINUE` not `ASYNC_BLOCK`, because this package's orchestration runs its own wait loops and a blocking strategy would swallow them (see the plan's survey finding 5); `sync_request_timeout` left at `apiclient`'s default rather than set to 1800, because with `ASYNC_CONTINUE` no single HTTP call waits for orchestration; and `apiclient.UnconfiguredException` allowed to propagate rather than translated, because an Ansible module turns it into `fail_json()` and a library caller wants the exception. The module docstring should say what the function is for -- phase 5's `sf_k3s_cluster` module and conductor -- and that the CLI does not use it, because the CLI is handed a client by `sf-client`. Add `tests/test_client.py` in the existing testtools/mock style, patching `shakenfist_client_k3s.client.apiclient.Client` and asserting on the kwargs: all three supplied gives `suppress_configuration_lookup=True` and the three values passed through; any one missing gives none of those four keys; `async_strategy` is `apiclient.ASYNC_CONTINUE` in both cases; and `UnconfiguredException` raised by the constructor reaches the caller unchanged. Do not import `cluster` or `primitives` from `client.py`, and do not call `make_client()` from `__init__.py`. |
| 2c | low | sonnet | none | Documentation only; no code. In `docs/library-api.md`, replace both `client = apiclient.Client()` examples (`:14-17` and `:152-156`) with `from shakenfist_client_k3s.client import make_client` and `client = make_client()`, and add a short subsection under "Constructing a `Cluster`" saying that `make_client()` exists because `apiclient.Client()`'s default async strategy is `ASYNC_BLOCK`, under which the client does the waiting internally and the reporter sees nothing, and that a caller who brings their own client should set `async_strategy=apiclient.ASYNC_CONTINUE` on it. Mention the `api_url`/`namespace`/`key` arguments and that supplying all three suppresses configuration discovery, as the Shaken Fist Ansible collection's modules do. In `docs/usage.md`, extend the paragraph at `:8-10` to say that the root `sf-client` options -- `--apiurl`, `--key` and `--namespace` -- configure the client these commands use, and that the root `--async` does not apply to them because each command runs its own wait loops and reports progress as it goes. Add one row for `shakenfist_client_k3s/client.py` to the Key Files table in `AGENTS.md` (`:35-42`) and change nothing else in that file -- it is an index, not a reference. Do not edit `ARCHITECTURE.md`: its overview at `:8-10` already describes the behaviour this phase implements, and the phase makes it true. Do not touch `README.md`; the pitch, the install story and the documentation links are all unchanged. |

## Risks and mitigations

- **Decision 2 mutates a client this package does not own.** If
  `sf-client` ever grew an in-process caller that invoked `cli()`
  twice, or a second command group that ran after `k3s` in the same
  process, it would inherit `ASYNC_CONTINUE`. Neither exists: Click
  dispatches one subcommand per invocation and the console script then
  exits. Mitigation: the assignment carries a comment saying why it is
  safe and what would make it unsafe, and the reviewer weighs
  decision 2 explicitly rather than by inspection -- the back brief
  gates it.

- **The test migration could silently weaken coverage.** Deleting six
  `mock.patch` calls removes the thing that guaranteed no real client
  was built, and a fake handed in through `obj=` does not replace it.
  Mitigation: step 2a adds the explicit "never constructed"
  assertion, which is a stronger statement than the patches made, and
  the plan forbids weakening existing assertions. The reviewer checks
  the diff for assertion changes, not just for a green run.

- **The end-to-end root-group test depends on the package being
  installed.** It reaches the `k3s` group through the entry point
  loader, so a bare `stestr run` in a tree where the package is not
  installed would fail on a missing subcommand rather than on the
  behaviour. Mitigation: `tox -epy3` installs the package, CI runs
  tox, and the test's docstring says so. If it proves flaky in CI it
  is deleted rather than weakened -- the assertion it makes is the
  phase's deliverable and a test that sometimes checks it is worse
  than none.

- **Nothing in unit tests can prove the fix against a live cloud.**
  The whole point is which credentials reach a real API server.
  Mitigation: the merge tier deploys a real cluster
  (`tools/ci_deploy_test.sh`, `docs/testing.md`), and it exercises the
  common path. The explicit-flags path it does not exercise is
  checked by the root-group test at the boundary instead, which is the
  strongest available claim and is stated as such rather than
  overclaimed.

- **A `KeyError` is an ugly failure for a caller who invokes the group
  without a client.** Accepted rather than mitigated: decision 1
  argues the alternative is worse, and the only callers who can reach
  it are tests, which now pass a client deliberately.

## Definition of done

Each of these is checkable, and most are one command:

- `grep -n 'apiclient.Client(' shakenfist_client_k3s/__init__.py`
  returns nothing.
- `grep -rln 'apiclient.Client(' shakenfist_client_k3s/` names
  `client.py` and nothing else outside `tests/`.
- `grep -rn 'apiclient.Client' shakenfist_client_k3s/tests/` names
  only `test_client.py` and `test_root_options.py`.
- A test invokes `shakenfist_client.main.cli` with `--apiurl`,
  `--key` and `--namespace` ahead of `k3s list` and asserts that the
  client the plugin used is the one those options built, and that its
  `async_strategy` is `apiclient.ASYNC_CONTINUE`.
- A test asserts `apiclient.Client` is never constructed while a k3s
  command runs, and fails if it is.
- `make_client()` has tests for all four of its behaviours: all three
  arguments supplied passes them plus
  `suppress_configuration_lookup=True`; any one missing passes none of
  those four; `async_strategy` is `ASYNC_CONTINUE` either way; and
  `UnconfiguredException` propagates.
- The golden `--help` fixtures pass unmodified: `git diff --stat`
  against `develop` shows no change to any file under
  `shakenfist_client_k3s/tests/` whose name contains `help`.
- `grep -n 'apiclient.Client()' docs/library-api.md` returns nothing.
- `ARCHITECTURE.md:8-10`'s claim that all communication happens
  through the client `sf-client` places in the Click context is true,
  which the first two greps above establish.
- `docs/usage.md` says that the root `--async` does not apply to `k3s`
  commands, so the one behaviour this phase changes for an operator is
  written down where they will look.
- `tox -epy3`, `tox -eflake8`, `flake8 shakenfist_client_k3s` and
  `pre-commit run --all-files` all pass.
- `python3 -c 'import shakenfist_client_k3s'` succeeds.
- The merge tier of CI is green, which is the only check that
  exercises this against a live Shaken Fist cluster.
- The master plan's Execution table records this phase's merge commit
  in its `Merged` column, because phase 6 audits the accumulated diff
  and that range is not recoverable afterwards.

## Back brief

Before executing any step, back brief the operator on your
understanding of this plan and how the work you intend to do aligns
with it.

One gate, before step 2a starts: **decision 2 needs agreement, not
just reading.** Forcing the async strategy by assigning to the root
client's attribute, rather than by constructing a second client from
its connection parameters, is the phase's one contestable choice, and
reversing it afterwards rewrites both the code and the tests step 2a
writes. Say which of the two you are implementing and why before you
edit anything.
