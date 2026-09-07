# Library API phase 1: a callable orchestration layer

## Prompt

This phase makes the k3s orchestration callable from Python. It moves
the command bodies out of the Click layer, replaces `sys.exit(1)` with
an exception hierarchy, and routes output through a reporter the
caller chooses. It adds no new cluster behaviour at all.

Ground every change in the tree rather than in this document. The
files that matter are `shakenfist_client_k3s/__init__.py` (the Click
group and the command bodies), `shakenfist_client_k3s/primitives.py`
(the orchestration primitives, the namespace-metadata cluster state
and the version caches), `shakenfist_client_k3s/progress.py` (the two
mode progress reporter), and the existing tests in
`shakenfist_client_k3s/tests/`. Two files in sibling repositories set
constraints this phase cannot break: `shakenfist_client/main.py` in
`client-python`, whose root `cli` callback builds the client and whose
`GroupCatchExceptions` already maps every `apiclient` exception to an
error line and exit 1; and `PLAN-k3s-ci-runners.md` in `33fl`, whose
design decisions 2 and 5 are why the library exists.

**Planning effort:** high. The master plan nominates phase 1 as its
keystone, and the review of this work has to be against pre-refactor
CLI behaviour rather than against the brief -- the CLI's output and
exit codes are the contract being preserved, and a sub-agent summary
cannot demonstrate that they were.

**Process note:** this phase plan is branched from
`plan-template-compliance` rather than from `develop`, because the
master plan it registers against has not merged yet. If that branch
lands first the stack collapses to nothing; if it does not, this
branch carries both commits.

## Scope

In: a `Cluster` state object and a reporter that together replace the
Click context inside the orchestration code; an exception hierarchy
replacing all seven `sys.exit(1)` calls in `primitives.py` and the
thirteen in `__init__.py`; routing all forty-eight `print()` calls in
those two files through the reporter; moving each Click command body
into a `Cluster` method; and unit tests for the exception paths, the
collecting reporter and CLI output equivalence.

Out, and deliberately so:

- **Phase 2's client construction fix.** `_bind_cluster_context()`
  discarding the root `--apiurl`/`--key`/`--namespace` is a real bug
  (see survey finding 5), and this phase moves the code that contains
  it without fixing it. Fixing it here would put a behaviour change
  inside a refactor whose entire safety argument is that behaviour is
  unchanged.
- **Every new verb.** No `remove-worker`, no health verb, no
  incremental `install_workers()`, no reconcile. Phase 3.
- **Making the kubeconfig side effects optional, and the MetalLB and
  Longhorn installs conditional.** The code moves into `Cluster`
  methods carrying exactly today's mandatory behaviour. Phase 3 makes
  it optional, and does so far more easily against a method than
  against a Click body.
- **Message wording.** Not one user-visible string changes in this
  phase. Improvements go in a follow-up so that any output diff during
  review is a bug rather than a judgement call.
- **`shakenfist.shakenfist`**, which this plan never modifies.

## What the survey found

The survey checked the master plan's phase 1 claims against `develop`
at 466f230. Most held exactly, including every `__init__.py` line
reference: `k3s_create()` at `__init__.py:93-286`, the state
transitions at `:177`, `:279` and `:442`, the unconditional MetalLB
and Longhorn calls at `:234-235`, the kubeconfig merge and its
`kubectl` requirement at `:249-253`, the `kubectl config unset` loop
at `:459-465`, and `install_workers()` at `primitives.py:537-542`.
`Progress` does accept a `stream` argument (`progress.py:41`).

Five things were wrong or missing, and three of them change the
design.

1. **The `print()` count is 24 in `primitives.py`, not 31, and the
   master plan omits `__init__.py` entirely, which has another 24.**
   The phase has to route 48 calls, not 31. Corrected in the master
   plan's Situation section as part of this planning commit.

2. **`primitives.py` has exactly seven `sys.exit(1)` calls, not
   "seven and others", and the last is at `:405`, not `:407`.** They
   are at `:91`, `:114`, `:124`, `:170`, `:198`, `:283` and `:405`.
   `__init__.py` has thirteen more, at `:136`, `:139`, `:148`,
   `:253`, `:268`, `:341`, `:346`, `:363`, `:386`, `:465`, `:485`,
   `:512` and `:537`. Corrected in the master plan.

3. **`ctx.obj` carries a sixth key, and it is mutable state rather
   than a parameter.** `get_cluster_metadata()` caches the cluster's
   namespace metadata under `METADATA_KEY % name` on first read;
   `set_cluster_metadata()` writes through to both the cache and the
   API; `delete_cluster_metadata()` removes both
   (`primitives.py:27-53`). The master plan's "five keys become
   explicit arguments" therefore does not reach. This is why decision
   1 below chooses an object over free functions.

4. **The master plan's citation for the unguarded plugin loader is
   stale.** It says `client-python/shakenfist_client/main.py:138-148`;
   the loader is now at `:259-269`, and `:138-148` is inside
   `GroupCatchExceptions`. Corrected in the master plan. The
   constraint itself still holds -- the loader is unguarded, so a
   top-level import error in this plugin takes `sf-client` down.

5. **The root `cli` callback already builds a client that honours
   `--apiurl`, `--key`, `--namespace` and `--async`**
   (`client-python/shakenfist_client/main.py:228-237`), and stores it
   in `ctx.obj['CLIENT']`. Both `_bind_cluster_context()`
   (`__init__.py:36`) and `k3s_create()` (`__init__.py:98`) then
   overwrite it with `apiclient.Client(async_strategy=ASYNC_CONTINUE)`.
   So phase 2 cannot simply stop overwriting the root client: the k3s
   code depends on `ASYNC_CONTINUE` and the root default is `pause`.
   Recorded here for phase 2 rather than acted on.

Two smaller findings shape the work without changing the plan:
`Progress.interactive` calls `self.stream.isatty()` (`progress.py:43`),
so a collecting reporter must be file-like rather than a list; and
`tests/test_primitives.py:14-26` already carries a `FakeContext` shim
whose only purpose is to hold an `obj` dict, which this phase deletes.

## Decisions this plan already takes

1. **The callable layer is a `Cluster` object in a new
   `shakenfist_client_k3s/cluster.py`, not free functions taking five
   arguments.** Survey finding 3 is the reason: the cluster metadata
   cache is per-call mutable state with write-through semantics, and
   threading it through free functions means either re-reading
   namespace metadata on every primitive call -- more API calls, and a
   wider lost-update window against conductor, which the master plan
   names as the other writer -- or passing a mutable dict alongside the
   other five arguments, which is the context object again with a worse
   name. `Cluster` holds the client, name, namespace, reporter and that
   cache.

2. **~~`primitives.py` keeps module-level functions; they take a
   `Cluster` as their first argument instead of a `ctx`.~~ Reversed at
   the post-1b gate by operator decision: the cluster-scoped primitives
   become methods on `Cluster`.**

   The original reasoning, kept because it is still the argument
   against doing this later rather than now: making them methods is the
   tidier end state, but it moves 657 lines into a class body in the
   same change that alters their error and output behaviour, and it
   rewrites every test. That risk is why the gate exists and why the
   conversion happens at the gate -- against a tree where step 1b has
   landed and steps 1c to 1e have not yet rewritten those same lines.
   Doing it after 1c and 1d would mean rewriting the raises and the
   reporter calls twice.

   The split is by what the code is scoped to, not by file:

   - Anything that reads or writes cluster metadata, or drives this
     cluster's nodes, becomes a `Cluster` method.
   - The two release lookups are **namespace**-scoped, not cluster
     scoped -- their caches live in namespace metadata under
     `K3S_VERSION_CACHE_KEY` and `LONGHORN_VERSION_CACHE_KEY`, and the
     `query-k3s-version` and `query-longhorn-version` commands have no
     cluster at all. They stay module-level functions taking a client,
     a namespace and a reporter.
   - Pure helpers with no state, `_describe_agent_op()` among them,
     stay module-level functions.

   This retires the `name=None` `Cluster` and the `ValueError` that
   guarded it: `list`, `query-k3s-version` and `query-longhorn-version`
   now build no `Cluster` at all, which is a better answer than
   tolerating a nameless one.

3. **The reporter is file-like, not a list.** `Progress` already takes
   a `stream` and calls `write()`, `flush()` and `isatty()` on it, so
   the collecting reporter is a small class implementing those three,
   with `isatty()` returning False. The library's own messages go
   through the same object, so there is one output channel rather than
   two, and the Ansible module's `log` list is the collector's
   accumulated lines. Rejected: adding a callback to `Progress`, which
   duplicates a mechanism the class already has and would change every
   call site in it.

4. **Verbosity moves onto the reporter.** `_emit_debug(ctx, m)` exists
   in both files and reads `ctx.obj['VERBOSE']`. It becomes a method on
   the reporter, which is what removes the last `ctx` read from
   `primitives.py`.

5. **The Click layer catches once, at the group.** A `click.Group`
   subclass overriding `invoke()` catches the new base exception,
   prints `str(e)` and exits 1. Twenty `sys.exit(1)` sites become one,
   and no future command can forget to handle it. This mirrors
   `GroupCatchExceptions` in the parent CLI. The new exceptions must
   not subclass anything in `apiclient`, so that the parent's handlers
   keep their current behaviour for API errors.

6. **Every exception carries structured attributes, and its `__str__`
   renders exactly what the CLI prints today.** That is what lets the
   Click layer print the current text with no per-command formatting,
   and what lets `fail_json()` return the same information as fields
   in phase 5. The two agent failure exceptions matter most here:
   `_abort_agent_op_error()` (`primitives.py:266-283`) and the
   non-zero return code path (`primitives.py:394-405`) between them
   print instance name and uuid, operation uuid, command line, return
   code, stdout, stderr and results, and all of that currently exists
   only as printed text.

7. **No output changes.** Decision 6's `__str__` requirement is
   enforced by a golden capture taken before any code moves, which
   step 1a records as a test fixture.

## Step plan

Each step is its own commit, and each must build and pass `tox -epy3`
on its own.

| Step | Effort | Model | Isolation | Brief for sub-agent |
|------|--------|-------|-----------|---------------------|
| 1a | medium | sonnet | none | Capture the pre-refactor contract as test fixtures, before anything moves. Add a test module that asserts `sf-client k3s --help` and the `--help` of all ten subcommands (`list`, `create`, `query-k3s-version`, `query-longhorn-version`, `getconfig`, `show`, `delete`, `expand-workers`, `expand-addresses`, `update-os`) match stored golden text, using `click.testing.CliRunner` the way `tests/test_commands.py:31-34` already does. Store the golden text as files under `shakenfist_client_k3s/tests/`, generated from the current tree, not hand-typed. Then add `shakenfist_client_k3s/exceptions.py` with the hierarchy named in decision 6 (`K3sClusterException` base; `ClusterExistsError`, `ClusterNotFoundError`, `NetworkNotFoundError`, `ReleaseLookupError`, `AgentOperationError`, `CommandFailedError`, `KubeconfigError`), each storing its fields as attributes and rendering the exact current message text in `__str__`. Nothing raises them yet. Do not subclass anything from `shakenfist_client.apiclient`. |
| 1b | high | opus | none | Add `shakenfist_client_k3s/cluster.py` with a `Cluster` class holding `client`, `name`, `namespace`, `reporter` and the cluster metadata cache, exposing `get_metadata()`, `set_metadata(md)` and `delete_metadata()` that reproduce `primitives.get_cluster_metadata/set_cluster_metadata/delete_cluster_metadata` (`primitives.py:27-53`) exactly -- fetch once on first read, write through on set, remove from both cache and API on delete. Add the reporter to `progress.py` or a new module: a file-like collector with `write`, `flush` and `isatty` (returning False), plus a `debug()` method carrying the verbosity that `_emit_debug` currently reads from `ctx.obj['VERBOSE']`, and a stdout-backed default. Then change every function in `primitives.py` to take a `Cluster` as its first argument instead of `ctx`, and update the call sites in `__init__.py` to build one. Do not change error handling or output in this step. Update `tests/test_primitives.py` to construct a `Cluster` instead of the `FakeContext` at `tests/test_primitives.py:14-26`, and delete that shim. Verification is unit tests only; the orchestration itself needs a live cluster. |
| 1b2 | high | opus | none | Convert the cluster-scoped functions in `primitives.py` into methods on `Cluster`, per the reversal recorded in decision 2. Cluster-scoped means anything reading or writing cluster metadata or driving this cluster's nodes. `get_k3s_release()` and `get_longhorn_release()` are namespace-scoped and stay module-level functions taking a client, a namespace and a reporter; `_describe_agent_op()` and any other stateless helper stay module-level too. Retire the `name=None` `Cluster` and its `ValueError` guard, since the three namespace-scoped commands stop constructing one. Imports must stay one-directional: `cluster.py` may import `primitives`, never the reverse. Change no error handling and no output -- the `sys.exit(1)` and `print()` counts must be unchanged when this step ends. |
| 1c | high | opus | none | Replace every `sys.exit(1)` with a raise. Step 1b2 moved these, so the map below is against the post-1b2 tree, not the pre-refactor one: five remain in `primitives.py`, at `:66`, `:89`, `:99`, `:143` and `:171` (all in the two release lookups), and two moved into `cluster.py`, at `:165` (`_abort_agent_op_error`) and `:283` (the return-code path in `reap_execute`). The exception docstrings written in step 1a still cite the pre-1b2 locations; correct them as part of this step. In `__init__.py` the thirteen are unmoved, at `:136`, `:139`, `:148`, `:253`, `:268`, `:341`, `:346`, `:363`, `:386`, `:465`, `:485`, `:512` and `:537`. Each raises the exception from `exceptions.py` matching its failure, populated with the values the surrounding `print()` calls currently interpolate -- `Cluster._abort_agent_op_error` and the return-code path in `Cluster.reap_execute` carry the most fields, see decision 6. Delete the `print()` calls those raises replace, since `__str__` now renders that text. Add a `click.Group` subclass in `__init__.py` overriding `invoke()` to catch `K3sClusterException`, print `str(e)` and `sys.exit(1)`, and use it for the `k3s` group. `import sys` may become unused in `primitives.py`; remove it if so. The exit code and the printed text must not change for any failure. |
| 1d | medium | sonnet | none | Route the remaining output through the reporter. Replace every surviving `print()` in `primitives.py`, `cluster.py` and `__init__.py` with a reporter call, and replace both surviving `_emit_debug()` definitions (one in `primitives.py`, taking a reporter since step 1b2; one in `__init__.py`, still taking a `ctx`) and all their call sites with the reporter's `debug()`. `Progress` is already constructed with a `stream`, so pass the reporter's stream where `progress.Progress(...)` is built (`__init__.py:110`) and where `progress.get_progress(ctx)` is called (`progress.py:139-144`, and throughout `primitives.py`). After this step `grep 'print(' shakenfist_client_k3s/primitives.py` and the same for `cluster.py` must return nothing (step 1b2 left 11 in the former and 13 in the latter); `__init__.py` may keep `print()` only where a Click command formats its own output for the terminal, and each such site needs a comment saying why. |
| 1e | high | opus | none | Move the body of each Click command into a `Cluster` method, leaving the command as argument parsing plus one call. `k3s_create()` (`__init__.py:93-286`) is the large one and becomes `Cluster.create(...)`; also `getconfig` (`:335-350`), `show` (`:357-372`), `delete` (`:379-470`), `expand-workers` (`:479-496`), `expand-addresses` (`:506-524`) and `update-os` (`:531-549`). `list`, `query-k3s-version` and `query-longhorn-version` are namespace-scoped rather than cluster-scoped -- all three bind with `name=None` today (`__init__.py:56`, `:298`, `:320`) -- so they become module-level functions taking a client, a namespace and a reporter, rather than methods on a `Cluster` whose name is None. Preserve behaviour exactly, including the mandatory `~/.kube/config` write and `kubectl` merge in create and the `kubectl config unset` calls in delete -- making those optional is phase 3. Methods return values rather than printing them: `show` returns the metadata dict, `getconfig` returns the kubeconfig string, and the Click command does the printing. |
| 1f | high | opus | none | Add the tests this phase owes. Cover: each exception type raised on its trigger and carrying the right attributes; the collecting reporter accumulating the same text stdout would have received, including through a `Progress` in non-interactive mode; the group-level handler mapping an exception to exit code 1 and the pre-refactor text; and an end-to-end drive of `Cluster` create-through-delete which asserts that nothing at all was written to `sys.stdout`. Use the scripted fake in `tests/fakes.py`, not a bare `mock.MagicMock`: a MagicMock attribute compares equal to none of the `'created'`/`'ready'` literals the wait loops test against, so every wait would spin forever. That last one is the phase's real deliverable expressed as a test. Follow the existing testtools/stestr style with the API mocked; the tests in `tests/test_primitives.py` and `tests/test_progress.py` are the pattern. Do not weaken the golden `--help` assertions from step 1a to make anything pass. |
| 1g | medium | sonnet | none | Document the library API in a new `docs/library-api.md`: constructing a `Cluster`, the reporter, the exception hierarchy, and a worked example that creates and deletes a cluster with no Click involvement and no kubeconfig side effects. Link it from `README.md` only if the documentation links section changes. Update `ARCHITECTURE.md`, because the shape of the system genuinely changed -- there is a new callable layer between the CLI and the primitives -- and add `cluster.py` and `exceptions.py` to the Key Files table in `AGENTS.md`. Do not restate the API reference in either file; both are an index into `docs/`. |

## Risks and mitigations

- **Silent output drift.** The refactor's whole safety argument is
  that observable behaviour is unchanged, and a sub-agent cannot prove
  that from its own summary. Mitigation: step 1a captures the golden
  `--help` text before anything moves, and step 1f asserts failure-path
  text and exit codes. The management session reads the 1c and 1e
  diffs against the pre-refactor file, not against the brief.

- **The reporter changes `Progress`'s output mode.** `interactive` is
  `not verbose and stream.isatty()` (`progress.py:43`), so a reporter
  whose `isatty()` answers wrongly silently switches the CLI between
  in-place updates and line mode. Mitigation: the default reporter
  wraps the real `sys.stdout` and delegates `isatty()` to it, and step
  1f asserts both modes.

- **The metadata cache semantics drift.** `Cluster` reproducing
  `get`/`set`/`delete` approximately rather than exactly would change
  how many namespace metadata reads a create performs, which matters
  because conductor is the other writer. Mitigation: step 1b's brief
  states the three semantics explicitly, and 1f drives them.

- **`tox -eflake8` only lints changes since HEAD**
  (`tox.ini:22-27`, `tools/flake8wrap.sh -HEAD`), so a seven-commit
  refactor is never linted as a whole by the project's own command.
  Mitigation: run `flake8 shakenfist_client_k3s` over the entire
  package once at the end of the phase, and record that in the pull
  request.

- **A top-level import error takes `sf-client` down.** Two new modules
  are being added to a package the plugin loader imports unguarded
  (`client-python/shakenfist_client/main.py:259-269`). Mitigation:
  `cluster.py` and `exceptions.py` import nothing beyond the standard
  library and what the package already imports, and the definition of
  done checks the import directly.

- **Phase 3 conflicts.** Phase 1 touches every line phase 3 will
  touch. Mitigation: the master plan already forces the ordering; do
  not start phase 3 in parallel.

## Definition of done

Each of these is checkable, and most are one command:

- `grep -c 'sys.exit' shakenfist_client_k3s/primitives.py` returns 0,
  and the same for `shakenfist_client_k3s/cluster.py`.
- `grep -n 'ctx' shakenfist_client_k3s/primitives.py` returns nothing.
- `grep -c 'print(' shakenfist_client_k3s/primitives.py` returns 0, as
  does the same for `cluster.py`. Every surviving `print()` in
  `__init__.py` has a comment saying why it is terminal formatting.
- The golden `--help` fixtures from step 1a still pass unmodified, for
  the group and all nine subcommands.
- The step 1f test that drives `Cluster` create-through-delete against
  a mocked client asserts an empty `sys.stdout`, and passes.
- That test asserts on `sys.stdout`, which is not the same as the
  process's file descriptor 1, and one known path escapes it. See
  "The kubectl unset leak" below; step 1f must pin that leak with a
  test that names it, rather than leaving the stronger claim implied.
- `tox -epy3`, `tox -eflake8`, `flake8 shakenfist_client_k3s` and
  `pre-commit run --all-files` all pass.
- `python3 -c 'import shakenfist_client_k3s'` succeeds.
- The merge tier of CI is green, which is the only check that
  exercises this against a live Shaken Fist cluster
  (`tools/ci_deploy_test.sh`, `docs/testing.md`).
- `docs/library-api.md` exists and its worked example runs against a
  mocked client.
- The master plan's Execution table records this phase's merge commit
  in its `Merged` column, because phase 6 audits the accumulated diff
  and that range is not recoverable afterwards.

## The kubectl unset leak

Found while verifying step 1e, by driving the whole lifecycle without
mocking `subprocess`.

`Cluster.delete()` runs `kubectl config unset` three times through
`subprocess.run(..., shell=True)` with no `capture_output`
(`cluster.py:862-863`). The child inherits the process's file
descriptor 1, so kubectl's three `Property "..." unset.` lines go
straight to the real stdout, bypassing Python's `sys.stdout` and
therefore the reporter entirely. Every other side effect in the file
is clean: the create-side merge at `cluster.py:712-715` passes
`capture_output=True`, and the kubeconfig writes are file writes.

This matters because it is exactly the property the phase exists to
establish. An Ansible module must own stdout for its JSON result, and
while `delete()` runs it does not. The phase's own empty-stdout test
cannot see this, because it mocks `subprocess.run`.

It is **not** fixed in this phase, for the reason decision 7 gives:
capturing that output would remove three lines a `sf-client k3s
delete` prints today, and this phase changes no user-visible output.
It belongs with phase 3's "kubeconfig side effects opt-out" work in
the master plan, which is already the row that owns this code, and it
is recorded there.

## Back brief

Before executing any step of this plan, please back brief the operator
as to your understanding of the plan and how the work you intend to do
aligns with that plan.

There is one further gate inside the phase. **After step 1b and before
step 1e**, back brief the shape of `Cluster` -- its constructor, its
attributes, and the signatures of the methods the command bodies will
move onto. Moving seven command bodies onto the wrong object is cheap
to propose and expensive to redo, and decision 2 (functions taking a
`Cluster` rather than methods on it) is the point a reviewer is most
likely to want changed.
