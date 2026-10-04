# Audit findings: style conformance

Lens 6c of the phase 6 push audit. Scope is
`docs/plans/audit/scope-files.txt` with everything under
`docs/plans/` excluded, which leaves eight non-test Python source
files, fifteen test modules, `tools/build-collection.py` and the
packaging and CI files.

One scope caveat, found while reading and worth recording because it
affects every lens: four commits have landed on source files *since*
`eb248bd`, the last of the ten merges in decision 1 -- `211d3f9`,
`73d4846`, `b6c4b7b` and `edabb7f`, which are the
`node-customisation` plan's per-role sizing work (216 lines of
`cluster.py`, 33 of `__init__.py`, 43 of `exceptions.py`). That work
is explicitly out of scope per the plan's exclusion 1. Reading the
worktree therefore over-reads, so every finding below was checked
against `git show eb248bd:<file>` before being recorded, and the
Python floor was verified against both trees.

## Summary

| Action | Count |
|--------|-------|
| fix | 0 |
| document | 0 |
| consider | 2 |
| none | 6 |

No finding gates the phase. The two `consider` items are small
duplications in code phases 1 and 3 added; neither changes
behaviour.

## The Python floor

**Survey finding 7 is confirmed, independently and by a stronger
method than the survey used.** The floor is
`requires-python = ">=3.7"` (`pyproject.toml:54`), and no scope merge
changed it -- grepping the ten diffs for `requires-python`,
`Programming Language :: Python` and `constraints` turns up nothing
but plan prose, so the shared block's "raising the floor is a
supported-platforms decision" clause has nothing to audit here.

Three checks, each run twice: once over the worktree, and once over
`git archive eb248bd` so the result describes the scope's own end
state rather than the node-customisation work layered on top.

**1. A real Python 3.7 interpreter.** Rather than reason about which
constructs are too new, I compiled every scope `.py` file with
`compile()` under `python:3.7-slim` in a network-isolated container:

```
23 files compiled under 3.7.17, 0 syntax errors   (worktree)
24 files compiled under 3.7.17, 0 errors          (eb248bd tree)
```

That is a complete and decisive answer for the *syntax* half of the
clause. It rules out, by construction and not by pattern matching:
the walrus operator, `match`/`case`, `except*`, positional-only
parameters, PEP 604 `X | Y` and builtin generics wherever they are
not string-ised, the f-string `=` specifier, and f-string
quote-reuse and backslashes. No grep can claim that coverage.

**2. An AST scan for standard library APIs**, which a 3.7 parse
cannot catch because they are syntactically valid at any version. I
walked every scope file for imports of post-3.7 modules (`tomllib`,
`zoneinfo`, `graphlib`, `importlib.metadata`), for roughly forty
post-3.7 module attributes (`datetime.UTC`, `math.prod`,
`shlex.join`, `functools.cached_property`,
`typing.Protocol`/`Literal`/`Final`/`TypedDict`/`Annotated`,
`ast.unparse`, `asyncio.to_thread`, `itertools.pairwise`,
`enum.StrEnum` and the rest), for post-3.7 method names
(`removeprefix`, `removesuffix`, `is_relative_to`, `with_stem`), and
for `|`/`|=` anywhere -- both as a PEP 604 annotation and as the 3.9
dict-merge operator.

The scan reports exactly one post-3.7 standard library hit across
both trees, and it is guarded:

```
shakenfist_client_k3s/cluster.py:37-40
try:
    from importlib.metadata import version as distribution_version
except ImportError:
    from importlib_metadata import version as distribution_version
```

The guard is correct and the backport is declared --
`"importlib-metadata; python_version < '3.9'"` in
`pyproject.toml`'s `dependencies` -- so on 3.7 the fallback resolves
to an installed package rather than a second `ImportError` at module
scope. That last point matters more than it looks: an unguarded or
undeclared version lookup here would raise during
`import shakenfist_client_k3s`, which is the one failure mode
`AGENTS.md` says takes the whole `sf-client` CLI down.

**3. Spot greps for the risks a table can miss**: post-3.7 keyword
arguments (`shutil.copytree(dirs_exist_ok=)`, bare `@lru_cache`,
`@cache`), `dataclass`, any `datetime` use at all, and every
`subprocess` call site. Nothing newer than 3.7 appears. The
`subprocess.run(..., capture_output=True)` and
`check_output(..., text=True)` calls in `cluster.py:1694`,
`cluster.py:2122` and `tools/build-collection.py:81` all sit exactly
on the 3.7 line, which is where both keywords were added.

The one `from __future__ import annotations`
(`collection/plugins/modules/sf_k3s_cluster.py:23`) is 3.7-safe and
is required by `validate-modules`; tracked by #94, not re-raised
here.

So the check `PUSH-AUDIT.md` ranks first -- "a real break on a real
user's machine" -- comes back clean, and I found no counter-example
the survey missed. What remains unproven is not the code but the
claim: nothing in CI ever runs 3.7, so this result is my scan plus a
containerised compile rather than a passing test suite. That gap is
exactly #82 and belongs there.

**The typing clause is settled, not re-raised.** Decision 5 of the
phase 6 plan declines type hints and mypy for the whole of phases
1-5's output: 128 definitions, zero annotations, no mypy
configuration anywhere. Recorded, filed, moving on.

**The walrus preference does not apply.** At a 3.7 floor the shared
block's "prefer the walrus operator" is unreachable, as the "In this
project" note says. The f-string half of the same sentence is
followed -- `cluster.py` and `__init__.py` use f-strings freely.

## Checklist questions answered

### Click commands attached to the `k3s` group

**Clean.** All twelve commands are registered on the group and all
twelve appear once each in
`shakenfist_client_k3s/tests/cli_contract/group.txt`: `create`,
`delete`, `expand-addresses`, `expand-workers`, `getconfig`,
`health`, `list`, `query-k3s-version`, `query-longhorn-version`,
`remove-worker`, `show`, `update-os`. The group itself is attached to
the host CLI by `load(cli)` at `__init__.py:510-511`, which is the
`shakenfist_client.plugin` entry point `pyproject.toml` declares.
Phase 3 added two commands (`health`, `remove-worker`) and both
follow the pattern. See F1 for the redundant second registration
each of them copied.

### `--namespace` handled consistently

**Clean.** Every one of the twelve commands declares a `--namespace`
option (verified against the per-command contract files), and every
one resolves it through exactly one of three binders, none of which a
command bypasses:

- `_bind_namespace_context()` (`__init__.py:11`) for the three
  namespace-scoped commands -- `list`, `query-k3s-version`,
  `query-longhorn-version` -- which name no cluster.
- `_bind_cluster_context()` (`:53`) for the eight cluster-scoped
  verbs.
- `_bind_new_cluster_context()` (`:65`) for `create` alone, which is
  the only command that may create the namespace.

The default is applied in one place (`:48-49`,
`if not namespace: namespace = client.namespace`), so `--namespace`
absent always means "the namespace the client authenticated as" and
never "some second lookup's idea of it". The distinction `create`
needs -- whether the option was *passed*, as opposed to what it
resolved to -- is Click information and is handled in the Click layer
at `:78`, with the docstring saying why it does not move onto
`Cluster.create()`. The collection module keeps the same split under
different names, `namespace` for the target and `auth_namespace` for
the identity (`sf_k3s_cluster.py:646-648`), and translates
`make_client()`'s message accordingly.

The only wart is a stale help string on `show` -- F3, pre-existing
and untouched by these phases.

### Orchestration reached through a `Cluster`, not a click context

**Clean, and enforced by the shape of the code rather than by
discipline.** `grep` for `ctx.obj`, `pass_context`,
`get_current_context` and `click.` across `cluster.py`,
`primitives.py`, `client.py`, `exceptions.py`, `progress.py` and
`sf_k3s_cluster.py` returns hits only inside comments and
docstrings. There is no `import click` anywhere outside
`__init__.py`. `ctx.obj` is touched in exactly two places, both
reads, both in the binders: `ctx.obj['CLIENT']` at `:33` and
`ctx.obj.get('VERBOSE', False)` at `:50`. Nothing is written back
into the context, which is what `_bind_cluster_context()`'s
docstring claims at `:56`, and the claim checks out.

Every cluster-scoped command body is one or two lines that call a
`Cluster` method -- `delete`, `expand-workers`, `remove-worker`,
`expand-addresses` and `update-os` are literally a single statement.
Phase 2 changed how the client is constructed (take sf-client's, do
not build a second one) and no command added after it regressed
that: `make_client()` in `client.py` has no CLI caller at all, which
its module docstring states and `grep` confirms.

### Long-running work reports through `Cluster.get_progress()`

**Substantially clean; one duplication, F2.** The reporter is
injected at construction in all three callers (`__init__.py:62`,
`:83`, `sf_k3s_cluster.py:696`) and `Cluster.__init__` falls back to
a default `progress.Reporter()` so a bare library caller still gets
output somewhere sensible. Eleven wait loops and phase openings go
through `self.get_progress()`; `Progress` writes to the reporter as a
stream, so there is a single output channel rather than two.

Five entry points -- `create`, `expand_workers`, `remove_worker`,
`expand_addresses`, `update_os` -- construct `progress.Progress(...)`
directly and assign `self.progress`. That is the *documented* design
("Commands which know how many phases they have build their own and
assign it", `cluster.py:434-475`) and all five do assign, so
behaviour is right; what is wrong is that the construction is
copy-pasted five times. F2.

Two near-misses I checked and cleared:

- `health()` opens no `Progress` at all. Correct: it is a bounded
  probe with a 30-second timeout whose output is a dict, and its
  docstring says it must not hang.
- `await_execute()` polls without reporting. Correct: its only
  unbounded caller is `reap_execute()`, reached from
  `execute_and_await()` *after* `await_idle()` has already waited
  with progress, by which point the operations are terminal and the
  loop returns immediately. The one caller that passes a timeout is
  `health()`'s probe.

### Every `print(` call, justified

Seven real `print()` calls survive in non-test source. Grepping
`print(` across the whole scope returns twenty-odd more hits, every
one of which is the word appearing inside a comment or docstring
explaining why the code does *not* print. Census:

| File:line | Category | Justification |
|-----------|----------|---------------|
| `__init__.py:124` | CLI error path | `GroupCatchClusterExceptions.invoke()` turning a `K3sClusterException` back into the error line and exit code the command used to produce. Goes to `sys.stderr`, matching `shakenfist_client.main`'s own handler. Comment at `:120-123` says exactly this. |
| `__init__.py:147` | CLI presentation | `k3s list` formatting the cluster list for a human; a library caller calls `primitives.list_clusters()` and gets the list. |
| `__init__.py:265` | CLI presentation | `query-k3s-version` rendering the looked-up version. |
| `__init__.py:289` | CLI presentation | `query-longhorn-version`, same. |
| `__init__.py:306` | CLI presentation | `getconfig` emitting the kubeconfig, which is the command's entire output and must be pipeable. |
| `__init__.py:321,323` | CLI presentation | `show` formatting the metadata dict. |
| `tools/build-collection.py:91` | Build script | A progress line from a script whose only consumer is a CI log. Legitimate per the brief. |

All six CLI calls sit in the Click layer, which owns the process's
stdout, and each carries a comment naming the library call a non-CLI
caller uses instead. **The library layer prints nothing.**
`cluster.py`, `primitives.py`, `client.py`, `exceptions.py` and
`progress.py` contain zero `print()` calls, and a wider grep for
`sys.stdout`, `sys.stderr`, `click.echo`, `logging`, `LOG.` and
`warnings.` across those five files plus `sf_k3s_cluster.py` returns
nothing at all. `progress.Reporter` is the only writer, and it looks
`sys.stdout` up per call rather than caching it, so a caller that
redirects stdout is still honoured.

**The Ansible module writes nothing to stdout**, which is the
invariant that matters most because its stdout *is* its JSON result.
Zero `print()`, a `CollectingReporter` injected at
`sf_k3s_cluster.py:696`, and every message handed back as the `log`
return value. The module's header comment and
`tests/test_ansible_module.py:14` both state the rule. One detail
worth recording as deliberate rather than accidental: `verbose` is
left `False` on that reporter (`:642`) because `delete()` debug-logs
the whole metadata document, kubeconfig and node token included -- a
security property, not a volume preference.

There is an acknowledged inconsistency, not a defect: `health`
renders through the reporter (`_render_health`, `:357`) while the
five older commands use bare `print()`. `_render_health`'s docstring
argues the case at `:360-364` -- the older commands pre-date the
reporter, and a health check is the command most likely to be run by
something using stdout for its own output. New code took the better
path; old code was left alone. That is the right call for an audit
phase and I am not reopening it.

### The plugin must never break `sf-client` startup

**Clean, and I measured it rather than inspecting it.**
`python3 -c 'import shakenfist_client_k3s'` exits 0; 6a timed it at
0.10-0.14s.

*Module scope contains no work.* I enumerated every top-level
statement in all eight source files via AST. Outside imports there
are: eleven `k3s.add_command()` calls (F1), fourteen constant
assignments, one `re.compile()` (`cluster.py:149`), the
`DOCUMENTATION`/`EXAMPLES`/`RETURN` string literals the Ansible
module needs, and two `if __name__ == '__main__'` guards. No network
call, no filesystem scan, no `glob`, no config read, no
`distribution_version()` invocation -- the version lookup is
imported at module scope but only *called* inside functions.

*No I/O at import.* I re-ran the import with `socket.socket`,
`socket.create_connection`, `socket.getaddrinfo`, `builtins.open`
and `os.listdir` all instrumented. Result: zero `getaddrinfo`, zero
`create_connection`, zero non-source files opened, zero directory
scans outside importlib's own machinery.

*The one socket is not ours.* See F4 -- it is urllib3's `HAS_IPV6`
probe, a loopback bind that is never connected, and it refines
`verification.md`'s attribution without changing its conclusion.

*Cost attribution.* `python3 -X importtime` gives 70.6ms total, of
which `shakenfist_client.apiclient` is 44.9ms (41.4ms of that
`requests`) and `click` is 14.5ms. Both are mandatory: the plugin
cannot register with Click without Click, and `apiclient` is the
thing it orchestrates through. This package's own marginal cost is
`cluster.py` at 8.6ms, 7.5ms of which is `yaml` (F5). So the
invariant holds with room to spare, and the only module-scope import
that could be deferred is worth 7ms.

*Reliability, not just speed.* The two ways this import could fail on
a user's machine are both handled: the `importlib.metadata` fallback
above, and the fact that nothing at module scope can raise on a
missing file, an unreachable API or a misconfigured `~/.shakenfist`
-- because nothing at module scope reads any of them.

### Cluster state belongs in namespace metadata, not local files

**Clean.** Every state read and write in the scope goes through the
API. The complete inventory:

| Key | Read | Written |
|-----|------|---------|
| `orchestrated_k3s_cluster_<name>` (`cluster.py:49`) | `Cluster.get_metadata()` `:367` | `set_metadata()` `:375`, `delete_metadata()` `:387` |
| `orchestrated_k3s_clusters` (`primitives.py:28`) | `primitives.py:42`, `cluster.py:1487`, `:2088` | `cluster.py:1508`, `:2100-2104` |
| `orchestrated_k3s_cluster_k3s_version_cache` (`primitives.py:30`) | `:52` | `:102` |
| `orchestrated_k3s_cluster_longhorn_version_cache` (`primitives.py:31`) | `:118` | `:178` |

All four are `client.get_namespace_metadata()` /
`set_namespace_metadata_item()` /
`delete_namespace_metadata_item()`. Nothing is read from or written
to a dotfile, a cache directory, `XDG_*`, `/var/lib` on the client
side, or a state file of any kind -- I grepped for all of those plus
`json.dump`, `yaml.dump` and `~/.`.

The only files the library writes are:

- `~/.kube/config` and a `tempfile.TemporaryDirectory()` copy of it
  during the merge (`cluster.py:1676-1715`). This is a user-facing
  artefact the caller asked for with `--kubeconfig`, not cluster
  state: `write_kubeconfig` defaults to `False` in the library so a
  library caller's kubeconfig is never edited unasked, and the
  authoritative copy lives in `md['kubeconfig']` (`:1659`), which is
  namespace metadata. Deleting `~/.kube/config` loses nothing --
  `sf-client k3s getconfig` reads it back out of the metadata.
- Nothing else. `json.dumps()` in `primitives.py:82`, `:98` and
  `:150` goes to `reporter.debug()`, not to a file.

The in-memory `self._metadata` cache (`cluster.py:351`) is scoped to
the `Cluster` object's lifetime and exists to keep the number of
namespace-metadata reads -- and so the width of the lost-update
window against conductor -- the same as it was before phase 1 moved
this state off `ctx.obj`. It is not persistence.

One naming nit: `orchestrated_k3s_clusters` does not match the
`orchestrated_k3s_cluster_*` prefix the checklist names. F8 -- it is
a wire-format value from the initial commit, and `none`.

## Findings

### F1: two redundant `k3s.add_command()` lines added by phase 3

- **File**: `shakenfist_client_k3s/__init__.py:424`,
  `shakenfist_client_k3s/__init__.py:478`
- **Action**: consider
- **Claim**: `@k3s.command(...)` already registers the command on the
  group, so the trailing `k3s.add_command(...)` line is a no-op, and
  the two commands phase 3 added each copied one.
- **Evidence**: `click.Group.command()`'s decorator calls
  `self.add_command(cmd)` itself. Verified empirically against the
  installed click 8.1.8: after the decorator alone,
  `g.commands == ['x']`; `add_command` changes nothing. Two further
  pieces of in-tree evidence agree. `getconfig`
  (`__init__.py:295-306`) has *no* `add_command` line and still
  appears in the group listing, and `tests/cli_contract/group.txt`
  lists each of the twelve commands exactly once -- a second
  registration under the same name is a dict assignment, so it
  cannot even produce a duplicate. `git blame` puts nine of the
  eleven lines before this plan (`e8ecd31` 2024-08-17, `0c344ee6`
  2024-09-03, `f345fed7` 2026-08-07) and the two named above in
  phase 3: `8ba16a09` for `health`, `a206fc96` for `remove-worker`,
  both 2026-09-25, both inside `d51cf59`.
- **Proposed change**: delete the two lines phase 3 added. If the
  house prefers one rule over a half-cleaned file, delete all eleven
  and leave the decorator as the single registration mechanism;
  `group.txt` is the regression test, and it will not move. The nine
  older lines are outside this plan's scope, so deleting them is an
  invitation rather than a finding.

### F2: five entry points duplicate `get_progress()`'s body

- **File**: `shakenfist_client_k3s/cluster.py:1475`, `:2162`,
  `:2261`, `:2491`, `:2513` (at `eb248bd`: `:1319`, `:1958`,
  `:2057`, `:2287`, `:2309`)
- **Action**: consider
- **Claim**: the three-line
  `progress.Progress(total_phases=..., verbose=self.reporter.verbose,
  stream=self.reporter)` plus `self.progress = p` idiom is repeated
  verbatim at five call sites and is identical to
  `Cluster.get_progress()`'s own body apart from the laziness, so the
  checklist's "reports through `Cluster.get_progress()`" is true of
  eleven call sites and bypassed at the five that matter most.
- **Evidence**: `get_progress()` at `cluster.py:434-475` builds
  exactly that object at `:471-473`. The five sites are `create()`
  (`:1475-1478`), `expand_workers()` (`:2162-2164`),
  `remove_worker()` (`:2261-2264`), `expand_addresses()`
  (`:2491-2493`) and `update_os()` (`:2513-2515`); each follows the
  construction with `self.progress = p`. The design is deliberate and
  documented -- `get_progress()`'s docstring explains the two-tier
  arrangement, why the lazy default is `total_phases=1`, and why a
  caller with a known phase count must assign its own -- so this is a
  duplication finding, not a correctness one. Confirmed present at
  `eb248bd`, so it is phase 1 and phase 3's code rather than the
  out-of-scope node-customisation work.
- **Proposed change**: add `Cluster.start_progress(total_phases)`
  which builds the `Progress`, assigns `self.progress` and returns
  it, and have `get_progress()` call it when `self.progress` is
  unset. The five sites become `p = self.start_progress(n)`, the
  construction exists once, and `get_progress()`'s docstring can
  point at the new method instead of describing what the five callers
  do by hand. Expect 6b's code-quality lens to report the same
  duplication from the other direction; de-duplicate in triage.

### F3: `show --namespace` help says "alter clusters" for a read-only verb

- **File**: `shakenfist_client_k3s/__init__.py:312`
- **Action**: none
- **Claim**: the help text reads "If you are an admin, you can alter
  clusters in a different namespace", but `show` alters nothing.
- **Evidence**: the string is copied from `delete`,
  `expand-workers`, `expand-addresses` and `update-os`, which do
  alter. `git blame` dates it to `e8ecd31`, 2024-08-17 -- the initial
  commit -- and no scope merge touched it; the contract file
  `tests/cli_contract/show.txt` has carried the wording unchanged
  throughout. The two verbs phases 1-5 added both got accurate
  wording: `health` says "report on a cluster in a different
  namespace" and `remove-worker` is genuinely altering.
- **Proposed change**: nothing, for this phase. If someone is editing
  the file anyway, `show` and `getconfig` want "inspect" rather than
  "alter"; it is a one-line help string plus the matching line in the
  contract fixture. Pre-existing, so it is an observation rather than
  a defect in this work.

### F4: the socket seen at import is urllib3's, by a second route

- **File**: `docs/plans/audit/verification.md:107-133`
- **Action**: none
- **Claim**: `verification.md` attributes the one network-family
  syscall pair at import to `primitives.py` importing `requests` at
  module scope. The call is urllib3's, and it would happen even if
  `primitives.py` imported nothing.
- **Evidence**: instrumenting `socket.socket` with a stack dump
  during `import shakenfist_client_k3s` puts the construction at
  `urllib3/util/connection.py:137`, `HAS_IPV6 = _has_ipv6("::1")`,
  reached through
  `__init__.py:2 -> shakenfist_client.apiclient:9 -> requests ->
  urllib3`. So `shakenfist_client.apiclient` pulls `requests` in
  before `primitives` is ever reached, and the probe is unavoidable
  for any consumer of the Shaken Fist client. The same run confirms
  zero `getaddrinfo`, zero `create_connection`, zero non-source file
  opens and zero directory scans, which matches 6a's strace: a
  loopback bind on port 0, never connected.
- **Proposed change**: nothing in the code. If `verification.md` is
  being edited for another reason, the attribution sentence could
  name `shakenfist_client.apiclient` instead of `primitives.py`. The
  invariant and its verdict are unchanged; this is recorded so a
  future reader does not try to "fix" `primitives.py` to remove a
  syscall that is not its fault.

### F5: `yaml` is the only deferrable module-scope import

- **File**: `shakenfist_client_k3s/cluster.py:35`
- **Action**: none
- **Claim**: `import yaml` costs 7.5ms of a 70.6ms plugin import and
  is used only in `Cluster`'s kubeconfig handling, so it is the one
  module-scope import a future change could move into a function.
- **Evidence**: `python3 -X importtime -c 'import
  shakenfist_client_k3s'` attributes 70586us total: 44931us to
  `shakenfist_client.apiclient` (41413us of it `requests`), 14456us
  to `click`, and 8629us to `shakenfist_client_k3s.cluster` of which
  7464us is `yaml`. Everything else this package imports (`copy`,
  `json`, `os`, `re`, `shlex`, `shutil`, `subprocess`, `tempfile`,
  `time`, `packaging.version`) is either already loaded or
  sub-millisecond, and `requests` in `primitives.py` is free because
  `apiclient` has already paid for it.
- **Proposed change**: nothing. 0.14s with no outbound network
  satisfies "cheap and reliable" by a wide margin, and moving an
  import into a function body to save 7ms trades a measurable clarity
  cost for an unmeasurable startup one. Recorded so that if startup
  ever does become a problem, the next reader knows where the only
  remaining 10% is.

### F6: the `importlib-metadata` marker is `< 3.9`, not `< 3.8`

- **File**: `pyproject.toml:63`
- **Action**: none
- **Claim**: `"importlib-metadata; python_version < '3.9'"` installs
  the backport on 3.8, where `importlib.metadata` is already in the
  standard library.
- **Evidence**: `importlib.metadata` was added in 3.8. The import at
  `cluster.py:37-40` tries the stdlib first and falls back only on
  `ImportError`, so on 3.8 the stdlib wins and the backport is an
  installed-but-unused dependency rather than a behaviour change. The
  conservative marker is also defensible on its own terms: the 3.8
  and 3.9 stdlib versions of this module had a moving API, and
  declaring the backport one version wider costs an install and
  nothing else.
- **Proposed change**: nothing. If the floor is ever revisited this
  marker moves with it, which is #82's territory.

### F7: the Ansible module imports `click` transitively and does not use it

- **File**: `collection/plugins/modules/sf_k3s_cluster.py:51-52`
- **Action**: none
- **Claim**: `from shakenfist_client_k3s import client as sf_client`
  executes `shakenfist_client_k3s/__init__.py`, which imports `click`
  and builds the twelve-command group, so a module that never touches
  Click pays ~14ms and a dependency for it.
- **Evidence**: the package `__init__.py` is the plugin entry point
  and necessarily imports `click` at `:1`; importing any submodule
  runs it. The cost is real but irrelevant in context: `click` is
  already a hard dependency of `shakenfist_client_k3s`, which
  `collection/requirements.txt` installs, and an Ansible module
  process that is about to spend tens of minutes building a cluster
  does not notice 14ms.
- **Proposed change**: nothing. Avoiding it would mean moving the
  Click group out of the package `__init__.py`, which changes the
  entry point every installed copy of the plugin registers under --
  far more disruptive than the cost it saves.

### F8: `orchestrated_k3s_clusters` misses the documented prefix

- **File**: `shakenfist_client_k3s/primitives.py:28`
- **Action**: none
- **Claim**: `PUSH-AUDIT.md` names `orchestrated_k3s_cluster_*` as
  the namespace metadata keys cluster state lives under, and the
  cluster-list key is `orchestrated_k3s_clusters` -- the same stem
  with a plural `s` rather than the `_` separator.
- **Evidence**: `git log -S"orchestrated_k3s_clusters"` puts the
  literal in `e8ecd31`, the initial commit. Phase 1 (`17df96c6`)
  moved the constant into `primitives.py` so that `cluster.py` and
  the CLI could both reach it without importing each other; it did
  not choose the string. The other three keys
  (`orchestrated_k3s_cluster_<name>`, `..._k3s_version_cache`,
  `..._longhorn_version_cache`) all match the prefix.
- **Proposed change**: nothing, and specifically not a rename. This
  is a wire-format value already written into the namespace metadata
  of every deployed cluster; changing it would orphan every existing
  cluster list. The right fix, if one is wanted, is to widen the
  checklist's wording to `orchestrated_k3s_cluster*`, which is a
  `PUSH-AUDIT.md` edit and not a code change.
