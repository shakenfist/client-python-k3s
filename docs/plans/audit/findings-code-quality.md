# Audit findings: code quality

Lens: the *Code quality* section of `PUSH-AUDIT.md`, including the
`comment-proportion v1` shared block. Scope: the 58 non-plan files in
`docs/plans/audit/scope-files.txt`; everything under `docs/plans/` is
left to the documentation lens. Read-only: nothing outside this file
was changed.

## Summary

| Action | Count | What it means here |
|---|---|---|
| `fix` | 0 | Nothing found that gates the phase. |
| `document` | 3 | Three comment or docstring changes. |
| `consider` | 7 | Real duplication in code these phases added. |
| `none` | 8 | Informational, or pre-existing before phase 1. |
| **Total** | **18** | |

The headline is that the two mechanical questions -- 120-character
wrapping and quoting -- come back completely clean, and the judgement
question does not. There is one duplication pattern that exists
*because* phases 1 and 3 both landed, in exactly the place the brief
predicted (F1), and five more cases of a helper that was never
extracted (F2-F5, F13). None of them is a latent break, which is why
none is a `fix`: the duplicates are all correct copies of correct code.

## Checklist questions answered

### Did the changes introduce any significant amount of duplicated code? Are there any missed opportunities for code reuse or refactoring?

Yes, and the most significant case is the cross-phase one the shared
block warns about. `exceptions.py` carries **four verbatim copies** of
the same nine-line `__init__`/`__str__` pair: the pattern was
introduced by phase 1 (`3e926a6`, in merge `7fb29e5`/#55) for
`ReleaseLookupError` and `KubeconfigError`, and phase 3 (`33b9f24` and
`4a660bd`, both in merge `d51cf59`/#75) copied it into `ManifestError`
and `ClusterInterruptedError` rather than lifting a base class out of
it. That is F1, and it is the clearest instance in the diff of a
duplicate that only exists because two phases landed separately.

Five further cases, each a missed extraction rather than a copy-paste
of a whole function: the `progress.Progress(...)` construction in
`cluster.py` (F2, six sites, three added by phase 3),
`await_fetch()`'s poll loop against the one phase 3 split out of
`reap_execute()` (F3), eleven no-op `k3s.add_command()` calls in
`__init__.py` (F4, three added by phase 3), the private
`_describe_agent_op()` reached across module boundaries (F5), and
`FakeTty` defined twice in the test tree (F13).

I also looked for and did **not** find: a wait loop added twice (the
four polling loops in `cluster.py` differ in what they poll and what
they report, so a single shared loop would need three parameters and
would not read better); a kubectl invocation built by two different
code paths (they are all distinct commands, though see F18 on the
repeated literal path); a metadata read-modify-write open-coded
outside `get_metadata()`/`set_metadata()`; and any logic duplicated
between `cluster.py` and `primitives.py`, which is the pair the brief
said to look hardest at. Phase 1's split left no function in both
modules. What it did leave is F5, a single helper on the wrong side of
the line.

The duplication inside `primitives.py` itself -- the two release
lookups share a thirteen-line cache-loading preamble, an identical
expiry test and an identical commit tail -- is real but predates phase
1 (F12), which rewrote the plumbing on each of those lines without
changing the shape.

### Should any new code be extracted into a shared helper in `primitives.py` or `progress.py`? Look for logic that a second command or wait loop would likely need.

Yes, twice, and both are already written out five or six times over,
so the "would likely need" is not hypothetical:

- A `Cluster` method that begins an operation's progress reporting.
  Five command bodies construct `progress.Progress(total_phases=N,
  verbose=self.reporter.verbose, stream=self.reporter)` and assign it
  to `self.progress`, and `get_progress()` constructs a sixth. Every
  new verb will need a seventh. This is F2, and the helper belongs on
  `Cluster` rather than in `progress.py`, because what is repeated is
  the wiring of the reporter into the stream, which is Cluster's
  knowledge.
- `_describe_agent_op()` should move from `primitives.py` to
  `progress.py`. It is a display helper that truncates a command to
  fit one status line, which is what `format_elapsed()` and
  `count_str()` next door in `progress.py` are; `cluster.py` already
  imports `progress` for both of those; its tests are already in
  `test_progress.py`; and moving it removes the only place in this
  package where one module reaches into another's underscore-prefixed
  name. This is F5.

Nothing new wants a helper in `primitives.py`: the module is
deliberately shrinking, and its own docstring says so.

### Are there any TODO comments we should address as part of this work?

No. `grep -rniE '\b(todo|fixme|xxx|hack)\b'` across every `.py`,
`.md`, `.yml`, `.sh`, `.toml`, `.ini` and `.txt` file in the tree
returns exactly one hit, and it is the question itself in
`PUSH-AUDIT.md:12`.

Two comments are TODO-shaped without carrying the word -- "We really
should do a pre-fetch on the disk image" and "I'd prefer to wait for
these as one thing" at `cluster.py:1599-1604`. Both date to the
initial commit (`e8ecd31`), and both name work this plan did not take
on, so they are not findings against it. Each describes a Shaken Fist
server capability that does not exist yet, which is the kind of note
correctly left in the code rather than filed.

### Please ensure all source code is wrapped at 120 characters.

Clean for Python, and clean in substance everywhere else. I re-ran the
check independently of step 6a rather than trusting it: across all 23
`.py` files in the scope there is **not one line over 120
characters**. `tools/flake8wrap.sh` hard-codes `flake8
--max-line-length=120`, so this is the project's own figure.

Five non-Python scope files have longer lines, and none of them is a
finding (F16): six in `docs/library-api.md` and three in
`docs/usage.md` are Markdown table rows, which cannot be wrapped
without breaking the table; one in `README.md:50` is a single absolute
URL in a link; one in `.github/workflows/release.yml:123` is a release
download URL; and two in `.github/workflows/functional-tests.yml` (276
and 297) are an identical 162-character `jq` one-liner, which is
pre-existing and untouched by these ten merges.

### Use single quotes for strings, double quotes for docstrings, and never triple single quotes.

Clean. `grep -rn "'''"` across the tree returns nothing, so there are
no triple-single-quoted strings anywhere. Every docstring in the eight
non-test source files uses `"""`. Every double-quoted string literal I
found is double-quoted because it contains an apostrophe or an
embedded single-quoted fragment -- `cluster.py:664`, `691-692`, `884`,
`926`; `exceptions.py:211-213`, `221-222`, `653`, `743`;
`__init__.py:175`, `186`, `190-194`, `382` -- which is the correct
reason to switch. The `r"""` on the Ansible module's `DOCUMENTATION`,
`EXAMPLES` and `RETURN` blocks is required by `validate-modules` and
is not a docstring.

### Comment proportion (shared block)

`cluster.py` is 53% comment and docstring lines (624 comment, 722
docstring, 2,519 total) and `exceptions.py` is 48% (354 docstring
lines of 763). Both are over any ratio one might pick, and per the
block that is a reason to look, not a verdict. I applied the block's
own test to each candidate instead.

Candidates by the block's stated threshold -- a prose block over
roughly fifteen lines on a body under ten -- are six functions, all in
`cluster.py`:

| Function | Line | Prose | Code |
|---|---|---|---|
| `_node_size()` | 476 | 25 | 1 |
| `get_progress()` | 434 | 35 | 5 |
| `validate_node_sizes()` | 280 | 34 | 6 |
| `_interrupted_state()` | 389 | 23 | 4 |
| `await_execute()` | 719 | 21 | 7 |
| `health()` | 1807 | 116 | 40 |

My judgement is that four of the six are justified and two are not
quite, which is F7. `validate_node_sizes()` earns its length: the
`isinstance(True, int)` trap and the deliberate refusal to invent a
memory floor are both things the code cannot say.
`_interrupted_state()` earns it: why a missing `state` key answers
`'unknown'` rather than None is the whole correctness argument.
`await_execute()` earns it: the `timeout` parameter's contract, and
why only `health()` may pass one. `health()`'s 101-line docstring is a
public API contract including the report schema, which the block
explicitly protects -- though see F8 for the part of it that is a
third copy of prose in `docs/`.

The two that are not quite justified are `get_progress()`, which
spends seventeen lines enumerating the five methods that call it (a
reverse call index that `grep` answers and that will rot), and
`_node_size()`, whose "A copy, so that a caller which adjusts what it
is handed changes neither..." restates what `dict(a, **b)` visibly
does. In both cases the fix is to cut the restatement and keep the
why, which is what the block asks for.

The module-level comment at `cluster.py:70-107` -- 35 lines attached
to five constant assignments -- is the strongest case *for* a long
comment in the diff, and I want to say so explicitly rather than list
it as a candidate and move on: it records why a wait tests "can this
still progress" instead of "is it complete or error", which is a
hard-won explanation of a loop that used to spin forever on `expired`.
The block names exactly that as justifying length.

## Findings

### F1: Four verbatim copies of the reasoned-exception constructor

- **File**: `shakenfist_client_k3s/exceptions.py:195`, `:380`, `:563`,
  `:726`
- **Action**: consider
- **Claim**: Four exception classes carry byte-identical nine-line
  `__init__(self, reason, message, **fields)` and `__str__`
  implementations, and the duplication exists only because phase 1 and
  phase 3 landed separately.
- **Evidence**: `ClusterInterruptedError.__init__` (195-202),
  `ManifestError.__init__` (380-387), `ReleaseLookupError.__init__`
  (563-570) and `KubeconfigError.__init__` (726-733) are the same five
  statements in the same order, differing only in the `super()` class
  name; all four `__str__` methods are `return self.message`; all four
  classes declare a `FIELDS` tuple and three of them point at
  `ReleaseLookupError.FIELDS` with the comment "See
  ``ReleaseLookupError.FIELDS`` for why this is not left implicit". A
  mechanical four-line-window scan over the eight source files reports
  this block at four copies, the highest count in the diff.
  Provenance: `git log -S"class ReleaseLookupError"` gives `3e926a6`
  ("Capture the CLI contract, and add the exceptions."), in merge
  `7fb29e5` (#55, phase 1); `git log -S"class ManifestError"` gives
  `33b9f24` and `git log -S"class ClusterInterruptedError"` gives
  `4a660bd`, both in merge `d51cf59` (#75, phase 3).
- **Proposed change**: Lift a
  `_ReasonedK3sException(K3sClusterException)` base holding
  `FIELDS = ()`, the `__init__` and the `__str__`, and make the four
  classes subclass it and declare only their own `FIELDS` and
  classmethods. Around 30 lines go, and the three cross-references to
  `ReleaseLookupError.FIELDS` become one comment on the base. Rendered
  messages do not change, so the `cli_contract` fixtures and
  `test_exceptions.py` should pass untouched -- which is also the check
  that the refactor is faithful.

### F2: Six open-coded Progress constructions

- **File**: `shakenfist_client_k3s/cluster.py:471`, `:1475`, `:2162`,
  `:2261`, `:2491`, `:2513`
- **Action**: consider
- **Claim**: The same three-argument `progress.Progress` construction
  is written out six times, and three of those were added by phase 3,
  so the next verb will write a seventh.
- **Evidence**: `get_progress()` builds one at 471-473; `create()` at
  1475-1478, `expand_workers()` at 2162-2164, `remove_worker()` at
  2261-2264, `expand_addresses()` at 2491-2493 and `update_os()` at
  2513-2515 each build one with `verbose=self.reporter.verbose,
  stream=self.reporter` and then assign `self.progress = p`. The five
  command-body sites are identical apart from the `total_phases`
  expression. `git log -S'self.progress = p'` attributes them to
  `a0fc378` and `17df96c` (phase 1) and `a206fc9` ("Add
  remove-worker.", merge `d51cf59`, phase 3); the `expand_addresses()`
  and `update_os()` sites are phase 3 verbs too. This is the question
  `PUSH-AUDIT.md` asks about logic "a second command or wait loop would
  likely need", answered five times over.
- **Proposed change**: Add a `Cluster._begin_progress(total_phases)`
  that constructs, assigns to `self.progress` and returns it, and have
  the five command bodies call `p = self._begin_progress(N)`. It cannot
  simply be `get_progress()`: that one only builds when
  `self.progress` is falsy, whereas these five deliberately replace
  whatever is there, so the two need to stay distinct methods.

### F3: `await_fetch()` duplicates the poll loop phase 3 extracted

- **File**: `shakenfist_client_k3s/cluster.py:702`
- **Action**: consider
- **Claim**: `await_fetch()` open-codes the pending-state poll that
  `await_execute()` exists to be, and repeats the
  `state != 'complete'` check from `reap_execute()`, so phase 3's
  extraction left one of its three callers behind.
- **Evidence**: `await_fetch()` (702-717) runs `while aop['state'] in
  AGENT_OP_PENDING_STATES: ... time.sleep(1); aop =
  self.client.get_agent_operation(aop['uuid'])`, which is
  `await_execute()`'s loop at 742-747 with a `p.update()` added and the
  deadline test removed. Lines 710-711 then repeat 755-756 in
  `reap_execute()` exactly. `await_execute()`'s own docstring (720-739)
  says it was "Split out of reap_execute()", and that split is phase 3
  work; `await_fetch()` predates it and was not revisited.
- **Proposed change**: Give the poll one implementation --
  `await_execute(aop, timeout=None, progress_key=None)`, emitting
  `p.update(progress_key, ...)` when a key is given -- and reduce
  `await_fetch()` to calling it, checking `complete`, and fetching the
  blob. Worth noting while doing it: `await_execute()` currently
  reports no progress at all during its wait, which is defensible only
  because `reap_execute()` runs after `await_idle()` has already
  waited.

### F4: Eleven no-op `k3s.add_command()` calls

- **File**: `shakenfist_client_k3s/__init__.py:150`, `:243`, `:269`,
  `:292`, `:326`, `:424`, `:443`, `:458`, `:478`, `:494`, `:507`
- **Action**: consider
- **Claim**: Eleven `k3s.add_command(...)` statements re-register
  commands the `@k3s.command(...)` decorator has already registered,
  and the one command that omits the call proves they do nothing.
- **Evidence**: There are twelve `@k3s.command` decorators and eleven
  `k3s.add_command` calls. `k3s_getconfig` (295-306) has no
  `add_command`. `python3 -c "import shakenfist_client_k3s as m;
  print(sorted(m.k3s.commands))"` lists all twelve including
  `getconfig`, so the decorator is what registers and the eleven calls
  are dead. `git log -S'k3s.add_command(k3s_health)'` attributes that
  one to `8ba16a0` ("Add a health verb.", merge `d51cf59`, phase 3);
  the `remove-worker` and `expand-addresses` lines arrived with the
  same verbs, so phase 3 copied a pre-existing dead pattern three more
  times.
- **Proposed change**: Delete all eleven lines. Eleven one-line
  deletions with a mechanical check -- the `commands` dict above must
  still hold twelve names, and `tests/cli_contract/group.txt` pins the
  rendered command list.

### F5: A module-private helper reached from another module

- **File**: `shakenfist_client_k3s/primitives.py:184`
- **Action**: consider
- **Claim**: `_describe_agent_op()` is named private but called three
  times from `cluster.py` and six times from `test_progress.py`, and
  it is a display helper sitting in the module that is not about
  display.
- **Evidence**: Defined at `primitives.py:184` with a leading
  underscore; called at `cluster.py:558`, `:661` and `:893`, and
  referenced in `exceptions.py:613`'s docstring. Its tests are in
  `tests/test_progress.py:381-415`, not `test_primitives.py`. Its two
  nearest relatives, `format_elapsed()` and `count_str()`, are public
  in `progress.py`, which `cluster.py` already imports for both. Its
  body is entirely about fitting a command onto one status line
  (`max_len` truncation at 207-208, first-line-only at 204-205), which
  is `progress.py`'s subject. `primitives.py`'s own docstring
  describes what is left there as "namespace scoped lookups and
  stateless helpers", so it is not wrong to be there -- only less
  right than the alternative.
- **Proposed change**: Move it to `progress.py` as
  `describe_agent_op()`, update the three `cluster.py` call sites, the
  `exceptions.py` docstring reference, and the six test references
  (which are already in `test_progress.py`, so no test moves). The
  private-name-across-modules problem goes away with the rename.

### F6: A comment citing line numbers in another file, already stale

- **File**: `collection/plugins/modules/sf_k3s_cluster.py:708`
- **Action**: document
- **Claim**: The comment cites `cluster.py` lines `:761`, `:2203` and
  `:2250` as the sites that catch `apiclient.APIException`, and all
  three were already wrong before this audit opened.
- **Evidence**: The comment reads "it catches
  `apiclient.APIException` at particular call sites (:761, :2203,
  :2250)". At `eb248bd`, the phase 5 merge that added the comment,
  those three lines were `except apiclient.APIException as e:`,
  `except apiclient.APIException:` and `except
  (exceptions.K3sClusterException, apiclient.APIException) as e:` --
  so it was accurate when written. On `develop` today
  `cluster.py:761` is an argument to a `CommandFailedError`
  constructor, `:2203` is a line of prose inside `remove_worker()`'s
  docstring, and `:2250` is a comment. The actual catch sites are now
  860, 2022, 2033, 2056, 2312, 2369, 2407 and 2454. One merge window
  was enough to rot it.
- **Proposed change**: Replace the three line numbers with the method
  names that catch -- `_probe_k3s_api()`, `delete()`,
  `remove_worker()` and `_uncordon()` -- which stay true across edits.
  It is a comment, so this is a `document` under decision 7's rule.

### F7: Two docstrings whose length is mostly restatement

- **File**: `shakenfist_client_k3s/cluster.py:434` (`get_progress()`),
  `shakenfist_client_k3s/cluster.py:476` (`_node_size()`)
- **Action**: document
- **Claim**: These two carry the diff's worst prose-to-code ratios,
  and unlike the other four candidates the surplus is a reverse call
  index and a restatement of what the code visibly does.
- **Evidence**: `get_progress()` is 35 docstring lines over five lines
  of code. Lines 443-459 name the five methods that call it
  (`create_and_await_instances()`, `install_extra_control_plane()`,
  `install_workers()`, `setup_metallb()`, `setup_longhorn()`) and then
  name the one that does not. That list is what `grep` answers.
  `_node_size()` is 25 docstring lines over the single statement
  `return dict(DEFAULT_NODE_SIZE, **md.get('node_sizes',
  {}).get(node_type, {}))`; lines 499-500, "A copy, so that a caller
  which adjusts what it is handed changes neither the cached metadata
  nor the module's default", restates the visible semantics of
  `dict(a, **b)`.
- **Proposed change**: In `get_progress()`, replace the caller
  enumeration with the one sentence it supports -- that 1 is the true
  count for the methods which open a single phase, not a placeholder --
  and keep the shared-Progress caveat at 461-468 intact, because that
  one is a genuine trap. In `_node_size()`, drop the copy sentence and
  keep the two fallback paragraphs, which are the real argument. Both
  are docstrings, so `document`.

### F8: The agent-operation-state reasoning exists in three places

- **File**: `shakenfist_client_k3s/cluster.py:74-102`,
  `shakenfist_client_k3s/exceptions.py:619-625`,
  `docs/library-api.md:285-308`
- **Action**: document
- **Claim**: The explanation of why `expired` is distinct from `error`
  is written out three times, once almost verbatim, which is the "one
  canonical home per fact" rule the shared blocks state.
- **Evidence**: `exceptions.py:624-625` reads '"run it again with a
  longer deadline" and "the command is broken" are different next
  steps.' `docs/library-api.md:288-290` reads '"run it again with a
  longer deadline" and "the command is broken" are different next
  steps.' The surrounding paragraphs make the same argument about the
  600-second deadline and about why waits enumerate states; the
  `cluster.py:74-102` comment makes it a third time at greater length,
  and `docs/library-api.md:295-302` re-narrates `await_idle()`'s
  unknown-state behaviour, which `cluster.py:629-654` also explains.
- **Proposed change**: Keep `cluster.py:74-102` as the canonical copy
  -- it sits on the constants it is about, and the block protects a
  hard-won bug explanation -- and reduce `exceptions.py:619-625` to the
  one fact a reader of that class needs (`state` is rendered only when
  it is not `error`) plus a pointer to the constants. Whether
  `docs/library-api.md` should also shorten is the documentation
  lens's call, not mine; I am recording the overlap so it is not found
  twice and acted on inconsistently. These are comments and prose, so
  `document`.

### F9: `delete()` clears two metadata keys that do not exist

- **File**: `shakenfist_client_k3s/cluster.py:2041`
- **Action**: consider
- **Claim**: `delete()` sets `api_floating_address` and
  `api_inner_address` to None, but `create()` writes
  `api_address_floating` and `api_address_inner`, so the delete invents
  two junk keys and never clears the two real ones.
- **Evidence**: `create()` writes `md['api_address_inner']` at 1626 and
  `md['api_address_floating']` at 1627; `install_k3s_component()`
  reads `md['api_address_inner']` at 1145 and
  `install_control_plane()` reads `md['api_address_floating']` at
  1055. `delete()` at 2041-2042 sets `md['api_floating_address']` and
  `md['api_inner_address']` -- the words transposed. Those two
  spellings appear nowhere else in the package, tests included. The
  effect is cosmetic in practice, because the metadata document is
  deleted a few lines later at 2106, but the intermediate
  `set_metadata(md)` at 2046 writes a document carrying four address
  keys instead of two, and the two that were meant to be cleared
  survive it.
- **Proposed change**: Correct the two key names to
  `api_address_floating` and `api_address_inner`. Stated plainly: this
  is pre-existing, not a defect in this work. `git log -S` puts both
  spellings in `e8ecd31`, the initial commit, and phase 1's `17df96c`
  moved them onto `Cluster` verbatim. It is `consider` rather than
  `none` only because it is a two-word fix in a file phase 1 rewrote.

### F10: A UUID field is cleared to an empty list

- **File**: `shakenfist_client_k3s/cluster.py:2067`
- **Action**: none
- **Claim**: `md['node_network'] = []` assigns a list to a field that
  holds a network UUID string everywhere else.
- **Evidence**: `create()` writes `md['node_network'] =
  node_network['uuid']` at 1556 and `create_instance()` reads it as a
  `network_uuid` at 1523. `delete()` sets it to `[]` at 2067 rather
  than to None. Pre-existing: `git log -S"md['node_network'] = []"`
  gives `e8ecd31`, moved verbatim by phase 1's `17df96c`.
- **Proposed change**: None taken. This is code that was already there
  before phase 1, and the document is deleted immediately afterwards,
  so nothing reads the value. Recorded so the next reader of
  `delete()` does not have to work out whether it matters.

### F11: Two redundant metadata writes

- **File**: `shakenfist_client_k3s/cluster.py:793`, `:1163`
- **Action**: none
- **Claim**: `create_and_await_instances()` and
  `install_k3s_component()` each end with a `set_metadata(md)` on a
  `md` nothing has modified since the previous write, costing an API
  call and widening the lost-update window the metadata cache exists
  to narrow.
- **Evidence**: In `create_and_await_instances()` the loop writes at
  787 after each append; 790-792 then boot and OS-update the nodes
  without touching `md`, and 793 writes it again. In
  `install_k3s_component()`, 1138 reads `md`, 1145 derives a local
  `join_address` from it, 1147 executes commands, and 1163 writes `md`
  back unchanged. The cache comment at 345-351 explains that each
  read-write is "an opportunity to lose someone else's update", which
  is what makes a write for no reason worth naming. Pre-existing: both
  appear in `e8ecd31` (`set_cluster_metadata(ctx, md)` at the tail of
  each function), and phase 1 moved them unchanged.
- **Proposed change**: None taken, for the same reason as F10. If
  somebody does remove them, the check is that `test_cluster.py`'s
  action-log clients count metadata writes in several places.

### F12: The two release lookups share a duplicated cache envelope

- **File**: `shakenfist_client_k3s/primitives.py:46`, `:113`
- **Action**: none
- **Claim**: `get_k3s_release()` and `get_longhorn_release()` repeat
  the same thirteen-line cache-loading preamble, the same 24-hour
  expiry test and the same commit tail, differing only in the key they
  validate.
- **Evidence**: Lines 48-62 and 114-128 are the same statements in the
  same order -- the `force_cache_update` branch, the namespace
  metadata read, the `isinstance(...) or <key> not in version_cache`
  clobber, the `updated` extraction and the debug line -- with
  `'releases'` against `'latest'` as the only difference. `if
  time.time() - updated > 24 * 3600:` appears at 64 and 130;
  `version_cache['updated'] = time.time()` followed by
  `set_namespace_metadata_item` appears at 101-103 and 177-179. The
  four-line-window scan reports four separate overlapping copies in
  this region.
- **Proposed change**: None taken, because this is pre-existing
  structure rather than this work. `git show 73bd499` has the
  identical preamble, with `_emit_debug(ctx, ...)` where the reporter
  now goes; phase 1 changed the plumbing on each of those lines and
  left the shape. If it is ever addressed, the extractable part is the
  envelope (load, expiry test, commit) with the fetch passed in as a
  callable -- not the whole function, because the two fetch bodies
  have nothing in common.

### F13: `FakeTty` defined twice while a shared fakes module exists

- **File**: `shakenfist_client_k3s/tests/test_cluster.py:29`,
  `shakenfist_client_k3s/tests/test_progress.py:46`
- **Action**: consider
- **Claim**: The same three-line `FakeTty(io.StringIO)` class is
  defined in two test modules, and phase 1 added the second copy
  rather than putting it in `tests/fakes.py`.
- **Evidence**: Both read `class FakeTty(io.StringIO):` / `def
  isatty(self):` / `return True`, byte for byte. `git log -S'class
  FakeTty'` gives `36a59fd` ("Rework startup progress reporting.",
  before phase 1) for the `test_progress.py` copy and `a0fc378`
  ("Replace the click context with a Cluster object.", merge
  `7fb29e5`, phase 1) for the `test_cluster.py` copy. `tests/fakes.py`
  already exists and already holds `FakeClusterClient`,
  `HealthClient`, `KUBECONFIG` and `not_found()`.
- **Proposed change**: Move it to `tests/fakes.py` and import it in
  both places. Three lines deleted, two imports touched.

### F14: Six identical `--namespace` help strings

- **File**: `shakenfist_client_k3s/__init__.py:311`, `:430`, `:451`,
  `:468`, `:487`, `:500`
- **Action**: none
- **Claim**: Six of the twelve `--namespace` options carry the
  byte-identical help text "If you are an admin, you can alter
  clusters in a different namespace.", so wording drift between
  commands is invisible until somebody diffs the `--help` output.
- **Evidence**: `grep -c "you can alter clusters in a"` reports 6, and
  the four-line-window scan reports the whole decorator block at five
  copies. The other six `--namespace` options have texts tailored to
  their verb (list, create, getconfig, health, and the two version
  queries), which is the right thing and is why this is not simply
  twelve copies.
- **Proposed change**: None taken. A reusable `_namespace_option()`
  decorator would remove the repetition, but stacked per-command
  `@click.option` decorators are the idiomatic Click spelling and the
  help text is the kind of thing a reader expects to find next to the
  command it belongs to. Recorded because the question was asked.

### F15: Built-cluster metadata inlined repeatedly in tests

- **File**: `shakenfist_client_k3s/tests/test_cluster.py` (twelve sites
  including `:500`, `:1115`, `:1801`, `:1888`, `:2073`, `:2263`,
  `:2329`, `:3010`, `:3185`, `:3204`)
- **Action**: none
- **Claim**: The "finished cluster" metadata dictionary is written out
  by hand about twelve times, so a new required metadata key means
  twelve edits.
- **Evidence**: `grep -c "'control_plane_nodes'" test_cluster.py` is
  25, of which roughly twelve are fresh inline literals rather than
  uses of the existing `_interrupted_md()` helper at 658 or the
  `_headers()` helper at 2322. The same file already demonstrates the
  better pattern twice, and `tests/fakes.py` is where a `built_md()`
  would live.
- **Proposed change**: None taken. Under decision 7 this is neither a
  one-liner nor a defect in code these phases added -- it is test
  fixture style -- and the Tests lens owns test adequacy. Recorded
  because duplication was the question and this is where the largest
  volume of it is.

### F16: Long lines outside Python, all unwrappable or pre-existing

- **File**: `docs/library-api.md:241`, `:246`, `:247`, `:249`, `:250`,
  `:252`; `docs/usage.md:31`, `:33`, `:38`; `README.md:50`;
  `.github/workflows/release.yml:123`;
  `.github/workflows/functional-tests.yml:276`, `:297`
- **Action**: none
- **Claim**: Thirteen non-Python lines exceed 120 characters, and none
  of them is a wrapping defect.
- **Evidence**: Nine are Markdown table rows, where a newline would
  break the table. `README.md:50` is 122 characters and is a single
  absolute URL in a link, as the README discipline block requires.
  `release.yml:123` is a 134-character release download URL inside a
  `curl`. `functional-tests.yml:276` and `:297` are the same
  162-character `jq` pipeline in the `can_enqueue` and `can_merge`
  jobs -- which is both over-length and duplicated, but is
  pre-existing: the only hunk any of the ten merges applies to that
  file adds the `check-wheel-build.sh` step at line 177.
- **Proposed change**: None. If the `jq` duplication is ever addressed
  it belongs in a script under `tools/`, and it is not this plan's
  change to make.

### F17: `?page=0` is fetched and is the same page as 1

- **File**: `shakenfist_client_k3s/primitives.py:134`
- **Action**: none
- **Claim**: The Longhorn release scan iterates `range(5)`, so it
  requests `?page=0`, which the GitHub API treats as page 1 -- one
  wasted request per cache refresh, and four distinct pages rather
  than the five the code reads as.
- **Evidence**: `for page in range(5):` at 134 and
  `...releases?page={page}` at 135. GitHub's pagination is 1-based.
  Pre-existing: `git log -S"for page in range(5)"` gives `0c344ee`
  ("Add longhorn as persistent storage, fix HA control planes."), well
  before phase 1.
- **Proposed change**: None taken -- pre-existing, and the duplicate
  page is harmless because the results are accumulated into a dict
  keyed by tag. `range(1, 6)` would be the fix. Recorded here rather
  than filed, since the release-lookup external-API shape is the Tests
  lens's territory.

### F18: The kubeconfig path is an eight-times-repeated literal

- **File**: `shakenfist_client_k3s/cluster.py:843`, `:1250`, `:1252`,
  `:1279`, `:1308`, `:2363`, `:2367`, `:2452`
- **Action**: none
- **Claim**: `/etc/rancher/k3s/k3s.yaml` is written out eight times as
  a literal in a module that defines named constants for every other
  on-node path.
- **Evidence**: `K3S_MANIFEST_DIR` (135), `K3S_MANIFEST_SUFFIXES`
  (136), `K3S_MANIFEST_BASENAME_RE` (149), `K3S_MANIFEST_DELIMITER`
  (159) and `KUBECTL_DRAIN_TIMEOUT` (122) are all named; the
  kubeconfig path is not. Four of the eight sites were added by phase 3
  (`_probe_k3s_api()` at 843, the drain and delete at 2363 and 2367,
  the uncordon at 2452). Three further kubectl invocations omit the
  flag entirely -- `kubectl apply` at 1256, `kubectl create ns` at
  1269 and `kubectl patch storageclass` at 1314 -- which is deliberate
  and is explained at 2340-2348, but means the module is inconsistent
  in a way a constant would make visible.
- **Proposed change**: None taken; this is a judgement call rather
  than a defect, and the phase should not be churning eight command
  strings to extract a constant. If it is ever done, a
  `K3S_KUBECONFIG` constant plus a decision about the three
  bare-kubectl sites is the shape, and `tests/test_cluster.py`'s
  `ShellQuotingTestCase` and `_control_plane_and_metallb_commands()`
  pin the resulting command strings.

## Pre-existing issues rediscovered

Per decision 6 these are comments on existing issues, not new
findings.

- **#96** (`Cluster.create()` does not range-check its counts). Found
  again from the other side: the range check lives in
  `collection/plugins/modules/sf_k3s_cluster.py:617-628`, and the
  comment at 612-616 already explains why it was not pushed down and
  names #96. The code-quality observation to add to that issue is that
  the floors table there (`control_plane_count` >= 1,
  `initial_workers` >= 0, `metal_address_count` >= 0) is a third
  statement of the same rule, after `click.IntRange(min=1)` in
  `__init__.py` and `validate_node_sizes()` in `cluster.py`, so the
  argument for pushing it down is a duplication argument as well as a
  correctness one.
- **#93** (twenty unencoded `open()` calls in `tests/`). Not
  re-counted here; the production `open()` calls in scope all state
  `encoding='utf-8'` (`cluster.py:236`, `:1540`, `:1683`, `:1692`,
  `:1714`), and `tools/build-collection.py` passes an explicit
  encoding to `read_text`/`write_text`.
- **#94** (`validate-modules` versus flake8). The one `from __future__
  import annotations` in the scope is at `sf_k3s_cluster.py:23`, where
  #94 says it has to be.
- **#82**, **#89**, **#91**: nothing in this lens touches them.
