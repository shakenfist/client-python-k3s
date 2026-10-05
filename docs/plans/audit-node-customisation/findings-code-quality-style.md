# Findings: code quality and style (step 4b)

Returned as text by the sub-agent and saved here by the management
session, because the harness does not let sub-agents write report files.
The management session spot-checked CQ-1, CQ-4 and CQ-9 against the
worktree before saving.

Lens 4b of `PLAN-node-customisation-phase-04-push-audit.md`. It covers
`PUSH-AUDIT.md`'s *Code quality* and *Style conformance* sections,
including the `comment-proportion v1` and `python-version-discipline v1`
blocks and their "In this project" notes.

* **Files read.** The Python and shell files in `scope-files.txt`:
  `__init__.py`, `cluster.py`, `exceptions.py`, the seven test and fake
  files, `cli_contract/create.txt` and `tools/ci_deploy_test.sh`.
  `docs/` was skipped, except to check what a code comment duplicates.
* **What this plan added.** Located with `git diff <m>^1 <m>` for
  `ddb1f3b`, `b791364` and `51ff6f4`, plus `git blame` against those
  merges' first parents.
* **Revision judged.** The worktree at `7f4efb1`. `git diff --stat
  3e84907 HEAD -- . ':!docs/plans'` is empty, so every line number below
  is also the line at `3e84907`.
* **Not re-raised.** What `2aba0e6` already did: the
  `_ReasonedK3sException` base, `heredoc()` in `_k3s_config_commands()`,
  and the `docs/usage.md` phase reference. CQ-3 concerns a comment that
  commit left behind, not the change itself. Typing (#100) is out of
  scope.
* **A naming note.** The brief's `PLUGIN_OWNED_*` constants are named
  `K3S_SERVER_OWNED_KEYS` and `K3S_AGENT_OWNED_KEYS` in the code
  (`cluster.py:207`, `:243`). This file uses the real names.
* **Checks reused, not re-run.** The test and lint results come from
  `verification.md`: 565 tests passed, and flake8 at 120 columns was clean
  over the scope's `.py` files.

## Findings

### CQ-1: `show()`'s docstring still says `node_sizes` is the only filled key

* **Where.** `shakenfist_client_k3s/cluster.py:2373`, and
  `shakenfist_client_k3s/tests/test_cluster.py:1083-1084`.
* **Rating.** `document`.

Phase 1 wrote that `node_sizes` is the one key `show()` may report
without it being stored. Phase 2 then made `show()` fill `server_config`
and `agent_config` the same way, and added a paragraph saying so, but
left the "one key" sentence in place. The docstring now contradicts
itself. The phase 1 test class docstring makes the same stale claim.

```
2373        ``node_sizes`` is the one key this reports which may not be stored.
...
2394        ``server_config`` and ``agent_config`` are filled the same way, with
2395        ``{}`` when absent, and for the same reason: ...
```

```
2426        for key in ('server_config', 'agent_config'):
2427            if key not in md:
2428                fills[key] = {}
```

`test_cluster.py:1083`: "That is the one place show reports something
other than what is stored".

**Fix.** Say "three keys", or "keys added after a cluster could already
exist", in both places.

### CQ-2: "decision 6 of the phase 2 plan" names the wrong plan by this module's own convention

* **Where.** `shakenfist_client_k3s/cluster.py:646`, in
  `check_k3s_release()`.
* **Rating.** `document`.

```
645    through, because a version which cannot be read is not one the plugin
646    can promise drop-in configuration on (decision 6 of the phase 2 plan).
```

`docs/plans/` holds two phase 2 plans:
`PLAN-library-api-and-collection-phase-02-client-construction.md` and
`PLAN-node-customisation-phase-02-k3s-config.md`. In this module, a bare
"the phase N plan" already means the library API plan:

* `:359-360`, "decision 8 of the phase 3 plan";
* `:1878`, "Decision 6 of the phase 3 plan";
* `:1903-1904`;
* `:2252`.

Read that way, line 646 points at the client-construction plan, which
has nothing to say about release floors. The decision it means is
decision 6 of the node customisation phase 2 plan ("k3s releases older
than v1.21.1+k3s1 are refused at create"). This is the only one of the
plan's plan references that is ambiguous. The others give the path in
full: `cluster.py:199-200`, `:1393`, and `exceptions.py:700-701`.

**Fix.** Name `docs/plans/PLAN-node-customisation-phase-02-k3s-config.md`.

### CQ-3: The `write()` closure's comment repeats, and partly contradicts, the block comment above the method

* **Where.** `shakenfist_client_k3s/cluster.py:1476-1483` and
  `:1425-1429`.
* **Rating.** `document`.

`2aba0e6` replaced phase 2's inline `write()` with a call to
`heredoc()`. The closure that is left binds one keyword argument, and
carries six lines of comment:

```
1476        def write(path, body):
1477            # Through heredoc() rather than built here, per rule 2 at the
1478            # top of this module. The validation above already refuses a
1479            # configuration whose YAML contains the delimiter and raises
1480            # K3sConfigError for it; routing the write through the helper
1481            # is what quotes the path, and what keeps the refusal beside
1482            # the write for a body this method composed itself.
1483            return heredoc(path, body, delimiter=K3S_CONFIG_DELIMITER)
```

The block comment above the method is phase 2's text, and `2aba0e6` did
not touch it:

```
1425    # Every body goes through a heredoc with a quoted delimiter, per rule 2
1426    # above read_manifests(). The paths are literals of this method, so
1427    # rule 1 has nothing to quote; ...
```

Three problems:

* Both comments explain the same quoted-heredoc rule.
* One says there is nothing to quote, and the other says the helper is
  what quotes the path.
* "The validation above" is textually below: `validate_k3s_config()` is
  called at `:1488`, after `write()` is defined at `:1476`.

**Fix.** Keep one explanation. Either drop the closure's comment and
correct lines 1426-1427 ("heredoc() quotes them anyway"), or drop the
closure and call `heredoc(..., delimiter=K3S_CONFIG_DELIMITER)` directly
at its three call sites.

### CQ-4: `yaml.safe_dump(..., sort_keys=True)` needs PyYAML 5.1 or newer, which `pyyaml` (unpinned) does not promise

* **Where.** `shakenfist_client_k3s/cluster.py:575-576`, `:1465-1466`
  and `:1501-1502`, against `pyproject.toml:61`.
* **Rating.** `consider`.

These are the plan's three YAML dumps, and the first `sort_keys` in the
package:

```
575    text = yaml.safe_dump(json.loads(json.dumps(config)),
576                          default_flow_style=False, sort_keys=True)
...
1465            main = yaml.safe_dump(plugin_config, default_flow_style=False,
1466                                  sort_keys=True)
...
1501                yaml.safe_dump({'disable+': ['servicelb']},
1502                               default_flow_style=False, sort_keys=True)))
```

The dependency is declared as `"pyyaml",  # mit`, with no lower bound.

`sort_keys` was added to PyYAML's dumper in 5.1. Before that, dumping
always sorted, and the keyword is a `TypeError`. That is a dependency
floor, not a Python one, but it bites the same population as the
`python-version-discipline` block: a Python 3.7 host using its
distribution's PyYAML (Debian buster ships 3.13) satisfies `pyyaml`, and
then every `create()` fails inside `validate_k3s_config()`.

In 5.1 and later, `sort_keys=True` and `default_flow_style=False` are
both the defaults. The installed 6.0.3 has `dump_all(...,
default_flow_style=False, ..., sort_keys=True)`. So `sort_keys=True`
buys nothing on any version.

**Fix.** Either drop `sort_keys=True`, which keeps the sorted output on
every PyYAML version, or declare `pyyaml >= 5.1`. Separately, the dumper
options are now written out three times. A small `_dump_k3s_yaml(mapping)`
helper would keep them in one place. #82 (the 3.7 floor is not tested) is
why CI would not catch this.

### CQ-5: `read_k3s_config()` is the third copy of the "read a caller's file" pattern

* **Where.** `shakenfist_client_k3s/cluster.py:616-620`, alongside
  `:410-414` (`read_manifests()`) and `:2102-2108` (`create()`'s ssh
  key).
* **Rating.** `consider`.

```
616    try:
617        with open(path, encoding='utf-8') as f:
618            config = yaml.safe_load(f.read())
619    except (OSError, UnicodeDecodeError, yaml.YAMLError) as e:
620        raise exceptions.K3sConfigError.unreadable(path, str(e))
```

```
410        try:
411            with open(path, encoding='utf-8') as f:
412                content = f.read()
413        except (OSError, UnicodeDecodeError) as e:
414            raise exceptions.ManifestError.unreadable(path, str(e))
```

```
2104            try:
2105                with open(sshkey, encoding='utf-8') as f:
2106                    ssh_key_content = f.read()
2107            except (OSError, UnicodeDecodeError) as e:
2108                raise exceptions.SshKeyError.unreadable(sshkey, str(e))
```

`AGENTS.md` makes this a convention: every `open()` states
`encoding='utf-8'`, and a read of a caller-supplied path catches
`UnicodeDecodeError` as well as `OSError`. It also says that
`FileEncodingIsStatedTestCase` enforces only the first half. The second
half, the one nothing enforces, now lives in three hand-written copies.

**Fix.** Use a helper such as `_read_caller_file(path, unreadable)`,
which takes the exception factory. It would make the unenforced half
impossible to forget at the next call site. Its docstrings would also
stop pointing at each other: `read_k3s_config()` says "for the reasons
read_manifests() gives", and `create()` says "the same two reasons
read_manifests() is". `read_k3s_config()` would then parse the returned
text in its own `try` for `yaml.YAMLError`.

### CQ-6: `check_k3s_release()` hand-parses versions while `packaging` is already a dependency and already used for this

* **Where.** `shakenfist_client_k3s/cluster.py:648-657`, against
  `shakenfist_client_k3s/primitives.py:16` and `:176`.
* **Rating.** `consider`.

```
648    match = None
649    if isinstance(release, str):
650        match = re.match(r'^v(\d+)\.(\d+)\.(\d+)', release)
651    if not match:
652        raise exceptions.UnsupportedReleaseError.unparseable(release, channel)
653
654    version = tuple(int(part) for part in match.groups())
655    if version < K3S_RELEASE_FLOOR:
```

`primitives.py` imports `from packaging.version import InvalidVersion,
Version` and uses it to compare Longhorn tags. `packaging` is a declared
dependency.

The docstring (`:635-641`) spends seven lines explaining a leniency the
regex introduces: it ignores `-rc3`, so a pre-release of the floor
release itself would be accepted. `Version('v1.18.2-rc3+k3s1')` parses,
since the leading `v`, `-rcN` and the `+k3s1` local label are all PEP
440. It also orders pre-releases before their release, so the leniency
and the paragraph explaining it would both go.

One caveat: `UnsupportedReleaseError.floor` is documented as a `(major,
minor, patch)` tuple, so `K3S_RELEASE_FLOOR` would stay a tuple for the
message, and the comparison would build a `Version` from it.

**Fix.** Optional, and judgement-dependent. The regex is correct for
everything it claims.

### CQ-7: The node-sizing rationale is restated at four or five sites each

* **Where.** See the table below.
* **Rating.** `document`. The fix is advisory, per `comment-proportion`.

Two arguments are each written out in full several times. The block's
rule is to keep the why in one place and reduce the others to pointers.

| Argument | Sites |
|---|---|
| (a) Only positive integers are enforced, because a floor would be a guess | `validate_node_sizes()` docstring, `cluster.py:465-471`; `create()` docstring, `cluster.py:1920-1934`; `NodeSizeError`, `exceptions.py:620-625`; `docs/usage.md:63-76` |
| (b) The missing-`node_sizes` fallback is exact rather than a guess | `DEFAULT_NODE_SIZE` comment, `cluster.py:68-71`; `_node_size()`, `:864-870`; the `create()` metadata comment, `:2155-2160`; `show()`'s docstring, `:2373-2392`; and again for the configs at `:2169-2172` and `:2394-2398` |

Details for (a):

* The `create()` docstring copies the measurement from
  `docs/usage.md:63-66` almost verbatim: "k3s's server process alone held
  709 MB".
* `NodeSizeError` restates the argument, and then says "See
  `validate_node_sizes()` for the reasoning".

On (b): `show()`'s 20 lines on the fill sit on a method body of about
fifteen lines.

**Fix.**

* For (a), keep `validate_node_sizes()` as the home. Reduce `create()` to
  "defaults are `DEFAULT_NODE_SIZE`; see `docs/usage.md` for the 4096 MB
  recommendation and `validate_node_sizes()` for why it is not enforced".
  Reduce `NodeSizeError` to its contract (fields, the bool rule) plus the
  pointer.
* For (b), keep `_node_size()`. Cut `show()` and the metadata comment to
  one sentence that names it.

Two candidates that the block's size heuristic flags were judged and
pass:

* `_node_size()`'s docstring is 25 lines on one line of code, but each
  paragraph carries a contract: the per-field fallback, and the copy.
* `validate_node_sizes()`'s docstring is 34 lines on 5. Its bool and float
  paragraph is exactly the failure-mode explanation the block exempts.

### CQ-8: `_k3s_config_commands()`'s block comment and `create()`'s docstring restate `docs/usage.md`'s description of the three files

* **Where.** `shakenfist_client_k3s/cluster.py:1378-1413` and
  `:1936-1961`, against `docs/usage.md:80-111` and `:132-140`.
* **Rating.** `document`. The fix is advisory, per `comment-proportion`.

The 46-line block comment above `_k3s_config_commands()` enumerates the
three files, what each holds, and k3s's merge rule. `docs/usage.md:91-111`
says the same thing in nearly the same words:

```
1385    # 1. /etc/rancher/k3s/config.yaml holds what the plugin sets or
1386    #    defaults for the node. On a server that is the kubeconfig mode,
1387    #    the floating API address as a SAN, cluster-init on the first
1388    #    server only, and the control plane taint. On an agent it is a
1389    #    single comment line: ...
```

The `docs/usage.md` version:

```
94 1. `config.yaml` holds what the plugin sets or defaults for the node.
95    On a server that is the kubeconfig mode, the floating API address as
96    a SAN, `cluster-init` on the first server only, and the control
97    plane taint (below). On a worker it is a single comment line.
```

`create()`'s 26-line `server_config` paragraph restates it a third time,
along with the taint and the servicelb rule.

The block says that prose documenting user-visible behaviour belongs in
`docs/`, with the comment reduced to a pointer. The implementation-only
reasons in the block comment do earn their place, and none of them is in
`docs/usage.md`:

* why an agent gets a comment-only `config.yaml` (k3s before mid-2024);
* why the enforced disable is not on the installer's command line;
* why `md['worker_nodes']` is a valid test at install time;
* how `heredoc()` is used.

**Fix.** Point at `docs/usage.md` for the file list and the merge rule,
and keep the reasons. `create()` is the library contract, so it can keep
a short statement of what the two arguments do, and link to
`docs/usage.md` and `docs/library-api.md` for the rest.

### CQ-9: `K3sConfigError.unreadable()` names its detail parameter `reason`, then needs a paragraph to explain why it is stored as `detail`

* **Where.** `shakenfist_client_k3s/exceptions.py:769-771` and
  `:707-709`.
* **Rating.** `consider`.

```
769    def unreadable(cls, path, reason):
770        message = 'Could not read k3s configuration %s: %s' % (path, reason)
771        return cls('unreadable', message, path=path, detail=reason)
```

The docstring then explains: "``unreadable()`` stores its reason as
``detail``, the name ``ManifestError`` uses, since ``reason`` is already
which refusal this is."

`ManifestError.unreadable(cls, path, detail)` (`:453`) and
`SshKeyError.unreadable(cls, path, detail)` (`:599`) both call it
`detail`. The only caller (`cluster.py:620`) passes it positionally.

**Fix.** Rename the parameter to `detail`. That is one line, and the
explanatory sentence can then go.

### CQ-10: The role dispatch and its `ValueError` are written twice

* **Where.** `shakenfist_client_k3s/cluster.py:534-541` and `:1448-1474`.
* **Rating.** `consider`.

```
534    if role == 'server':
535        owned = K3S_SERVER_OWNED_KEYS
536    elif role == 'agent':
537        owned = K3S_AGENT_OWNED_KEYS
538    else:
539        # A programming error rather than a caller's: role is never
540        # something a user supplies.
541        raise ValueError("role must be 'server' or 'agent', not %r" % (role,))
```

`_k3s_config_commands()` repeats the same three-way branch, with the same
message at `:1473-1474`, and a comment pointing back ("as in
`validate_k3s_config()`").

Both methods also look the role's configuration up by string building:
`md.get(role + '_config', {})` at `:1489`.

**Fix.** A single `K3S_OWNED_KEYS = {'server': ..., 'agent': ...}` would
give both functions one lookup and one refusal (`KeyError` →
`ValueError`). This is small, and taking it is optional.

### CQ-11: Phase 2's tests copied phase 1's test helpers rather than sharing them

* **Where.** `shakenfist_client_k3s/tests/test_library_api.py:460-477`
  and `:587-602`, and `shakenfist_client_k3s/tests/test_cluster.py:1091-1096`
  and `:1171-1176`.
* **Rating.** `consider`.

There are three duplicated helpers:

* `NodeSizingTestCase._stored()` and `K3sConfigTestCase._stored()` are
  identical (`return self.client.metadata[MD_KEY]`).
* `NodeSizingTestCase._assert_refused_before_anything_is_built(**kwargs)`
  is `K3sConfigTestCase`'s version with the exception fixed to
  `NodeSizeError`. The five post-condition assertions are identical:
  `calls`, `metadata`, `allocated_networks`, `instances`, `executed`.
* `ShowReportsNodeSizesTestCase._show()` and
  `ShowReportsK3sConfigTestCase._show()` are byte-for-byte the same.

**Fix.** Move `_stored()` and the exception-parameterised
`_assert_refused_before_anything_is_built()` onto `LibraryTestCase`
(`test_library_api.py:57`). Share `_show()` through a small mixin or a
module-level function. That puts the "nothing was built" definition in
one place for the next pre-registration refusal.

### CQ-12: `ci_deploy_test.sh` now has four different parsers of `k3s show` output

* **Where.** `tools/ci_deploy_test.sh:105-111`, `:113-120`, `:122-135`
  and `:146-171`.
* **Rating.** `consider`.

| Helper | Lines | How it reads the output | Who added it |
|---|---|---|---|
| `worker_uuids()` | 109-110 | unanchored `grep 'worker_nodes'` | pre-existing; this plan added the `\|\| return 1` |
| `control_plane_uuids()` | 118-119 | anchored `grep '^    control_plane_nodes = '` | this plan |
| `count_routed_addresses()` | 133-134 | unanchored `grep 'routed_addresses'` | pre-existing |
| `assert_node_sizes()` | 154-155 | `sed -n 's/^    node_sizes = //p'` | this plan |

Phase 3 added the anchored helper next to the unanchored one, so two
neighbouring helpers that do the same job now match differently.

**Fix.** A single `show_value KEY` helper would give every reader the
anchored match and the `|| return 1` rule in one place. That rule is the
one the 4-line comment at `:128-131` explains. Each reader would keep
only its own post-processing. Whatever changes here cannot be verified
without the merge tier.

### CQ-13: The `--server-config` and `--agent-config` help text points at a repository path

* **Where.** `shakenfist_client_k3s/__init__.py:212-220`.
* **Rating.** `consider`.

```
213              help=('A YAML mapping of k3s configuration keys, applied to every '
214                    'control plane node. A few keys the plugin depends on are '
215                    'refused. See docs/usage.md.'))
```

A user who installed the wheel has no `docs/usage.md`. These are the only
two options in `__init__.py` whose help text refers to a file. An absolute
URL to the file on GitHub would work for every user.

### CQ-14: Two role vocabularies, and the configuration fallback is not routed through one helper the way `node_sizes` is

* **Where.** `shakenfist_client_k3s/cluster.py:857-883`, `:1489` and
  `:2426-2428`.
* **Rating.** `none`. This is the answer to the brief's duplication
  question, recorded for completeness.

**Two vocabularies.** Node sizes are keyed `'control_plane'` / `'worker'`,
the plugin's vocabulary, matching `md['control_plane_nodes']`. The k3s
configuration is keyed `'server'` / `'agent'`, which is k3s's vocabulary.
This is deliberate and documented in `validate_k3s_config()`'s docstring,
and the CLI follows suit with `--control-plane-*` and `--server-config`.
The sizing and configuration paths otherwise share no logic that could be
factored out: one checks integers, the other checks mappings. Their only
common shape is "validate before the name is registered". `create()`
states that once (`:1981-1999`), and it needs no helper.

**The fallback.** The one asymmetry is in the fallback. `show()`'s comment
says `node_sizes` goes through `_node_size()` "so that what show reports
and what create_instance() builds come from the same line"
(`:2414-2418`). The configuration fallback is written twice instead:

* `md.get(role + '_config', {})` at `:1489`;
* `if key not in md: fills[key] = {}` at `:2427-2428`.

The two differ only for a stored JSON `null`. `show()` reports `None`,
and the install path treats it as `{}`. Both mean "no configuration", and
the plugin never writes `null`. So nothing to do, unless CQ-10's mapping
is taken and a `_k3s_config(md, role)` accessor comes with it.

## Existing issues

These are rediscoveries. They are noted here and not raised as findings.

* **#82** (`requires-python >= 3.7` is unverified). This is why CQ-4
  would not fail in CI. Nothing runs the package on 3.7, or against a
  distribution PyYAML.
* **#96** (`Cluster.create()` does not range-check its counts). This plan
  made the gap more visible. The six sizes are validated as positive
  integers in the library (`validate_node_sizes()`) and with
  `click.IntRange(min=1)` in the CLI. The counts beside them are still
  `click.INT` (`__init__.py:152-160`) and unchecked in `create()`. The
  inconsistency is within one option list.
* **#100** (no type hints). None of this plan's new functions carry
  hints: `validate_node_sizes`, `validate_k3s_config`, `read_k3s_config`,
  `check_k3s_release`, `_node_size`, `_k3s_config_commands`. That is
  consistent with the rest of the tree, and out of scope per decision 7.
* **#93** (unencoded `open()` in tests). Not reproduced by this plan. Its
  test `open()` calls are either binary (`test_cluster.py:1604`, `:2151`,
  `:4471`) or state `encoding='utf-8'` (`:3190-3191`).

## Checked and clean

* **The `>=3.7` floor (`python-version-discipline`).**
  * No walrus, `match`/`case`, `removesuffix`/`removeprefix`,
    `functools.cache`, `zoneinfo`, `tomllib`, `datetime.UTC`, `X | Y`
    annotation, `dict[...]`/`list[...]` subscript, `shlex.join` or
    `f'{x=}'` anywhere in the scope's Python. This was checked by grep
    over every `.py` file in `scope-files.txt`.
  * `cluster.py:552-555` uses `rstrip('+')` and says why (`removesuffix`
    is 3.9).
  * `importlib.metadata` is still behind its guard (`cluster.py:38-41`).
  * The tests use the `mock` backport, not 3.8+ `unittest.mock` APIs.
  * Nothing found. The one floor problem is a dependency floor, not a
    Python one (CQ-4).
* **120-column wrap.** No line over 120 columns in any scope `.py` or
  `.sh` file (`awk 'length > 120'`). flake8 at 120 is clean per
  `verification.md`. Nothing found.
* **Quote style.** Every scope `.py` file was tokenized:
  * no triple single quotes;
  * every double-quoted non-docstring string either contains a single
    quote (the `ValueError` messages, `"role must be 'server' ..."`) or
    pre-dates this plan (`cluster.py:1169`, `__init__.py:387`,
    `test_exceptions.py:96-202`, `test_cluster.py:4098`, all blamed to
    commits before `ddb1f3b^1`).

  Nothing found.
* **Trailing whitespace.** None in any scope `.py` or `.sh` file. Nothing
  found.
* **TODO comments.** No `TODO`, `FIXME`, `XXX` or `HACK` in any scope
  file. Nothing found.
* **Click conventions in `__init__.py`.** Nothing found.
  * `create` is attached with `@k3s.command`.
  * `--namespace` is handled by `_bind_new_cluster_context()`.
  * The orchestration goes through `Cluster.create()`. The Click layer
    only reads the two files (`:229-237`). It reads them before binding
    so that a bad file leaves no namespace behind, and says so.
  * The six sizing options use `click.IntRange(min=1)` with defaults read
    from `DEFAULT_NODE_SIZE`, the single source the constant's comment
    promises.
  * `--server-config` and `--agent-config` use `click.Path(exists=True,
    dir_okay=False)`.
  * The help-text pointer is CQ-13.
* **Progress reporter, not bare prints.** The plan's new code in
  `cluster.py` prints nothing. Output goes through the existing
  `p.phase()` and `p.note()` calls. Nothing found.
* **Cheap, reliable top-level imports.** The only new import is
  `DEFAULT_NODE_SIZE, read_k3s_config` from the already-imported
  `cluster` (`__init__.py:8`). `yaml`, `json` and `re` were already
  imported by `cluster.py`. `verification.md` records the import check
  passing. Nothing found.
* **Cluster state in namespace metadata.** `node_sizes`, `server_config`
  and `agent_config` all go into the `orchestrated_k3s_cluster_*`
  document, at `cluster.py:2161` and `:2173-2174`. Nothing is written to
  local files. Nothing found.
* **The `AGENTS.md` encoding rule.** `read_k3s_config()` states
  `encoding='utf-8'` and catches `UnicodeDecodeError`. Nothing found; the
  duplication is CQ-5.
* **Elapsed time.** The plan added no elapsed-time measurement, so the
  `time.monotonic()` rule is not engaged. Nothing found.
* **The owned-key lists and their comments.** They are accurate against
  the plugin's own writes. Nothing found.

  | Key | What the comment cites | Checked against |
  |---|---|---|
  | `cluster-init` | set on the first server | `:1460-1461` |
  | `data-dir` | the token fetches and `K3S_MANIFEST_DIR` hardcode the path | `:1617`, `:1623`, `:146` |
  | `https-listen-port` | `K3S_URL` hardcodes 6443 | `:1654` |
  | `node-name`, `with-node-id` | `remove_worker()` lowercases the instance name, then drains and deletes by it | `:2979`, `:3021-3027` |
  | `server` | `K3S_URL` | `:1654` |
  | `tls-san` | the plugin sets it | `:1458` |
  | `token`, `token-file` | `K3S_TOKEN` | `:1655-1657` |
  | `write-kubeconfig` | every server-side `kubectl` and `helm` reads `/etc/rancher/k3s/k3s.yaml` | explicit `--kubeconfig` at `:1224`, `:1769-1771`, `:1798`, `:1827`, `:3023`, `:3027`; the rest by k3s's own default, which `:3001-3002` already notes |
  | `write-kubeconfig-mode` | the plugin sets it | `:1452` |
  | Agent list | "nothing reads an agent's data directory" | every `/var/lib/rancher` read targets `control_plane_nodes[0]` |

  Every key the plugin writes is either owned, or deliberately unowned
  and said so (`node-taint` and `disable+`, `:202-206`). That covers
  `write-kubeconfig-mode`, `tls-san` and `cluster-init` in `config.yaml`;
  `node-taint`; `disable+` in the enforced file; and `K3S_URL` /
  `K3S_TOKEN` on the installer.
* **Shared helpers in `primitives.py` or `progress.py`.** The plan's four
  new pure functions are `validate_node_sizes`, `validate_k3s_config`,
  `read_k3s_config` and `check_k3s_release`. They sit in `cluster.py`
  beside `read_manifests()`, which is the placement decision 1 of the
  phase 2 plan chose. None is something a second command or a wait loop
  needs, and nothing belongs in `progress.py`. Nothing found beyond CQ-5
  and CQ-6.
* **Duplication between the sizing and k3s config code.** No shared
  logic is worth extracting; see CQ-14. Duplication inside each path is
  in CQ-4, CQ-5, CQ-10 and CQ-11.
* **The delimiter check in both `validate_k3s_config()` and `heredoc()`.**
  This is deliberate. `2aba0e6`'s message explains it as keeping caller
  errors as `K3sConfigError`. It is not re-raised.
* **Comment proportion in `tools/ci_deploy_test.sh`.** Each of phase 3's
  comments was judged against the block and earns its length. Nothing
  found.
  * `:21-34` (`MINIMAL_CLUSTER`) explains why the minimal cluster has a
    worker, and that it is the positive control.
  * `:36-40` explains why every size differs.
  * `:127-131` explains why `|| return 1` is needed, since `set -e` is
    cleared in `$(...)`.
  * `:146-152` and `:173-181` give each parser's input format and why
    only one line is printed.
  * `:251-257` explains why the bare `disable`.
  * `:351-360` explains why the absence checks run after the
    LoadBalancer test.
  * `:403-406`, `:510-513`, `:549-552` and `:566-572` each explain one
    assertion's positive control.

  The positive-control idea recurs, but each mention is attached to the
  assertion it justifies, and none restates another at length.
  `:278-281` ("Cleaned up here the way...") is the weakest, at four lines
  on one `rm -rf`, but it is harmless.
* **Comment proportion in `cluster.py`'s new blocks, beyond CQ-3, CQ-7
  and CQ-8.** Each earns its length. Nothing further found.
  * The per-key owned-key comments (`:207-263`) are required by phase 2's
    decision 3, and give each key's dependency.
  * `K3S_RELEASE_FLOOR` (`:265-274`) cites k3s commits.
  * `validate_k3s_config()`'s docstring is 35 lines on a body of about
    40. Its JSON round-trip paragraph explains a non-obvious choice.
  * `create_instance()`'s nine lines explain why `node_type` has no
    default.
  * The `check_k3s_release()` call comment (`:2044-2048`) explains why it
    cannot sit with the other early checks.

## Summary

Fourteen findings: 0 `fix`, 5 `document` (CQ-1, 2, 3, 7, 8), 8 `consider`
(CQ-4, 5, 6, 9, 10, 11, 12, 13) and 1 `none` (CQ-14). Nothing gates the
phase. The real defect is CQ-4, the PyYAML 5.1 dependency floor, which
dropping a redundant keyword fixes.
