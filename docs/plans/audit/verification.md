# Mechanical verification

## Before the triage (step 6a)

All commands below were run in this worktree
(`client-python-k3s-wt-phase06`, branch `library-api-phase-06`, HEAD
`a56e0a6`) except the fresh-clone build check in the last section,
which runs in its own scratch clone as required by survey finding 8.

| Command | Exit | Verdict |
|---|---|---|
| `tox -epy3` | 0 | Pass |
| `tox -eflake8` | 0 | Pass, but see below -- proves little |
| `flake8 --max-line-length=120 <scope .py files>` | 0 | Pass (the meaningful style check) |
| `pre-commit run --all-files` | 0 | Pass |
| `tools/check-wheel-build.sh` | 0 | Pass |
| `python3 -c 'import shakenfist_client_k3s'` | 0 | Pass |
| Fresh-clone `pip install -e .` + `sf-client k3s --help` | 0 | Pass |

### `tox -epy3`

```
$ tox -epy3
```

461 tests ran, 0 failed, 0 skipped. Exit 0.

### `tox -eflake8`

```
$ tox -eflake8
```

Exit 0, but this diffs against `HEAD~1`, and on this branch that
commit is `921aec0` ("Close out phase 5 and correct the record."),
which touches only plan files. `flake8wrap.sh -HEAD` found no `.py`
files in that diff and printed "No python files in change.", so this
run says nothing about the audit scope:

```
flake8: commands[0]> bash tools/flake8wrap.sh -HEAD
No python files in change.
  flake8: OK (14.34=setup[14.32]+cmd[0.02] seconds)
```

### `flake8` over the scope's Python files directly

The meaningful style check. `tools/flake8wrap.sh` sets
`--max-line-length=120`, matching the project's 120-character wrap
(`tox.ini`); there is no `.flake8` or `setup.cfg` section overriding
it. Filtering `docs/plans/audit/scope-files.txt` for `*.py` entries
that still exist on disk gives 23 files (all 23 scope `.py` files are
still present; none was deleted since its merge). Ran with the flake8
installed into the shared tox environment:

```
$ .tox/shared/bin/flake8 --max-line-length=120 \
    collection/plugins/modules/sf_k3s_cluster.py \
    shakenfist_client_k3s/__init__.py \
    shakenfist_client_k3s/client.py \
    shakenfist_client_k3s/cluster.py \
    shakenfist_client_k3s/exceptions.py \
    shakenfist_client_k3s/primitives.py \
    shakenfist_client_k3s/progress.py \
    shakenfist_client_k3s/tests/fakes.py \
    shakenfist_client_k3s/tests/module_harness.py \
    shakenfist_client_k3s/tests/test_ansible_module.py \
    shakenfist_client_k3s/tests/test_build_collection.py \
    shakenfist_client_k3s/tests/test_check_dist.py \
    shakenfist_client_k3s/tests/test_cli_contract.py \
    shakenfist_client_k3s/tests/test_cli_errors.py \
    shakenfist_client_k3s/tests/test_client.py \
    shakenfist_client_k3s/tests/test_cluster.py \
    shakenfist_client_k3s/tests/test_commands.py \
    shakenfist_client_k3s/tests/test_exceptions.py \
    shakenfist_client_k3s/tests/test_library_api.py \
    shakenfist_client_k3s/tests/test_primitives.py \
    shakenfist_client_k3s/tests/test_progress.py \
    shakenfist_client_k3s/tests/test_root_options.py \
    tools/build-collection.py
```

Exit 0, no output. All 23 scope Python files are clean against the
project's 120-character line-length style.

### `pre-commit run --all-files`

```
$ pre-commit run --all-files
skillsaw.................................................................Passed
Lint GitHub Actions workflow files.......................................Passed
shellcheck...............................................................Passed
Lint the shakenfist.k3s collection.......................................Passed
```

Exit 0, all four hooks passed.

### `tools/check-wheel-build.sh`

```
$ tools/check-wheel-build.sh
```

Exit 0. Built the sdist and wheel from this worktree, which has no
stale `_version.py` (unlike the primary clone finding 8 describes).
`check-dist.sh` reports the wheel OK with 12 entries and no `/tests/`
paths; the sdist carries no committed build artefacts.

### `python3 -c 'import shakenfist_client_k3s'`

Timed over three repeated subprocess invocations: consistently
0.10-0.14 seconds (0.1417s measured via `time.monotonic()` around a
`subprocess.run`). Exit 0 every time.

Network access was checked with `strace -e trace=network`. The import
makes exactly one network-family syscall pair:

```
socket(AF_INET6, SOCK_STREAM|SOCK_CLOEXEC, IPPROTO_IP) = 3
bind(3, {sa_family=AF_INET6, sin6_port=htons(0), ..., "::1", ...}, 28) = 0
```

No `connect()` and no DNS resolution occurs anywhere in the trace.
Isolating the cause: `strace -e trace=network python3 -c 'pass'`
(baseline) makes no network-family syscalls at all; `strace -e
trace=network python3 -c 'import requests'` reproduces the identical
socket/bind pair. The call is `requests`' own IPv6-capability probe
(a local socket bound to the loopback address on port 0, never
connected). It is reached through `shakenfist_client.apiclient`, which
imports `requests` before this package's own modules are touched, so it
is unavoidable for any consumer of the Shaken Fist client and would
happen even if `primitives.py` imported nothing -- a correction to
the attribution first written here, which named `primitives.py` and
would have sent somebody to "fix" a syscall that is not its fault.
It is not caused by anything in `shakenfist_client_k3s` itself, and it
never leaves the host. So the
invariant holds in the sense that matters: no outbound connection, no
DNS lookup, no data sent over the wire.

### Fresh-clone build verification

Per decision 8 / finding 8, run outside any long-lived checkout to
avoid a stale gitignored `shakenfist_client_k3s/_version.py` being
globbed into the wheel.

Clone path:
`/tmp/claude-1000/-srv-kasm-profiles-mikal-vscode-src-shakenfist-client-python-k3s/ed96f16c-9b79-464f-8f95-dcc48b2af04e/scratchpad/fresh-clone/client-python-k3s`

Venv path:
`/tmp/claude-1000/-srv-kasm-profiles-mikal-vscode-src-shakenfist-client-python-k3s/ed96f16c-9b79-464f-8f95-dcc48b2af04e/scratchpad/fresh-clone/venv`

```
$ git clone /srv/kasm_profiles/mikal/vscode/src/shakenfist/client-python-k3s \
    <scratch>/fresh-clone/client-python-k3s
$ cd <scratch>/fresh-clone/client-python-k3s
$ git checkout library-api-phase-06
$ python3 -m venv <scratch>/fresh-clone/venv
$ <scratch>/fresh-clone/venv/bin/pip install --quiet --upgrade pip
$ <scratch>/fresh-clone/venv/bin/pip install -e .
$ <scratch>/fresh-clone/venv/bin/sf-client k3s --help
```

The clone checked out at `a56e0a6`, matching this worktree's HEAD.
`pip install -e .` resolved `shakenfist_client>=0.7.7` from PyPI as a
declared dependency and installed `shakenfist_client-0.8.3` alongside
`shakenfist_client_k3s-0.2.1.dev2+ga56e0a6` -- no separate
`shakenfist-client` install step was needed, since it is a normal
dependency of the package. `sf-client k3s --help` exits 0 and lists
all twelve subcommands:

```
Commands:
  create                  Create a new k3s cluster
  delete                  Destroy a k3s cluster
  expand-addresses        Add floating addresses for metallb to a k3s...
  expand-workers          Add workers to a k3s cluster
  getconfig               Get kubeconfig for an existing k3s cluster
  health                  Report the health of a k3s cluster
  list                    List managed k3s clusters
  query-k3s-version       Lookup the current version for a k3s release...
  query-longhorn-version  Lookup the current longhorn version
  remove-worker           Remove workers from a k3s cluster
  show                    Show details of a k3s cluster
  update-os               Update the OS on all nodes
```

No failures found in step 6a. Nothing here was fixed; findings, if
any, belong to the lens steps (6b-6f) and the triage in 6g.

## After the triage (step 6g), 2026-10-05

Re-run after the sixteen commits the triage took, in the same worktree
at HEAD `3163b79`, except the fresh-clone check, which clones that
commit into its own scratch directory per survey finding 8. Both runs
are kept so that the before and the after are each checkable.

| Command | Exit | Before | After |
|---|---|---|---|
| `tox -epy3` | 0 | 461 tests, 0 failed, 0 skipped | **492** tests, 0 failed, 0 skipped |
| `tox -eflake8` | 0 | Pass, proves little | Pass, proves little (same reason: it diffs against `HEAD~`) |
| `flake8 --max-line-length=120 <23 scope .py files>` | 0 | No output | No output |
| `flake8 --max-line-length=120 <every tracked .py>` | 0 | -- | No output |
| `pre-commit run --all-files` | 0 | 4 hooks pass | 4 hooks pass |
| `tools/check-wheel-build.sh` | 0 | 12 entries, clean sdist | 12 entries, clean sdist |
| `python3 -c 'import shakenfist_client_k3s'` | 0 | 0.10-0.14s, no `connect()` | 0.139-0.143s, no `connect()` |
| Fresh-clone `pip install -e .` + `sf-client k3s --help` | 0 | all twelve subcommands | all twelve subcommands |

### The test count: 461 to 492

Thirty-one tests were added and none removed. Accounted for in full,
because an unexplained difference is the one thing a count like this
cannot be allowed to have:

| Tests | Where | What they pin |
|---|---|---|
| 9 | `test_progress.RedactCommandLineTestCase` | the redaction helper itself: both shapes `shlex.quote()` produces, the `'"'"'` idiom, a longer name ending in a secret name, non-string input, every declared name, the nested walk over results, and `describe_agent_op()` |
| 6 | `test_cluster.SecretRedactionTestCase` | the real installer command line through the real `reap_execute()`, both exception classes' rendered text and stored attributes, the progress path, and `delete()`'s debug dump of the metadata document |
| 1 | `test_ansible_module.SecretsTestCase` | the member the class was missing: a create whose worker install exits non-zero, asserting the node token is absent from the module's fd 1. **This is the test that would have failed before the fix and passes after**, confirmed by watching it fail against the unredacted code |
| 4 | `test_cluster.HeredocDelimiterTestCase` | a metadata address which would end the heredoc early, and three on the shared `heredoc()` builder |
| 2 | `test_library_api.OptionalKubeconfigTestCase` | 0600 on a new kubeconfig and 0700 on `~/.kube`, with `umask 022` set explicitly rather than inherited; and that an existing directory keeps its own mode |
| 2 | `test_primitives` | the bounded response body, one per release lookup |
| 1 | `test_exceptions.TotalAttributesTestCase` | every `_ReasonedK3sException` subclass initialises all of its `FIELDS`, driven by `__subclasses__()` so a sixth is covered the day it lands |
| 2 | `test_cluster.DeleteClearsTheKeysCreateWroteTestCase` | the metadata key names `delete()` clears, and `node_network` cleared to None rather than `[]` |
| 1 | `test_cluster.ProgressIsStartedInOnePlaceTestCase` | `start_progress()` is the only function in `cluster.py` that constructs a `Progress` |
| 1 | `test_cluster.NoShellInvocationTestCase` | nothing in the tree passes `shell=True` |
| 1 | `test_cli_contract.CliContractTestCase` | `SUBCOMMANDS` is the group, which is what the eleven deleted `add_command()` calls were not doing |
| 1 | `test_cluster.FileEncodingIsStatedTestCase` | counted here because the shell check moved out of that class into its own, which is a net move rather than an addition -- the arithmetic above already accounts for it |
| **31** | | |

Skips are still 0. The five conditional `skipTest()` sites are
unchanged and none of them fires in this environment or in CI.

### One thing worth recording about running the suite locally

`test_ansible_module.py` runs the module in a subprocess, which imports
`shakenfist_client_k3s` from `site-packages` rather than from the
worktree: the harness is executed as a script, so `sys.path[0]` is
`shakenfist_client_k3s/tests/` and the repository root is not on the
path at all. A bare `stestr run` in a checkout therefore tests the
*installed* copy for those 29 tests. `tox -epy3` reinstalls the package,
so CI is unaffected and so is any run through tox -- but a bare
`stestr run` can report a pass for code it never loaded, which it
did once while this work was in progress. Use `tox -epy3`.
