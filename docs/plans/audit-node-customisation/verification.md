# Mechanical verification

Run in the worktree `client-python-k3s-wt-nc-phase04` (branch
`node-customisation-phase-04-push-audit`), HEAD `e7a2e94`. That is one
commit of plan files on top of `3e84907` (the revision the lenses
judge); the only changes since `3e84907` are plan files, so these
results apply to `3e84907`'s code. No cluster was used.

| Command | Exit | Verdict |
|---|---|---|
| `tox -epy3` | 0 | Pass, 565 passed |
| `tox -eflake8` | 0 | Pass, but vacuous (see below) |
| `flake8 --max-line-length=120 <scope .py files>` | 0 | Pass (the meaningful style check) |
| `pre-commit run --all-files` | 0 | Pass |
| `tools/check-wheel-build.sh` | 0 | Pass |
| `python3 -c 'import shakenfist_client_k3s'` | 0 | Pass |
| Fresh-clone install, import and `--help` check | 0 | Pass |

## `tox -epy3`

565 tests ran: 565 passed, 0 failed, 0 skipped, 0 expected failures,
0 unexpected successes (matches #107's recorded 565). Nothing was
skipped, so there is nothing to explain. No warnings worth noting.

## `tox -eflake8`

Exit 0, but it diffs against `HEAD~1`, which here is `e7a2e94`, a
plan-only commit:

```
flake8: commands[0]> bash tools/flake8wrap.sh -HEAD
No python files in change.
  flake8: OK
```

So it says nothing about the audit scope. As a substitute, flake8 was
run directly over the scope's `.py` files (`tools/flake8wrap.sh` uses
`--max-line-length=120`; same setting), using the flake8 installed in
the scratch clone's venv:

```
flake8 --max-line-length=120 $(grep '\.py$' docs/plans/audit-node-customisation/scope-files.txt)
```

No output, exit 0.

## `pre-commit run --all-files`

Exit 0. Hooks that reported (all Passed): skillsaw, Lint GitHub
Actions workflow files, shellcheck, Lint the shakenfist.k3s collection.

## `tools/check-wheel-build.sh`

Exit 0. Built the sdist and wheel; `check-dist` reported the wheel OK
(12 entries, no `/tests/` paths); the sdist has no committed build
artefacts, 129 entries, 1758149 bytes uncompressed, size OK.

## `python3 -c 'import shakenfist_client_k3s'`

Exit 0, run with `.tox/py3/bin/python` (the tox env where the package
is installed).

## Build verification (fresh clone)

* Clone: `/tmp/claude-1000/-srv-kasm-profiles-mikal-vscode-src-shakenfist-client-python-k3s/9848ecef-3d30-4ef9-8c11-27f0cfd70875/scratchpad/audit-clone`
* Cloned at HEAD `e7a2e9412bd662939bacad09ee326ad444d3394c`
* Fresh venv in the clone (`venv/`, Python 3.13.5)
* `pip install shakenfist-client` succeeded
* `pip install -e .` succeeded
* `python3 -c 'import shakenfist_client_k3s'` printed OK
* `sf-client k3s create --help | grep -e server-config -e worker-disk`
  showed the new options:

```
  --worker-disk INTEGER RANGE     The disk for each worker node, in GB.
  --server-config FILE            A YAML mapping of k3s configuration keys,
```

The grep above does not match `--agent-config`, so it was checked
separately in the same venv, by the management session at review:
`sf-client k3s create --help | grep -o -e --agent-config -e
--server-config -e --control-plane-cpus | sort -u` printed all three.
