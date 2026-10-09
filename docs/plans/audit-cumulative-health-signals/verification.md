# Mechanical verification

Run in the worktree `client-python-k3s-wt-health-signals` (branch
`cumulative-health-signals-phase-04-push-audit`), HEAD `975d45b`. That
is one commit of plan files on top of `bd9bead` (the revision the
lenses judge); the only change since `bd9bead` is plan files, so these
results apply to `bd9bead`'s code. No cluster was used.

| Command | Exit | Verdict |
|---|---|---|
| `tox -epy3` | 0 | Pass, 988 passed |
| `tox -eflake8` | 0 | Pass, but vacuous (see below) |
| `flake8 --max-line-length=120 <scope .py files>` | 0 | Pass (the meaningful style check) |
| `pre-commit run --all-files` | 0 | Pass |
| `python3 tools/mutation-check.py` | 0 | Pass, 90 of 90 caught |
| `tools/check-wheel-build.sh` | 0 | Pass |
| `python3 -c 'import shakenfist_client_k3s'` | 0 | Pass |
| Fresh-clone install and import check | 0 | Pass |

Nothing failed.

## `tox -epy3`

988 tests ran: 988 passed, 0 failed, 0 skipped, 0 expected failures,
0 unexpected successes (matches phase 3's recorded 988).

## `tox -eflake8`

Exit 0, but it diffs against `HEAD~1`, which here is a plan-only
commit:

```
flake8: commands[0]> bash tools/flake8wrap.sh -HEAD
No python files in change.
  flake8: OK
```

So it says nothing about the audit scope. As a substitute, flake8 was
run directly over the scope's `.py` files, with the same
`--max-line-length=120` as `tools/flake8wrap.sh`:

```
.tox/py3/bin/flake8 --max-line-length=120 $(grep '\.py$' docs/plans/audit-cumulative-health-signals/scope-files.txt)
```

No output, exit 0.

## `pre-commit run --all-files`

Exit 0. Hooks that reported, all Passed: skillsaw, Lint GitHub Actions
workflow files, shellcheck, Lint the shakenfist.k3s collection. This
was run before the audit directory existed, and `--all-files` skips
untracked files in any case.

## `python3 tools/mutation-check.py`

Exit 0, run after `tox -epy3` because it uses the tox venv's stestr.
"All 90 mutations were caught." 90 caught, 0 survived (matches phase
3's recorded 90).

### State of the tox venv afterwards

Survey finding 4 predicts that the `'tox'` runner leaves `.tox/py3`
holding the last mutation. Checked from outside the worktree (a
`python -c` run inside it imports the tree, not the venv):

```
cmp "$(.tox/py3/bin/python -c 'import shakenfist_client_k3s, os; print(os.path.dirname(shakenfist_client_k3s.__file__))')/cluster.py" shakenfist_client_k3s/cluster.py
```

`cluster.py` and `progress.py` were both byte-identical to the tree,
and a recursive diff of the installed package against the tree (apart
from `tests/`) found no difference. So the venv was clean on this run;
the defect was not observed. This does not show the defect is absent:
it may depend on which mutation ran last, and that run's last
mutations (89 and 90) may not use the tox runner. Nothing was fixed.

The management session then ran `python3 tools/mutation-check.py --only
'node token is redacted'`, the only tox-runner entry that mutates the
installed package (the other mutates `collection/`, which is not
installed). The mutation was caught, and afterwards `progress.py` in
the venv still matched the tree. The defect does not reproduce, and
the plan withdraws decision 9.
Afterwards the venv was reinstalled anyway, so later steps start
clean:

```
.tox/py3/bin/python -m pip install -q --no-deps --force-reinstall .
```

## `tools/check-wheel-build.sh`

Exit 0. Built the sdist and wheel; `check-dist` reported the wheel OK
(12 entries, no `/tests/` paths); the sdist has no committed build
artefacts, 105 entries, 1678891 bytes uncompressed, size OK.

## `python3 -c 'import shakenfist_client_k3s'`

Exit 0 with the host `python3`, and exit 0 with `.tox/py3/bin/python`.

## Build verification (fresh clone)

* Clone: `/tmp/claude-1000/-srv-kasm-profiles-mikal-vscode-src-shakenfist-client-python-k3s/7ddd9dd5-5a5a-4ba9-b9fa-6daf0077772a/scratchpad/audit-clone`
* Cloned at HEAD `975d45b50d1b387d2a7e5ee63b15364b119574be`
* Fresh venv in the clone (`venv/`, Python 3.13.5)
* `pip install shakenfist-client` succeeded
* `pip install -e .` succeeded
* `python3 -c 'import shakenfist_client_k3s'` in that venv printed OK
* `sf-client k3s create --help | grep -o -e --agent-config -e
  --server-config -e --control-plane-cpus | sort -u` printed all
  three.
