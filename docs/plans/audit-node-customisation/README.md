# Audit scope and evidence (node customisation phase 4)

Fixed inputs for the push audit described in
`PLAN-node-customisation-phase-04-push-audit.md`, so that every lens
judges the same files instead of re-deriving a range. This directory
is independent of `docs/plans/audit/`, which belongs to the library API
audit.

## The three merges

Each is diffed against its own first parent.

| Phase | Merge | PR |
|---|---|---|
| 1. Per-role sizing | `ddb1f3b` | #92 |
| 2. k3s configuration pass-through | `b791364` | #98 |
| 3. Live validation | `51ff6f4` | #108 |

`scope-files.txt` is the union of the files they changed, 20 lines, 5
of them under `docs/plans/`. It was generated with:

```
for m in ddb1f3b b791364 51ff6f4; do
  git diff --name-only $m^1 $m
done | sort -u > docs/plans/audit-node-customisation/scope-files.txt
```

Read `scope-files.txt`; do not derive your own range. Only the
documentation lens reads the `docs/plans/` files.

## Judge the code at 3e84907, not at the merge

The merges say which code is in scope, but lenses read it as it stands
at `3e84907`: #107's `2aba0e6` swept the library API audit's rules over
phase 2's code after it merged (exception base class, `heredoc()`
helper, a phase reference), so the merged revision no longer exists and
auditing it would re-raise defects that are already fixed.

## The diffs are not committed

Deliberately (decision 2 of the plan). Regenerate any of them with
`git diff <m>^1 <m>`, for example `git diff ddb1f3b^1 ddb1f3b`. Since
the code has moved on, read the current file for the finding itself.

## Files

* `README.md` -- this file.
* `scope-files.txt` -- the audit scope.
* `verification.md` -- the mechanical checks.
* `findings-code-quality-style.md`
* `findings-tests.md`
* `findings-docs.md`
* `findings-security.md`
* `triage.md` -- decisions on the findings.
