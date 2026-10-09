# Audit scope and evidence (cumulative health signals phase 4)

Fixed inputs for the push audit described in
`PLAN-cumulative-health-signals-phase-04-push-audit.md`, so that every
lens judges the same files instead of re-deriving a range. The audit
runs `PUSH-AUDIT.md` over everything phases 1 to 3 of the cumulative
health signals plan landed.

## The three merges

Each is diffed against its own first parent. All three phases have
merged, so a diff of the accumulated work against `develop` is empty;
the merges are the scope.

| Phase | Merge | PR |
|---|---|---|
| 1. Agent-read signals | `78c9df1` | #116 |
| 2. Kubernetes-read signals | `00109a6` | #122 |
| 3. Live validation | `bd9bead` | #124 |

`scope-files.txt` is the union of the files they changed, 22 lines, 5
of them under `docs/plans/`. The plan first said 6, a miscount since
corrected. It was generated with:

```
for m in 78c9df1 00109a6 bd9bead; do
  git diff --name-only $m^1 $m
done | sort -u > docs/plans/audit-cumulative-health-signals/scope-files.txt
```

Read `scope-files.txt`; do not derive your own range. Only the
documentation lens reads the `docs/plans/` files.

## Judge the code at bd9bead; lines #121 added are not in scope

Lenses read the code as it stands at `bd9bead`. #121 (`243a91c`,
argument validation) landed between phases 1 and 2 and changed 15 of
the 22 files, so a lens reading the current files sees its validation
woven through phase 1's code and could audit it as this plan's. The
merge diffs say which lines are this plan's; #121 had its own review.

Nothing has merged since `bd9bead` other than #123 (a workflow action
bump), so this is also the code as it stands today.

## The diffs are not committed

Deliberately (decision 2 of the plan). Regenerate any of them with
`git diff <m>^1 <m>`:

```
git diff 78c9df1^1 78c9df1
git diff 00109a6^1 00109a6
git diff bd9bead^1 bd9bead
```

Add `-- . ':!docs/plans'` to leave out the plan files.

## Files

* `README.md` -- this file.
* `scope-files.txt` -- the audit scope.
* `verification.md` -- the mechanical checks.
* `findings-*.md` -- what each of the four lenses found.
* `triage.md` -- what was done about each finding, and why.
