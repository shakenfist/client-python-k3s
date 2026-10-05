# Audit scope and evidence (phase 6, step 6a)

Fixed inputs for every later audit step: the union of the ten merges
in decision 1 of
`PLAN-library-api-and-collection-phase-06-push-audit.md`, each diffed
against its own first parent, so lenses 6b-6f all judge the same 74
files instead of re-deriving a range.

## Files

- `scope-files.txt` -- `git diff --name-only <merge>^1 <merge>` per
  merge, unioned and `sort -u`'d. 74 lines.
- `diffs/<sha>.diff` -- `git diff <merge>^1 <merge>` per merge.
- `verification.md` -- step 6a's mechanical checks (tox, flake8,
  pre-commit, wheel build, import check, fresh-clone install).

## Generated with

```
for sha in 7fb29e5 1c32d12 4e76704 871f6ee 539b50d 205e4c3 \
           d51cf59 2506c19 d2e43d1 eb248bd; do
  git diff --name-only ${sha}^1 ${sha}
done | sort -u > docs/plans/audit/scope-files.txt
# repeat the loop with `git diff ${sha}^1 ${sha} > diffs/${sha}.diff`
```

Read `scope-files.txt`; do not derive your own range. 16 of the 74
files are under `docs/plans/`; only the documentation lens (6e) reads
those.

The SHAs in the loop above are the authoritative list, and so are the
`diffs/` filenames, which were written by it. The phase plan's scope
table is a hand transcription of the same ten and had two of them
wrong; `ls diffs/` answers the question without trusting any prose.

## Why the diffs are in git but not in a release

`diffs/` is 1.2MB, which `MANIFEST.in` prunes from the sdist.
setuptools_scm offers every tracked file to the sdist, so without that
these would ship to everyone installing from source, forever, for a
record of this project's history rather than a part of it. They stay in
git because a reader checking a finding needs the code as it was, and
they are regenerable from the loop above if they are ever dropped
entirely. The findings and triage files cannot be regenerated -- they
are judgements -- so those are small and do ship.
`tools/check-wheel-build.sh` bounds the sdist's size and entry count so
the next large tracked directory fails the gate instead of riding
along.
