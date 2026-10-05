# Audit findings: documentation

## Summary

| Action   | Count |
|----------|-------|
| fix      | 7     |
| document | 0     |
| consider | 0     |
| none     | 4     |

## The phase-reference grep

The exact command from `PUSH-AUDIT.md` and the step 6e brief:

```
$ grep -rn 'phase [0-9]' README.md docs/ --exclude-dir=plans
docs/collection.md:21:each needs a human to act (see the master plan's phase 5 row). Running
docs/library-api.md:264:delete <name>` as the way out, because phase 3 deliberately built
```

Two hits, neither carrying `<!-- audit-ok: phase-reference -->`. Both
are findings (F6 below).

**The command itself under-counts.** It is anchored on lowercase
`phase`, and three more references use a capital `P`:

```
$ grep -rni 'phase [0-9]' README.md docs/ --exclude-dir=plans
docs/collection.md:21:each needs a human to act (see the master plan's phase 5 row). Running
docs/library-api.md:100:stable public surface. Phase 4 cuts `v0.1.0` to PyPI, so whatever
docs/library-api.md:126:Phase 3 reshaped two of these rather than leaving them for later, and
docs/library-api.md:172:PyPI, so `sf-client k3s` is the only caller in the tree. Phase 4 is the
docs/library-api.md:264:delete <name>` as the way out, because phase 3 deliberately built
```

Five hits total, none marked `audit-ok`. This matches the open,
automatically-maintained consistency issue
[shakenfist/client-python-k3s#58](https://github.com/shakenfist/client-python-k3s/issues/58)
verbatim (it lists the same five locations), so the case-sensitivity
gap in the audit's own command is not news to the project -- the
automated check already uses the case-insensitive form. Verdict: all
five are genuine `plan-phase-references` violations and none is
exempt (none is "not about an implementation plan"); see F6. Fixing
them will very likely satisfy #58 as a side effect, but #58 is not one
of the six issues decision 6 protects from re-litigation, so this is a
new finding against this phase's own `docs/library-api.md`
(added/touched by phases 1 and 3) and `docs/collection.md` (added by
phase 5), not a duplicate of pre-existing work.

## Checklist questions answered

### Is `ARCHITECTURE.md` current for the modules and commands phases 1-5 added?

Mostly, but with two real gaps:

- The `Cluster` layer, the `exceptions.py` hierarchy, `client.py`'s
  role, and the collection are all described in prose, each pointing
  at the right `docs/*.md` page for depth. The "Three layers, not
  two" section correctly lists the current nine `Cluster` methods
  (`create`, `get_kubeconfig`, `show`, `delete`, `expand_workers`,
  `remove_worker`, `expand_addresses`, `update_os`, `health`) and the
  cluster-assembly description correctly covers the MetalLB/Longhorn
  opt-outs phase 3 added (worded generically as "unless a caller
  opts out" rather than naming the flags, which is appropriate for a
  map).
- The `## Module Structure` ASCII tree omits `client.py` entirely
  (F5) -- a plain inventory error, not a judgement call.
  `client.py`'s role is there, lower down, but the file listing at
  the top of the file is wrong on its face.
- The collection section's closing paragraph describes the Galaxy
  publish as not yet done (F1) -- true when phase 5's plan was
  written, false since `v0.2.0` published `shakenfist.k3s` 0.2.0 to
  Galaxy (confirmed in the master plan's Execution table and in
  commit `921aec0`). The phase 5 closeout corrected the plan files;
  it did not touch `ARCHITECTURE.md`.
- Separately, the `## Progress reporting` section (added/expanded by
  phase 3's merge `d51cf59`) is no longer "the shape" -- it is a
  genuine subsystem deep dive (agent-operation-state enumeration,
  the two wait-loop failure semantics, stall-note timing) with no
  "see `docs/...`" pointer and no `docs/` page covering the same
  ground. Every other section in this file ends with exactly such a
  pointer; this one does not (F8).

So: **mostly current, not fully.** Two defects (F1, F5) are plain
staleness/omission; one (F8) is the kind of growth the
`llm-doc-discipline` block calls a finding by itself.

### Does `AGENTS.md` state only conventions, with no reference material?

Checked against the diff across all ten merges (`diff --git
a/AGENTS.md` appears in `7fb29e5.diff`, `871f6ee.diff` and
`d51cf59.diff`; zero changes in the other seven). Growth is +17
lines over the whole plan (75 -> 92), all of it one of two shapes:

- Three one-line additions to the Key Files table naming new modules
  (`cluster.py`, `exceptions.py`, `client.py`) -- an index entry, not
  reference material.
- Three short bullets added by phase 3 (`d51cf59`): the
  `shlex.quote()` / heredoc-delimiter shell-quoting rule, the
  `encoding='utf-8'` + `UnicodeDecodeError` convention, and the
  "monotonic clock, not wall clock" rule. Each is 3-5 lines, each
  names the enforcing test class, and each is exactly a convention an
  agent cannot infer by reading one call site -- the shell-quoting
  rule in particular is security-relevant and the kind of invariant
  this file exists to carry.

None of the six additions restates a reference manual, a wire
protocol, or a step-by-step procedure. **No finding.**

### Has `README.md` grown bullets for new features instead of linking into `docs/`?

Checked the same way: `README.md` changed in `7fb29e5.diff` (added
the library-api.md link) and `eb248bd.diff` (added the collection.md
link). Both are exactly "curated absolute links into `docs/`" for
genuinely new documentation pages, which is what `readme-discipline`
permits without even calling it a change to the pitch. No new usage
bullets, no restated feature list. **No finding.**

### Deferred work and pre-existing errors: all listed under `docs/plans/`?

All six issues this plan is required to keep traceable are present
in `docs/plans/PLAN-library-api-and-collection-phase-05-collection.md`
and referenced again from
`docs/plans/PLAN-library-api-and-collection-phase-06-push-audit.md`:

| Issue | Traceable in a plan file | Open on GitHub |
|-------|---------------------------|-----------------|
| #82 | yes | yes |
| #89 | yes | yes |
| #91 | yes | yes |
| #93 | yes | yes |
| #94 | yes | yes |
| #96 | yes | yes |

All six confirmed open via `gh issue list`. **No finding** -- this is
a pass, stated explicitly per the brief's instruction to answer every
question including "nothing found".

### Plan registration: is every plan this work added registered in `docs/plans/index.md` with a shared-vocabulary status?

Yes. `docs/plans/index.md`'s row for "Library API, missing verbs, and
the shakenfist.k3s collection" reads `In progress` (correct -- phase 6
is still running), links all six phase files including this one, and
its `Intent` column already states phases 1-5 are complete and names
`v0.2.0`'s dual publication accurately. The master plan's own
Execution table agrees: phase 5 is `Complete` with `eb248bd` (#90),
phase 2's row correctly names all five merges including `205e4c3`
(#70), and phase 6 is `In progress` with no `Merged` cell, matching
the "push audit records no merge cell" rule. No status outside the
vocabulary (`Proposed`, `Not started`, `In progress`, `Blocked`,
`Complete`, `Abandoned`, `Superseded`) was found anywhere in
`index.md`. **No finding.**

### One canonical home per fact

Checked `README.md`, `AGENTS.md`, `ARCHITECTURE.md`,
`docs/collection.md`, `docs/library-api.md`, `docs/usage.md` and
`RELEASE-SETUP.md` against each other, plus `collection/README.md`
since it carries the same fact a third time. Found one fact stated in
three places, two of the three now false (F1/F2/F3 below) -- whether
`ansible-galaxy collection install shakenfist.k3s` works. All three
say essentially the same thing in different words, and all three are
stale in the same direction. `collection/README.md` even says
"Delete it when the first version is published," and the first
version has been published.

Also checked, and *not* flagged: `shakenfist_client` >= 0.7.7 is
stated in both `README.md` and `ARCHITECTURE.md`. This is a
deliberate, consistent duplication -- the README states the install
requirement (appropriate per `readme-discipline`'s "minimal
installation instructions"), and `ARCHITECTURE.md` explains *why*
(the `sf-agent2` dependency) as part of the map. Both give the same
version number, so there is no drift to fix, and the README's role is
exactly to restate the install-relevant subset of this fact.
`RELEASE-SETUP.md`'s "no secret exists" claim (F4) is a second
instance of the Galaxy-publication staleness rather than a distinct
duplicated fact -- grouped with F1-F3 in spirit but kept as its own
finding because the fix (and the wrong verification command) is
specific to that file.

## Findings

### F1: `ARCHITECTURE.md` says the collection has not had a first release

- **File**: `ARCHITECTURE.md:262-265`
- **Action**: fix
- **Claim**: The collection section's closing paragraph says
  "As of this writing the collection has not yet had a first
  release -- the credential and the first tag are both outstanding --
  so `ansible-galaxy collection install shakenfist.k3s` does not
  resolve", which is now false.
- **Evidence**: The master plan's Execution table (phase 5 row) and
  the push-audit plan's survey finding 3 both confirm
  `ANSIBLE_GALAXY_TOKEN` exists as a `release` environment secret,
  `v0.2.0` published `shakenfist.k3s` 0.2.0 to Galaxy, and
  `ansible-galaxy collection install shakenfist.k3s` resolves today.
  Commit `921aec0` ("Close out phase 5 and correct the record")
  corrected the plan files to say this but did not touch
  `ARCHITECTURE.md`.
- **Proposed change**: Replace the paragraph with the current state
  (published, installable via Galaxy) and drop the phase-5 citation
  per the `plan-phase-references` block -- describe the behaviour
  plainly rather than naming which phase shipped it.

### F2: `docs/collection.md`'s entire "Status: not yet published" section is stale

- **File**: `docs/collection.md:16-46`
- **Action**: fix
- **Claim**: The section heading, the bolded "does not work today"
  claim, and the worked-around tarball-install instructions all
  describe a pre-release state that no longer holds.
- **Evidence**: Same as F1. This is the operator-facing reference
  page for the collection -- the most consequential of the three
  stale copies, since a reader following it today would build a
  local tarball instead of running the one-line
  `ansible-galaxy collection install shakenfist.k3s` that now works.
  Line 21 also carries a `plan-phase-references` violation ("see the
  master plan's phase 5 row"), folded into F6 rather than duplicated
  here.
- **Proposed change**: Replace the section with install instructions
  for the published collection; keep the tarball-build path only if
  it is still useful for local development against an unreleased
  checkout, framed as that rather than as the only way in.

### F3: `collection/README.md` repeats the same stale claim a third time, with an unexecuted deletion note

- **File**: `collection/README.md:20-43`
- **Action**: fix
- **Claim**: "The first command does not work yet... Delete it when
  the first version is published" -- the first version has been
  published and this was not deleted.
- **Evidence**: Same underlying fact as F1/F2. This file is what
  Galaxy itself renders on the collection's page (stated in its own
  line 40-43), making it the most visible of the three stale copies
  to a Galaxy user, not just a GitHub reader.
- **Proposed change**: Remove the "does not work yet" block per its
  own instruction, leaving the two-line install (`ansible-galaxy
  collection install shakenfist.k3s` / `pip install
  shakenfist_client_k3s`) as the only install story.

### F4: `RELEASE-SETUP.md` says the Galaxy token does not exist, and checks the wrong secret scope

- **File**: `RELEASE-SETUP.md:126-136`
- **Action**: fix
- **Claim**: "At the time of writing, this secret does not exist" is
  false, and the verification command given
  (`gh api repos/.../actions/secrets`) checks the wrong location --
  the token is an *environment*-scoped secret under
  `repos/.../environments/release/secrets`, not a repository-level
  one.
- **Evidence**: Push-audit survey finding 4 states this exactly:
  "Phase 5's secret criterion cannot be satisfied as written... the
  secret is environment-scoped... a third case the criterion did not
  anticipate." The master plan file
  (`PLAN-library-api-and-collection-phase-05-collection.md`) was
  corrected at source for this; `RELEASE-SETUP.md` -- the actual
  human-facing runbook a maintainer would follow before a release --
  was not. This is the file most likely to mislead someone doing
  this by hand, since it would send them to the wrong secrets listing
  and they would (correctly, but confusingly) see nothing there.
- **Proposed change**: Update the paragraph to state the secret
  exists as a `release` environment secret, and change the
  verification command to `gh api
  repos/shakenfist/client-python-k3s/environments/release/secrets
  --jq '.secrets[].name'`.

### F5: `ARCHITECTURE.md`'s Module Structure tree omits `client.py`

- **File**: `ARCHITECTURE.md:19-27`
- **Action**: fix
- **Claim**: The `## Module Structure` ASCII tree lists
  `__init__.py`, `cluster.py`, `exceptions.py`, `primitives.py`,
  `progress.py` and `tests/`, but not `client.py`, which exists on
  disk (`shakenfist_client_k3s/client.py`) and is described in prose
  later in the same file and in `AGENTS.md`'s Key Files table.
- **Evidence**: `ls shakenfist_client_k3s/*.py` lists six files
  including `client.py`; the tree lists five. This is the plainest
  kind of component-inventory error a "map" document can have --
  this is literally the file listing.
- **Proposed change**: Add a `client.py` line to the tree, e.g.
  `client.py           # make_client(): builds an API client for a caller with no Click context`,
  matching the wording already used in `AGENTS.md`.

### F6: Five phase-number citations outside `docs/plans/`

- **File**: `docs/collection.md:21`; `docs/library-api.md:100,126,172,264`
- **Action**: fix
- **Claim**: All five cite a phase number to explain an already-
  implemented behaviour, which is exactly what
  `plan-phase-references` forbids: "Do not write... 'since phase 3 of
  the two-tier CI plan'... If a documented behaviour is implemented,
  describe it plainly."
- **Evidence**: See the grep section above. None carries
  `<!-- audit-ok: phase-reference -->`, and none is exempt -- every
  one is about an implementation plan (the public-surface freeze at
  `v0.1.0`, the `install_workers()` reshaping, the kubeconfig
  opt-out's absence of a resume path). This matches open issue #58's
  automated finding exactly (same five locations), which is strong
  independent confirmation these are real.
- **Proposed change**: Rewrite each sentence to describe the current
  behaviour without naming the phase that delivered it:
  - `docs/collection.md:21` -- "the credential and the first tagged
    release are both outstanding, and each needs a human to act" with
    no phase-5 pointer (or link the master plan if detail is wanted).
  - `docs/library-api.md:100` -- "whatever is public is a
    compatibility surface for external callers" (drop "Phase 4 cuts
    `v0.1.0` to PyPI, so").
  - `docs/library-api.md:126` -- state the `install_workers()` and
    `install_control_plane()` signatures as they are today; drop
    "Phase 3 reshaped two of these... and the reshaping is worth
    naming because it corrects what this page used to say."
  - `docs/library-api.md:172` -- "this is the last point at which the
    default could change for free" without "Phase 4 is the first PyPI
    release, so".
  - `docs/library-api.md:264` -- "...because detection and teardown
    were deliberately built rather than a way to resume a half built
    cluster" without "phase 3 deliberately built".

### F7: Broken relative link in `docs/plans/PLAN-cumulative-health-signals.md`, caused by this plan's own file rename

- **File**: `docs/plans/PLAN-cumulative-health-signals.md:106`
- **Action**: fix
- **Claim**: `[p3]: library-api-and-collection-phase-03-missing-verbs.md`
  points at a filename that no longer exists -- this plan's phase 3
  file was renamed to
  `PLAN-library-api-and-collection-phase-03-missing-verbs.md` at some
  point in phases 1-5 (confirmed: the old name is absent from the
  current tree; the new name is present and is what `index.md` links
  to).
- **Evidence**: `grep -n` confirms the stale reference-style link.
  This exact dangling link is also the sole finding of the open,
  automated consistency issue
  [#99](https://github.com/shakenfist/client-python-k3s/issues/99)
  ("Links out of docs/ are absolute"). It is in scope for this lens
  because `PLAN-cumulative-health-signals.md` is one of the 74 files
  `scope-files.txt` names, and the break was introduced by this
  plan's own rename of the target file, even though the broken link
  lives in a different (out-of-scope) master plan's document. It is
  not one of the six issues decision 6 protects from re-litigation.
- **Proposed change**: Update the link target to
  `PLAN-library-api-and-collection-phase-03-missing-verbs.md`.

### F8: `ARCHITECTURE.md`'s Progress reporting section is a subsystem deep dive, not "the shape"

- **File**: `ARCHITECTURE.md:137-197`
- **Action**: fix
- **Claim**: Phase 3's merge (`d51cf59`) replaced a short paragraph
  describing wait-loop failure detection with roughly five paragraphs
  enumerating every agent-operation state, the two different
  unknown-state behaviours for the two kinds of wait, and the
  stall-note timing contract. This is exactly the shape of content
  the `llm-doc-discipline` block says belongs in `docs/`: "A deep
  dive on one subsystem belongs in `docs/`, where humans benefit from
  it too." Growth in `ARCHITECTURE.md` is itself a finding per that
  block, and this is where the growth concentrated.
- **Evidence**: The `d51cf59.diff` hunk touching `ARCHITECTURE.md`
  replaces ~15 lines with ~48. Every *other* section of
  `ARCHITECTURE.md` ends with an explicit "this section is only the
  shape; `docs/X.md` is the reference" pointer (see the "Three
  layers, not two" and "The `shakenfist.k3s` Ansible collection"
  sections); the Progress reporting section has no such pointer, and
  no page under `docs/` covers the same ground --
  `docs/library-api.md`'s "## The reporter" section documents
  `Reporter`/`CollectingReporter`'s public interface, not the
  wait-loop state machine or stall detection.
- **Proposed change**: Move the detailed wait-loop/stall-detection
  material to `docs/library-api.md` (or a new page, since it is
  arguably user-facing behaviour of `await_boot()`/`await_idle()`/
  `await_fetch()` that a library caller should be able to read about
  without `cluster.py`'s docstrings), and reduce
  `ARCHITECTURE.md`'s section back to the shape -- what `progress.py`
  is for and why wait loops enumerate states rather than name two
  endings -- with a link to the fuller page, matching every other
  section's pattern.

## Not flagged (checked and clean)

- `AGENTS.md` growth (+17 lines across the whole plan): all
  convention/invariant material or Key Files index entries, nothing
  that belongs in `docs/`.
- `README.md` growth (+5 lines): two curated `docs/` links for
  genuinely new pages, nothing else.
- All six deferred-work issues (#82, #89, #91, #93, #94, #96) are
  traceable from a plan file and confirmed open.
- `docs/plans/index.md` and the master plan's Execution table use
  only shared-vocabulary statuses and are otherwise current.
- `RELEASE-SETUP.md` uses "steps"/"one-time setup steps" throughout
  and never says "phase" -- it does not trip the
  `plan-phase-references` rule about procedural runbooks, even
  though it does carry the separate staleness defect in F4.
- `shakenfist_client` `>= 0.7.7` is stated in both `README.md` and
  `ARCHITECTURE.md`, consistently and for different, legitimate
  reasons (pitch vs. map); not treated as a duplicated-fact finding.
