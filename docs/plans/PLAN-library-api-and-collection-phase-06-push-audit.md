# Push audit phase 6: the accumulated diff of phases 1-5

## Prompt

Read `PUSH-AUDIT.md` in full before answering anything about this
phase. It is the specification; this file only says how to run it and
what the survey already settled. Then read the `plan-push-audit-phase
v3` shared block quoted in `PLAN-TEMPLATE.md`'s Execution section,
because it -- not the one-line phase 6 row in the master plan -- is the
authority on what "the accumulated diff" means.

Two things make this phase unlike the five before it. It changes almost
nothing by design: its deliverable is a judgement about work that has
already merged, and the temptation is to turn every finding into a
commit. And it is the only phase whose scope cannot be derived from the
tree, only from a record kept while the other phases landed -- so the
first thing to verify is that the record is complete.

## Planning effort

**Medium**, and deliberately not high. The audit's *content* is
high-effort work, which is why six of the nine steps below are rated
high and run on opus. Planning it is not: `PUSH-AUDIT.md` already
states every question, the shared blocks already define the standards
those questions are measured against, and the division of labour
follows the checklist's own section structure.

The one genuinely ambiguous thing -- what diff is being audited -- took
real work to resolve and is resolved here in survey finding 1, so no
step has to re-derive it. The two judgement calls that remained
(decisions 5 and 7) are taken below rather than deferred.

## Scope

In scope:

1. Running every section of `PUSH-AUDIT.md` over the accumulated diff
   defined in decision 1: code quality, style conformance, tests,
   documentation, security review and build verification.
2. Fixing what the audit finds that must be fixed before the plan is
   called done, under the triage rule in decision 7.
3. Filing issues for what is real but out of scope, so nothing
   disappears silently.
4. Closing out phase 5 in the master plan and `docs/plans/index.md`,
   including the four corrections at source listed below.
5. Closing out phase 6 and the master plan itself, in this phase's own
   pull request.

Out of scope, explicitly:

1. **The other two master plans' work that is interleaved on
   `develop`.** `node-customisation` (#87, #92) and
   `cumulative-health-signals` (#88) each carry their own push-audit
   phase. Auditing them here would both duplicate that work and charge
   their findings to this plan.
2. **The nine renovate bumps, the Debian 13 EOL change (#67), the
   reusable-workflow-secrets change (#83) and the exported-config
   change (#95)** that also landed in the window. None is this plan's
   work.
3. **Introducing type hints or mypy.** See decision 5.
4. **The six open issues this plan already filed** (#82, #89, #91,
   #93, #94, #96). The audit may add a newly found occurrence to one
   of them, but it does not re-litigate them or close them.
5. New features, and any behaviour change not required by a finding.

## What the survey found

### 1. The scope is ten merge commits, not a commit range

The master plan's phase 6 row says "the accumulated diff of phases 1-5
against `develop`". Read literally as a range that is
`73bd499..develop`, which is **78 files, 20,115 insertions, 115
commits** -- and includes two other master plans plus sixteen unrelated
merges. The `plan-push-audit-phase v3` shared block anticipates exactly
this mistake: "unrelated work lands on the default branch between
phases, so anything anchored on 'since the plan file appeared' is far
too wide. It has to be recorded."

The record is the `Merged` column. The scope is therefore the union of
`git diff <merge>^1 <merge>` over the ten merge commits the column
names -- **74 files, 18,122 insertions**:

| Phase | Merges | Insertions |
|-------|--------|-----------|
| 1 | `7fb29e5` (#55) | 4,742 |
| 2 | `1c32d12` (#63), `4e76704` (#65), `871f6de` (#68), `539b50d` (#69), `205e4c3` (#70) | 1,009 |
| 3 | `d51cf59` (#75) | 7,388 |
| 4 | `2506b19` (#81), `d2e43d1` (#86) | 1,078 |
| 5 | `eb248bd` (#90) | 3,905 |

Of the 74 files, 16 are under `docs/plans/` and 58 are not. The
non-test Python the audit's code-quality and security lenses actually
read is eight files:

| File | Lines changed |
|------|---------------|
| `shakenfist_client_k3s/cluster.py` | 2,518 |
| `shakenfist_client_k3s/exceptions.py` | 762 |
| `collection/plugins/modules/sf_k3s_cluster.py` | 755 |
| `shakenfist_client_k3s/__init__.py` | 686 |
| `shakenfist_client_k3s/primitives.py` | 578 (mostly removals -- code moved to `cluster.py`) |
| `tools/build-collection.py` | 129 |
| `shakenfist_client_k3s/progress.py` | 110 |
| `shakenfist_client_k3s/client.py` | 82 |

Tests add a further ~8,000 insertions across fifteen files.

### 2. The `Merged` record was missing a merge

Phase 2's cell named four merges; there are five. `205e4c3` (#70,
`library-api-phase-02-closeout`) was not recorded. It contains exactly
one commit changing one line of the master plan, so nothing auditable
was lost this time -- but the column *is* the scope definition, and an
incomplete scope definition is a defect whether or not it happens to
be benign. Corrected at source.

### 3. Phase 5 is complete in fact, and was `Blocked` in the records

Both records agreed with each other, so no half-finished closeout had
to be untangled. Steps 5e and 5g were genuinely outstanding when they
were written and are now done:

- `ANSIBLE_GALAXY_TOKEN` exists as a `release` **environment** secret,
  created 2026-10-04T19:17:01Z.
- `v0.2.0` was tagged at `0c856ed`, signed into tag object `da6a11f`
  by `github-actions[bot]`, and published to PyPI and to Galaxy
  (`shakenfist.k3s` 0.2.0, 2026-10-04T20:32:05Z).
- `ansible-galaxy collection install shakenfist.k3s` resolves, and
  `ansible-doc -t module shakenfist.k3s.sf_k3s_cluster` renders from
  the installed tree.

`publish-collection` failed on its first attempt with HTTP 403
`permission_denied`: the token authenticates as Galaxy user
`shakenfist-bot`, which held no role on the `shakenfist` namespace
(owned solely by `mikalstill`). Granting the bot
`collection_namespace_owner` and re-running that one job completed the
release without a new tag.

### 4. Phase 5's secret criterion cannot be satisfied as written

Its definition of done requires that `gh api
repos/shakenfist/client-python-k3s/actions/secrets` list
`ANSIBLE_GALAXY_TOKEN`, "or an organisation-level secret of that name
is confirmed to cover this repository". Neither holds. The secret is
*environment*-scoped, under
`repos/.../environments/release/secrets` -- a third case the criterion
did not anticipate, and the correct one, because `publish-collection`
declares `environment: release` and so resolves environment secrets.
Corrected at source.

### 5. Phase 5's recovery instruction conflates two different failures

Step 5g says that if `publish-collection` fails you must burn the
version and tag the next patch, "which Galaxy's refusal to replace a
version makes mandatory". That is true of a *partial* publish and
false of a *refused* one. The 403 stored nothing, so 0.2.0 remained
free on Galaxy and re-running the single failed job published it --
no new tag, no burned version, and retryable as many times as the
credential needed. Following the instruction literally would have
spent a version per attempt and left Galaxy's history starting at a
different number from PyPI's. Corrected at source.

### 6. The typing clause of `python-version-discipline` is unmet, plan-wide

The shared block requires that "new and modified code carries type
hints, and mypy is expected to be clean over it". Across the eight
non-test source files above there are **128 function definitions and
zero annotations**, and mypy appears in no configuration file --
not `pyproject.toml`, not `tox.ini`, not `.pre-commit-config.yaml`.

This is a real finding against the standard and it is not a defect
the audit should fix. See decision 5.

### 7. The Python floor is otherwise clean

`requires-python = ">=3.7"`, and the block names this "the finding to
look for first". The scan found nothing: no walrus operator, no `match`
statement, no `tomllib`, no `datetime.UTC`, no `zoneinfo`, no builtin
generics or PEP 604 unions in annotations evaluated at runtime, and no
f-string `=` specifier. `importlib.metadata` is correctly guarded with
an `importlib_metadata` fallback at `shakenfist_client_k3s/cluster.py:38`.
The one `from __future__ import annotations` is in
`collection/plugins/modules/sf_k3s_cluster.py`, where it is required by
`validate-modules` and tracked by #94.

This is a real result and worth stating plainly: the check that
`PUSH-AUDIT.md` ranks first comes back clean, and step 6c should
confirm rather than rediscover it.

### 8. Local build verification is booby-trapped by a stale artefact

`tools/check-wheel-build.sh` failed in the primary clone with 13
entries against `MAX_ENTRIES=12`. The thirteenth was
`shakenfist_client_k3s/_version.py`: a gitignored file dated 8 August
claiming version `0.1.dev69+g9d5adaf3c`, left behind when phase 4
removed the `setuptools_scm` `write_to` setting that generated it.
`[tool.setuptools.packages.find]` globs the package directory from
disk, so the fossil was compiled into every wheel built in that
checkout. CI never saw it, because CI checks out fresh.

The file has been removed and the check now passes with 12 entries and
a clean sdist. The lesson is a constraint on step 6a: **build
verification runs in a fresh checkout**, never in a long-lived clone.

### 9. Deferred work is recorded, not dropped

All six issues this plan filed are open: #82 (unverified
`requires-python` floor), #89 (no integration coverage for
`sf_k3s_cluster`), #91 (unverified `requires_ansible` floor), #93
(twenty unencoded `open()` calls in `tests/`), #94
(`validate-modules` versus flake8), #96 (`Cluster.create()` does not
range-check its counts).

### Corrections made at source

Folded into this phase's first commit, which closes out phase 5,
so no later step redoes them:

1. Phase 2's `Merged` cell gains `205e4c3` (#70), marked as plan
   bookkeeping only.
2. Phase 5's secret criterion is reworded to the environment-scoped
   path that is actually true.
3. Step 5g's recovery instruction distinguishes a refused publish from
   a partial one.
4. The phase 6 row names the merge-commit union rather than "the
   accumulated diff ... against `develop`", so the next reader does not
   have to rediscover finding 1.

## Decisions this plan already takes

1. **Scope is the union of the ten recorded merge diffs**, each
   computed as `git diff <merge>^1 <merge>`. Not `73bd499..develop`,
   which finding 1 shows is too wide by two master plans. Every audit
   step is given the same file list, generated once in 6a and written
   to the worktree so the steps cannot disagree about what they
   reviewed.

2. **`docs/plans/` is excluded from the code-quality, style and
   security lenses, and included for the documentation lens.** Sixteen
   of the 74 files are plan prose. Asking whether it duplicates code or
   wraps at 120 characters is noise; asking whether deferred work is
   recorded there and registered in `index.md` is exactly the
   documentation section's question.

3. **One sub-agent per audit lens, not per file region.** Each section
   of `PUSH-AUDIT.md` is a different way of looking at the same 58
   files, and findings cluster by lens rather than by file. Splitting
   by region would make every agent re-read the checklist and would
   hide the cross-file findings -- the duplicated helper, the doc page
   an earlier phase made wrong -- that the shared block says this phase
   exists to catch.

4. **Audit agents are read-only. All fixes happen in step 6g.** An
   agent that both finds and fixes cannot be trusted to report what it
   decided not to fix, and six agents editing one worktree concurrently
   would conflict. The audit steps write findings to files under
   `docs/plans/audit/` in the worktree; 6g triages and edits.

5. **The typing finding is recorded and filed, not fixed.** Annotating
   128 definitions and wiring mypy into `tox.ini` and pre-commit is a
   larger change than anything in phases 1-5, it touches every file the
   audit is supposed to be judging, and it would be landing unreviewed
   under the banner of an audit. It is also not a latent break: unlike a
   3.10 syntax error on a 3.7 interpreter, missing hints cost nothing at
   runtime. File it as its own issue, note it in Future work, and say so
   in the report.

   This is the decision most likely to be argued with, because the
   shared block states the requirement flatly and this plan declines it
   for the whole of a five-phase plan's output. The counter-argument is
   that a standard unmet across 13,000 lines is evidence the standard
   was never adopted here, not evidence of five phases of negligence --
   there is no mypy config to be clean against, so there is no staged
   rollout to be held to the new part of.

6. **Pre-existing issues are not re-audited.** Where a lens rediscovers
   #82, #89, #91, #93, #94 or #96, the finding is a comment on that
   issue, not a new finding and not a fix.

7. **Findings are triaged `fix` / `document` / `consider` / `none`**,
   with the same exit rule the `respond-to-review` skill uses: `fix`
   gates the phase, `document` is taken only when it is a comment or a
   docstring, `consider` is taken only when it is a one-liner or a real
   defect in code these phases added, `none` is informational. The
   checklist is a generator of questions with no notion of good enough,
   and six agents pointed at 18,000 insertions will produce more than
   the phase should act on.

8. **Build verification runs in a fresh clone.** Finding 8 is the
   reason. 6a clones to a scratch directory rather than trusting any
   working copy, and reports the clone path so the result is
   reproducible.

## Step plan

| Step | Effort | Model | Isolation | Brief for sub-agent |
|------|--------|-------|-----------|---------------------|
| 6a | medium | sonnet | worktree | **Done, `b25e062`.** Produce the audit's inputs and run its mechanical half. First write the file list: for each of the ten merges in decision 1, `git diff --name-only <merge>^1 <merge>`, union them, sort unique, and write to `docs/plans/audit/scope-files.txt`; write the matching unified diff per merge to `docs/plans/audit/diffs/<sha>.diff`. Then run, recording exact output for each: `tox -epy3`; `tox -eflake8`; `pre-commit run --all-files`; `tools/check-wheel-build.sh`; and `python3 -c 'import shakenfist_client_k3s'`. Then the build verification from `PUSH-AUDIT.md`'s last section **in a fresh clone, not this worktree** (finding 8): `git clone` the repository to a scratch directory, create a venv, `pip install -e .` alongside `shakenfist-client`, and confirm `sf-client k3s --help` lists the subcommands. Write results to `docs/plans/audit/verification.md` as a table of command, exit status and any output that is not a pass. Do not fix anything you find; record it. Commit subject: `Record the audit scope and verification run.` |
| 6b | high | opus | none | **Done, findings in `79bbb01`.** **Read-only.** Audit the scope for `PUSH-AUDIT.md`'s *Code quality* section, including the `comment-proportion v1` shared block. Read `docs/plans/audit/scope-files.txt` and the diffs beside it; ignore anything under `docs/plans/`. The specific questions: duplicated code introduced across phases (the shared block warns the duplicate may only exist *because* two phases both landed -- phase 1 moved code out of `primitives.py` into `cluster.py` and phase 3 added verbs to both, so that pair is where to look hardest); logic that should be a shared helper in `primitives.py` or `progress.py`; TODO comments; 120-character wrapping; single quotes for strings and triple-double for docstrings. For comment proportion, `cluster.py` and `exceptions.py` carry very long explanatory comments by house style -- apply the block's own test (does it say what the code cannot?) rather than a line ratio, and remember the block calls these candidates, not verdicts. Write findings to `docs/plans/audit/findings-code-quality.md`, each with file, line, the claim, and a proposed action from `fix`/`document`/`consider`/`none`. Change no source file. |
| 6c | high | opus | none | **Done, findings in `79bbb01`.** **Read-only.** Audit the scope for `PUSH-AUDIT.md`'s *Style conformance* section. Survey finding 7 already establishes that the `python-version-discipline` syntax checks come back clean against the `>=3.7` floor -- **confirm that rather than rediscovering it**, and say explicitly if you find a counter-example it missed. Do not re-raise the typing clause: decision 5 settles it. The substantive work is the project conventions in `AGENTS.md`: Click commands attached to the `k3s` group, `--namespace` handling, orchestration reached through a `Cluster` rather than a click context, and long-running work reporting through `Cluster.get_progress()` rather than bare prints. Grep for `print(` across the scope and justify every surviving call. Also verify the two invariants the checklist states: `python3 -c 'import shakenfist_client_k3s'` must stay cheap and reliable (no network, no heavy import at module scope), and cluster state must live in `orchestrated_k3s_cluster_*` namespace metadata rather than local files. Write findings to `docs/plans/audit/findings-style.md` in the 6b format. Change no source file. |
| 6d | high | opus | none | **Done, findings in `79bbb01`.** **Read-only.** Audit the scope for `PUSH-AUDIT.md`'s *Tests* section and the `functional-test-coverage v1` shared block. 6a has already run the suites, so read `docs/plans/audit/verification.md` rather than re-running them. The questions that need judgement: which of the new subcommands and library verbs have no functional coverage, measured against `tools/ci_deploy_test.sh`, which is the only place a `k3s` subcommand runs for real -- the "In this project" note makes that the whole of the functional question; what is skipped and why; whether adversarial and external-API-shape cases are covered for the k3s update API, the GitHub releases API and agent operation payloads; and whether any error path or argument-validation branch reachable from outside the process has no test. Note that #89 already records the absence of integration coverage for `sf_k3s_cluster` and #91 the unexercised `requires_ansible` floor -- per decision 6 those are comments on existing issues, not new findings. Write findings to `docs/plans/audit/findings-tests.md` in the 6b format. Change no source file. |
| 6e | medium | sonnet | none | **Done, findings in `79bbb01`.** **Read-only.** Audit the scope for `PUSH-AUDIT.md`'s *Documentation* section and its three shared blocks: `llm-doc-discipline`, `readme-discipline` and `plan-phase-references`. This is the one lens that **does** read `docs/plans/`. Concretely: is `ARCHITECTURE.md` current for the modules and commands phases 1-5 added (`cluster.py`, `exceptions.py`, `client.py`, the collection) and for the cluster assembly flow phase 3 changed; does `AGENTS.md` state only conventions an agent cannot infer, with no reference material that belongs in `docs/`; has `README.md` grown bullets for new features instead of linking into `docs/`; and run the audit's own grep -- `grep -rn 'phase [0-9]' README.md docs/ --exclude-dir=plans` -- confirming every hit is either absent or carries `<!-- audit-ok: phase-reference -->`. Also confirm all deferred work is listed in a plan file and that every plan this work added is registered in `docs/plans/index.md` with a status from the shared vocabulary. Write findings to `docs/plans/audit/findings-docs.md` in the 6b format. Change no file. |
| 6f | high | opus | none | **Done, findings in `79bbb01`.** **Read-only.** Audit the scope for `PUSH-AUDIT.md`'s *Security review* section and the `path-traversal-review v1` shared block. The threat model the checklist names: cluster names, release-channel data from the k3s update API, tag names from the GitHub releases API, and namespace metadata are all outside-controlled, and agent execute commands run through a shell on the guest -- so trace each of those values from where it enters to every point it reaches a command line, a file path, or YAML written to a guest, and say whether it is quoted or validated at that point. For path traversal the "In this project" note names the two untrusted components that reach filenames and kubectl context names: the cluster name and the namespace. Check the `~/.kube/` merge and the `tempfile` uses in the create path. Separately, check for secret leakage -- node tokens, kubeconfigs and SSH keys reaching logs, progress output or committed files; note that the `key` parameter in `sf_k3s_cluster.py` is `no_log: True` in the argument spec, and verify nothing else re-emits it. Write findings to `docs/plans/audit/findings-security.md` in the 6b format, most severe first. Change no source file. |
| 6g | high | opus | worktree | **Done, `1a667af`..`a44c63d` (17 commits).** Triage and fix. Read all five findings files plus `verification.md`, de-duplicate across them (the same defect will be reported by more than one lens), and apply decision 7's triage: take every `fix`; take `document` only where it is a comment or docstring; take `consider` only where it is a one-liner or a genuine defect in code phases 1-5 added; leave `none`. For each item not taken, say why in one line -- "pre-existing, see #93", "observation about code this plan did not touch", "API behaviour change, filed as #NNN". File issues for the real-but-out-of-scope items, including the typing finding from decision 5, and add occurrences to #82/#89/#91/#93/#94/#96 rather than duplicating them. Then re-run everything 6a ran and confirm it still passes. Write the triage table to `docs/plans/audit/triage.md`. One commit per concern area, not one giant commit; subjects name the change, not the audit. |
| 6h | low | -- | none | **Done, this commit.** **Management commit, no sub-agent.** Close out phase 6 and the master plan in this phase's own pull request -- per the `plan-push-audit-phase` shared block the push-audit phase records no `Merged` cell, because it cannot know its own merge commit. Set phase 6's `Status` to `Complete`, set the master plan's row in `docs/plans/index.md` to `Complete`, and fill in the master plan's Future work and "Bugs fixed during this work" sections from `triage.md`. Confirm the Success criteria section's claims are all now true, and correct any that are not. Commit subject: `Close out the library API plan.` |

## What running the phase found

Five lenses produced 62 numbered findings. Nine duplicate groups
collapsed 24 of them into 9 rows; 23 rows were taken, 33 declined, 7
issues filed and 6 comments added to existing ones. The suite went from
461 tests to 492. Full detail is in `docs/plans/audit/`.

### The Isolation column was not followed, deliberately

The step table asks for `worktree` isolation on 6a and 6g. All eight
steps in fact ran in this phase's own worktree. An isolated worktree
would have put their commits and their findings files on a different
branch from the one that becomes the pull request, which is the opposite
of what those two steps are for. The column should have read `none`
throughout, with the read-only constraint carried by the briefs -- which
is where it was carried, and it held: the five lenses modified nothing
but their own findings file.

### The scope is a point in history, and a worktree is not

This phase's worktree was branched from `develop`, which by then carried
744 insertions across 10 files that postdate the scope boundary
`eb248bd` -- the `node-customisation` plan's per-role sizing work, which
has its own push-audit phase. Reading the files as they stand therefore
over-reads the scope, and four of the five lenses were given a brief
that said "read `scope-files.txt`" without saying "read the files as
they stood at `eb248bd`". Only the style lens guarded deliberately, by
checking every finding against `git show eb248bd:<path>`; the other
three were lucky in that the code they cited happened to predate the
drift.

No whole finding was invalidated, but one was halved and two provenance
claims were wrong. The lesson generalises past this plan: the `Merged`
column fixes *which diffs* the audit reads, and a later step still has
to pin *which revision* of each file it reads them against. A future
push-audit brief should name the boundary commit, not just the file
list. That is a candidate correction to the `plan-push-audit-phase`
shared block in `shakenfist/development`.

### Two success criteria are not met, and the master plan now says so

Nothing has run the collection against a real Shaken Fist API, because
no CI tier installs Ansible ([#89]). And phase 2's `make_client()` has
no functional coverage either ([#102]) -- a gap the master plan's
criteria did not think to ask about, because phase 2's deliverable is
not a verb. Both are recorded against the criteria themselves rather
than ticked.

[#89]: https://github.com/shakenfist/client-python-k3s/issues/89
[#102]: https://github.com/shakenfist/client-python-k3s/issues/102

### The audit's own verification had a blind spot

The Ansible module harness runs as a script, so the repository root is
never on `sys.path` and those tests import the *installed* package. A
bare `stestr run` passed against the unredacted code of the token leak
while the fix was in progress; only `tox -epy3`, which reinstalls,
caught it. Filed as
[#106](https://github.com/shakenfist/client-python-k3s/issues/106),
because a security fix appearing verified when it is not is the worst
shape this failure mode can take.


## Risks and mitigations

| Risk | Mitigation |
|------|------------|
| The audit becomes a rewrite. Six agents against 18,000 insertions will generate far more suggestions than this phase should act on, and every one arrives phrased as a defect. | Decision 7's triage, and the rule that audit agents cannot edit. The exit condition is "no `fix` items outstanding", not "no agent had anything left to say". |
| An agent audits the wrong diff -- the whole of `73bd499..develop`, or only phase 5 -- and reports a clean result for work it never read, or findings that belong to another plan. | 6a writes the file list and the per-merge diffs once, and every audit brief reads that file rather than deriving a range. A finding naming a file outside `scope-files.txt` is out of scope by construction, which is checkable in 6g. |
| Build verification passes or fails for reasons local to a working copy rather than to the repository, as it already did once in finding 8. | Decision 8: 6a clones fresh and reports the path. The fossil that caused it is gone, and the sdist artefact guard added in phase 5 would catch a committed equivalent. |
| The phase finds a genuine latent break -- a shell injection through a cluster name, or a 3.10 construct on the 3.7 floor -- late, after five phases have shipped and `v0.2.0` is public on two indexes. | This is the phase's purpose rather than a risk to avoid, but the response is bounded: fix it, and release a patch version. The 403 recovery in finding 5 establishes that re-running one publishing job is cheap; a bad release is fixed forward, not by withdrawing 0.2.0, which neither index permits. |
| Six read-only agents plus a fix step is a lot of parallel work against one worktree, and a findings file written by two agents at once would be lost. | Each agent writes its own `findings-<lens>.md`; none writes a file another writes. 6g is the only step that edits source, and it runs after all five have finished. |
| The audit's own output -- five findings files and a triage table under `docs/plans/audit/` -- becomes permanent clutter that no later reader needs. | Keep it. It is the evidence for the judgements 6g made, it is what makes "we audited this" checkable rather than asserted, and it is the record a future phase-6 of another plan can model itself on. It lives under `docs/plans/`, which is already where plan history belongs. |

## Definition of done

Each of these is checkable, and most are one command:

- Every section of `PUSH-AUDIT.md` has a corresponding
  `docs/plans/audit/findings-*.md`, and every section of the checklist
  is answered in one of them -- including the ones whose answer is
  "nothing found".
- `docs/plans/audit/scope-files.txt` lists exactly the union of
  `git diff --name-only <merge>^1 <merge>` over the ten merges in
  decision 1, and `wc -l` reports 74.
- `docs/plans/audit/triage.md` accounts for every finding in every
  findings file, each with an action and -- where not taken -- a
  one-line reason.
- No finding classified `fix` is outstanding.
- `tox -epy3`, `tox -eflake8`, `pre-commit run --all-files` and
  `tools/check-wheel-build.sh` all pass, recorded in
  `verification.md` and re-confirmed after 6g's edits.
- `pip install -e .` succeeds in a fresh venv alongside
  `shakenfist-client`, in a fresh clone, and `sf-client k3s --help`
  lists the subcommands.
- `python3 -c 'import shakenfist_client_k3s'` succeeds and does no
  network access.
- `grep -rn 'phase [0-9]' README.md docs/ --exclude-dir=plans` returns
  nothing without an `audit-ok` marker.
- Every issue 6g filed is linked from either the master plan's Future
  work section or the triage table, and the six pre-existing issues
  are untouched except for added occurrences.
- The master plan's phase 5 row reads `Complete` with `eb248bd` and
  `v0.2.0`; phase 2's row names five merges; phase 6's row reads
  `Complete` with no `Merged` cell; and `docs/plans/index.md`'s row for
  the plan reads `Complete`.
- The master plan's phase 6 row and the "In this project" note under
  its Execution table both describe the scope as the union of the
  recorded merge commits diffed against their first parents, so a
  reader who never opens this file cannot mistake it for a range
  ending at `develop` (finding 1).

## Back brief

Before starting, confirm:

1. That the ten merges in decision 1 are the right scope, and in
   particular that excluding the other two master plans' interleaved
   work is correct rather than convenient.
2. **Decision 5**, the typing finding. This declines a stated
   requirement of a shared block for the whole of a five-phase plan's
   output. If the preference is to adopt mypy and annotate, that is a
   master plan of its own and this phase should say so and file it,
   not do it.
3. That `docs/plans/audit/` is the right home for the findings files,
   and that they are meant to be committed rather than discarded once
   triaged.

Gate before 6g edits anything: the triage table is cheap to produce and
expensive to redo once commits exist on top of it, so 6g should present
its triage -- what it will take and what it will decline -- before it
starts changing files.
