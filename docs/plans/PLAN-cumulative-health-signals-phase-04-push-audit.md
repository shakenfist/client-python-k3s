# Cumulative health signals phase 4: push audit

## Prompt

Before responding to questions or discussion points in this document,
read `PUSH-AUDIT.md` end to end, the master plan
[PLAN-cumulative-health-signals.md](PLAN-cumulative-health-signals.md)
(especially its Execution table and its `Merged` column), and the "What
running the phase found" section of
[the node customisation push audit](PLAN-node-customisation-phase-04-push-audit.md).
That is the most recent push audit in this repository, and this plan
follows its shape. Its lessons are taken up below rather than repeated.

## Planning effort

Medium. The audit pattern is established here, and this is its third
run. The judgement is in survey finding 2 (what in the scope is not
this plan's code) and decision 8 (what to do about `cluster.py`'s
length).

Review effort: high for the triage step (4f), which decides what gets
fixed. Medium for the rest.

## Scope

In:

* Running `PUSH-AUDIT.md` over everything the three phases landed. That
  is the union of the three recorded merges below, each diffed against
  its first parent.
* Triage, and fixes for what triage takes.
* Filing issues for what it declines but should not be lost.
* Closing out phase 4 and the master plan in this phase's own pull
  request. Per the `plan-push-audit-phase` shared block, this phase
  records no `Merged` cell.

Out:

* #121's argument validation, which landed between phases 1 and 2 and
  touched 15 of the scope's files (survey finding 2).
* Pre-existing issues: #48, #72, #73, #74, #82, #89, #91, #93, #94,
  #100, #101, #102, #103, #105, #106, #110, #111, #112, #118, #119. A
  lens that rediscovers one lists it separately; it does not re-raise
  it.
* Splitting `cluster.py` (decision 8).
* Code this plan did not add, unless a lens finds that this plan's code
  broke it.

## What the survey found

The master plan's phase 4 row said "Run `PUSH-AUDIT.md` over the
accumulated diff of phases 1-3 against `develop`". All three phases
have merged, so that diff is empty, which is the trap the shared block
describes. The row is corrected at source in this planning commit to
name the three merges. Nothing else in the master plan was wrong.

### 1. The scope is three merge commits

| Phase | Merge | Non-plan diff | Plan diff |
|---|---|---|---|
| 1. Agent-read signals | `78c9df1` (#116) | 10 files, +2777 / −163 | 3 files, +595 / −26 |
| 2. Kubernetes-read signals | `00109a6` (#122) | 13 files, +4109 / −262 | 3 files, +585 / −5 |
| 3. Live validation | `bd9bead` (#124) | 6 files, +1657 / −6 | 3 files, +503 / −3 |

These come from `git diff --shortstat <merge>^1 <merge> -- . ':!docs/plans'`
and the same with `-- docs/plans`.

Their union is 22 files, 5 of them under `docs/plans/`. The other 17
are:
* `ARCHITECTURE.md`, `docs/library-api.md`, `docs/testing.md`,
  `docs/usage.md`;
* `collection/plugins/modules/sf_k3s_cluster.py`;
* `shakenfist_client_k3s/__init__.py`, `cluster.py`, `exceptions.py`;
* `shakenfist_client_k3s/tests/fakes.py`, `module_harness.py`,
  `test_ci_health_signals.py`, `test_cluster.py`, `test_commands.py`,
  `test_library_api.py`;
* `tools/ci_deploy_test.sh`, `tools/ci_health_signals.py`,
  `tools/mutation-check.py`.

That is about 8,500 insertions outside `docs/plans/`, roughly three
times the node customisation audit's scope.

### 2. Other work landed between the phases, and none since

#121 (argument validation, `243a91c`) merged between phases 1 and 2.
It changed 15 of the 22 files, +1956 / −311, including phase 1's code.
#123 (`9df9e8e`) only bumped a workflow action. Nothing has merged
since `bd9bead`, and the worktree's base is `bd9bead`.

So, unlike the node customisation audit, no later commit has rewritten
this plan's code. The lenses judge the code as it stands at `bd9bead`,
which is also where it stands today. The trap here is the other way
round: reading `bd9bead`'s files, a lens will see #121's validation
woven through phase 1's code and could audit it as this plan's. The
merge diffs say which lines are this plan's. #121 had its own review.

### 3. `cluster.py` is 5,196 lines, and this plan added about 1,640 of them

Phase 1 added a net 659 lines to it, and phase 2 a net 979. It was
3,310 lines before phase 1. The `source-file-size v1` block in
`PUSH-AUDIT.md` says a file over about 1,500 lines wants a stated
reason to stay whole. Nobody has written that reason down. Decision 8
says what this audit does about it.

### 4. Deferred work is recorded in the phase plans, not yet in the master plan

The master plan's Future work has five bullets. These are deferred in
the phase plans or found during the work, but are not in it yet:
* phase 3 decision 5's declined provocations: a global OOM, memory
  and PID pressure, a reboot, an unregistered node, and a probe
  failure;
* the unconfirmed cause of `oom_kills` rising by 2 for one container
  (phase 3 *Live results*);
* Mach33Labs/33fl#938, filed during phase 2 against 33fl's tier 1
  health check. It is cross-repository, and belongs under "Bugs fixed
  during this work", which still holds the template's `...`;
* #76, which phase 2 fixed and which also belongs there.

This finding also claimed an unfiled defect: that
`tools/mutation-check.py`'s `'tox'` runner leaves `.tox/py3` holding
the last mutation, which phase 2 had blamed for a local
`SecretsTestCase` failure. **Step 4a disproved it.** After a full run,
and again after `--only` on the `progress.py` tox entry alone, the
installed package matched the tree byte for byte
(`audit-cumulative-health-signals/verification.md`). Phase 2's failure
is better explained by a stale install, which is #106's shape and
already filed. Decision 9, which would have fixed the defect, is
withdrawn.

### 5. Verification baseline

At `bd9bead`:
* #124's merge queue run (37870996780) passed;
* the phase 3 `workflow_dispatch` (37758352711) passed every
  provocation;
* phase 3 recorded 988 tests and 90 mutations, all caught.

Step 4a re-runs everything rather than trusting those runs.

## Decisions

1. **The scope is the union of the three merge diffs, judged at
   `bd9bead`.** Every lens is told the three merges, the revision, and
   that #121's lines are not in scope (survey finding 2). Step 4a writes
   the file list and the generating commands into the audit directory.

2. **The diffs are not committed.** This is the node customisation
   audit's decision 2, for the same reason: each regenerates from one
   line, and the library API audit's 1.2 MB of committed diffs needed an
   sdist prune afterwards.

3. **The audit lives in `docs/plans/audit-cumulative-health-signals/`**,
   a sibling of the two earlier audits' directories.

4. **Four lenses: code quality and style together, tests,
   documentation, and security.** The node customisation audit's
   decision 4 reasoning holds. Code quality and style read the same
   files, so one lens covers both. The other three ask different
   questions. This scope is three times larger, but most of it is in
   two files (`cluster.py` and the tests), and a fifth lens would only
   read those again.

5. **Lenses are read-only, run in parallel, and return their findings
   as text.** The management session saves each findings file after
   spot-checking two or three findings against the tree. The node
   customisation audit found that sub-agents could not write report
   files, and that the briefs should say "return as text". These
   briefs do. All fixes happen in 4f, so an agent that finds a problem
   never also decides not to fix it.

6. **Isolation is `none` throughout.** This holds for the same reason as
   the earlier audits, and the read-only constraint lives in the briefs.

7. **Triage uses `fix` / `document` / `consider` / `none`**, with the
   same exit rule as the last two audits:
   * `fix` gates the phase;
   * `document` is taken when it is a comment or docstring;
   * `consider` is taken when it is a one-liner, or a real defect in
     code these phases added;
   * `none` is informational.

   Typing (#100) is not re-raised.

8. **`cluster.py`'s length is a finding about seams, not a split.** The
   code quality lens names the seams a split would follow, for example
   whether the health probes and their parsers form a module of their
   own, and rates the length `consider`. 4f does not split the file. If
   the lens finds a clean seam, 4f files an issue that names it, and the
   master plan's Future work links that issue.

   This is the decision most likely to be argued with. The block's
   threshold is 1,500 lines, the file is three and a half times that,
   and this plan wrote a third of it. That is a fair argument for
   splitting now. But a split moves most of the file. It would
   invalidate the review of every line it moves, and it would conflict
   with #120, which is open against `cluster.py`. A split belongs in
   its own pull request, with nothing else in its diff, so that review
   can confirm it moved code without changing it. An audit's fixes do
   not belong mixed into that diff.

9. *Withdrawn.* It fixed a mutation-check defect that step 4a showed
   does not exist (survey finding 4).

## Step plan

| Step | Effort | Model | Isolation | Brief for sub-agent |
|------|--------|-------|-----------|---------------------|
| 4a | medium | sonnet | none | Produce the audit's inputs and run its mechanical half, in `docs/plans/audit-cumulative-health-signals/` in the worktree `/srv/kasm_profiles/mikal/vscode/src/shakenfist/client-python-k3s-wt-health-signals`. (1) `scope-files.txt`: `for m in 78c9df1 00109a6 bd9bead; do git diff --name-only $m^1 $m; done \| sort -u`. Expect 22 lines, 5 of them under `docs/plans/`. (2) `README.md`: what the directory is (the push audit in `PLAN-cumulative-health-signals-phase-04-push-audit.md`), the three merges with their phase names, the generating loop, and the instruction "judge the code at `bd9bead`; lines #121 added are not in scope", with one sentence on why (survey finding 2). Also say that the diffs are deliberately not committed (decision 2), and give the `git diff <m>^1 <m>` command that regenerates each. (3) `verification.md`: the exact command and a summary of the result for each of `tox -epy3` (test count), `tox -eflake8`, `pre-commit run --all-files`, `python3 tools/mutation-check.py` (mutation count), `tools/check-wheel-build.sh` and `python3 -c 'import shakenfist_client_k3s'`, all run in this worktree. Then the build verification from `PUSH-AUDIT.md`: `git clone` this worktree's HEAD into the session scratchpad, make a fresh venv there, `pip install shakenfist-client` and `pip install -e .`, run the import check, and record the clone path. Record failures as failures, and do not fix anything. Run unit tests and local commands only, no cluster. Commit subject: `Record the audit scope and verification run.` |
| 4b | high | opus | none | **Read-only. Change no file. Return your findings as text.** Audit `PUSH-AUDIT.md`'s *Code quality* and *Style conformance* sections over the Python and shell files in `docs/plans/audit-cumulative-health-signals/scope-files.txt` (skip `docs/`). That includes the `comment-proportion v1`, `source-file-size v1`, `plan-references-in-code v1` and `python-version-discipline v1` blocks and the "In this project" notes. Read that directory's `README.md` first. Use `git diff <m>^1 <m>` for the three merges to find what this plan added, and judge it as it stands at `bd9bead`. Lines #121 (`243a91c`) added are out of scope. Specific questions. (a) Duplication: between the signals probe, the Kubernetes probe and the pre-existing `api` probe (`_submit_probe`, `_collect_probe`, their parsers, their None-when-unread handling); between `_signal_*` and the Kubernetes parsers; and between `tools/ci_health_signals.py` and existing helpers in `tools/` or the package. (b) `source-file-size`: `cluster.py` is 5,196 lines, and this plan added about 1,640 of them. Per decision 8 of `docs/plans/PLAN-cumulative-health-signals-phase-04-push-audit.md`, name the seams a split would follow, and say whether each is clean (what crosses it). Do not propose doing the split here. `tools/ci_health_signals.py` is 827 lines, so judge it too. (c) Anything that breaks the `>=3.7` floor, in the package *and* in `tools/ci_health_signals.py` if it imports the package. (d) Plan references this plan's diff added to code, comments, docstrings or test names ("phase 3", "decision 4", "step 3b"); `tools/ci_health_signals.py` and `ci_deploy_test.sh` are the likely places. (e) Comment proportion in `cluster.py`'s new blocks, especially the `health()` docstring, which is long by design, so judge whether each part earns its length or belongs in `docs/library-api.md`. (f) 120-column wrap, quote style, and Click conventions in `__init__.py`. (g) TODO comments. Typing is out of scope (#100). For each finding give a short id (CQ-n), file:line at `bd9bead`, the claim, a proposed `fix` / `document` / `consider` / `none` rating, and the evidence. Say "nothing found" explicitly for each checklist item that came back clean. |
| 4c | high | opus | none | **Read-only. Change no file. Return your findings as text.** Audit `PUSH-AUDIT.md`'s *Tests* section and the `functional-test-coverage v1` block over the scope. Read `docs/plans/audit-cumulative-health-signals/README.md` first, judge code at `bd9bead`, and read `verification.md` rather than re-running suites. The central question, per the block: for each user-visible behaviour these three phases added, which functional test would have failed before it and passes after? The behaviours: each `signals` key; each `kubernetes` key, including `oom_killed`, `unmatched_nodes` and `registered: False`; the top-level `healthy` gaining "every node Ready"; `create()` and `expand_workers()` waiting for Ready; `health --strict`; the probe budget and its abandonment; and the collection module's health output. Phase 3 covered six provocations live, and its decision 5 declined five (a global OOM, memory and PID pressure, a reboot, an unregistered node, a probe failure). Say whether each refusal still holds up, and name any cheap one that was missed. Check that the unit tests mock the boundary (the Shaken Fist client, the agent, `HealthClient` in `tests/fakes.py`), not the code under test. Flag missing adversarial cases around agent and `kubectl` output: truncated output, extra fields, non-integer counters, a node name with unusual case, duplicate entries, an empty go-template result. Check that `tools/ci_health_signals.py`'s `check_*` tests fail for the reason they claim. Check whether the mutation entries this plan added in `tools/mutation-check.py` each target a property no other test also pins. Say what is skipped, and why. Lines #121 added are out of scope. List rediscoveries of #101, #102 and #106 separately; do not re-raise them. Finding ids TC-n, in the same format as the other lenses. |
| 4d | medium | sonnet | none | **Read-only. Change no file. Return your findings as text.** Audit `PUSH-AUDIT.md`'s *Documentation* section and its `llm-doc-discipline`, `readme-discipline`, `plan-phase-references` and `diagram-discipline` blocks. This lens does read `docs/plans/`. Read `docs/plans/audit-cumulative-health-signals/README.md` first, and judge files at `bd9bead`. Questions. Does `docs/library-api.md`'s `health()` description match the code, key by key? Check the `signals` and `kubernetes` keys, their None-versus-`[]` rules and the top-level `healthy` terms against `cluster.py`'s `health()` and its docstring schema (around `cluster.py:4148`). Is any fact stated on two pages, or stated differently on two pages, across `docs/library-api.md`, `docs/usage.md`, `ARCHITECTURE.md` and the `health()` docstring? Is `docs/usage.md` accurate about `health` and `health --strict`'s output and exit codes? Is `docs/testing.md` accurate about what the merge tier now asserts? Did `ARCHITECTURE.md`'s changes stay a map, not a reference? Should `AGENTS.md` change: did any convention change? Run `git grep -n -i 'phase [0-9]' -- README.md docs ':!docs/plans'` and report each hit. Is deferred work recorded? Survey finding 4 already lists what is missing from the master plan, so confirm rather than rediscover. Do the master plan's Execution table, `docs/plans/index.md` and the four phase plans agree? Do the phase plans' Definition of done boxes match the tree? Finding ids DOC-n, in the same format as the other lenses. |
| 4e | high | opus | none | **Read-only. Change no file. Return your findings as text.** Audit `PUSH-AUDIT.md`'s *Security review* section and the `path-traversal-review v1` block over the scope. Read `docs/plans/audit-cumulative-health-signals/README.md` first, and judge code at `bd9bead`. The threat model for these phases runs both ways. Outbound: the signals and Kubernetes probe commands run through the agent in a shell on every node. Is any part of them built from a value the plugin did not choose: the cluster name, node names, the etcd snapshot directory from caller k3s config, or the namespace? And can that value reach the shell unquoted? Inbound: everything `health()` reports is parsed from guest output, and a compromised or confused node controls it. Trace the signals and `kubectl` go-template output through their parsers. Can crafted output (embedded tabs or newlines, a forged extra record, a pod name carrying a separator, enormous output, non-UTF-8) forge another node's readings, or inject into `unmatched_nodes` or `oom_killed`? Can it raise an exception that escapes `health()`'s contract, make `health()` slow, or reach a terminal as escape sequences through the CLI or the collection module? Is the shape of #105 (agent results trusted) worse here, or new here? Do not re-raise #105 itself. Secrets: does anything new put the node token, a kubeconfig or caller k3s config into the health report, a progress line, an exception message, `tools/ci_health_signals.py`'s failure output, or the public CI log? `tools/ci_health_signals.py` runs `kubectl` and `sf-client` as subprocesses, and a `sh -c` disk fill on a node: check its argv construction, and whether it could ever point at a cluster other than the one it was given. Say plainly where something is safe by construction, and why. Finding ids SEC-n, in the same format as the other lenses. |
| 4f | high | opus | none | Triage and fix. Read the four findings files and `verification.md`, then de-duplicate across them. Re-confirm every finding at the current HEAD before acting on it, since a finding about code that has moved is not a finding. Apply decision 7. Apply decision 8 to `cluster.py`'s length: no split, and an issue only if a clean seam was named. Write `docs/plans/audit-cumulative-health-signals/triage.md` as a table: finding ids, rating, taken or declined, and one line of why, or the issue number. Make each taken fix its own commit, with a subject naming the change, not the audit. Before each commit, run the usual verification: `tox -epy3`, `tox -eflake8`, `pre-commit run --all-files`, `python3 tools/mutation-check.py`, and the import check. For any test added to guard a fix, break the fix on purpose and confirm the test fails. Prefer adding the mutation to `tools/mutation-check.py`, and record it in `triage.md`. File issues for declined items worth keeping, in the existing issue style. Comment on existing issues a lens rediscovered rather than filing duplicates. A fix to `tools/ci_deploy_test.sh` or `tools/ci_health_signals.py`'s live behaviour cannot be verified without a cluster: say so in its commit, and the management session dispatches the merge tier before the pull request opens. Commit subjects: one per fix, plus `Record the audit triage.` |
| 4g | low | — | none | **Management session, no sub-agent.** Close out phase 4 and the master plan in this phase's own pull request. Set phase 4's `Status` to `Complete`, with no `Merged` cell, and set the master plan's row in `docs/plans/index.md` to `Complete`. Fill in the master plan's "Bugs fixed during this work" (#76, Mach33Labs/33fl#938, and anything from `triage.md`) and its Future work: survey finding 4's items, decision 8's issue if one was filed, and declined items from `triage.md`. Add a "What running the phase found" section to this file, with every figure derivable from `triage.md`. Check the master plan's Success criteria one by one, and correct any that are not true rather than ticking them. Commit subject: `Close out the cumulative health signals plan.` |

The management session reviews each step against the files, not the
sub-agent's summary. 4b to 4e run concurrently once 4a is committed. The
management session saves their findings files and commits them
together, as `Record what the four audit lenses found.`, before 4f
starts.

## What running the phase found

Four lenses produced 37 findings: code quality and style 17, tests 12,
documentation 6, security 2. Six were rated `fix`: four plan-reference
findings (CQ-1 to CQ-4), the missing live check that `expand-workers`
waits for Ready (TC-1), and SEC-1. Triage
(`audit-cumulative-health-signals/triage.md`) has 41 rows after
merging three pairs and adding the unnumbered security finding, the
rediscovered issues and one item for 4g. Of those rows, **24 were
taken** in 22 commits, **5 declined**, **6 informational**, **5
rediscovered issues** and **1 routed to 4g**. Five issues were filed:
#125 (decision 8's seam), #126 (two declined live provocations), #127
(the collection module's health tests), #128 (reading a probe's blob)
and #129 (raw API probe output on the terminal). #105 and #89 got
comments. The suite went from 988 tests to 1005, and
`tools/mutation-check.py` from 90 mutations to 101, all caught.

Because 4f changed the probe templates, `tools/ci_deploy_test.sh` and
`tools/ci_health_signals.py`, the merge tier was dispatched on the
branch at `6f0703c`. The
[run](https://github.com/shakenfist/client-python-k3s/actions/runs/37885956506)
passed first time, and its `Cluster deployment` job took 27 minutes.
The new `health --strict` after `expand-workers` passed with all four
nodes Ready. Every provocation passed, including the new baseline
checks and `ready_since` moving across the stop and start (1791523125
-> 1791523392). `oom_kills` again rose by 2 for one container killed
once, so phase 3's reading was not a one-off.

The most valuable finding was SEC-1, and phase 2's survey had looked
for it and missed it. Shaken Fist returns an agent command's stdout
inline only up to 10 KiB, and stores anything longer as a blob, so on
a large cluster `health()` read the Kubernetes probe as "nothing
registered, nothing killed". The "None, never an empty list, when
unread" rule was applied to every parser and still failed one layer
down, in what "unread" meant.

### Survey finding 4 was wrong, and step 4a caught it

The plan's survey claimed that `mutation-check.py`'s tox runner leaves
the tox venv holding a mutation, and decision 9 planned to fix it. Step
4a's verification run, and a single-entry run by the management
session, both left the venv matching the tree. Decision 9 was withdrawn
before 4f started. Phase 2's failure was most likely a stale install,
which is #106. A survey claim about tooling behaviour should be
reproduced before a decision is built on it.

### SEC-2 was tested against real API servers

No Go template engine runs in the unit tests, so 4f checked the
template change against k3s v1.21.1 and v1.33.5 API servers in local
containers, with forged node and pod statuses. The old templates let a
forged status report a down node as Ready, invent a node, and add OOM
kills; the new ones did none of that, and still listed real kills. The
merge tier run then confirmed that a real cluster reads as before.

### Phase 2's "one prose place" box was not literally true

Phase 2's Definition of done said `oom_killed` and `ready` are defined
in exactly one prose place. The `health()` docstring restated much of
`docs/library-api.md` (DOC-5, CQ-5). 4f cut the docstring from 249
lines to about 190 and pointed it at the docs, but "latest
termination, not a count" is still said in the docstring,
`docs/library-api.md` and `docs/usage.md`. That is recorded here
rather than by editing phase 2's ticked box.

### Lenses returning text worked

The briefs told every lens to return findings as text, which the node
customisation audit recommended. All four did, and the management
session saved each file after spot-checking two to four findings
against the tree and, for SEC-1, against Shaken Fist's source. No lens
changed a file.

## Risks and mitigations

1. **A lens audits #121's code as this plan's.** Phase 1's code at
   `bd9bead` has #121's validation through it, and a lens reading the
   file cannot tell the two apart.

   Mitigation: every brief names #121 and the merge diffs. When the
   management session reviews the findings, it checks each cited line
   with `git blame` and drops those that came from `243a91c`.
2. **The lenses re-raise pre-existing issues.**

   Mitigation: the Scope lists them. 4f re-confirms each finding at
   HEAD, and comments on the existing issue rather than filing a new
   one.
3. **Triage grows the phase.** Four agents pointed at 8,500 insertions
   will produce more than the phase should act on.

   Mitigation: decision 7's exit rule, and decision 8 removes the
   largest temptation. Before 4g, the management session reviews
   `triage.md`'s taken column. For each item it asks whether it is a
   defect in this plan's code or an observation about code that was
   already there.
4. **A fix conflicts with #120.** #120 is open against `cluster.py`
   for the Longhorn chart choice.

   Mitigation: 4f's fixes touch the health code, which #120 does not.
   Whichever lands second rebases. Decision 8 keeps the split, which
   would conflict with everything, out of this phase.
5. **A fix to the functional tier cannot be verified locally.**

   Mitigation: 4f says so in the commit. The management session
   dispatches the merge tier on the branch before opening the pull
   request, as phase 3 did.

## Definition of done

* `docs/plans/audit-cumulative-health-signals/` contains these files
  and no `diffs/` directory:
  * `README.md`;
  * `scope-files.txt` (22 lines);
  * `verification.md`;
  * the four `findings-*.md` files;
  * `triage.md`.
* Every row of `triage.md` is either taken, with a commit, or declined,
  with a reason or an issue number. A finding with neither does not
  exist.
* `tox -epy3`, `tox -eflake8`, `pre-commit run --all-files`,
  `python3 tools/mutation-check.py` and
  `python3 -c 'import shakenfist_client_k3s'` pass at the branch head.
  The mutation count is at least 90.
* `git grep -n -i 'phase [0-9]' -- README.md docs ':!docs/plans'` finds
  nothing that is not marked `audit-ok: phase-reference`.
* The master plan's "Bugs fixed during this work" holds no `...`, and
  names #76 and Mach33Labs/33fl#938.
* The master plan's Future work names phase 3's five declined
  provocations and the `oom_kills` double count.
* The master plan's Execution table shows phase 4 `Complete` with an
  empty `Merged` cell. Its row in `docs/plans/index.md` reads
  `Complete`.
* If 4f changed `tools/ci_deploy_test.sh` or
  `tools/ci_health_signals.py`, a `workflow_dispatch` run of the merge
  tier on this branch passed, and its URL is in the commit or in
  `triage.md`.

## Back brief

Before executing any step of this plan, back brief the operator on
your understanding of the plan and how the work you intend to do
aligns with it. In particular, confirm decision 8: `cluster.py` is not
split in this phase. That decision sets the ceiling on how large 4f
can grow.
