# Node customisation phase 4: push audit

## Prompt

Before responding to questions or discussion points in this document,
read `PUSH-AUDIT.md` end to end, the master plan
[PLAN-node-customisation.md](PLAN-node-customisation.md) (especially
its Execution table and its `Merged` column), and the "What running the
phase found" section of
[the library API plan's push audit](PLAN-library-api-and-collection-phase-06-push-audit.md).
That audit is the only previous push audit in this repository. Its
lessons are taken up below rather than repeated.

## Planning effort

Medium. The audit pattern is already established in this repository,
and the scope is three merges. The judgement is in survey finding 2:
which revision of the code to audit, given that a sibling audit has
already rewritten part of it.

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

* Re-auditing what #107 already swept over this plan's code (survey
  finding 2).
* Pre-existing issues: #82, #93, #96, #97, #100, #101, #102, #104,
  #105. A lens that rediscovers one comments on it; it does not
  re-raise it.
* Code this plan did not add, unless a lens finds that this plan's code
  broke it.

## What the survey found

The master plan's phase 4 row said "Run `PUSH-AUDIT.md` over the
accumulated diff of phases 1-3 against `develop`". Since all three
phases have merged, a diff against `develop` is empty. That is exactly
the trap the shared block describes. The row was corrected at source in
this planning commit to name the three merges. Nothing else in the
master plan was wrong.

### 1. The scope is three merge commits

| Phase | Merge | Non-plan diff |
|---|---|---|
| 1. Per-role sizing | `ddb1f3b` (#92) | 12 files, +793 / −28 |
| 2. k3s configuration pass-through | `b791364` (#98) | 10 files, +1776 / −60 |
| 3. Live validation | `51ff6f4` (#108) | 2 files, +330 / −17 |

These come from `git diff --shortstat <merge>^1 <merge> -- . ':!docs/plans'`.

Their union is 15 files outside `docs/plans/`, plus 5 plan files:
* `docs/library-api.md`, `docs/testing.md`, `docs/usage.md`;
* `shakenfist_client_k3s/__init__.py`, `cluster.py`, `exceptions.py`;
* `shakenfist_client_k3s/tests/cli_contract/create.txt`, `fakes.py`,
  `test_cli_contract.py`, `test_cluster.py`, `test_commands.py`,
  `test_exceptions.py`, `test_library_api.py`, `test_progress.py`;
* `tools/ci_deploy_test.sh`.

That is roughly a sixth of the library API audit's scope.

### 2. A sibling audit has already rewritten part of this code

Since `51ff6f4`, #107 (the library API plan's push audit) has changed
12 of those 15 files, +1435 / −349. Two of its commits were about this
plan's code specifically:

* `2aba0e6` merged #98 into the audit branch and swept that audit's
  rules over what #98 added:
  * `K3sConfigError` and `UnsupportedReleaseError` now derive the
    lifted `_ReasonedK3sException` base;
  * `_k3s_config_commands()` writes through the shared `heredoc()`
    helper, which quotes the path and refuses a body that ends its own
    heredoc;
  * a "phase 3" reference in `docs/usage.md` was replaced with a link
    to the master plan;
  * a test asserting that a hostile `api_address_floating` raises now
    asserts that `yaml.safe_dump()` makes the attack impossible by
    construction.
* `948cfba` merged #108. It changed no code.

This is the opposite of the library API audit's lesson. There, the
worktree had moved *ahead* of the audit's scope, so lenses had to read
files as they stood at the scope boundary. Here the scope's own code
was rewritten after it merged. A lens reading it at `51ff6f4` would
audit code that no longer exists, and would re-raise defects #107
already fixed. So the merges define *which code* is in scope, and the
lenses judge that code *as it stands at this worktree's base,
`3e84907`*. That commit is the named revision the library API audit
asked future briefs to carry.

### 3. Deferred work is recorded, with one gap

The master plan's Future work lists six items:
* base image choice;
* extra NICs;
* extra disks;
* per-invocation `expand-workers` overrides;
* Longhorn's `defaultReplicaCount`;
* surfacing the new options in the `shakenfist.k3s` collection.

No issue tracks the last of these. It is not a someday item: 33fl's CI
runner plan cannot adopt the collection until it is done.

Phase 3 also found that the homelab OpenStack-Helm prototype's notes
are now wrong in two places: the taint opt-out, and Longhorn's replica
count. Those notes live in a private repository, and were reported to
the operator rather than edited. The master plan does not record this
yet.

### 4. Verification baseline

At `3e84907`:
* #108's merge queue run (37272761564) passed;
* #107's (37285368223) passed;
* #107's own record says 565 tests pass.

Step 4a re-runs everything rather than trusting those runs.

## Decisions

1. **The scope is the union of the three merge diffs, judged at
   `3e84907`.** Survey finding 2 gives the reason. Step 4a writes the
   file list and the three generating commands into the audit
   directory. Every lens is told both the merges and the revision to
   read.

2. **The diffs are not committed.** The library API audit committed
   1.2 MB of diffs and then needed a `MANIFEST.in` prune to keep them
   out of the sdist. These three diffs regenerate from one line each,
   and since the code has since changed, a reader checking a finding
   wants the current file anyway. The audit directory holds the scope
   list, the generating commands, the verification record, the findings
   and the triage.

3. **The audit lives in `docs/plans/audit-node-customisation/`.**
   `docs/plans/audit/` already holds the library API audit, and its
   README describes that audit alone. A sibling directory keeps each
   audit's evidence readable on its own.

4. **Four lenses, not five.** Code quality and style are one lens here.
   The scope is a sixth of the size, both sections read the same three
   Python files, and splitting them buys a second full read of
   `cluster.py` for little. Tests, documentation and security stay
   separate, because each asks a different kind of question. Security
   matters most: phase 2 writes caller-supplied YAML onto every node
   through a shell, and it gets a lens of its own with that as its
   brief.

5. **Lenses are read-only and run in parallel. All fixes happen in
   4f.** This keeps the library API audit's decision 4 and its reason:
   an agent that both finds and fixes cannot be trusted to report what
   it chose not to fix.

6. **Isolation is `none` throughout.** The library API audit found that
   worktree isolation put commits on the wrong branch, and recorded
   that the column should have read `none`. The read-only constraint
   lives in the briefs.

7. **Triage uses `fix` / `document` / `consider` / `none`**, with the
   same exit rule as last time:
   * `fix` gates the phase;
   * `document` is taken when it is a comment or docstring;
   * `consider` is taken when it is a one-liner or a real defect in
     code these phases added;
   * `none` is informational.

   Typing (#100) is not re-raised. That audit's decision 5 settled it
   repository-wide, and this plan's code is no different.

8. **The collection follow-on gets an issue.** Survey finding 3. It is
   filed in 4g, linked from Future work, and cross-referenced to 33fl's
   CI runner plan. The prototype notes become a Future work bullet that
   names the repository. They get no issue here, because the work lives
   elsewhere.

   This is the decision most likely to be argued with. A reviewer could
   say the collection options are this plan's unfinished business, not
   a follow-on, since the plan's own mission names "both the CLI and
   the `Cluster` library API" and the collection is the third surface.
   But the mission does not name the collection. Phase 5 of the library
   API plan explicitly chose not to wait for this one. And the change is
   additive to a published argument spec. It belongs in a release of
   the collection, not in an audit.

## Step plan

| Step | Effort | Model | Isolation | Brief for sub-agent |
|------|--------|-------|-----------|---------------------|
| 4a | medium | sonnet | none | Produce the audit's inputs and run its mechanical half, in `docs/plans/audit-node-customisation/`. (1) `scope-files.txt`: `for m in ddb1f3b b791364 51ff6f4; do git diff --name-only $m^1 $m; done \| sort -u`. Expect 20 lines, 5 of them under `docs/plans/`. (2) `README.md`: what the directory is (the push audit in `PLAN-node-customisation-phase-04-push-audit.md`), the three merges with their phase names, the generating loop, the instruction "judge the code at `3e84907`, not at the merge" with one sentence on why (survey finding 2), and that the diffs are deliberately not committed (decision 2) with the `git diff <m>^1 <m>` command to regenerate each. (3) `verification.md`: the exact command and the result summary for each of `tox -epy3` (test count), `tox -eflake8`, `pre-commit run --all-files`, `tools/check-wheel-build.sh` and `python3 -c 'import shakenfist_client_k3s'`, all run in this worktree. Then the build verification from `PUSH-AUDIT.md`: `git clone` this worktree's HEAD into the session scratchpad, make a fresh venv there, `pip install shakenfist-client` and `pip install -e .`, and run the import check, recording the clone path. Record failures as failures; do not fix anything. Unit tests and local commands only; no cluster. Commit subject: `Record the audit scope and verification run.` |
| 4b | high | opus | none | **Read-only. Write only `docs/plans/audit-node-customisation/findings-code-quality-style.md`.** Audit `PUSH-AUDIT.md`'s *Code quality* and *Style conformance* sections, including the `comment-proportion v1` and `python-version-discipline v1` shared blocks and the "In this project" notes, over the Python and shell files in `scope-files.txt` (skip `docs/`). Read `README.md` in that directory first. Use `git diff <m>^1 <m>` for the three merges to locate what this plan added, then judge that code **as it stands in this worktree at `3e84907`**, because #107 has since rewritten parts of it (survey finding 2 of `docs/plans/PLAN-node-customisation-phase-04-push-audit.md`). Do not re-raise what `2aba0e6` already did: the exception base class, the `heredoc()` helper, or the phase reference. Specific questions: duplication between the per-role sizing code (`validate_node_sizes`, `node_size`, `create_instance`) and the k3s config code (`validate_k3s_config`, `read_k3s_config`, `_k3s_config_commands`), and between those and pre-existing helpers; whether `PLUGIN_OWNED_*` keys and their comments are still accurate against the plugin's own writes; anything that breaks the `>=3.7` floor (no walrus, no `match`, no 3.8+ stdlib); 120-column wrap, quote style, Click conventions in `__init__.py`; comment proportion in `cluster.py`'s new blocks and in `tools/ci_deploy_test.sh`, whose phase 3 comments are long by design, so judge whether each earns its length. Typing is out of scope (#100). For each finding give: file:line at `3e84907`, the claim, a proposed `fix` / `document` / `consider` / `none` rating, and the evidence. End with an explicit "nothing found" for any checklist item that came back clean. |
| 4c | high | opus | none | **Read-only. Write only `docs/plans/audit-node-customisation/findings-tests.md`.** Audit `PUSH-AUDIT.md`'s *Tests* section and the `functional-test-coverage v1` block over the scope (read `README.md` there first; judge code at `3e84907`; read `verification.md` rather than re-running suites). The central question, per the block: for each user-visible behaviour these three phases added, which functional test in `tools/ci_deploy_test.sh` would have failed before it and passes after? The behaviours are: the six sizing flags, `--server-config` and `--agent-config`, `read_k3s_config()`, the owned-key refusal, the release-floor refusal (`UnsupportedReleaseError`), servicelb disabled under MetalLB, the default taint, the zero-worker exception, and `expand-workers` reusing recorded config. Phase 3 deliberately left the zero-worker exception without live coverage (its decision 2). Say whether that still holds up, and name any other gap. For example: does anything run a create that the release floor refuses, which would be cheap because it fails before building anything? Also check the unit tests mock the boundary (the Shaken Fist client, the agent) rather than the code under test; flag adversarial cases missing around caller YAML (non-mapping, nested structures, unicode, a key with a trailing `+`); and say what is skipped and why. Do not re-raise #101 or #102; comment-worthy rediscoveries go in a separate "existing issues" list. Same finding format as the other lenses. |
| 4d | medium | sonnet | none | **Read-only. Write only `docs/plans/audit-node-customisation/findings-docs.md`.** Audit `PUSH-AUDIT.md`'s *Documentation* section and its `llm-doc-discipline`, `readme-discipline` and `plan-phase-references` blocks. This lens does read `docs/plans/`. Read `README.md` in the audit directory first and judge files at `3e84907`. Questions: does `docs/usage.md` state every user-visible behaviour these phases added, and state each fact once? Check the release floor `v1.21.1+k3s1`, the owned-key list against `PLUGIN_OWNED_SERVER_KEYS` / `PLUGIN_OWNED_AGENT_KEYS` in `cluster.py`, the three files on the node and their order, the taint and its zero-worker exception, and the sizing floor. Does `docs/library-api.md` match `Cluster.create()`'s real signature and the exceptions table match `exceptions.py`? Is `docs/testing.md` accurate about what the merge tier asserts? Should `ARCHITECTURE.md` or `AGENTS.md` change? The phases added no module or command, so probably not, but check that the cluster assembly description still holds now that config files are written before each installer. Are there any `phase <number>` references outside `docs/plans/`? Is deferred work recorded (survey finding 3 already names the collection follow-on and the prototype notes, so confirm rather than rediscover)? Do the master plan's Execution table, `docs/plans/index.md` and the four phase plans agree with each other? Same finding format. |
| 4e | high | opus | none | **Read-only. Write only `docs/plans/audit-node-customisation/findings-security.md`.** Audit `PUSH-AUDIT.md`'s *Security review* section and the `path-traversal-review v1` block over the scope (read `README.md` there first; judge code at `3e84907`). The threat model, specific to these phases: `--server-config` and `--agent-config` content is caller-supplied YAML that is re-serialised and written to every node through an agent command that runs in a shell; it is also stored in namespace metadata and printed by `k3s show`. Trace it from `read_k3s_config()` through `validate_k3s_config()` to `_k3s_config_commands()` and `heredoc()`, and answer each of these. Can any key, value, or YAML construct (tags, anchors, multi-document, binary, very large input, a value containing the delimiter line) reach the guest shell unquoted or end the heredoc? Can a caller use a key the plugin does not own to defeat the plugin's own guarantees? For example `disable` to remove something the plugin installs, `data-dir` aliases, `etcd-*` / `datastore-*`, `kubelet-arg` / `kube-apiserver-arg` that weaken auth, or `token` through some alias. Decide for each whether it is a privilege the caller already has (they own the cluster) or a real escalation. Can a config file path from the CLI be abused (symlinks, special files)? Does anything new leak the node token or kubeconfig: progress output, exception messages that echo config, or `tools/ci_deploy_test.sh`'s failure diagnostics? Check `release` strings from the k3s update API reaching `check_k3s_release()` and any message. Do not re-raise what `2aba0e6` fixed. Same finding format, and say plainly where something is safe by construction and why. |
| 4f | high | opus | none | Triage and fix. Read the four findings files and `verification.md`, then de-duplicate across them. Re-confirm every finding at the current HEAD before acting on it, since a finding about code that has moved is not a finding. Apply decision 7. Write `docs/plans/audit-node-customisation/triage.md` as a table: finding ids, rating, taken or declined, and one line of why, or the issue number. Make each taken fix as its own commit with a subject naming the change, not the audit, and the usual verification (`tox -epy3`, `tox -eflake8`, `pre-commit run --all-files`, import check) before each commit. For any test added to guard a fix, break the fix on purpose and confirm the test fails, and record the mutation in `triage.md`. File issues for declined items worth keeping, using the existing issue style, and comment on existing issues a lens rediscovered rather than filing duplicates. A fix to `tools/ci_deploy_test.sh` cannot be verified without a cluster: say so in its commit, and the management session dispatches the merge tier before the pull request opens. Commit subjects: one per fix, plus `Record the audit triage.` |
| 4g | low | — | none | **Management session, no sub-agent.** Close out phase 4 and the master plan in this phase's own pull request. Set phase 4's `Status` to `Complete`, with no `Merged` cell, and set the master plan's row in `docs/plans/index.md` to `Complete`. Fill in the master plan's "Bugs fixed during this work" and Future work from `triage.md`. File the collection follow-on issue and link it from Future work (decision 8). Add the prototype-notes bullet. Add a "What running the phase found" section to this file, with every figure derivable from `triage.md`. Check the master plan's Success criteria one by one, and correct any that are not true rather than ticking them. Commit subject: `Close out the node customisation plan.` |

The management session reviews each step against the files, not the
sub-agent's summary. 4b-4e run concurrently once 4a is committed. Their
findings files are committed together, as `Record what the four audit
lenses found.`, before 4f starts.

## Risks and mitigations

1. **A lens audits the wrong revision.** That was the library API
   audit's near miss, inverted.

   Mitigation: every brief names `3e84907` and the reason. When
   reviewing, the management session spot-checks two findings per file
   against the worktree, and confirms none cites code that `2aba0e6`
   replaced.
2. **The lenses re-raise #107's fixes, or pre-existing issues.**

   Mitigation: the briefs list both, and 4f's re-confirmation at HEAD
   drops anything already fixed.
3. **Triage grows the phase.** Four agents pointed at 2,900 insertions
   will produce more than the phase should act on.

   Mitigation: decision 7's exit rule. The management session reviews
   `triage.md`'s taken column before 4g, and asks whether each item is
   a defect in this plan's code or an observation about code that was
   already there.
4. **A fix to the functional script cannot be verified locally.**

   Mitigation: 4f says so in the commit. The management session
   dispatches the merge tier on the branch before opening the pull
   request, as phase 3 did.

## Definition of done

* `docs/plans/audit-node-customisation/` contains these files and no
  `diffs/` directory:
  * `README.md`;
  * `scope-files.txt` (20 lines);
  * `verification.md`;
  * the four `findings-*.md` files;
  * `triage.md`.
* Every row of `triage.md` is either taken, with a commit, or declined,
  with a reason or an issue number. A finding with neither does not
  exist.
* `tox -epy3`, `tox -eflake8`, `pre-commit run --all-files` and
  `python3 -c 'import shakenfist_client_k3s'` pass at the branch head.
* `git grep -n -i 'phase [0-9]' -- README.md docs ':!docs/plans'` finds
  nothing that is not marked `audit-ok: phase-reference`.
* An issue exists for surfacing the sizing and config options in the
  collection, and the master plan's Future work links it.
* The master plan's Execution table shows phase 4 `Complete` with an
  empty `Merged` cell. Its row in `docs/plans/index.md` reads
  `Complete`.
* If 4f changed `tools/ci_deploy_test.sh`, a `workflow_dispatch` run of
  the merge tier on this branch passed, and its URL is in the commit or
  in `triage.md`.

## Back brief

Before executing any step of this plan, back brief the operator on
your understanding of the plan and how the work you intend to do
aligns with it. In particular, confirm decision 1: auditing at
`3e84907` rather than at each merge. That decision is the one that
determines what every lens reads.
