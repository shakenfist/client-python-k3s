# Lens 4b findings: code quality and style

Returned as text by the step 4b lens (opus, high effort), as the plan's
decision 5 asks, and saved here by the management session. The
management session confirmed CQ-1 (`cluster.py:753`, and `:2788`, which
blames to `916b6d1`, inside phase 2's merge), CQ-3
(`ci_health_signals.py:822`), CQ-8 (`__init__.py:524-532`) and CQ-10
(`cluster.py:2469-2478`) against the worktree before saving.

Scope: the Python and shell files in `scope-files.txt`, judged at
`bd9bead`. The worktree's HEAD differs from `bd9bead` only in plan
files, so the line numbers hold at both. Every cited line was checked
with `git blame` against `git rev-list <m>^1..<m>` for the three merges
and for `243a91c`. Lines from #121 or earlier work are left out, or
marked as backlog.

Summary: 17 findings.

| Ids | Rating | Subject |
|---|---|---|
| CQ-1 to CQ-4 | fix | plan references added to code |
| CQ-5 to CQ-8, CQ-11, CQ-16 | document | comment proportion, stale or repeated prose |
| CQ-9, CQ-10, CQ-12, CQ-13 | consider | small duplication, naming, `cluster.py`'s seams |
| CQ-14, CQ-15, CQ-17 | none | informational |

## Findings

### CQ-1 (fix): plan references added to package code

- `shakenfist_client_k3s/cluster.py`: 364-365, 387-388, 753, 845-846,
  4347-4348 (phase 1); 1048-1049, 1072, 1085-1086, 1134-1135,
  1307-1308, 1312-1313, 1345, 2747-2748, 2788, 2812, 2819, 2831-2832,
  3201-3202, 3840-3841, 4260-4261, 4355-4356, 4846-4847 (phase 2).
- `shakenfist_client_k3s/tests/fakes.py:328`, "which is why the plan's
  live run exists" (phase 1).
- `plan-references-in-code v1`: a plan reference a diff adds to code is
  "a finding to fix before pushing".
- Six do not even name the plan: 753 "decision 6", 1072 "survey finding
  9", 2788 "decision 4", 2812 "decision 3", 2819 and 2831 "decision 5".
- Most already give the reason beside the citation, so the fix is to
  delete the parenthetical. Three use the citation in place of the
  reason:
  - 4347-4348, "which decision 8 of the cumulative health signals phase
    1 plan accepts": write that a few API calls are negligible against
    a 30-second budget.
  - 3201-3202, "nothing waited for either until decision 8 of
    PLAN-…phase-02…": history; delete it.
  - 753, "decision 6 reports it as None rather than as 0": the clause
    before it already gives the reason.
- Backlog, not findings: the pre-existing references at `cluster.py`
  5-6, 261-262, 1366-1371, 1862-1863, 2902-2917, 3475, 3501, 3736,
  3884, 4075-4077, 4519, 4533, 4862, 4913, and `__init__.py:266`
  (#121).

### CQ-2 (fix): plan references added to test docstrings and comments

- `tests/test_cluster.py`: 334, 948, 4152, 4285, 4692-4693, 4761,
  4766-4767, 4991, 5046, 6407, 6420, 6963, 6984.
- `tests/test_library_api.py`: 312-313.
- `tests/test_ci_health_signals.py`: 6-7, 92, 129, 178, 230, 387, 468,
  599.
- The block covers "test names, fixture descriptions". These cite
  "Decision 9 of the cumulative health signals phase 2 plan", "step 2e
  of that plan", "decision 4a" and so on, where they could state the
  property under test.
- `test_cluster.py:499` and `:507` ("step 3a", "Step 3c") blame to
  `00109a6` but are a moved docstring that existed at
  `00109a6^1:434,442`: backlog. No test method names carry plan
  references.

### CQ-3 (fix): `tools/ci_health_signals.py` names its steps by plan decision, including in the public CI log

- The whole file is from `bd9bead`.
- Module docstring: 13, 19-22, 36. Comments and docstrings: 68, 80,
  105, 137, 180, 207, 213, 257, 267, 330, 339, 349-351, 381, 408-411,
  433, 448, 462-467, 478, 505-508, 579-580, 595, 721, 752. Step
  docstrings labelled "4a." to "4f.": 661, 671, 720, 734, 766, 778.
- Printed output: the `say()` calls at 664, 710, 728, 741, 756, 760,
  772 and 789 prefix each line with `4a` … `4f`, and 822 prints "Every
  provoked signal was reported as decision 4 expects."
- A reader of the merge tier's log sees step ids and "decision 4",
  which mean nothing without the phase 3 plan. Fix: name the steps by
  what they provoke (baseline, OOM kill, automatic restart, NotReady,
  etcd snapshot, disk pressure), and drop the docstring citations.
  462-467 and 579-580 ("step 3b … uses to judge the bounds") are
  plan-process notes and can go.
- Changing the printed labels changes the tool's live output, which
  needs a merge tier `workflow_dispatch`.

### CQ-4 (fix): `ci_deploy_test.sh` points at the phase 3 plan

- `tools/ci_deploy_test.sh:778-779` (`bd9bead`): "See
  docs/plans/PLAN-cumulative-health-signals-phase-03-live-validation.md."
  The comment above (773-778) already says what the step does and why
  it runs last. Delete the sentence. Line 9's pointer is pre-existing.

### CQ-5 (document): the `health()` docstring restates `docs/library-api.md`

- `cluster.py:4072-4320`: a 249-line docstring over a body of about
  150 lines (4321-4473).
- These parts earn their length: the "nothing raises" contract
  (4082-4089), the report schema (4124-4192), the `healthy` terms with
  the argument against an "at least one node" term (4256-4275), why
  node-level `healthy` does not take `ready` (4281-4286), and the
  `_require_usable` note (4296-4302).
- These are caller guidance `docs/library-api.md` already holds, often
  word for word, and can shrink to a pointer: the budget (4091-4122,
  same sentence as `library-api.md:410-413`), the `signals` semantics
  (4201-4217, also in `parse_node_signals()`'s docstring at 860-875),
  the caller's baseline rule (4219-4233), the `kubernetes` semantics
  (4235-4254, which itself ends "docs/library-api.md defines each of
  these in full"), and abandoned operations (4304-4319). About 100
  lines.

### CQ-6 (document): the one-budget rationale is written out five times

- `cluster.py:123-136` (the `HEALTH_PROBE_TIMEOUT_SECONDS` comment),
  2551-2568 (`_collect_probe`), 4091-4122 (`health()` docstring),
  4331-4348 (a comment in `health()`'s body), and
  `docs/library-api.md:404-417`.
- Each says: one deadline, every probe submitted first, bounds the
  waiting not the call, serial API round trips, the slowest probe and
  not the sum. 4331-4348 is 18 lines over one statement (4349).
- Keep the constant's comment as the canonical copy; cut the others to
  what is local to them.

### CQ-7 (document): other rationale repeated across sites

- "One command for every node rather than one per node":
  `cluster.py:183-185`, 612-614, 3225-3230 (phase 2).
- "A guessed name is a node never found, two minutes of polling, a
  message that blames the node": `cluster.py:3215-3217` and
  `exceptions.py:358-362`. The `NodeUnnamedError` docstring is 24 lines
  for a 10-line class it calls unreachable.
- "Only probed and error are kept from the probe": 2720-2723 (phase 1)
  and 2751-2754 (phase 2).
- The `create()` comment at 3839-3869 is 31 lines over a two-line call.
  Its first paragraph restates the `create()` docstring (3457-3468),
  and its last restates `await_nodes_ready()`'s (3239-3241). The middle
  paragraph, on ordering, earns its place.
- `nodes_ready_command()`'s and `node_name_for_instance()`'s long
  docstrings were checked and are mostly justified.

### CQ-8 (document): a stale comment in `_render_signals()` duplicates `_utc_iso()`'s docstring

- `shakenfist_client_k3s/__init__.py:524-531`. Phase 2 moved the
  timestamp handling into `_utc_iso()` (571-588), whose docstring
  explains the overflow, but left the old 8-line paragraph above
  `booted = _utc_iso(...)` at 532, which no longer does any of it.
  Delete it, or cut it to a pointer.

### CQ-9 (consider): `_render_kubernetes()` re-implements the nested `counted()`

- `__init__.py:682-683` against 502-507. Phase 2 hoisted `_is_count`,
  `_text` and `_count` to module level but left `counted()` nested in
  `_render_signals`, then wrote the same logic inline. Hoist it as
  `_counted` and call it at 682. One line.

### CQ-10 (consider): "not run" builders duplicate shapes defined elsewhere

- `_new_probe` (`cluster.py:2469-2478`, phase 1) and `_unprobed`
  (2665-2674, pre-existing) build the same eight-key dict
  independently. A key added to one and not the other breaks the "same
  keys in every outcome" rule both docstrings state.
- `_unprobed_signals` (2703-2706, phase 1) reproduces exactly what
  `parse_node_signals(None, role)` returns.
- Each fix is about one line.

### CQ-11 (document): stale pointers after phase 2's refactor

- `cluster.py:965` says the name match is done "(see remove_worker())".
  It is defined by `node_name_for_instance()` (557), which `health()`,
  `remove_worker()` and `await_nodes_ready()` all use.
- `cluster.py:2655` says "Two callers". After phase 2, `_unprobed` is
  called from one branch, twice (4394-4395).

### CQ-12 (consider): `NODE_SIGNAL_INTEGER_RE` is shared but named for signals only

- `cluster.py:406-420`, reused by `_kubernetes_integer` (1210-1218) for
  `restartCount`. The name, and the comment's "every one of them is a
  kernel or systemd counter", describe only node signals. A rename
  touches tests and `mutation-check.py` strings; a sentence in the
  comment would do.

### CQ-13 (consider): `cluster.py`'s length, and the seams a split would follow

Per decision 8, seams named, no split proposed.

1. **Probe commands and their parsers: clean in code.** About 790
   lines, module-level and pure (123-143, 356-443, 539-555, 694-1358),
   importing only stdlib. What crosses: the shell-quoting rule block
   (445-468) cited as "rule 1 above" from five docstrings and two test
   cases; about 100 test references to `cluster_module.<symbol>`; 36
   `mutation-check.py` entries that target `cluster.py` strings; and
   comment references to `K3S_SERVER_OWNED_KEYS` and the installer.
2. **The readiness wait: clean, with one shared helper.** About 260
   lines (152-195, 557-691, 3192-3269). `node_name_for_instance()` is
   also used by `remove_worker()` and `_kubernetes_from_probe()`, and
   `await_nodes_ready` is a `Cluster` method, so it would become a
   function taking a cluster.
3. **Probe orchestration on `Cluster`: not clean.** About 820 lines
   (2461-2877, 4071-4473) of methods reaching `self.client`,
   `self.reporter`, `get_metadata`, `await_execute` and more. A split
   is a design change, not a move.

Seam 1 is the clean candidate for decision 8's issue. Not this plan's
code: `read_manifests` through `check_k3s_release` (1361-1878) is
another clean seam.

### CQ-14 (none): `tools/ci_health_signals.py` at 827 lines

Just over the 800-line candidate mark. Its seam already exists (pure
judgements at 136-551, live driver at 553-827), and it is a
one-purpose CI tool that changes rarely, so a split does not pay. About
15 `check_*` branches hand-format the same message, and `step_oom_kill`
(695-701) re-implements `poll()`'s give-up message; neither is worth a
commit on its own.

### CQ-15 (none): duplication between the probes is mostly good reuse

The Kubernetes probe reuses `_submit_probe`, `_collect_probe`,
`_unprobed` and `_cannot_answer`. None-when-unread is one pattern,
carried by the key tuples. `_signal_*` and `_kubernetes_*` differ by
design (per-field None against whole-record drop). The tool's copies
of `BOOT_ID_RE` and `UNIT_BY_ROLE` are deliberate, with the reason at
114-121. Nothing wants to move to `primitives.py` or `progress.py`.

### CQ-16 (document): history in the collection module's documentation

`collection/plugins/modules/sf_k3s_cluster.py:392-394` (phase 2):
"Before this release it did not consider Kubernetes at all…". "This
release" has no referent in `ansible-doc`. Overlaps DOC-3.

### CQ-17 (none): a repeated kubeconfig path literal

`--kubeconfig /etc/rancher/k3s/k3s.yaml` is written 12 times in
`cluster.py`, five of them by this plan, following the established
pattern. A constant is a file-wide cleanup outside this scope.

## Checklist: items that came back clean

- **TODO comments:** nothing found in the three diffs' added lines.
- **120-column wrap:** nothing found; flake8 clean per
  `verification.md`.
- **Quote style:** nothing found. Double quotes only where the content
  holds a single quote; no `'''`.
- **Click conventions:** nothing found. No command or option added; the
  renderers write through the reporter.
- **Progress reporter convention:** nothing found.
- **`sf-client` startup:** nothing found; the new top-level imports
  are stdlib only.
- **Cluster state in namespace metadata:** nothing found.
- **Python `>=3.7` floor:** nothing found in the package or in
  `tools/ci_health_signals.py`. The `:=` hits are go-template text.
- **Elapsed time uses `time.monotonic()`:** nothing found.
- **`open()` encoding:** nothing found.
- **Typing (#100):** out of scope.

## Rediscovered pre-existing issues

- #82: the `>=3.7` floor is not verified in CI. The floor check above
  is a reading, not a run.
