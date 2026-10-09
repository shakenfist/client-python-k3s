# Triage (step 4f)

Step 4f of `PLAN-cumulative-health-signals-phase-04-push-audit.md`: the
decisions on the four lenses' 37 findings, and the commits that acted
on them. Every finding was re-confirmed at the branch head before it
was acted on. None had moved: the branch head differed from `bd9bead`
only in plan files when this step began. Where an earlier fix commit
rewrote a line a later finding named, the later row says so.

Decision 7's exit rule was applied:
* `fix` is taken;
* `document` is taken when it is a comment or a docstring;
* `consider` is taken when it is a one-liner, or a real defect in code
  these phases added;
* `none` is informational.

Decision 8 was applied to `cluster.py`'s length: no split. The code
quality lens named one clean seam, so it is filed as #125.

## De-duplication

* **DOC-1 and TC-9** are the same stale hedge about `NRestarts`. They
  are one row. The fix swept for every hedge of that class outside
  `docs/plans/`, and found three more than the lenses named:
  `tools/ci_health_signals.py`'s module docstring ("whether the plan
  guessed systemd ... right"), `check_oom_kill()`'s docstring (the
  `oom_kill` claim "never observed"), and a test docstring in
  `test_ci_health_signals.py` saying the same.
* **CQ-16 and DOC-3** are the same release-history note in the
  collection module's `RETURN`. They are one row. DOC-3 also named two
  other pages, handled in the same row.
* **TC-4 and TC-5** are two declined live provocations for the same two
  readings, filed together as #126.
* **CQ-5 and CQ-6** overlap on two paragraphs of `health()`'s
  docstring. They are separate rows and commits: CQ-6's commit removed
  the budget paragraphs, and CQ-5's handled the rest.
* **CQ-1** named `cluster.py:1048-1049` and `:1072`. Those comments
  were rewritten by the SEC-2 and SEC-1 commits, which dropped the
  citations; the CQ-1 row counts them.
* **DOC-5**'s observation that `health()`'s docstring restates
  `docs/library-api.md` is CQ-5's subject, and is addressed there.
* **TC-12** overlaps the security lens's account of the snapshot
  directory as safe by construction. It is triaged as TC-12, because
  what was missing was the test that keeps it safe.
* **Rediscovered issues.** #105 was rediscovered by two lenses and has
  one row. The security lens also found a pre-existing terminal
  injection with no finding id and no issue, which has its own row.

## Decisions

| Finding | Rating | Disposition | Why, commit, or issue |
|---|---|---|---|
| CQ-1 | fix | taken | `a61726f` Replace plan citations in the health code. Three citations that stood in for a reason were replaced by the reason. `278350a` and `2ddd92f` rewrote two of the cited comments. |
| CQ-2 | fix | taken | `15a07c5` Replace plan citations in the health tests. The two moved docstring lines the lens marked as backlog are left alone. |
| CQ-3 | fix | taken | `5b062bc` Name the live health steps by what they provoke. The printed step labels are now "baseline", "pod limit kill", "automatic restart", "k3s-agent stopped", "k3s-agent started", "etcd snapshot" and "disk pressure". `68031b4` rewrote the docstring lines that were also stale hedges. This changes the merge tier's output. |
| CQ-4 | fix | taken | `9da4ac1` Drop a plan pointer from the merge tier script. Comment only. |
| CQ-5 | document | taken | `cc3b816` Point health()'s docstring at the caller docs. The parts the lens said earn their length are kept. |
| CQ-6 | document | taken | `0e710f9` Say the probe budget's rationale once in code. The `HEALTH_PROBE_TIMEOUT_SECONDS` comment is the one copy in code; `docs/library-api.md` keeps the caller's. |
| CQ-7 | document | taken | `256f283` Trim repeated rationale from the Ready wait code. |
| CQ-8 | document | taken | `81e5660` Correct stale comments in the health code. |
| CQ-9 | consider | taken | `f8b5d61` Share the health renderer's singular count helper. A one-line hoist; two mutation-check entries were re-pointed and are still caught. |
| CQ-10 | consider | taken | `86cab9f` Build unrun probe reports from one definition. One line at each of the two sites. |
| CQ-11 | document | taken | `81e5660` Correct stale comments in the health code. |
| CQ-12 | consider | taken | `81e5660` Correct stale comments in the health code. The comment sentence, not the rename, which would touch tests and mutation-check entries for no behaviour. |
| CQ-13 | consider | declined | Filed as #125, per decision 8: seam 1, the probe commands and their parsers, is clean, and the issue names what crosses it. No split here. |
| CQ-14 | none | informational | `tools/ci_health_signals.py`'s length; its seam already exists. |
| CQ-15 | none | informational | The lens's answer to the duplication question: mostly good reuse. |
| CQ-16, DOC-3 | document / consider | taken | `b63c92c` Name the release health() changed in. The collection module's "Before this release" and `docs/usage.md`'s "the release after v0.2.0" now say what was true up to v0.2.0. `docs/library-api.md`'s note names #76 rather than a release, so it already has a referent and is unchanged. |
| CQ-17 | none | informational | A file-wide kubeconfig path constant is outside this scope. |
| TC-1 | fix | taken | `b58bbbe` Check live that expand-workers waits for Ready. Also rewrites the `wait_for_nodes()` comment that phase 2 made stale. Changes what the merge tier runs; verifiable only by a dispatch. |
| TC-2 | consider | taken | `3c1acf6` Pin the order of the Kubernetes probe's columns. Taken as a real gap in this plan's code: the template was being changed for SEC-2, and a column swap was invisible to every check. |
| TC-3 | consider | taken | `02e65d3` Check more health readings in the merge tier. Each assertion is small and rides on the dispatch TC-1 and CQ-3 already need. One part declined: `booted_at` is not compared between steps, because `/proc/stat`'s btime moves when the wall clock is stepped, and an equality check would fail on a node whose NTP corrected the time mid-run. |
| TC-4, TC-5 | consider | declined | Filed as #126. Each adds a live provocation with side effects (deleting a Node, a ghost Node object) that needs its own dispatch to confirm, which is new scope rather than an audit fix. |
| TC-6 | consider | taken | `d134d10` Render live OOM kills and pressure in the CLI. Two calls of an existing helper, on the same dispatch. |
| TC-7 | consider | declined | `disk_fill_command()`'s arithmetic and skip branch. Not a one-liner (it needs a fake `df` and `fallocate` on PATH), and not a defect: the fill branch runs, and is checked by its outcome, on every merge tier run. Not worth an issue on its own. |
| TC-8 | consider | taken | `b6f17b3` Add mutations for three unpinned live checks. Includes the lens's `NOT_READY_STATUSES` entry, and re-points "expand-workers waits only for the workers it added" at the boundary-only test. The side note (the uncommitted-changes warning checks only the package directory) is informational. |
| DOC-1, TC-9 | document | taken | `68031b4` State the observed health signal behaviour. Swept for the whole class (see de-duplication). Changes one printed line of the merge tier. |
| TC-10 | consider | declined | Filed as #127: the module's health result is only tested healthy, and its harness would answer the Kubernetes probe with the API probe's output if it modelled submission as the server does. Neither is a defect in the module today. #89 commented to link it. |
| TC-11 | none | informational | kubectl cannot print a duplicate `oom` line. |
| TC-12 | consider | taken | `600fb7c` Pin that the snapshot directory is read last. One assertion in an existing test, plus a mutation. |
| DOC-2 | consider | taken | `13aea26` Describe health's unknown readings in usage. A sentence each for the `kubernetes:` and signals lines. |
| DOC-4 | consider | taken | `3079229` Say what the merge tier's health checks assert. Two sentences, which also describe TC-1's and TC-3's additions. |
| DOC-5 | none | informational | The facts stated in several places agree. Its point that phase 2's "one prose place" box is not literally true is addressed in part by `cc3b816`, and is for 4g to record against that plan. |
| DOC-6 | none | informational | `library-api.md`'s stable-surface paragraph covers it. |
| SEC-1 | fix | taken | `278350a` Refuse probe output Shaken Fist stored as a blob. The smaller fix, as the management session preferred: a probe whose `stdout` did not come back has not answered, with an error naming the blob. Confirmed against Shaken Fist's `daemons/sidechannel/main.py:854-861`. Reading the blob is a feature, filed as #128. `docs/library-api.md` says what happens and at what size. |
| SEC-2 | consider | taken | `2ddd92f` Stop one node forging another's Kubernetes record. Taken as a real defect in this plan's code, and the fix was contained to the two templates and a comment. A condition's status is printed only once `eq` has matched it against Kubernetes' three; a container's name is taken from the pod's spec. Verified against real API servers, see below. Changes every live `health()` read; needs the dispatch. |
| (security lens, no id) | -- | declined | Filed as #129: `_render_health()` prints the API probe's `kubectl get nodes` output to the terminal raw, so a compromised first control plane node can send escape sequences. Pre-existing code (`8ba16a09`), not this plan's. |
| #105 (TC, SEC rediscovered) | -- | commented | https://github.com/shakenfist/client-python-k3s/issues/105#issuecomment-6074470220: `await_nodes_ready()` is a new caller of the unguarded `reap_execute()`, and the guarded probe path's absent-`stdout` hole is fixed by `278350a`. |
| #89 (TC rediscovered) | -- | commented | https://github.com/shakenfist/client-python-k3s/issues/89#issuecomment-6074472219, linking #127. |
| #101, #102 (TC rediscovered) | -- | nothing new | Both were commented on after phase 3's dispatch with exactly what the lens found: `health --strict`'s exit 1 and `make_client()` discovery are now covered live, and the other halves remain. |
| #106 (TC rediscovered) | -- | nothing new | The new `tools/` mutation entries correctly use `stestr`. |
| #82 (CQ rediscovered) | -- | nothing new | The `>=3.7` floor reading came back clean; the issue is that nothing runs it. |
| Future work item (documentation lens checklist) | -- | 4g | Phase 3's `--kill-who=main` run did not check that the pods' containers survived. For 4g's Future work. |

## Verification of the fixes

Each test added to guard a fix was checked by breaking the fix. Where a
`tools/mutation-check.py` entry was added, the script applied it, ran
the named test, and reported it caught; each was also run with `-v` to
confirm the test that failed was the one added for it.

| Fix | Mutation | Result |
|---|---|---|
| SEC-1 | `elif probe['stdout'] is None:` replaced by `elif False:` (new entry) | Fails `test_a_table_too_long_to_return_inline_is_not_an_answer`, `test_output_too_long_to_return_inline_reads_nothing` and `test_a_result_with_no_output_reads_nothing` |
| SEC-2 | A condition's status printed as `{{.status}}` again (new entry) | Fails `test_a_condition_status_is_printed_only_once_eq_has_matched_it` |
| SEC-2 | A container's name printed from its status again (new entry) | Fails `test_a_container_name_is_taken_from_the_pod_spec` |
| TC-2 | MemoryPressure and PIDPressure columns swapped (new entry) | Fails `test_the_node_columns_are_in_the_order_the_parser_reads` |
| TC-2 | An `oom` line's restart count and finish time swapped (new entry) | Fails `test_the_oom_columns_are_in_the_order_the_parser_reads` |
| TC-8 | `NOT_READY_STATUSES` gains `None` (new entry) | Fails `CheckKubeletSilentTestCase.test_unread_is_not_an_answer` |
| TC-8 | `restarts != restarts_before + 1` loosened to `<` (new entry) | Fails `CheckAutomaticRestartTestCase.test_restarted_twice` |
| TC-8 | `now <= floor` loosened to `<` (new entry) | Fails `CheckSnapshotSavedTestCase.test_no_growth` |
| TC-3 | The `unmatched_nodes` baseline check disabled (new entry) | Fails `CheckBaselineTestCase.test_an_unmatched_node` |
| TC-3 | `now <= before` in `check_ready_since_moved()` loosened to `<` (new entry) | Fails `CheckReadySinceMovedTestCase.test_the_same_time` |
| TC-3 | The `booted_at`, worker `etcd_snapshot_bytes`, `ready_since` and `oom_killed` baseline checks each disabled (by hand) | Each fails its own test: `test_no_boot_time`, `test_a_snapshot_size_on_the_worker`, `test_no_ready_time`, `test_oom_killed_unread` |
| TC-12 | The snapshot reading moved to the front of the command (new entry) | Fails `test_an_absolute_snapshot_directory_is_still_sized` |

SEC-2 was also checked against real Kubernetes API servers: k3s
v1.21.1+k3s1, whose kubectl is the oldest the plugin supports, and
v1.33.5+k3s1, each run in a local container with no agent. Node and
pod statuses were patched through the status subresource with a Ready
status carrying a newline and forged `node` and `oom` records, a
`PIDPressure` status of `Weird`, a container status whose name carried
a forged `oom` line, and one naming a container the spec does not have.
Both API servers accepted every patch. With the old templates, both
kubectls rendered a down node as Ready, a node that does not exist, and
forged OOM entries. With the new ones, neither did: the forged statuses
printed empty and read as `None`, both forged container names printed
empty and their lines were dropped, and a real container's and an init
container's kills were still listed. That is the template engine check
the unit tests cannot make; the merge tier dispatch is the check that a
real cluster still reads as before.

TC-1, TC-6 and the live half of TC-3 run only in the merge tier, so
they have no local mutation. CQ-9's two re-pointed entries are still
caught.

## Live CI

These commits change what the merge tier runs or prints, and cannot be
verified without a cluster. The management session dispatches the
merge tier on this branch before the pull request opens:

* `2ddd92f`: the Kubernetes probe's templates, which every live
  `health()` reads;
* `b58bbbe`: `health --strict` after `expand-workers`;
* `02e65d3`: new baseline and `ready_since` assertions;
* `d134d10`: `health` and `health --strict` in the OOM kill and disk
  pressure steps;
* `5b062bc` and `68031b4`: printed step labels and one printed line.

`278350a` changes `health()` only for output over 10 KiB, which the
merge tier's clusters do not produce. `9da4ac1` is a comment.

## Counts

There are 41 rows.

| Disposition | Rows |
|---|---|
| Taken | 24 |
| Declined | 5 |
| Informational | 6 |
| Rediscovered issues | 5 |
| 4g | 1 |

* **Taken (24):** CQ-1 to CQ-12, CQ-16/DOC-3, TC-1, TC-2, TC-3, TC-6,
  TC-8, DOC-1/TC-9, TC-12, DOC-2, DOC-4, SEC-1, SEC-2. These went into
  22 commits before this file.
* **Declined (5):** CQ-13 (#125), TC-4/TC-5 (#126), TC-7, TC-10 (#127),
  and the security lens's unnumbered terminal finding (#129).
* **Informational (6):** CQ-14, CQ-15, CQ-17, TC-11, DOC-5, DOC-6.
* **Rediscovered issues (5):** #105, #89, #101/#102, #106, #82.
* **4g (1):** the `--kill-who=main` Future work item.

The rows cover all 37 findings: 37, less three merged pairs (DOC-1 with
TC-9, CQ-16 with DOC-3, TC-4 with TC-5), is 34, plus the unnumbered
security finding, five rediscovered-issue rows and one 4g item, is 41.

* **Issues filed:** 5. #125 is decision 8's seam (CQ-13). #126 is the
  two declined live provocations (TC-4, TC-5). #127 is the collection
  module's health tests (TC-10). #128 is reading a probe's blob, the
  feature SEC-1's fix deliberately left out. #129 is the raw API probe
  output on the terminal.
* **Issues commented on:** 2, #105 and #89.
* **Tests:** 988 before, 1005 after.
* **Mutations:** 90 before, 101 after, all caught.
