# Audit triage

Step 6g of
`PLAN-library-api-and-collection-phase-06-push-audit.md`, first half:
the triage, presented before anything is edited. Decision 7's rule is
applied as written -- `fix` gates the phase, `document` is taken only
when it is a comment or a docstring, `consider` is taken only when it
is a one-liner or a genuine defect in code phases 1-5 added, `none` is
informational. Decisions 5 and 6 are taken as settled.

The triage was presented for approval before anything was edited, as
the plan's back brief requires. Two of its calls were overturned on
review and this file records the decisions that were acted on, not the
ones first proposed:

1. **Documentation F6 and F7 are taken, not declined.** Decision 6
   exists to stop this phase re-litigating substantial pre-existing
   issues, not to forbid a one-line edit in a page this plan wrote,
   and a "still true" comment on a bot-filed consistency issue is
   worth much less than the fix. Both are fixed here and **#58** and
   **#99** are closed by `Fixes:` trailers on the commit that carries
   them.
2. **Tests F3 stays declined and gets an issue of its own**, rather
   than a bullet in a shared functional-tier issue. Adding
   `ci_deploy_test.sh` lines this phase cannot execute would mean
   asserting a test nobody has seen pass; the issue carries the
   under-ten-lines assessment and the structural reason the gap
   matters.

The counts below are the decided ones.

## Summary

| Quantity | Count |
|---|---|
| Numbered findings in | **62** (code quality 18, style 8, tests 15, documentation 8, security 13) |
| Duplicate groups merged | **9**, collapsing 24 findings into 9 triage rows |
| Findings dropped as out of scope (post-`eb248bd`) | **0 whole**; one finding halved, two cited line references dropped |
| Taken as `fix` | **9 rows** |
| Taken as `document` | **4 rows** |
| Taken as `consider` | **13 rows**, covering 16 findings |
| Taken although rated `none` | **2 findings**, one of which has its own row |
| **Rows in "What I will take"** | **27** (9 + 4 + 13 + 1) |
| **Findings taken** | **31** |
| **Findings declined** | **31**, one row each |
| **Accounted for** | **62** = 31 + 31 |
| Issues to file | **6** (#100-#105; **#106** was filed outside this table) |
| Issue comments to add | **4** (#96, #82, #89, #91) |

The row and finding totals are spelled out because the phase's
definition of done is that this table accounts for every finding, and
that is only checkable if the sums are stated. The `document` count
above read 5 until the pull request review did the arithmetic; the
table has always had four such rows (CQ F6, CQ F7, sec F9, sec F10).

Two bookkeeping corrections to the findings files themselves, because
the phase's definition of done is that this table accounts for every
finding and the stated totals do not add up:

- The documentation lens's summary table says `fix 7 / none 4`, total
  11. It lists **eight** numbered findings (F1-F8), every one of them
  `fix`, and its "Not flagged (checked and clean)" section has six
  bullets, not four. The brief's headline figure of 65 findings comes
  from trusting that table; counting numbered findings across all five
  lenses gives 62.
- The tests lens's summary table says `consider 6 / none 9`. Its
  findings are `consider` seven times (F1, F3, F4, F5, F6, F8, F10)
  and `none` eight times. The total of 15 is right; the split is not.

Neither changes a verdict. Both are recorded because an audit whose
own arithmetic is wrong is not checkable, which is the property the
plan says this directory exists to provide.

## Scope verification

The worktree over-reads the audit scope. The scope ends at `eb248bd`
(phase 5's merge); four commits have landed on audited files since,
and all four belong to the **`node-customisation`** master plan, which
carries its own push-audit phase:

| Commit | Subject | Files in the audit scope |
|---|---|---|
| `211d3f9` | Drop an apt-get install that names nothing. | `cluster.py` (-1) |
| `73d4846` | Size control plane and worker nodes separately. | `cluster.py` (+194), `exceptions.py` (+43), `tests/fakes.py`, `tests/test_cluster.py`, `tests/test_exceptions.py`, `tests/test_library_api.py` |
| `b6c4b7b` | Add per-role sizing options to k3s create. | `__init__.py` (+33), `tests/cli_contract/create.txt`, `tests/test_cli_contract.py`, `tests/test_commands.py` |
| `edabb7f` | Complete partial node size records per field. | `cluster.py` (+36/-19), `docs/library-api.md` (2 lines), `tests/test_cluster.py` |

Every finding was re-checked against `git show eb248bd:<path>`, by
content rather than by line number. The result is better than the
brief feared: **no finding is invalidated**, because four of the five
lenses happened to cite code that predates the drift. The style lens
guarded deliberately; the other three were lucky. What the drift did
invalidate:

- **Code quality F7 is halved.** Its second subject, `_node_size()`,
  does not exist at `eb248bd`. `_node_size()`, `validate_node_sizes()`,
  `node_sizes`, `DEFAULT_NODE_SIZE` and `NodeSizeError` are all
  `73d4846`/`edabb7f`'s work. The `get_progress()` half is in scope and
  is taken; the `_node_size()` half is dropped to
  `node-customisation`'s audit.
- **Two rows of the code-quality lens's comment-proportion candidate
  table** -- `_node_size()` at `:476` and `validate_node_sizes()` at
  `:280` -- are out of scope for the same reason. Neither was a
  finding, and the lens cleared `validate_node_sizes()` anyway.
- **Code quality F16 cites one post-scope line.**
  `docs/library-api.md:249` is the `NodeSizeError` table row, added by
  `b06087ee` on 2026-10-04. The other twelve long lines are in scope.
  F16 is declined regardless.
- **Security F5's sink 4** (`md['node_sizes']` through `_node_size()`)
  is post-scope. F5's own subject -- the metadata key collision -- is
  in scope.

Line numbers in the table below are the **worktree's**, verified by
content, because that is where any edit lands. Where a finding quoted
`eb248bd` numbers as well, those are ignored. The code-quality lens
already used worktree numbers and they check out; the style lens gave
both.

Two provenance corrections found while verifying, both material to the
triage:

- **Code quality F4 says phase 3 copied the dead `k3s.add_command()`
  pattern three times. It copied it twice.** `git blame` puts nine of
  the eleven lines in `e8ecd31` (2024-08-17) and `0c344ee6`
  (2024-09-03); only `:424` (`8ba16a09`, health) and `:478`
  (`a206fc96`, remove-worker) are this plan's. The style lens's F1 has
  this right and the code-quality lens does not: `expand-addresses`
  dates to the initial commit.
- **Code quality F6's proposed replacement names four methods, and
  there are three.** The three sites the comment cites at `eb248bd`
  (`:761`, `:2203`, `:2250`) are `_probe_k3s_api()`, `remove_worker()`
  and `_uncordon()`. `delete()` catches no `apiclient.APIException`.

## What I will take

Ordered with the security fix first, then the rest of the `fix`
items, then `document`, then `consider`.

| ID(s) | file:line (corrected) | Action | What and why |
|---|---|---|---|
| **sec F1** | `cluster.py:1155`, `:558`, `:762`; `exceptions.py:647`, `:681`; `sf_k3s_cluster.py:734` | fix | Redact `K3S_TOKEN=` (and any future secret-bearing assignment) at the two rendering boundaries, so a failed k3s install stops publishing the node or server token to stderr, to `fail_json(msg=...)` and to the public CI log. Chain confirmed independently; see the last section for the design. |
| **docs F1** | `ARCHITECTURE.md:262-265` | fix | "As of this writing the collection has not yet had a first release" is false since `v0.2.0` published `shakenfist.k3s` 0.2.0 to Galaxy. Replace with the current state, with no phase citation. |
| **docs F2** | `docs/collection.md:16-46` | fix | The whole "Status: not yet published" section tells an operator to build a tarball instead of running the one-line install that now works. This is the operator-facing page, so it is the most consequential of the three stale copies. |
| **docs F3** | `collection/README.md:20-43` | fix | Third copy of the same stale fact, carrying its own instruction to "Delete it when the first version is published". Galaxy renders this file on the collection's page. |
| **docs F4** | `RELEASE-SETUP.md:126-136` | fix | Says the Galaxy token does not exist, and gives a verification command pointed at repository secrets when the token is a `release` **environment** secret. The runbook a human follows before a release, so a wrong command here is worse than a wrong sentence. |
| **docs F5** | `ARCHITECTURE.md:19-27` | fix | The Module Structure tree omits `client.py`, which phase 2 added. A plain component-inventory error in the one place that is literally the file listing. |
| **docs F6** | `docs/collection.md:21`; `docs/library-api.md:100`, `:126`, `:172`, `:264` | fix | Five phase-number citations explaining behaviour that is already shipped, which `plan-phase-references` forbids and none of which carries an `audit-ok` marker. Rewrite each to describe the behaviour plainly. Closes **#58**, whose automated finding names these exact five locations. |
| **docs F7** | `docs/plans/PLAN-cumulative-health-signals.md:106` | fix | `[p3]: library-api-and-collection-phase-03-missing-verbs.md` points at a filename this plan renamed, so the link dangles. Retarget it at `PLAN-library-api-and-collection-phase-03-missing-verbs.md`. Closes **#99**, whose sole automated finding is this link. |
| **docs F8** + **CQ F8** | `ARCHITECTURE.md:137-197`; `exceptions.py:619-625` | fix | Reduce the Progress reporting section to the shape with a pointer, as every other section in that file does. **Merged, and the merge changes the fix:** docs F8 proposed moving ~48 lines into `docs/library-api.md`, and CQ F8 shows `docs/library-api.md:283-310` already states the same agent-state argument -- so moving the prose would create a fourth copy. Point at what is already there, add only the stall-note timing it lacks, and trim `exceptions.py:619-625` to the one fact that class's reader needs plus a pointer to the constants at `cluster.py:70-102`, which stays canonical. `docs/library-api.md` is left alone. |
| **CQ F6** | `sf_k3s_cluster.py:708` | document | The comment cites `cluster.py:761, :2203, :2250`; correct at `eb248bd`, all three wrong today (`:2203` is now docstring prose, `:2250` a comment). Replace with `_probe_k3s_api()`, `remove_worker()` and `_uncordon()` -- three methods, not the four the lens proposed. Phase 5 wrote the comment, so correcting it is this phase's job. |
| **CQ F7** (first half only) | `cluster.py:434` (`get_progress()`) | document | Cut the seventeen-line reverse call index that `grep` answers and that will rot; keep the shared-`Progress` caveat, which is a real trap. The `_node_size()` half is out of scope. |
| **sec F9** | `cluster.py:1676`, `:1677`, `:1691` | document | One sentence saying every component of these three joins is process-chosen, so no containment check is needed and an outside value appearing later would need one. The `path-traversal-review` block asks for exactly this comment. |
| **sec F10** | `cluster.py:138-149` with `:1105` | document | Add a clause saying `K3S_MANIFEST_BASENAME_RE` is also the containment proof for the remote join -- no `/`, no leading dot, so no `..` -- because today the comment argues only shell safety and someone relaxing the class would not know they were touching a traversal guard. |
| **sec F2** | `cluster.py:1049-1055`, `:1216-1232` | consider | The only two sinks in the review that are neither quoted, escaped nor validated. Refuse a newline (or validate as addresses), reusing the delimiter-collision check `read_manifests()` already has at `:270`, and widen rule 2 at `:170-173` to say a body carrying interpolated content must also be unable to contain the delimiter line. Taken because phases 1-5 *changed* these heredocs -- `73bd499` had bare `<< EOF`, this plan made them `<< 'EOF'` -- so rule 2 and its incompleteness are both this work's. |
| **sec F3** + **sec F9** (same block) | `cluster.py:1678`, `:1683` | consider | `os.makedirs(..., mode=0o700)` and create the kubeconfig through `os.open(..., 0o600)` so a shared host's other users cannot read cluster-admin credentials. Taken because phase 3 (`12bbdc5e`, `51262090`) rewrote both lines, and because setting the mode at creation rather than chmod'ing after is two lines. |
| **sec F4** | `cluster.py:2009-2011` | consider | Redact `node_token`, `server_token`, `kubeconfig` and `ssh_key` in `delete()`'s debug loop. Taken because phase 5 built a *stated* security property on this loop staying unreached (`sf_k3s_cluster.py:636-641`, `docs/collection.md:291-294`), and a security property that holds because one caller left a flag alone is one flag from not holding. Also correct `PLAN-functional-ci.md:194-198`, whose recorded key list omits `server_token`. |
| **sec F6** | `primitives.py:77`, `:146`; `exceptions.py:576-585` | consider | `r.text[:512]` at both call sites, matching the sibling classmethod two lines below that already documents its argument as "already-truncated". Two lines plus a docstring sentence, in code phase 1 wrote. |
| **sec F7** (comment only) | `cluster.py:2112-2124` | consider | Add two sentences saying the argument list answers injection but not kubectl's own dot-separated path grammar, so a name containing a dot still breaks `unset` after the cluster is gone. The code change is declined -- it is name validation, which is #96. Taken because the existing comment reads as though the name needs no further thought. |
| **sec F8** | `cluster.py:1695` | consider | `['kubectl', 'config', 'view', '--flatten']` with `shell=True` dropped. A one-liner that makes the module's own third rule at `:175-176` true without exception; behaviour identical. Check that no test pins the string form. |
| **CQ F1** | `exceptions.py:195`, `:380`, `:563`, `:726` | consider | Lift a `_ReasonedK3sException` base holding the byte-identical nine-line `__init__`/`__str__` pair. Phase 1 wrote two copies, phase 3 copied it twice more -- the clearest instance in the diff of a duplicate that exists *because* two phases landed, which the shared block names as this phase's reason to exist. ~30 lines go; rendered messages do not change, so `cli_contract` and `test_exceptions.py` passing untouched is the faithfulness check. `NodeSizeError` has a different signature and is out of scope, so the base covers four of five. |
| **CQ F2** = **style F2** | `cluster.py:471`, `:1475`, `:2162`, `:2261`, `:2491`, `:2513` | consider | One `Cluster` method that builds the three-argument `Progress`, assigns `self.progress` and returns it; the five command bodies call it and `get_progress()` uses it for the lazy case. Six sites, three added by phase 3, reported independently by two lenses from opposite directions. |
| **CQ F4** = **style F1** | `__init__.py:150`, `:243`, `:269`, `:292`, `:326`, `:424`, `:443`, `:458`, `:478`, `:494`, `:507` | consider | Eleven dead `k3s.add_command()` lines; `@k3s.command` already registers, which `getconfig` having no such line proves. Delete all eleven, not only the two phase 3 added: a half-cleaned file leaves the dead pattern for the next author to copy, which is exactly how phase 3 acquired it. `tests/cli_contract/group.txt` is the regression test and will not move. (Nine of the eleven predate the plan; see the provenance correction above.) |
| **CQ F5** | `primitives.py:184`; call sites `cluster.py:558`, `:661`, `:893`; docstring `exceptions.py:613` | consider | Move `_describe_agent_op()` to `progress.py` as `describe_agent_op()`. It is a display helper living in the module that is not about display, and it is the only place in the package where one module reaches into another's underscore-prefixed name -- a defect of phase 1's split. Sequenced **before** sec F1, so the redaction lands in its final home rather than being moved afterwards. Its tests are already in `test_progress.py`. |
| **CQ F9** + **CQ F10** | `cluster.py:2041-2042`, `:2067` | consider | `delete()` sets `api_floating_address`/`api_inner_address`, but `create()` writes `api_address_floating`/`api_address_inner` -- the words transposed, so two junk keys are invented and the two real ones are never cleared. Two words. F10 (`node_network = []` where every other site holds a UUID string) is one word in the same function and is taken with it; declining it while taking F9 would be arbitrary. Both predate phase 1, which is why the reason is "two words in a function phase 1 rewrote" rather than "a defect in this work". |
| **CQ F13** | `tests/test_cluster.py:29`, `tests/test_progress.py:46` | consider | Move the three-line `FakeTty` to `tests/fakes.py`, which already holds four shared fakes. Phase 1 added the second copy. Three lines deleted, two imports. |
| **tests F5** | `tests/test_cli_contract.py:23-36` | consider | One `assertEqual(set(SUBCOMMANDS), set(k3s.commands))` so a missing `--help` fixture names itself instead of arriving as a `group.txt` diff the author has to interpret. A genuine one-liner guarding this plan's own contract fixtures. |
| **style F4** | `docs/plans/audit/verification.md:107-133` | none, taken | Correct the attribution: the one socket at import is urllib3's `HAS_IPV6` probe reached through `shakenfist_client.apiclient`, not `primitives.py`'s `requests` import. Taken although rated `none` because it is one sentence in this phase's own record, and 6g re-runs and re-edits `verification.md` anyway. Verdict and invariant unchanged. |

## What I will decline, and why

Every declined finding appears here.

| ID(s) | Reason |
|---|---|
| **CQ F3** | `await_fetch()` duplicating the poll loop phase 3 extracted. Declined and filed: the proposed change alters `await_execute()`'s signature and `await_fetch()`'s progress semantics, and the lens leaves an open design question inside its own proposal ("`await_execute()` currently reports no progress at all"). Not something to settle inside an audit phase. |
| **CQ F11** | Two redundant `set_metadata()` writes. Pre-existing (`e8ecd31`), and removing them changes the metadata-write counts several tests assert. |
| **CQ F12** | The two release lookups' duplicated cache envelope. Pre-existing structure; `git show 73bd499` has the identical preamble. Phase 1 changed the plumbing on each line and not the shape. |
| **CQ F14** | Six identical `--namespace` help strings. Stacked per-command `@click.option` is the idiomatic Click spelling and the lens rated it `none`. |
| **CQ F15** | Built-cluster metadata inlined twelve times in `test_cluster.py`. Test fixture style, not a defect in code these phases added. |
| **CQ F16** | Thirteen long non-Python lines, all Markdown table rows or single URLs, none wrappable. One of them (`docs/library-api.md:249`) is post-scope anyway. |
| **CQ F17** | `for page in range(5)` fetches `?page=0`, which GitHub serves as page 1. Pre-existing (`0c344ee6`, 2024-09-03) and harmless -- results accumulate into a dict keyed by tag. Folded into the release-lookup issue as a one-line bullet. |
| **CQ F18** | `/etc/rancher/k3s/k3s.yaml` written out eight times as a literal. A judgement call, not a defect; churning eight command strings to extract a constant is not this phase's work. (The brief predicted this overlapped the style lens's `print(` census. It does not -- the census is about `print()` and F18 is about a path literal. No overlap found; F18 stands alone.) |
| **style F3** | `show --namespace` help says "alter clusters" for a read-only verb. `e8ecd31`, 2024-08-17; no scope merge touched it. Pre-existing, so an observation and not a defect in this work. |
| **style F5** | `import yaml` is 7.5ms of a 70.6ms import. Moving an import into a function body to save 7ms trades measurable clarity for unmeasurable startup. |
| **style F6** | `importlib-metadata; python_version < '3.9'` is one version wider than needed. Costs an unused install and nothing else; the conservative marker is defensible. Occurrence on **#82**. |
| **style F7** | The Ansible module pays ~14ms for a transitive `click` import. Avoiding it means moving the Click group out of the package `__init__.py`, which changes the entry point every installed copy registers under. |
| **style F8** | `orchestrated_k3s_clusters` misses the documented `orchestrated_k3s_cluster_*` prefix. A wire-format value in every deployed cluster's metadata; renaming it orphans every cluster list. The right fix is widening `PUSH-AUDIT.md`'s wording, which lives in another repository. |
| **tests F1** | No functional coverage for `update-os`. Declined and filed: a `dist-upgrade` adds minutes to an 80-minute budget and a dependency on the guest's apt mirror, which is a new flake source. The lens itself offers "say so in a tracking issue" as the alternative. |
| **tests F2** | `getconfig` and `show` have no unit test for what they print. Rated `none`; both are asserted functionally where it matters (`ci_deploy_test.sh:159`, `:86`, `:96`). |
| **tests F3** | Phase 2's deliverable has no functional coverage. Real, and the largest gap this lens found -- but closing it means ~10 new lines in `ci_deploy_test.sh`, which this phase cannot run (it needs a live Shaken Fist cluster). Landing an unverified change to the functional tier under an audit banner is exactly what decision 5 declines for typing. Declined and filed. |
| **tests F4** | Nothing keeps `ci_deploy_test.sh` in sync with the command list. A good idea, ~20 lines, and neither a one-liner nor a defect in code added. Filed with F1/F3/F15. |
| **tests F6** | `get_longhorn_release()` indexes `reldata['prerelease']`/`['tag_name']` where the k3s lookup guards. The hard subscripts are `0c344ee6` (2024-09-03), which phase 1 moved and did not write. A guard plus two tests, not a one-liner. Filed. |
| **tests F7** | Neither release lookup survives a non-JSON 200. Rated `none`; a loud traceback on a read-only command rather than a wrong answer. Filed with F6. |
| **tests F8** | `reap_execute()` and `await_fetch()` trust `results['0']` where three siblings guard it. Real inconsistency, ~10 lines plus two tests, and the unguarded reads are phase 1 moves of pre-existing code (`dc7c4d41`, `3e4762df`). The lens says filing is reasonable and that leaving it unnamed is not. Filed. |
| **tests F9** | `_probe_k3s_api`'s "recorded no result" branch has no direct test. Rated `none`; eight lines, no code change. Folded into F8's issue so the guard and its test arrive together. |
| **tests F10** | `tools/build-collection.py`'s `main()` runs for the first time in the job that publishes a release. Real risk -- phase 5's own 403 recovery is the argument -- but closing it needs an `ansible-core` install in `sanity_checks` or a stubbed `ansible-galaxy`. Filed. |
| **tests F11** | Confirms **#89** rather than rediscovering it. Comment on #89. |
| **tests F12** | Confirms **#91**. Comment on #91. |
| **tests F13** | The declared `click >= 8.0.0` floor is never resolved, so `separated_runner()`'s compatibility branch is dead. Same defect class as **#82**. Comment on #82. |
| **tests F14** | `expand-workers`/`expand-addresses` range-check nothing. Same defect as **#96** on two more verbs, plus the CLI/module asymmetry. Comment on #96. |
| **tests F15** | `health --strict` is only exercised in the direction that passes. Rated `none`; deliberately wrecking a cluster mid-script changes what the subsequent `delete` is being asked to do. Bullet in the functional-tier issue. |
| **sec F5** | Two cluster names collide with the reserved version-cache keys. The narrow fix (refuse two names in `_metadata_key()`) treats a symptom; the lens's own better fix is validating the name once on the way in, which is **#96**. Comment on #96. |
| **sec F11** | Neither release lookup passes `timeout=`. Rated `none`; availability only, for the caller's own process, and pre-existing. One line each, so folded into the release-lookup issue rather than taken. |
| **sec F12** | `k3s show` prints cluster-admin credentials. Rated `none`, by design, warned about in `docs/usage.md:295-297`, and already recorded as future work in `PLAN-functional-ci.md:194-198`. The only part taken is correcting that note's key list to include `server_token`, under sec F4. |
| **sec F13** | The Galaxy token is passed on `ansible-galaxy`'s command line. Rated `none`; `ansible-galaxy collection publish` offers no environment-variable equivalent to `--api-key`, and the step already does the harder half right. |

**What changed on review.** docs F6 and F7 were first declined under
decision 6, because #58 and #99 are open and name those exact
locations. That was overturned: decision 6 is about not
re-litigating substantial pre-existing work, and these are one-line
edits in pages phases 1, 3 and 5 wrote, so the fix is worth more than
a comment on a bot-rewritten issue. Both are taken above and both
issues are closed by trailer.

**The declines still worth arguing about.** tests F3 is the largest
real gap the audit found and it is declined on a procedural ground
rather than a substantive one, which is why it gets its own issue
rather than a bullet. CQ F10 was taken despite a `none` rating (one
word, same function as F9) while sec F11's one-line `timeout=` was
declined despite being a one-liner, because it is pre-existing and
`none`-rated; those two cut opposite ways and are where this triage's
line is least crisp.

## Issues filed

All six were filed, and the number each got is in its heading.


**#100 -- No type hints anywhere, and mypy is configured nowhere.**
Across the eight non-test source files phases 1-5 wrote or rewrote
there are 128 function definitions and zero annotations, and mypy
appears in no `pyproject.toml`, `tox.ini` or `.pre-commit-config.yaml`
-- so the `python-version-discipline` block's typing clause is unmet
plan-wide. Closing it means annotating every definition and wiring
mypy into both tox and pre-commit, which is a larger change than any
single phase of the plan it would be judging, so it is a master plan
of its own rather than an audit fix (decision 5).

**#101 -- `tools/ci_deploy_test.sh`: `update-os`, `health --strict`
and the census have no coverage.** `update-os` is the only one of the
twelve subcommands never run against a real cluster and its body is
`apt-get dist-upgrade` on every node; `health --strict` is only ever
exercised in the direction that passes, so the tier proves it exits 0
when it should and never that it exits 1 when it should; and no test
compares the script against `k3s.commands`, so the next gap arrives as
an audit finding rather than a failing test. Each is small on its own,
but all three change the functional tier, which cannot be verified
without a live Shaken Fist cluster. (tests F1, F4, F15)

**#104 -- The two release lookups are thin against unexpected
upstream shapes.** `get_longhorn_release()` reads
`reldata['prerelease']` and `['tag_name']` with hard subscripts while
iterating whatever `r.json()` returned, so GitHub's error *object*
raises `TypeError` rather than `ReleaseLookupError`, and the k3s
lookup guards the equivalent read; neither survives an HTTP 200
carrying HTML; neither passes `timeout=`; and the Longhorn scan
iterates `range(5)`, fetching `?page=0`, which GitHub serves as page
1. All four are pre-existing and none is a one-liner together with its
test, which is why they are filed rather than fixed. (tests F6, F7,
sec F11, CQ F17)

**#105 -- Agent operation results are trusted in two places and
guarded in three.** `reap_execute()` reads
`aop['results']['0']['return-code']` and `await_fetch()` reads
`['content_blob']` with hard subscripts, while `_probe_k3s_api()`,
`_agent_op_error()` and `_describe_agent_op()` all treat the same
field as possibly absent -- and `_probe_k3s_api()`'s comment says why
it must. The same two methods also duplicate the poll loop phase 3
extracted into `await_execute()`, so the guard and the extraction want
doing together, which means settling whether `await_execute()` should
report progress. (tests F8, F9, CQ F3)

**#103 -- `build-collection.py`'s `main()` runs for the first time in
the release job.** Only `semver_from()` has
tests; the parts that rewrite `galaxy.yml`, invoke
`ansible-galaxy collection build` and revert the rewrite are exercised
nowhere but the tag-triggered `build-collection` job. A regression in
the `re.sub`, in the `galaxy_bin` resolution, or in the `finally`
revert is discovered at the moment it is most expensive, and the
`finally` exists because an earlier branch committed a machine-chosen
version by accident. (tests F10)

**#102 -- No functional coverage for client construction, and the bug
it fixed is invisible to the merge tier.** Phase
2 exists to make client construction correct -- `make_client()` for
library callers, and the removal of the plugin's own
`apiclient.Client` so that `sf-client`'s `--apiurl`, `--key` and
`--namespace` are honoured -- and asked "which functional test would
have failed before this change and passes after", the answer is none.
`ci_deploy_test.sh` invokes `sf-client` with **no root options
anywhere**, relying on `~/.shakenfist` discovery, so a plugin that
built its own client from discovered configuration would pass the
whole tier; and no tier has ever called
`apiclient.Client(suppress_configuration_lookup=True, ...)` against a
live API, because `make_client()` is reached only by the Ansible
module (#89) and by `test_client.py` with `apiclient.Client` patched
out. Closing it is under ten read-only lines against credentials the
runner already carries -- re-run one harmless command with the
runner's own values passed explicitly, and a `python3 -c` calling
`make_client()` with no arguments -- but it cannot be written here,
because this phase has no live cluster and asserting a test nobody has
seen pass is worse than recording the gap. (tests F3)

## Issue comments added

| Issue | What the new occurrence adds |
|---|---|
| **#96** (`Cluster.create()` does not range-check its counts) | Four occurrences from three lenses, which together turn it from a counts issue into a name-and-argument validation issue. (a) `expand-workers --worker-count 0` and `expand-addresses --address-count 0`/negative are accepted, do nothing, and report success (`Added 0 workers to cluster banana`), while the Ansible module *does* validate its equivalents -- so the library's two front doors disagree (tests F14). (b) The cluster names `k3s_version_cache` and `longhorn_version_cache` produce byte-identical metadata keys to the two reserved version caches, so create refuses them with a wrong explanation and delete ends in an uncaught `KeyError`; a conservative name validator makes both unreachable as a side effect (sec F5). (c) A name containing a dot breaks `kubectl config unset`'s path grammar, so a delete destroys everything correctly and then fails at its last step with no recovery, because the metadata is already gone (sec F7). (d) The floors are now stated three times -- `click.IntRange(min=1)`, `validate_node_sizes()`, and `sf_k3s_cluster.py:617-628` -- so the argument for pushing validation down is a duplication argument as well as a correctness one. |
| **#82** (`requires-python = ">=3.7"` is unverified) | Two more declared floors that are never resolved, which widens the issue from Python to every lower bound the project declares. `click >= 8.0.0`: `separated_runner()` (`test_cli_errors.py:74-94`) exists to work on click 8.0/8.1 where `CliRunner` merges the streams, and `tox.ini` pins nothing, so that branch never executes and the floor it defends is untested. `importlib-metadata; python_version < '3.9'` is one version wider than the stdlib module's 3.8 introduction. Also worth recording: the style lens compiled all 23 scope files under a real 3.7.17 interpreter in a network-isolated container with zero syntax errors, and AST-scanned for ~40 post-3.7 stdlib APIs with one hit, correctly guarded -- so the code is clean and it is still only a scan, because nothing in CI runs 3.7. |
| **#89** (`sf_k3s_cluster` has no integration coverage) | Confirmed rather than rediscovered: no `molecule/`, no `tests/integration/`, `ansible-lint` in pre-commit is static, `release.yml` builds and publishes but runs no play, and `ci_deploy_test.sh` never installs the collection. The 29 unit tests run the module in a real subprocess against a faked `apiclient.Client`, which is the correct shape and is not a substitute. New detail worth the comment: the security review found that the module's *failure* path was the one leaking the node token into `fail_json(msg=...)`, and `SecretsTestCase` had no failure-path case -- so the missing tier is also where a secrets regression would hide. |
| **#91** (`requires_ansible '>=2.15.0'` is unverified) | Confirmed, with nothing to add beyond that it still holds: `tox.ini` installs `ansible-core` unbounded so the resolver always picks the newest, and `tox.ini:15-23` already records both behaviour changes found inside the advertised range. |


**#58** and **#99** receive no comment: they are fixed here instead,
and closed by `Fixes:` trailers on the commit that carries the
documentation edits. Two details are worth leaving in this file rather
than on a bot-rewritten issue. The audit's own grep in `PUSH-AUDIT.md`
is anchored on lowercase `phase` and so finds only two of #58's five
locations; the case-insensitive form the automated check uses is the
correct one. And #99's dangling link exists because this plan renamed
its own phase 3 file, which is a failure mode worth knowing about: a
plan renaming its files breaks reference-style links from other plans,
and nothing checks them.

No comment is needed on **#93** (unencoded `open()` calls in
`tests/`) or **#94** (`validate-modules` versus flake8). Both were
checked and neither gained a new occurrence: every production `open()`
in the scope states `encoding='utf-8'`, `tools/build-collection.py`
passes an explicit encoding to `read_text`/`write_text`, and the one
`from __future__ import annotations` is at `sf_k3s_cluster.py:23`,
exactly where #94 says it has to be.

## How I propose to fix security F1

There are three places the redaction could go, and they differ in
what a future call site has to remember. **At the point the command
line is built** (`cluster.py:1155`) is the most attractive sounding
and the only one that is unavailable: the installer command must
actually carry `K3S_TOKEN=` to work, so the only way to keep the
token out of the string is to deliver it some other way -- a file
staged first, or the agent's environment -- which is a behaviour
change to the one command in this package that cannot be tested
without a live cluster. **At the two points the command line is
rendered** -- `_describe_agent_op()` and `reap_execute()`'s
`CommandFailedError` construction -- is correct today and is exactly
the shape that rots: it is two call sites now, and the third one
(a new exception, a new progress line, a new verb that reports a
failed command) inherits nothing. The file already demonstrates the
failure mode, in that `cluster.py:558` passes `max_len=None` and so
singlehandedly disables the truncation that protects every other
path.

My recommendation is **both, with one helper, applied at the two
boundaries rather than at any call site**: a single
`redact_command_line(text)` in `progress.py` -- next to
`describe_agent_op()` once CQ F5 has moved it there -- that
substitutes `<redacted>` for the value of every assignment in a
module-level tuple of secret-bearing environment variable names
(`K3S_TOKEN` today, so that adding the next one is a one-line
change), handling the bare and `'...'`-quoted forms `shlex.quote()`
produces. Call it in **`AgentOperationError.__init__` and
`CommandFailedError.__init__`**, not at the raise sites, so the
attribute those classes store is already safe and every present and
future construction is covered whether or not the raiser thought
about it; and call it in `describe_agent_op()` as well, so the
progress and debug paths are covered even when nothing raises. Apply
it to `AgentOperationError`'s `json.dumps(self.results)` dump too --
the same one-line call -- which closes the agent-stdout channel the
lens flagged and could not confirm, at no extra cost.

The reason the constructor is the place and not the call site is the
standard this project's own review history holds such fixes to, and
the file argues it twice already: rule 1 at `cluster.py:161-169` is
stated "as a rule rather than a case by case judgement because the
next reader cannot be expected to re-derive which values are attacker
reachable", and `_bind_cluster_context()` applies the namespace
default in exactly one place for the same reason. A redaction a
raiser must remember is a case-by-case judgement wearing a helper's
clothes. Redacting inside the exception means the dangerous text
cannot be stored, so there is no second question about where it is
later printed. The test that makes it stick is the missing
`SecretsTestCase` member the lens identified -- drive a create whose
worker install returns non-zero and assert
`module_harness.SECRET_NODE_TOKEN` is absent from `run.stdout` -- plus
a unit test on the helper and one asserting `CommandFailedError`
renders `K3S_TOKEN=<redacted>`. With that in place,
`tools/ci_deploy_test.sh:40-44`'s reasoning about world-readable job
logs and `docs/collection.md:275`'s "Secrets never come back in the
module's output" both become true, and
`PLAN-functional-ci.md:199-202`'s future-work bullet -- which records
only the `reap_execute()` half -- is superseded and should say so.

## What was committed

Sixteen commits, one per concern area, on `library-api-phase-06`.
This file and the re-verification are a seventeenth, which takes no
finding except style F4 and so is not in the table:

| SHA | Subject | Findings |
|---|---|---|
| `1a667af` | Move describe_agent_op to progress.py. | CQ F5 |
| `997f396` | Redact secrets from error and debug output. | sec F1 (the fix), sec F4 |
| `d927124` | Refuse a heredoc body that ends itself. | sec F2 |
| `51e6d3b` | Create the local kubeconfig private. | sec F3, sec F9 |
| `7189c44` | Bound the release lookup's error body. | sec F6 |
| `082d572` | Run kubectl config view without a shell. | sec F8 |
| `b0cc917` | Say what the quoting does not prove. | sec F7 (comment), sec F10 |
| `f4df561` | Lift a base for the reasoned exceptions. | CQ F1 |
| `e5cb8a6` | Start progress reporting in one place. | CQ F2 = style F2, CQ F7a |
| `b1094eb` | Delete the dead add_command calls. | CQ F4 = style F1, tests F5 |
| `d797473` | Share FakeTty between the test modules. | CQ F13 |
| `b585d07` | Clear the metadata keys create actually wrote. | CQ F9, CQ F10 |
| `da8b4cf` | Cite methods rather than line numbers. | CQ F6 |
| `bcea613` | Say that the collection is published. | docs F1-F4 |
| `fc77ca9` | Describe behaviour instead of plan phases. | docs F6, docs F7 (closes #58, #99) |
| `3163b79` | Keep ARCHITECTURE.md to the shape. | docs F5, docs F8, CQ F8 |

`verification.md` gained a second section recording the re-run, and
style F4's correction to its socket attribution went in with it. The
suite went from 461 tests to 492; `verification.md` accounts for all
thirty-one.
